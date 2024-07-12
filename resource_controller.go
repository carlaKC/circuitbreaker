package main

import (
	"context"
	"fmt"
	"time"

	"github.com/carlakc/lrc"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/routing/route"
)

// Compile time check that resourceController implements the controller interface.
var _ controller = (*resourceController)(nil)

// resourceController provides resource management using local reputation, HTLC
// endorsement and resource bucketing. This struct simply wraps an external
// implementation in the controller interface.
type resourceController struct {
	lnd lndclient

	// Caches our height so that we don't have to query it often.
	height    uint32
	heightAge time.Time

	htlcCompleted htlcCompletedFunc
	htlcThreshold htlcThresholdFunc
	lrc.LocalResourceManager
}

// queries block height, caching values for 3 minutes.
func (r *resourceController) getHeight() (uint32, error) {
	if r.heightAge.Add(time.Minute*3).After(time.Now()) && r.height != 0 {
		return r.height, nil
	}

	info, err := r.lnd.getInfo()
	if err != nil {
		return 0, err
	}

	r.height = info.height
	r.heightAge = time.Now()

	return r.height, nil
}

type htlcCompletedFunc func(context.Context, *HtlcInfo) error

type htlcThresholdFunc func(context.Context, *htlcThresholds) error

func circuitbreakerToLRCHistory(htlcs []*HtlcInfo) []*lrc.ForwardedHTLC {
	htlcList := make([]*lrc.ForwardedHTLC, len(htlcs))
	for i, htlc := range htlcs {
		incomingChannel := lnwire.NewShortChanIDFromInt(
			htlc.incomingCircuit.channel,
		)
		outgoingChannel := lnwire.NewShortChanIDFromInt(
			htlc.outgoingCircuit.channel,
		)

		htlcList[i] = &lrc.ForwardedHTLC{
			InFlightHTLC: lrc.InFlightHTLC{
				TimestampAdded:   htlc.addTime,
				OutgoingEndorsed: htlc.outgoingEndorsed,
				ProposedHTLC: &lrc.ProposedHTLC{
					IncomingChannel:  incomingChannel,
					OutgoingChannel:  outgoingChannel,
					IncomingIndex:    int(htlc.incomingCircuit.channel),
					IncomingEndorsed: htlc.incomingEndorsed,
					IncomingAmount:   htlc.incomingMsat,
					OutgoingAmount:   htlc.outgoingMsat,
					CltvExpiryDelta:  htlc.cltvDelta,
				},
			},
			Resolution: &lrc.ResolvedHTLC{
				TimestampSettled: htlc.resolveTime,
				IncomingIndex:    int(htlc.incomingCircuit.htlc),
				IncomingChannel:  incomingChannel,
				OutgoingChannel:  outgoingChannel,
				Success:          htlc.settled,
			},
		}
	}

	return htlcList
}

type historyFunc func(channelID lnwire.ShortChannelID, incomingOnly bool) (
	[]*lrc.ForwardedHTLC, error)

// newResourceController creates a new resource controller, using default values. It
// takes a set of previously forwarded htlcs and the node's known channels as parameters
// to bootstrap the state of the manager.
func newResourceController(lnd lndclient, htlcCompleted htlcCompletedFunc,
	htlcThreshold htlcThresholdFunc, historyFunc historyFunc,
	channels map[uint64]*channel) (*resourceController, error) {

	params := lrc.ManagerParams{
		// We used reduced values (1h / 12h) with the same ratio as
		// our proposal.
		RevenueWindow:        time.Hour,
		ReputationMultiplier: 12,
		ProtectedPercentage:  50,
		ResolutionPeriod:     time.Second * 90,
		BlockTime:            5,
	}
	clock := clock.NewDefaultClock()

	manager, err := lrc.NewResourceManager(
		params, clock,
		// Reputation bootstrap with incoming htlcs.
		func(id lnwire.ShortChannelID) (*lrc.DecayingAverageStart,
			error) {

			forwards, err := historyFunc(id, true)
			if err != nil {
				return nil, err
			}

			return lrc.BootstrapReputation(
				id, params, forwards, clock,
			)
		},
		// Revenue bootstrap with outgoing htlcs.
		func(id lnwire.ShortChannelID) (*lrc.DecayingAverageStart,
			error) {

			forwards, err := historyFunc(id, false)
			if err != nil {
				return nil, err
			}

			return lrc.BootstrapRevenue(
				id, params, forwards, clock,
			)
		},
		log,
	)
	if err != nil {
		return nil, err
	}

	channelMap := make(map[lnwire.ShortChannelID]lrc.ChannelInfo)
	for chanID, channel := range channels {
		channelMap[lnwire.NewShortChanIDFromInt(chanID)] = lrc.ChannelInfo{
			InFlightHTLC: uint64(channel.outgoingSlotLimit),
			// NBNBNB: LND doesn't currently implement the
			// "oakland protocol" of setting the max in flight to
			// 45% of your channel capacity, but it's a reasonable
			// enough expectation that this will be used long term.
			// So we limit our in-flight accordingly.
			InFlightLiquidity: (channel.capacityMsat * 45) / 100,
		}
	}

	info, err := lnd.getInfo()
	if err != nil {
		return nil, err
	}

	return &resourceController{
		lnd,
		info.height,
		time.Now(),
		htlcCompleted,
		htlcThreshold,
		manager,
	}, nil
}

func (r *resourceController) process(ctx context.Context, event peerInterceptEvent,
	chanOut *channel) error {

	proposed, err := r.proposedHTLCFromIntercepted(&event.interceptEvent)
	if err != nil {
		return err
	}

	action, err := r.ForwardHTLC(proposed, &lrc.ChannelInfo{
		InFlightLiquidity: chanOut.outgoingLiquidityLimit,
		InFlightHTLC:      uint64(chanOut.outgoingSlotLimit),
	},
	)
	if err != nil {
		return err
	}

	log.Infof("Resource Controller %v -> outgoing endorsed: %v "+
		"(incoming revenue: %v - htlc risk: %v vs outgoing revenue: %v)",
		event.interceptEvent, action.ForwardOutcome,
		action.ReputationCheck.IncomingRevenue,
		action.ReputationCheck.HTLCRisk,
		action.ReputationCheck.OutgoingRevenue,
	)

	threshold := thresholdFromFwdDecision(
		time.Now(), action, event.incomingCircuitKey,
		event.outgoingChannel, event.paymentHash,
	)
	if err := r.htlcThreshold(context.Background(), threshold); err != nil {
		return err
	}

	switch action.ForwardOutcome {
	case lrc.ForwardOutcomeEndorsed:
		event.resume(true, lrc.EndorsementTrue)

	case lrc.ForwardOutcomeUnendorsed:
		event.resume(true, lrc.EndorsementFalse)

	case lrc.ForwardOutcomeNoResources:
		event.resume(false, lrc.EndorsementNone)

	default:
		return fmt.Errorf("Unexpected forward action: %v", action)
	}

	return nil
}

func (r *resourceController) resolved(ctx context.Context,
	key peerResolvedEvent) error {

	inFlight, err := r.ResolveHTLC(
		resolvedHTLCFromIntercepted(key.resolvedEvent),
	)
	if err != nil {
		return err
	}

	htlc := &HtlcInfo{
		addTime:      inFlight.TimestampAdded,
		resolveTime:  time.Now(),
		settled:      key.settled,
		incomingMsat: inFlight.IncomingAmount,
		outgoingMsat: inFlight.OutgoingAmount,
		// TODO: we don't care about this.
		incomingPeer:     route.Vertex{},
		outgoingPeer:     route.Vertex{},
		incomingCircuit:  key.incomingCircuitKey,
		outgoingCircuit:  key.outgoingCircuitKey,
		incomingEndorsed: inFlight.IncomingEndorsed,
		outgoingEndorsed: inFlight.OutgoingEndorsed,
		cltvDelta:        inFlight.CltvExpiryDelta,
	}

	return r.htlcCompleted(context.Background(), htlc)
}

func (r *resourceController) proposedHTLCFromIntercepted(i *interceptEvent) (
	*lrc.ProposedHTLC, error) {

	/*height, err := r.getHeight()
	if err != nil {
		return nil, err
	}*/

	return &lrc.ProposedHTLC{
		IncomingChannel: lnwire.NewShortChanIDFromInt(
			i.incomingCircuitKey.channel,
		),
		OutgoingChannel: lnwire.NewShortChanIDFromInt(
			i.outgoingChannel,
		),
		IncomingIndex:    int(i.incomingCircuitKey.htlc),
		IncomingEndorsed: i.endorsed,
		IncomingAmount:   i.incomingMsat,
		OutgoingAmount:   i.outgoingMsat,
		// The revenue window that we use is supposed to be set by the
		// maximum time a HTLC can be held for (ie, 2016 blocks). But
		// since we're reducing this window to make an attack easier (to
		// one hour), we reduce the CLTV expiry reflect this - 60
		// minutes with 5 minute blocks = 12). This is backwards (we
		// should be setting the window according to this value, not the
		// reverse), but we want to have HTLC costs which are
		// proportionate to the window we're dealing with.
		CltvExpiryDelta: 12, //i.outgoingExpiry - height,
	}, nil
}

func resolvedHTLCFromIntercepted(resolved resolvedEvent) *lrc.ResolvedHTLC {
	return &lrc.ResolvedHTLC{
		IncomingIndex: int(resolved.incomingCircuitKey.htlc),
		IncomingChannel: lnwire.NewShortChanIDFromInt(
			resolved.incomingCircuitKey.channel,
		),
		OutgoingChannel: lnwire.NewShortChanIDFromInt(
			resolved.outgoingCircuitKey.channel,
		),
		Success:          resolved.settled,
		TimestampSettled: resolved.timestamp,
	}
}

func thresholdFromFwdDecision(ts time.Time, fwd *lrc.ForwardDecision,
	chanIn circuitKey, chanOut uint64, hash lntypes.Hash) *htlcThresholds {

	return &htlcThresholds{
		paymentHash:     hash,
		forwardTs:       ts,
		incomingCircuit: chanIn,
		outgoingChannel: chanOut,
		incomingRevenue: fwd.ReputationCheck.IncomingRevenue,
		inFlightRisk:    fwd.ReputationCheck.InFlightRisk,
		htlcRisk:        fwd.ReputationCheck.HTLCRisk,
		outgoingRevenue: fwd.ReputationCheck.OutgoingRevenue,
		outcome:         fwd.ForwardOutcome,
	}
}
