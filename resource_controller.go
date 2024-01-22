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
	htlcCompleted htlcCompletedFunc
	htlcThreshold htlcThresholdFunc
	lrc.LocalResourceManager
}

// listHistoryFunc is the signature of a function used to look up historical
// htlcs.
type listHistoryFunc func(start, end time.Time) ([]*lrc.ForwardedHTLC, error)

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

// newResourceController creates a new resource controller, using default values. It
// takes a set of previously forwarded htlcs and the node's known channels as parameters
// to bootstrap the state of the manager.
func newResourceController(htlcCompleted htlcCompletedFunc,
	htlcThreshold htlcThresholdFunc, listHistory listHistoryFunc,
	channels map[uint64]*channel) (*resourceController, error) {

	// Assess revenue over 2016 blocks, ~2 weeks.
	revenueWindow := time.Hour * 24 * 14

	// Assess reputation over 20 weeks (10x revenue)
	reputationMultiplier := 10

	// The only validation that we perform is on the 50% reserve, which
	// we know is valid because we hardcode it.

	manager, _ := lrc.NewReputationManager(
		revenueWindow,
		reputationMultiplier,
		// Expect HTLCs to resolve within 90 seconds.
		time.Second*90,
		clock.NewDefaultClock(),
		// Reserve 50% of resources for protected HTLCs.
		50,
	)

	channelMap := make(map[lnwire.ShortChannelID]lrc.ChannelInfo)
	for chanID, channel := range channels {
		channelMap[lnwire.NewShortChanIDFromInt(chanID)] = lrc.ChannelInfo{
			InFlightHTLC:      uint64(channel.outgoingSlotLimit),
			InFlightLiquidity: channel.outgoingLiquidityLimit,
		}
	}

	// We want to bootstrap the reputation manager with historical htlcs. We want
	// all of our history that falls within the reputation window we're concerned
	// with (which is the revenue window * reputation multiplier).
	reputationWindow := revenueWindow * time.Duration(-1*reputationMultiplier)
	endTime := time.Now()
	startTime := endTime.Add(reputationWindow)

	htlcs, err := listHistory(startTime, endTime)
	if err != nil {
		return nil, err
	}

	// Bootstrap the manager with any HTLCs that we've previously forwarded.
	if err := manager.AddHistoricalHTLCs(htlcs, channelMap); err != nil {
		return nil, err
	}

	return &resourceController{
		htlcCompleted,
		htlcThreshold,
		manager,
	}, nil
}

func (r *resourceController) process(ctx context.Context, event peerInterceptEvent,
	chanOut *channel) error {

	action, err := r.ForwardHTLC(
		proposedHTLCFromIntercepted(&event.interceptEvent), &lrc.ChannelInfo{
			InFlightLiquidity: chanOut.outgoingLiquidityLimit,
			InFlightHTLC:      uint64(chanOut.outgoingSlotLimit),
		},
	)
	if err != nil {
		return err
	}

	log.Infof("Resource Controller %v -> outgoing endorsed: %v",
		event.interceptEvent, action.ForwardOutcome)

	threshold := thresholdFromFwdDecision(
		time.Now(), action, event.incomingCircuitKey.channel,
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

	inFlight := r.ResolveHTLC(resolvedHTLCFromIntercepted(key.resolvedEvent))

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

func proposedHTLCFromIntercepted(i *interceptEvent) *lrc.ProposedHTLC {
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
		CltvExpiryDelta:  i.cltvDelta,
	}
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

func thresholdFromFwdDecision(ts time.Time, fwd *lrc.ForwardDecision, chanIn,
	chanOut uint64, hash lntypes.Hash) *htlcThresholds {

	return &htlcThresholds{
		paymentHash:     hash,
		forwardTs:       ts,
		incomingChannel: chanIn,
		outgoingChannel: chanOut,
		incomingRevenue: fwd.ReputationCheck.IncomingRevenue,
		inFlightRisk:    fwd.ReputationCheck.InFlightRisk,
		htlcRisk:        fwd.ReputationCheck.HTLCRisk,
		outgoingRevenue: fwd.ReputationCheck.OutgoingRevenue,
		outcome:         fwd.ForwardOutcome,
	}
}
