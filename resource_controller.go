package main

import (
	"context"
	"fmt"
	"time"

	"github.com/carlakc/lrc"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/lntypes"
	"github.com/lightningnetwork/lnd/lnwire"
)

// Compile time check that resourceController implements the controller interface.
var _ controller = (*resourceController)(nil)

// resourceController provides resource management using local reputation, HTLC
// endorsement and resource bucketing. This struct simply wraps an external
// implementation in the controller interface.
type resourceController struct {
	logOnly      bool
	htlcComplete func(context.Context, *HtlcInfo) error
	lrc.LocalResourceManager

	// Track payment hash by incoming circuit key.
	// Note: this is a bit of a hacky workaround required to surface the
	// payment hash of our forwarded payments for analysis.
	paymentHashes map[circuitKey]lntypes.Hash
}

// newResourceController creates a new resource controller, using default values.
func newResourceController(logOnly bool, htlcComplete func(context.Context,
	*HtlcInfo) error) *resourceController {

	// The only validation that we perform is on the 50% reserve, which
	// we know is valid because we hardcode it.
	manager, _ := lrc.NewReputationManager(
		// Assess revenue over 2016 blocks, ~2 weeks.
		time.Hour*24*14,
		// Assess reputation over 20 weeks (10x revenue)
		10,
		// Expect HTLCs to resolve within 90 seconds.
		time.Second*90,
		clock.NewDefaultClock(),
		// Reserve 50% of resources for protected HTLCs.
		50,
	)

	return &resourceController{
		logOnly:              logOnly,
		htlcComplete:         htlcComplete,
		LocalResourceManager: manager,
		paymentHashes:        make(map[circuitKey]lntypes.Hash),
	}
}

func (r *resourceController) process(ctx context.Context, event peerInterceptEvent,
	chanOut *channel) error {

	action, err := r.ForwardHTLC(
		proposedHTLCFromIntercepted(&event.interceptEvent), &lrc.ChannelInfo{
			InFlightLiquidity: lnwire.MilliSatoshi(chanOut.liquidityInFlight),
			InFlightHTLC:      uint64(chanOut.countInFlight),
		},
	)
	if err != nil {
		return err
	}

	log.Infof("Resource Controller - %v: %v", action, event.interceptEvent)

	// Add hash to list of in-flight HTLCs.
	r.paymentHashes[event.incomingCircuitKey] = event.paymentHash

	switch action {
	case lrc.ForwardOutcomeEndorsed:
		event.resume(true, lrc.EndorsementTrue)

	case lrc.ForwardOutcomeUnendorsed:
		event.resume(true, lrc.EndorsementFalse)

	case lrc.ForwardOutcomeNoResources:
		// If we're going to drop the HTLC, we don't need to keep its
		// payment hash around.
		//
		// Note: we wastefully add and remove here, should refactor
		// this switch.
		if !r.logOnly {
			delete(r.paymentHashes, event.incomingCircuitKey)
		}

		event.resume(!r.logOnly, lrc.EndorsementFalse)

	default:
		return fmt.Errorf("Unexpected forward action: %v", action)
	}

	return nil
}

func (r *resourceController) resolved(ctx context.Context, key peerResolvedEvent) error {
	inFlight := r.ResolveHTLC(resolvedHTLCFromIntercepted(key))
	htlc := &HtlcInfo{
		addTime:          key.timestamp,
		resolveTime:      time.Now(),
		settled:          key.settled,
		incomingMsat:     inFlight.IncomingAmount,
		outgoingMsat:     inFlight.OutgoingAmount,
		incomingPeer:     key.incomingPeer,
		outgoingPeer:     key.outgoingPeer,
		incomingCircuit:  key.incomingCircuitKey,
		outgoingCircuit:  key.outgoingCircuitKey,
		incomingEndorsed: inFlight.ProposedHTLC.IncomingEndorsed,
		outgoingEndorsed: inFlight.OutgoingEndorsed,
	}

	return r.htlcComplete(ctx, htlc)
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

func resolvedHTLCFromIntercepted(resolved peerResolvedEvent) *lrc.ResolvedHLTC {
	return &lrc.ResolvedHLTC{
		IncomingIndex: int(resolved.incomingCircuitKey.htlc),
		IncomingChannel: lnwire.NewShortChanIDFromInt(
			resolved.incomingCircuitKey.channel,
		),
		OutgoingChannel: lnwire.NewShortChanIDFromInt(
			resolved.outgoingCircuitKey.channel,
		),
		Success: resolved.settled,
	}
}
