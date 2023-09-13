package main

import (
	"context"
	"fmt"
	"time"

	"github.com/carlakc/lrc"
	"github.com/lightningnetwork/lnd/clock"
	"github.com/lightningnetwork/lnd/lnwire"
)

// Compile time check that resourceController implements the controller interface.
var _ controller = (*resourceController)(nil)

// resourceController provides resource management using local reputation, HTLC
// endorsement and resource bucketing. This struct simply wraps an external
// implementation in the controller interface.
type resourceController struct {
	logOnly bool
	lrc.LocalResourceManager
}

// newResourceController creates a new resource controller, using default values.
func newResourceController(logOnly bool) *resourceController {
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
		logOnly,
		manager,
	}
}

func (r *resourceController) process(ctx context.Context, event peerInterceptEvent,
	chanOut *channel) error {

	action, err := r.ForwardHTLC(
		proposedHTLCFromIntercepted(&event.interceptEvent), &lrc.ChannelInfo{
			InFlightLiquidity: chanOut.maxInFlight,
			InFlightHTLC:      uint64(chanOut.maxAcceptedHtlcs),
		},
	)
	if err != nil {
		return err
	}

	log.Infof("Resource Controller - %v: %v", action, event.interceptEvent)

	switch action {
	case lrc.ForwardOutcomeEndorsed:
		event.resume(true)

	case lrc.ForwardOutcomeUnendorsed:
		event.resume(true)

	case lrc.ForwardOutcomeNoResources:
		event.resume(!r.logOnly)

	default:
		return fmt.Errorf("Unexpected forward action: %v", action)
	}

	return nil
}

func (r *resourceController) resolved(ctx context.Context, key resolvedEvent) error {
	r.ResolveHTLC(resolvedHTLCFromIntercepted(key))

	return nil
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
		IncomingEndorsed: false,
		IncomingAmount:   i.incomingMsat,
		OutgoingAmount:   i.outgoingMsat,
		CltvExpiryDelta:  i.cltvDelta,
	}
}

func resolvedHTLCFromIntercepted(resolved resolvedEvent) *lrc.ResolvedHLTC {
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
