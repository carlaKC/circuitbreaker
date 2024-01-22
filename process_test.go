package main

import (
	"context"
	"testing"
	"time"

	"github.com/lightningnetwork/lnd/routing/route"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func TestProcess(t *testing.T) {
	defer Timeout()()

	t.Run("settle", func(t *testing.T) {
		testProcess(t, resolveEventSettle)
	})
	t.Run("forward fail", func(t *testing.T) {
		testProcess(t, resolveEventForwardFail)
	})
	t.Run("link fail", func(t *testing.T) {
		testProcess(t, resolveEventLinkFail)
	})
}

type resolveEvent int

const (
	resolveEventSettle resolveEvent = iota
	resolveEventForwardFail
	resolveEventLinkFail
)

func testProcess(t *testing.T, event resolveEvent) {
	client := newLndclientMock(testChannels, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	log := zaptest.NewLogger(t).Sugar()

	cfg := &Limits{
		PerPeer: map[route.Vertex]Limit{
			{2}: {
				MaxHourlyRate: 60,
				MaxPending:    1,
			},
			{3}: {
				MaxHourlyRate: 60,
				MaxPending:    1,
			},
		},
	}

	p, _ := NewProcess(client, log, cfg, db)

	resolved := make(chan struct{})
	p.resolvedCallback = func() {
		close(resolved)
	}

	exit := make(chan error)
	go func() {
		exit <- p.Run(ctx)
	}()

	key := circuitKey{
		channel: 2,
		htlc:    5,
	}
	client.htlcInterceptorRequests <- &interceptedEvent{
		incomingCircuitKey: key,
	}

	resp := <-client.htlcInterceptorResponses
	require.True(t, resp.resume)

	htlcEvent := &resolvedEvent{
		incomingCircuitKey: key,
		outgoingCircuitKey: outgoingKey,
	}

	switch event {
	case resolveEventForwardFail:
		htlcEvent.settled = false

	case resolveEventLinkFail:
		htlcEvent.settled = false

	case resolveEventSettle:
		htlcEvent.settled = true
	}

	client.htlcEvents <- htlcEvent

	<-resolved

	cancel()
	require.ErrorIs(t, <-exit, context.Canceled)
}

func TestLimits(t *testing.T) {
	for _, mode := range []Mode{ModeFail, ModeQueue, ModeQueuePeerInitiated} {
		t.Run(mode.String(), func(t *testing.T) {
			t.Run("rate limit", func(t *testing.T) { testRateLimit(t, mode) })
			t.Run("max pending", func(t *testing.T) { testMaxPending(t, mode) })
		})
	}
}

func testRateLimit(t *testing.T, mode Mode) {
	defer Timeout()()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	cfg := &Limits{
		PerPeer: map[route.Vertex]Limit{
			{2}: {
				MaxHourlyRate: 1800,
				Mode:          mode,
			},
			{3}: {
				MaxHourlyRate: 1800,
				Mode:          mode,
			},
		},
	}

	client := newLndclientMock(testChannels, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := zaptest.NewLogger(t).Sugar()

	p, _ := NewProcess(client, log, cfg, db)
	p.burstSize = 2

	exit := make(chan error)
	go func() {
		exit <- p.Run(ctx)
	}()

	var chanId uint64 = 2
	if mode == ModeQueuePeerInitiated {
		// We are the initiator of the channel. Not queueing is expected in this
		// mode.
		chanId = 3
	}

	key := circuitKey{
		channel: chanId,
		htlc:    5,
	}
	interceptReq := &interceptedEvent{
		incomingCircuitKey: key,
	}

	// First htlc accepted.
	client.htlcInterceptorRequests <- interceptReq
	resp := <-client.htlcInterceptorResponses
	require.True(t, resp.resume)

	// Second htlc right after is also accepted because of burst size 2.
	interceptReq.incomingCircuitKey.htlc++
	client.htlcInterceptorRequests <- interceptReq
	resp = <-client.htlcInterceptorResponses
	require.True(t, resp.resume)

	// Third htlc again right after should hit the rate limit.
	interceptReq.incomingCircuitKey.htlc++
	client.htlcInterceptorRequests <- interceptReq

	interceptStart := time.Now()

	resp = <-client.htlcInterceptorResponses

	if mode == ModeQueue {
		require.True(t, resp.resume)
		require.GreaterOrEqual(t, time.Since(interceptStart), time.Second)
	} else {
		require.False(t, resp.resume)

		htlcEvent := &resolvedEvent{
			incomingCircuitKey: key,
			outgoingCircuitKey: outgoingKey,
			settled:            false,
		}

		client.htlcEvents <- htlcEvent

		// Allow some time for the peer controller to process the failed forward
		// event.
		time.Sleep(time.Second)
	}

	cancel()
	require.ErrorIs(t, <-exit, context.Canceled)
}

func testMaxPending(t *testing.T, mode Mode) {
	defer Timeout()()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	cfg := &Limits{
		PerPeer: map[route.Vertex]Limit{
			{2}: {
				MaxHourlyRate: 60,
				MaxPending:    1,
				Mode:          mode,
			},
			{3}: {
				MaxHourlyRate: 60,
				MaxPending:    1,
				Mode:          mode,
			},
		},
	}

	client := newLndclientMock(testChannels, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := zaptest.NewLogger(t).Sugar()

	p, _ := NewProcess(client, log, cfg, db)
	p.burstSize = 2

	exit := make(chan error)
	go func() {
		exit <- p.Run(ctx)
	}()

	var chanId uint64 = 2
	if mode == ModeQueuePeerInitiated {
		// We are the initiator of the channel. Not queueing is expected in this
		// mode.
		chanId = 3
	}

	key := circuitKey{
		channel: chanId,
		htlc:    5,
	}
	interceptReq := &interceptedEvent{
		incomingCircuitKey: key,
	}

	// First htlc accepted.
	client.htlcInterceptorRequests <- interceptReq
	resp := <-client.htlcInterceptorResponses
	require.True(t, resp.resume)

	// Second htlc should be hitting the max pending htlcs limit.
	interceptReq.incomingCircuitKey.htlc++
	client.htlcInterceptorRequests <- interceptReq

	if mode == ModeQueue {
		select {
		case <-client.htlcInterceptorResponses:
			require.Fail(t, "unexpected response")

		case <-time.After(time.Second):
		}
	} else {
		resp = <-client.htlcInterceptorResponses
		require.False(t, resp.resume)
	}

	cancel()
	require.ErrorIs(t, <-exit, context.Canceled)
}

func TestNewPeer(t *testing.T) {
	// Initialize lnd with test channels.
	client := newLndclientMock(testChannels, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	log := zaptest.NewLogger(t).Sugar()

	cfg := &Limits{}

	p, _ := NewProcess(client, log, cfg, db)

	// Setup quick peer refresh.
	p.peerRefreshInterval = 100 * time.Millisecond

	exit := make(chan error)
	go func() {
		exit <- p.Run(ctx)
	}()

	state, err := p.getRateCounters(ctx)
	require.NoError(t, err)
	require.Len(t, state, 3)

	// Add a new peer.
	log.Infow("Add a new peer")
	client.channels[100] = &channel{peer: route.Vertex{100}}

	// Wait for the peer to be reported.
	require.Eventually(t, func() bool {
		state, err := p.getRateCounters(ctx)
		require.NoError(t, err)

		return len(state) == 4
	}, time.Second, 100*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-exit, context.Canceled)
}

func TestBlocked(t *testing.T) {
	defer Timeout()()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	cfg := &Limits{
		PerPeer: map[route.Vertex]Limit{
			{2}: {
				MaxHourlyRate: 1800,
				Mode:          ModeBlock,
			},
		},
	}

	client := newLndclientMock(testChannels, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	log := zaptest.NewLogger(t).Sugar()

	p, _ := NewProcess(client, log, cfg, db)

	exit := make(chan error)
	go func() {
		exit <- p.Run(ctx)
	}()

	var chanId uint64 = 2

	key := circuitKey{
		channel: chanId,
		htlc:    5,
	}
	interceptReq := &interceptedEvent{
		incomingCircuitKey: key,
	}

	// Htlc blocked.
	client.htlcInterceptorRequests <- interceptReq
	resp := <-client.htlcInterceptorResponses
	require.False(t, resp.resume)

	cancel()
	require.ErrorIs(t, <-exit, context.Canceled)
}

// TestChannelNotFound tests that we'll successfully exit when we cannot lookup the
// channel that a htlc belongs to.
func TestChannelNotFound(t *testing.T) {
	client := newLndclientMock(testChannels, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	log := zaptest.NewLogger(t).Sugar()

	cfg := &Limits{}

	p, _ := NewProcess(client, log, cfg, db)

	exit := make(chan error)

	go func() {
		exit <- p.Run(ctx)
	}()

	// Next, send a htlc that is from an unknown channel.
	key := circuitKey{
		channel: 99,
		htlc:    4,
	}
	client.htlcInterceptorRequests <- &interceptedEvent{
		incomingCircuitKey: key,
	}

	select {
	case err := <-exit:
		require.ErrorIs(t, err, errChannelNotFound)

	case <-time.After(time.Second * 10):
		t.Fatalf("timeout on process error")
	}
}

// TestOutgoingChannelNotFound tests the case where the outgoing channel for a htlc is
// not found in two cases:
// 1. The HTLC was settled: the channel must exist, so we fail if it's not found
// 2. The HTLC was failed: the outgoing channel could be bogus, so we handle the error
func TestOutgoingChannelNotFound(t *testing.T) {
	tests := []struct {
		name          string
		settled       bool
		outgoingFound bool
		err           error
	}{
		{
			name:          "outgoing found, settled",
			settled:       true,
			outgoingFound: true,
		},
		{
			name:          "outgoing found, not settled",
			settled:       false,
			outgoingFound: true,
		},
		{
			name:          "outgoing not found, settled",
			settled:       true,
			outgoingFound: false,
			err:           errChannelNotFound,
		},
		{
			name:          "outgoing not found, not settled",
			settled:       false,
			outgoingFound: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			testLookupOutgoingChannel(
				t, test.settled, test.outgoingFound, test.err,
			)
		})
	}
}

func testLookupOutgoingChannel(t *testing.T, settled, outgoingFound bool,
	exitErr error) {

	client := newLndclientMock(testChannels, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	log := zaptest.NewLogger(t).Sugar()

	cfg := &Limits{}

	p, _ := NewProcess(client, log, cfg, db)

	resolved := make(chan struct{})
	p.resolvedCallback = func() {
		close(resolved)
	}

	exit := make(chan error)
	go func() {
		exit <- p.Run(ctx)
	}()

	// Send a htlc with a known incoming channel.
	key := circuitKey{
		channel: 2,
		htlc:    5,
	}
	client.htlcInterceptorRequests <- &interceptedEvent{
		incomingCircuitKey: key,
	}

	resp := <-client.htlcInterceptorResponses
	require.True(t, resp.resume)

	// Set the outgoing channel based on whether we want it to be found by our
	// lookup or not.
	outgoingKey := outgoingKey
	if !outgoingFound {
		outgoingKey.channel = 9999
	}

	htlcEvent := &resolvedEvent{
		incomingCircuitKey: key,
		outgoingCircuitKey: outgoingKey,
		settled:            settled,
	}

	client.htlcEvents <- htlcEvent

	// If expected, assert that we exit with an error, otherwise ensure that the htlc
	// is settled and we exit cleanly.
	if exitErr != nil {
		require.ErrorIs(t, <-exit, exitErr)
	} else {
		<-resolved

		cancel()
		require.ErrorIs(t, <-exit, context.Canceled)
	}
}

// TestClosedChannelHtlc tests that we can handle intercepted htlcs that are associated
// with closed channels.
func TestClosedChannelHtlc(t *testing.T) {
	// Initialize lnd with a closed channel.
	var testClosedChannels = map[uint64]*channel{
		5: {peer: route.Vertex{2}},
	}
	client := newLndclientMock(testChannels, testClosedChannels)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	db, cleanup := setupTestDb(t, defaultFwdHistoryLimit)
	defer cleanup()

	log := zaptest.NewLogger(t).Sugar()

	cfg := &Limits{}

	p, _ := NewProcess(client, log, cfg, db)

	exit := make(chan error)

	go func() {
		exit <- p.Run(ctx)
	}()

	// Send a htlc that is from a closed channel, it should be given the go-ahead to
	// resume.
	key := circuitKey{
		channel: 5,
		htlc:    3,
	}
	client.htlcInterceptorRequests <- &interceptedEvent{
		incomingCircuitKey: key,
	}

	resp := <-client.htlcInterceptorResponses
	require.Equal(t, key, resp.key)

	cancel()
	require.ErrorIs(t, <-exit, context.Canceled)
}
