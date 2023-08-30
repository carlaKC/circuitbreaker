package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/lightningequipment/circuitbreaker/circuitbreakerrpc"
	"github.com/lightningnetwork/lnd/lnwire"
	"github.com/lightningnetwork/lnd/queue"
	"github.com/lightningnetwork/lnd/routing/route"
	"go.uber.org/zap"
)

type server struct {
	process *process
	lnd     lndclient
	db      *Db
	log     *zap.SugaredLogger

	aliases     map[route.Vertex]string
	aliasesLock sync.Mutex

	circuitbreakerrpc.UnimplementedServiceServer
}

func NewServer(log *zap.SugaredLogger, process *process,
	lnd lndclient, db *Db) *server {

	return &server{
		process: process,
		lnd:     lnd,
		db:      db,
		log:     log,
		aliases: make(map[route.Vertex]string),
	}
}

func (s *server) getAlias(key route.Vertex) (string, error) {
	s.aliasesLock.Lock()
	defer s.aliasesLock.Unlock()

	alias, ok := s.aliases[key]
	if ok {
		return alias, nil
	}

	alias, err := s.lnd.getNodeAlias(key)
	switch {
	case err == ErrNodeNotFound:

	case err != nil:
		return "", err
	}

	s.aliases[key] = alias

	return alias, nil
}

func (s *server) GetInfo(ctx context.Context,
	req *circuitbreakerrpc.GetInfoRequest) (*circuitbreakerrpc.GetInfoResponse,
	error) {

	info, err := s.lnd.getInfo()
	if err != nil {
		return nil, err
	}

	return &circuitbreakerrpc.GetInfoResponse{
		NodeKey:     hex.EncodeToString(info.nodeKey[:]),
		NodeVersion: info.version,
		NodeAlias:   info.alias,

		Version: BuildVersion,
	}, nil
}

func unmarshalLimit(rpcLimit *circuitbreakerrpc.Limit) (Limit, error) {
	limit := Limit{
		MaxHourlyRate: rpcLimit.MaxHourlyRate,
		MaxPending:    rpcLimit.MaxPending,
	}

	switch rpcLimit.Mode {
	case circuitbreakerrpc.Mode_MODE_FAIL:
		limit.Mode = ModeFail

	case circuitbreakerrpc.Mode_MODE_QUEUE:
		limit.Mode = ModeQueue

	case circuitbreakerrpc.Mode_MODE_QUEUE_PEER_INITIATED:
		limit.Mode = ModeQueuePeerInitiated

	case circuitbreakerrpc.Mode_MODE_BLOCK:
		limit.Mode = ModeBlock

	default:
		return Limit{}, errors.New("unknown mode")
	}

	return limit, nil
}

func (s *server) UpdateLimits(ctx context.Context,
	req *circuitbreakerrpc.UpdateLimitsRequest) (
	*circuitbreakerrpc.UpdateLimitsResponse, error) {

	// Parse and validate request.
	limits := make(map[route.Vertex]Limit)
	for nodeStr, rpcLimit := range req.Limits {
		node, err := route.NewVertexFromStr(nodeStr)
		if err != nil {
			return nil, err
		}

		if node == defaultNodeKey {
			return nil, fmt.Errorf("set default limit for %v through "+
				"UpdateDefaultLimit", node)
		}

		if rpcLimit == nil {
			return nil, fmt.Errorf("no limit specified for %v", node)
		}

		limit, err := unmarshalLimit(rpcLimit)
		if err != nil {
			return nil, err
		}

		limits[node] = limit
	}

	// Apply limits.
	for node, limit := range limits {
		node, limit := node, limit

		s.log.Infow("Updating limit", "node", node, "limit", limit)

		if err := s.db.UpdateLimit(ctx, node, limit); err != nil {
			return nil, err
		}

		if err := s.process.UpdateLimit(ctx, &node, &limit); err != nil {
			return nil, err
		}
	}

	return &circuitbreakerrpc.UpdateLimitsResponse{}, nil
}

func (s *server) ClearLimits(ctx context.Context,
	req *circuitbreakerrpc.ClearLimitsRequest) (
	*circuitbreakerrpc.ClearLimitsResponse, error) {

	for _, nodeStr := range req.Nodes {
		node, err := route.NewVertexFromStr(nodeStr)
		if err != nil {
			return nil, err
		}

		s.log.Infow("Clearing limit", "node", node)

		err = s.db.ClearLimit(ctx, node)
		if err != nil {
			return nil, err
		}

		err = s.process.UpdateLimit(ctx, &node, nil)
		if err != nil {
			return nil, err
		}
	}

	return &circuitbreakerrpc.ClearLimitsResponse{}, nil
}

func (s *server) UpdateDefaultLimit(ctx context.Context,
	req *circuitbreakerrpc.UpdateDefaultLimitRequest) (
	*circuitbreakerrpc.UpdateDefaultLimitResponse, error) {

	limit, err := unmarshalLimit(req.Limit)
	if err != nil {
		return nil, err
	}

	s.log.Infow("Updating default limit", "limit", limit)

	err = s.db.UpdateLimit(ctx, defaultNodeKey, limit)
	if err != nil {
		return nil, err
	}

	err = s.process.UpdateLimit(ctx, nil, &limit)
	if err != nil {
		return nil, err
	}

	return &circuitbreakerrpc.UpdateDefaultLimitResponse{}, nil
}

func marshalLimit(limit Limit) (*circuitbreakerrpc.Limit, error) {
	rpcLimit := &circuitbreakerrpc.Limit{
		MaxHourlyRate: limit.MaxHourlyRate,
		MaxPending:    limit.MaxPending,
	}

	switch limit.Mode {
	case ModeFail:
		rpcLimit.Mode = circuitbreakerrpc.Mode_MODE_FAIL

	case ModeQueue:
		rpcLimit.Mode = circuitbreakerrpc.Mode_MODE_QUEUE

	case ModeQueuePeerInitiated:
		rpcLimit.Mode = circuitbreakerrpc.Mode_MODE_QUEUE_PEER_INITIATED

	case ModeBlock:
		rpcLimit.Mode = circuitbreakerrpc.Mode_MODE_BLOCK

	default:
		return nil, errors.New("unknown mode")
	}

	return rpcLimit, nil
}

func (s *server) ListLimits(ctx context.Context,
	req *circuitbreakerrpc.ListLimitsRequest) (
	*circuitbreakerrpc.ListLimitsResponse, error) {

	limits, err := s.db.GetLimits(ctx)
	if err != nil {
		return nil, err
	}

	counters, err := s.process.getRateCounters(ctx)
	if err != nil {
		return nil, err
	}

	marshalCounter := func(count rateCounts) *circuitbreakerrpc.Counter {
		return &circuitbreakerrpc.Counter{
			Success: count.success,
			Fail:    count.fail,
			Reject:  count.reject,
		}
	}

	var rpcLimits = []*circuitbreakerrpc.NodeLimit{}

	createRpcState := func(peer route.Vertex, state *peerState) (
		*circuitbreakerrpc.NodeLimit, error) {

		alias, err := s.getAlias(peer)
		if err != nil {
			return nil, err
		}

		return &circuitbreakerrpc.NodeLimit{
			Node:             hex.EncodeToString(peer[:]),
			Alias:            alias,
			Counter_1H:       marshalCounter(state.counts[0]),
			Counter_24H:      marshalCounter(state.counts[1]),
			QueueLen:         state.queueLen,
			PendingHtlcCount: state.pendingHtlcCount,
		}, nil
	}

	for peer, limit := range limits.PerPeer {
		counts, ok := counters[peer]
		if !ok {
			// Report all zeroes.
			counts = &peerState{
				counts: make([]rateCounts, len(rateCounterIntervals)),
			}
		}

		rpcState, err := createRpcState(peer, counts)
		if err != nil {
			return nil, err
		}

		rpcLimit, err := marshalLimit(limit)
		if err != nil {
			return nil, err
		}
		rpcState.Limit = rpcLimit

		delete(counters, peer)

		rpcLimits = append(rpcLimits, rpcState)
	}

	for peer, counts := range counters {
		rpcLimit, err := createRpcState(peer, counts)
		if err != nil {
			return nil, err
		}

		rpcLimits = append(rpcLimits, rpcLimit)
	}

	defaultLimit, err := marshalLimit(limits.Default)
	if err != nil {
		return nil, err
	}

	return &circuitbreakerrpc.ListLimitsResponse{
		DefaultLimit: defaultLimit,
		Limits:       rpcLimits,
	}, nil
}

func (s *server) ListForwardingHistory(ctx context.Context,
	req *circuitbreakerrpc.ListForwardingHistoryRequest) (
	*circuitbreakerrpc.ListForwardingHistoryResponse, error) {

	var (
		// By default query from the epoch until now.
		startTime = time.Time{}
		endTime   = time.Now()
	)

	if req.StartTimeNs != 0 {
		startTime = time.Unix(0, req.StartTimeNs)
	}

	if req.EndTimeNs != 0 {
		endTime = time.Unix(0, req.EndTimeNs)
	}

	if startTime.After(endTime) {
		return nil, fmt.Errorf("start time: %v after end time: %v", startTime,
			endTime)
	}

	// To report report on channel utilization, we need to get all the HTLCs that were in
	// flight at the beginning of our window. If our start time is zero, we won't have anything
	// in flight.
	inFlight, err := s.db.ListInFlightAt(ctx, startTime)
	if err != nil {
		return nil, err
	}

	htlcs, err := s.db.ListForwardingHistory(ctx, startTime, endTime)
	if err != nil {
		return nil, err
	}

	rpcHtlcs, err := s.marshalFwdHistoryWithUsage(inFlight, htlcs)
	if err != nil {
		return nil, err
	}

	return &circuitbreakerrpc.ListForwardingHistoryResponse{
		Forwards: rpcHtlcs,
	}, nil
}

type inFlightTracker map[uint64]*channelUtilization

func (s *server) newInFlightTracker() (inFlightTracker, error) {
	tracker := inFlightTracker(
		make(map[uint64]*channelUtilization),
	)

	openChannels, err := s.lnd.listChannels()
	if err != nil {
		return tracker, err
	}

	// TODO - do closed channels.

	for chanID, channel := range openChannels {
		tracker[chanID] = &channelUtilization{
			availableSlots:   int(channel.maxAcceptedHtlcs),
			availableBalance: channel.maxInFlight,
		}
	}

	return tracker, nil
}

type channelUtilization struct {
	availableSlots   int
	availableBalance lnwire.MilliSatoshi

	inFlightTotal lnwire.MilliSatoshi
	inFlight      queue.PriorityQueue
}

func (c *channelUtilization) addHtlc(htlc *HtlcInfo) {
	for c.inFlight.Len() != 0 {
		// If the top item in-flight was resolved after the htlc's add timestamp the it
		// was still in-fligh at the time the new HTLC was added.
		top := c.inFlight.Top().(*HtlcInfo)
		if top.resolveTime.After(htlc.addTime) {
			break
		}

		// Otherwise, it was no longer in-flight by the time this HTLC was added so we
		// can pop it off.
		c.inFlight.Pop()
		c.inFlightTotal -= top.outgoingMsat
	}

	c.inFlight.Push(htlc)
	c.inFlightTotal += htlc.outgoingMsat
}

func (c *channelUtilization) utilization() (float32, float32) {
	return float32(c.inFlight.Len()) / float32(c.availableSlots),
		float32(c.inFlightTotal) / float32(c.availableBalance)
}

func (s *server) marshalFwdHistoryWithUsage(inFlight, htlcs []*HtlcInfo) (
	[]*circuitbreakerrpc.Forward, error) {

	// To be able to track utilization of our outgoing channels, we need to run through
	// everything that was in flight and then update our running totals per-htlc.
	outgoingUtilization, err := s.newInFlightTracker()
	if err != nil {
		return nil, err
	}

	for _, htlc := range inFlight {
		channel, ok := outgoingUtilization[htlc.outgoingCircuit.channel]
		if !ok {
			s.log.Debugf("HTLC outgoing channel: %v not found",
				htlc.outgoingCircuit.channel)
			continue
		}
		channel.addHtlc(htlc)
	}

	rpcHtlcs := make([]*circuitbreakerrpc.Forward, len(htlcs))

	for i, htlc := range htlcs {
		var slotUsage, liquidityUsage float32
		if channel, ok := outgoingUtilization[htlc.outgoingCircuit.channel]; ok {
			channel.addHtlc(htlc)
			slotUsage, liquidityUsage = channel.utilization()
		}

		forward := &circuitbreakerrpc.Forward{
			AddTimeNs:     uint64(htlc.addTime.UnixNano()),
			ResolveTimeNs: uint64(htlc.resolveTime.UnixNano()),
			Settled:       htlc.settled,
			FeeMsat: uint64(
				htlc.incomingMsat - htlc.outgoingMsat,
			),
			IncomingPeer: htlc.incomingPeer.String(),
			IncomingCircuit: &circuitbreakerrpc.CircuitKey{
				ShortChannelId: htlc.incomingCircuit.channel,
				HtlcIndex:      uint32(htlc.incomingCircuit.htlc),
			},
			OutgoingPeer: htlc.outgoingPeer.String(),
			OutgoingCircuit: &circuitbreakerrpc.CircuitKey{
				ShortChannelId: htlc.outgoingCircuit.channel,
				HtlcIndex:      uint32(htlc.outgoingCircuit.htlc),
			},
			SlotUtilization:     slotUsage,
			CapacityUtilizatoin: liquidityUsage,
		}

		rpcHtlcs[i] = forward
	}

	return rpcHtlcs, nil
}
