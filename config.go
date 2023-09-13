package main

type Mode int

const (
	ModeFail Mode = iota
	ModeQueue
	ModeQueuePeerInitiated
	ModeBlock
	ModeLRCActive
        ModeLRCLogging
)

func (m Mode) String() string {
	switch m {
	case ModeFail:
		return "FAIL"

	case ModeQueue:
		return "QUEUE"

	case ModeQueuePeerInitiated:
		return "QUEUE_PEER_INITIATED"

	case ModeBlock:
		return "BLOCK"

	case ModeLRCActive:
		return "LRC_ACTIVE"

	case ModeLRCLogging:
                return "LRC_LOGGING"


	default:
		panic("unknown mode")
	}
}
