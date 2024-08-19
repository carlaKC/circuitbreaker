package main

import "context"

type controller interface {
	process(ctx context.Context, event peerInterceptEvent,
		chainIn, chanOut *channel) error

	resolved(ctx context.Context, key peerResolvedEvent) error
}
