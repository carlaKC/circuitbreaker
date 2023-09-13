package main

import "context"

type controller interface {
	process(ctx context.Context, event peerInterceptEvent, chanOut *channel) error
	resolved(ctx context.Context, key peerResolvedEvent) error
}
