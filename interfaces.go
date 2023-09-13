package main

import "context"

type controller interface {
	process(ctx context.Context, event peerInterceptEvent) error
	resolved(ctx context.Context, key resolvedEvent) error
}
