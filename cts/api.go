package cts

import (
	"context"

	"github.com/luojun96/isync/registry"
)

type ArtifactSync interface {
	Sync(ctx context.Context, artifacts []string) error
	Source() registry.Registry
	Destination() registry.Registry
}
