package cts

import "context"

type ArtifactSync interface {
	Sync(ctx context.Context, artifacts []string) error
}
