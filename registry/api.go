package registry

import "context"

type Registry interface {
	Ping() error
	ManifestsExists(ctx context.Context, repo string, ref string) (bool, error)
}
