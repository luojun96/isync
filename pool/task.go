package pool

import "context"

type Task interface {
	Execute(ctx context.Context)
}
