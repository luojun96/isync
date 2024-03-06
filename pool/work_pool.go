package pool

import (
	"context"
	"log"

	"golang.org/x/sync/semaphore"
)

type Pool interface {
	Run(ctx context.Context)
}

type WorkPool struct {
	size  int64
	sem   *semaphore.Weighted
	tasks []Task
}

func NewWorkPool(maxWorkers int) *WorkPool {
	return &WorkPool{
		size:  int64(maxWorkers),
		sem:   semaphore.NewWeighted(int64(maxWorkers)),
		tasks: make([]Task, 0),
	}
}

func (p *WorkPool) AddTask(task Task) {
	p.tasks = append(p.tasks, task)
}

func (p *WorkPool) Run(ctx context.Context) {
	for _, task := range p.tasks {
		if err := p.sem.Acquire(ctx, 1); err != nil {
			log.Printf("failed to acquire semaphore: %v", err)
			break
		}

		go func(task Task) {
			defer p.sem.Release(1)
			task.Execute(ctx)
		}(task)
	}

	if err := p.sem.Acquire(ctx, p.size); err != nil {
		log.Printf("failed to acquire semaphore: %v", err)
	}
}
