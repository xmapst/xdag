package xdag

import "context"

type Task interface {
	Name() string
	Dependencies() []string
	// RetryPolicy returns the retry policy for the task
	RetryPolicy() *RetryPolicy
	// PreExecution is called before Execute
	PreExecution(ctx context.Context, attempt int64, input map[string]any)
	// Execute is called after PreExecution
	Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error)
	// PostExecution is called after Execute
	PostExecution(ctx context.Context, attempt int64, output map[string]any, err error)
}
