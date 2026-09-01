// helpers_test.go —— 全部 xdag 测试共用的脚手架：一个可编程的 ITask 实现，外加构图与断言的小工具。
//
// 测试按主题分散在同目录的其他 _test.go 里。

package xdag_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// testTask 是一个可配置的 ITask 实现。
type testTask struct {
	budget time.Duration
	name   string
	deps   []string
	policy *xdag.RetryPolicy
	fn     func(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error)

	mu       sync.Mutex
	runCount int
}

func (t *testTask) Name() string           { return t.name }
func (t *testTask) Dependencies() []string { return t.deps }

func (t *testTask) RetryPolicy() *xdag.RetryPolicy {
	if t.policy != nil {
		return t.policy
	}
	return &xdag.RetryPolicy{MaxAttempts: 1}
}
func (t *testTask) PreExecution(context.Context, int64, map[string]any)         {}
func (t *testTask) PostExecution(context.Context, int64, map[string]any, error) {}

func (t *testTask) Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error) {
	t.mu.Lock()
	t.runCount++
	t.mu.Unlock()
	if t.fn != nil {
		return t.fn(ctx, attempt, input)
	}
	return map[string]any{"task": t.name}, nil
}

func (t *testTask) runs() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runCount
}

func newTask(name string, deps ...string) *testTask {
	return &testTask{name: name, deps: deps}
}

// retries 配置一个快速重试策略：固定间隔 1ms，避免拖慢测试。
func (t *testTask) retries(maxAttempts int64) *testTask {
	t.policy = &xdag.RetryPolicy{
		Interval:    time.Millisecond,
		MaxInterval: time.Millisecond,
		MaxAttempts: maxAttempts,
		Multiplier:  1,
	}
	return t
}

func (t *testTask) returns(out map[string]any) *testTask {
	t.fn = func(context.Context, int64, map[string]any) (map[string]any, error) { return out, nil }
	return t
}

func (t *testTask) fails(err error) *testTask {
	t.fn = func(context.Context, int64, map[string]any) (map[string]any, error) { return nil, err }
	return t
}

func tasksOf(list ...*testTask) map[string]xdag.ITask {
	m := make(map[string]xdag.ITask, len(list))
	for _, task := range list {
		m[task.name] = task
	}
	return m
}

func assertState(t *testing.T, dag *xdag.Scheduler, name string, want xdag.State) {
	t.Helper()
	if got := dag.State(name); got != want {
		t.Errorf("state of %q = %v, want %v", name, got, want)
	}
}

func mustNew(t *testing.T, tasks map[string]xdag.ITask, opts ...xdag.Option) *xdag.Scheduler {
	t.Helper()
	dag, err := xdag.New(tasks, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return dag
}

// stopAsync 发起停机但不等排空——多数用例还要继续操作 dag，
// 同步等待会和它们自己的收尾逻辑互相阻塞。
func stopAsync(t *testing.T, dag *xdag.Scheduler) {
	t.Helper()
	go func() { _ = dag.Cancel(context.Background()) }()
	// 等取消真的被记下之后再返回，避免用例里的后续断言抢在前面
	for range 200 {
		if dag.Canceled() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("Cancel 未能在 200ms 内置位")
}

// countMatches 遍历错误树，统计有多少个分支能匹配 target。
// 与 countLeafErrors 的区别：它按语义计数，不受 fmt.Errorf 多 %w 包裹影响。
func countMatches(err, target error) int {
	if err == nil {
		return 0
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var n int
		for _, e := range joined.Unwrap() {
			n += countMatches(e, target)
		}
		return n
	}
	if errors.Is(err, target) {
		return 1
	}
	return 0
}

// countLeafErrors 展开 errors.Join 的树，统计叶子错误个数。
func countLeafErrors(err error) int {
	if err == nil {
		return 0
	}
	if joined, ok := err.(interface{ Unwrap() []error }); ok {
		var n int
		for _, e := range joined.Unwrap() {
			n += countLeafErrors(e)
		}
		return n
	}
	return 1
}
