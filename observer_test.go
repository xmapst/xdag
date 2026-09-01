// observer_test.go —— 观察者回调及其 panic 兜底。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// 观察者
// ---------------------------------------------------------------------------

// 每个任务恰好触发一次事件，包括成功、失败、被跳过的。
func TestObserverFiresOncePerTask(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]xdag.Event{}
	counts := map[string]int{}

	obs := func(ev xdag.Event) {
		mu.Lock()
		defer mu.Unlock()
		seen[ev.Task] = ev
		counts[ev.Task]++
	}

	ok := newTask("ok").returns(map[string]any{"v": 1})
	bad := newTask("bad").retries(2).fails(errors.New("boom"))
	skipped := newTask("skipped", "bad")

	dag := mustNew(t, tasksOf(ok, bad, skipped), xdag.WithObserver(obs))
	_, _ = dag.Execute(context.Background())

	mu.Lock()
	defer mu.Unlock()
	if len(seen) != 3 {
		t.Fatalf("收到 %d 个任务的事件, want 3: %v", len(seen), seen)
	}
	for name, n := range counts {
		if n != 1 {
			t.Errorf("%s 触发了 %d 次事件, want 1", name, n)
		}
	}
	if ev := seen["ok"]; ev.State != xdag.StateSuccess || ev.Attempts != 1 {
		t.Errorf("ok 事件 = %+v", ev)
	}
	if ev := seen["bad"]; ev.State != xdag.StateFailed || ev.Err == nil || ev.Attempts != 2 {
		t.Errorf("bad 事件 = %+v", ev)
	}
	if ev := seen["skipped"]; ev.State != xdag.StateSkipped || ev.Attempts != 0 {
		t.Errorf("skipped 事件 = %+v", ev)
	}
}

// 事件必须在锁外触发：回调里调用查询方法不能死锁。
func TestObserverCanQueryWithoutDeadlock(t *testing.T) {
	done := make(chan struct{})
	var once sync.Once
	obs := func(ev xdag.Event) {
		once.Do(func() { close(done) })
	}

	var dag *xdag.Scheduler
	probe := func(ev xdag.Event) {
		obs(ev)
		// 这几个方法都会抢 d.mu；在锁内回调的话这里必然自锁死
		_ = dag.States()
		_ = dag.Results()
		_ = dag.Phase()
		_ = dag.State(ev.Task)
	}

	a := newTask("a")
	b := newTask("b", "a")
	dag = mustNew(t, tasksOf(a, b), xdag.WithObserver(probe))

	finished := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(finished) }()
	select {
	case <-finished:
	case <-time.After(5 * time.Second):
		t.Fatal("回调里调用查询方法导致死锁")
	}
	<-done
}

// 回调 panic 不打垮进程，转成 ErrObserverPanic 汇入 Execute 的返回值，
// 任务终态不受影响。
func TestObserverPanicIsContained(t *testing.T) {
	obs := func(ev xdag.Event) { panic("observer boom") }
	a := newTask("a").returns(map[string]any{"v": 1})
	dag := mustNew(t, tasksOf(a), xdag.WithObserver(obs))

	results, err := dag.Execute(context.Background())
	if !errors.Is(err, xdag.ErrObserverPanic) {
		t.Fatalf("want ErrObserverPanic, got %v", err)
	}
	// 任务本身照常成功
	assertState(t, dag, "a", xdag.StateSuccess)
	if results["a"]["v"] != 1 {
		t.Errorf("任务结果不该受回调 panic 影响: %v", results)
	}
}

// 不注册观察者时行为完全不变。
func TestNoObserverIsHarmless(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertState(t, dag, "b", xdag.StateSuccess)
}

// 观察者会被并发调用，-race 下验证。
func TestObserverIsCalledConcurrently(t *testing.T) {
	var n atomic.Int64
	obs := func(ev xdag.Event) { n.Add(1) }

	const roots = 40
	list := make([]*testTask, 0, roots)
	for i := range roots {
		list = append(list, newTask(fmt.Sprintf("t%02d", i)))
	}
	dag := mustNew(t, tasksOf(list...), xdag.WithObserver(obs))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if got := n.Load(); got != roots {
		t.Errorf("收到 %d 个事件, want %d", got, roots)
	}
}
