// concurrency_test.go —— 并发上限，以及 goroutine 不泄漏。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// 并发上限
// ---------------------------------------------------------------------------

// concurrencyProbe 记录同时执行的任务数峰值。
type concurrencyProbe struct {
	cur, peak atomic.Int64
}

func (p *concurrencyProbe) enter() {
	n := p.cur.Add(1)
	for {
		old := p.peak.Load()
		if n <= old || p.peak.CompareAndSwap(old, n) {
			return
		}
	}
}
func (p *concurrencyProbe) leave() { p.cur.Add(-1) }

func TestMaxConcurrencyIsRespected(t *testing.T) {
	const (
		roots = 40
		limit = 4
	)
	var probe concurrencyProbe
	list := make([]*testTask, 0, roots)
	for i := range roots {
		task := newTask(fmt.Sprintf("t%02d", i))
		task.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			probe.enter()
			time.Sleep(2 * time.Millisecond)
			probe.leave()
			return nil, nil
		}
		list = append(list, task)
	}

	dag := mustNew(t, tasksOf(list...), xdag.WithMaxConcurrency(limit))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if peak := probe.peak.Load(); peak > limit {
		t.Errorf("并发峰值 %d 超过上限 %d", peak, limit)
	}
	if peak := probe.peak.Load(); peak < 2 {
		t.Errorf("并发峰值只有 %d，闸门似乎把执行串行化了", peak)
	}
}

// 不设上限时保持原有行为：宽图可以真正并发起来。
func TestUnlimitedConcurrencyByDefault(t *testing.T) {
	const roots = 30
	var probe concurrencyProbe
	release := make(chan struct{})
	var ready sync.WaitGroup
	ready.Add(roots)

	list := make([]*testTask, 0, roots)
	for i := range roots {
		task := newTask(fmt.Sprintf("t%02d", i))
		task.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			probe.enter()
			ready.Done()
			<-release // 全部到齐才放行，不限并发时必然能同时到齐
			probe.leave()
			return nil, nil
		}
		list = append(list, task)
	}

	dag := mustNew(t, tasksOf(list...))
	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()

	waited := make(chan struct{})
	go func() { ready.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(5 * time.Second):
		t.Fatal("默认应当不限并发，但任务没能同时跑起来")
	}
	close(release)
	<-done
	if peak := probe.peak.Load(); peak != roots {
		t.Errorf("并发峰值 %d, want %d", peak, roots)
	}
}

// 被挂起的任务停在挂起门上，不该占着并发名额把别的任务饿死。
func TestSuspendedTaskDoesNotHoldConcurrencySlot(t *testing.T) {
	blocked := newTask("blocked")
	other := newTask("other")
	otherDone := make(chan struct{})
	other.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(otherDone)
		return nil, nil
	}

	// 上限设成 1：如果挂起占名额，other 永远等不到机会
	dag := mustNew(t, tasksOf(blocked, other), xdag.WithMaxConcurrency(1))
	if err := dag.SuspendTask("blocked"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()

	select {
	case <-otherDone:
	case <-time.After(5 * time.Second):
		t.Fatal("挂起的任务占住了并发名额，其他任务被饿死")
	}

	if err := dag.ResumeTask("blocked"); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("解挂后 Execute 未返回")
	}
	assertState(t, dag, "blocked", xdag.StateSuccess)
	assertState(t, dag, "other", xdag.StateSuccess)
}

// 退避等待期间同样不该占名额：一个无限重试的任务不能霸占唯一的名额。
func TestBackoffDoesNotHoldConcurrencySlot(t *testing.T) {
	forever := newTask("forever")
	forever.policy = &xdag.RetryPolicy{
		MaxAttempts: xdag.InfiniteAttempts,
		Interval:    30 * time.Millisecond,
		MaxInterval: 30 * time.Millisecond,
		Multiplier:  1,
	}
	forever.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		return nil, errors.New("boom")
	}

	other := newTask("other")
	otherDone := make(chan struct{})
	other.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(otherDone)
		return nil, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	dag := mustNew(t, tasksOf(forever, other), xdag.WithMaxConcurrency(1))

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(ctx); close(done) }()

	select {
	case <-otherDone:
	case <-time.After(3 * time.Second):
		t.Fatal("无限重试的任务在退避期间霸占了并发名额")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}
}

// 被跳过的任务不占名额：上限设成 1 时，一大片被跳过的下游仍应迅速跑完。
func TestSkippedTasksDoNotConsumeSlots(t *testing.T) {
	list := []*testTask{newTask("root").fails(errors.New("boom"))}
	for i := range 30 {
		list = append(list, newTask(fmt.Sprintf("d%02d", i), "root"))
	}
	dag := mustNew(t, tasksOf(list...), xdag.WithMaxConcurrency(1))

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("被跳过的任务卡在并发闸门上")
	}
	assertState(t, dag, "root", xdag.StateFailed)
	assertState(t, dag, "d00", xdag.StateSkipped)
}

// 等待名额期间整场被取消，等待者必须立刻放弃，而不是干等到名额释放。
//
// 三个设计要点，少一个就抓不到「acquire 不响应取消」这个缺陷：
//   - 占名额的任务**无视 ctx**。它若一取消就退出并归还名额，阻塞式的
//     acquire 也能让等待者顺利跑完。
//   - 等待者用**预挂起**拦在闸门之前。全部根任务是同时派生的，不拦的话
//     瞬时完成的等待者会赶在占名额者之前轮流通过那唯一的槽位。挂起门在
//     acquire 之前，且挂起不占名额，正好用来做这个排序。
//   - 解挂之后等待者才涌向 acquire，此时名额已被牢牢占住。
func TestCancelWhileWaitingForSlot(t *testing.T) {
	const waiters = 20

	hold := make(chan struct{})
	hog := newTask("hog")
	hog.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(hold)
		time.Sleep(1500 * time.Millisecond) // 故意不看 ctx，一直占着名额
		return nil, nil
	}

	list := []*testTask{hog}
	names := make([]string, 0, waiters)
	for i := range waiters {
		n := fmt.Sprintf("w%02d", i)
		names = append(names, n)
		list = append(list, newTask(n))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dag := mustNew(t, tasksOf(list...), xdag.WithMaxConcurrency(1))

	// 预挂起：保证没有等待者能赶在 hog 之前拿到名额
	for _, n := range names {
		if err := dag.SuspendTask(n); err != nil {
			t.Fatalf("SuspendTask(%s): %v", n, err)
		}
	}

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(ctx); close(done) }()
	<-hold // hog 此刻确定持有唯一的名额

	for _, n := range names {
		if err := dag.ResumeTask(n); err != nil {
			t.Fatalf("ResumeTask(%s): %v", n, err)
		}
	}
	time.Sleep(50 * time.Millisecond) // 让等待者涌到 acquire 上
	cancel()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}

	for i, n := range names {
		if runs := list[i+1].runs(); runs != 0 {
			t.Fatalf("%s 执行了 %d 次——取消后它仍在闸门上干等到名额释放", n, runs)
		}
		assertState(t, dag, n, xdag.StateCanceled)
	}
	assertState(t, dag, "hog", xdag.StateSuccess)
}

// panic 被转成结构化的 PanicError：errors.As 能取出任务名、尝试次数、
// panic 值与调用栈，而错误文案本身保持简短。
func TestPanicErrorIsStructured(t *testing.T) {
	a := newTask("a").retries(3)
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		panic("boom")
	}
	dag := mustNew(t, tasksOf(a))
	_, err := dag.Execute(context.Background())

	if !errors.Is(err, xdag.ErrTaskPanic) {
		t.Fatalf("errors.Is(err, ErrTaskPanic) 应成立, got %v", err)
	}

	pe, ok := errors.AsType[*xdag.PanicError](err)
	if !ok {
		t.Fatalf("errors.As 取不到 *PanicError: %v", err)
	}
	if pe.Task != "a" {
		t.Errorf("ITask = %q, want %q", pe.Task, "a")
	}
	if pe.Attempt != 3 {
		t.Errorf("Attempt = %d, want 3（最后一次尝试）", pe.Attempt)
	}
	if pe.Value != "boom" {
		t.Errorf("Value = %v, want boom", pe.Value)
	}
	if len(pe.Stack) == 0 {
		t.Error("Stack 为空")
	}
	if !strings.Contains(string(pe.Stack), "goroutine") {
		t.Errorf("Stack 看起来不像调用栈: %.80s", pe.Stack)
	}

	// 关键：错误文案里不能带调用栈，否则一次 panic 就淹掉整屏日志
	if strings.Contains(pe.Error(), "goroutine") {
		t.Errorf("Error() 不应包含调用栈: %s", pe.Error())
	}
	if n := len(err.Error()); n > 300 {
		t.Errorf("聚合错误文案 %d 字节，太长了——调用栈应该只在 Stack 字段里", n)
	}
}

// Attempt 为 0 表示 panic 发生在任务体之外的路径上，文案里不带尝试次数。
func TestPanicErrorZeroAttemptWording(t *testing.T) {
	pe := &xdag.PanicError{Task: "x", Value: "boom"}
	if got, want := pe.Error(), "task x panicked: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
	if !errors.Is(pe, xdag.ErrTaskPanic) {
		t.Error("errors.Is(pe, ErrTaskPanic) 应成立")
	}

	withAttempt := &xdag.PanicError{Task: "x", Attempt: 2, Value: "boom"}
	if got, want := withAttempt.Error(), "task x panicked on attempt 2: boom"; got != want {
		t.Errorf("Error() = %q, want %q", got, want)
	}
}

// ---------------------------------------------------------------------------
// goroutine 泄漏
// ---------------------------------------------------------------------------

func goroutinesSettled(t *testing.T, base int) int {
	t.Helper()
	for range 100 {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
		if n := runtime.NumGoroutine(); n <= base {
			return n
		}
	}
	return runtime.NumGoroutine()
}

func TestNoGoroutineLeak(t *testing.T) {
	scenarios := map[string]func(t *testing.T){
		"正常完成": func(t *testing.T) {
			d := mustNew(t, tasksOf(newTask("a"), newTask("b", "a"), newTask("c", "a")))
			_, _ = d.Execute(context.Background())
		},
		"失败级联": func(t *testing.T) {
			d := mustNew(t, tasksOf(newTask("a").fails(errors.New("x")), newTask("b", "a")))
			_, _ = d.Execute(context.Background())
		},
		"任务panic": func(t *testing.T) {
			a := newTask("a")
			a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) { panic("boom") }
			d := mustNew(t, tasksOf(a, newTask("b", "a")))
			_, _ = d.Execute(context.Background())
		},
		"整场取消": func(t *testing.T) {
			a := newTask("a")
			a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			d := mustNew(t, tasksOf(a, newTask("b", "a")))
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			_, _ = d.Execute(ctx)
		},
		"Cancel排空": func(t *testing.T) {
			d := mustNew(t, tasksOf(newTask("a"), newTask("b")))
			_ = d.SuspendTask("a")
			go func() { _, _ = d.Execute(context.Background()) }()
			time.Sleep(30 * time.Millisecond)
			_ = d.Cancel(context.Background())
		},
		"无限重试被ctx掐断": func(t *testing.T) {
			a := newTask("a").fails(errors.New("x"))
			a.policy = &xdag.RetryPolicy{
				MaxAttempts: xdag.InfiniteAttempts, Interval: 5 * time.Millisecond,
				MaxInterval: 5 * time.Millisecond, Multiplier: 1,
			}
			// 给任务限时的方式就是在 ctx 上加 deadline，库里没有另一套预算。
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			d := mustNew(t, tasksOf(a))
			_, _ = d.Execute(ctx)
		},
		"观察者panic": func(t *testing.T) {
			d := mustNew(t, tasksOf(newTask("a"), newTask("b")),
				xdag.WithObserver(func(xdag.Event) { panic("obs") }))
			_, _ = d.Execute(context.Background())
		},
		"并发上限下大量排队": func(t *testing.T) {
			list := make([]*testTask, 0, 80)
			for i := range 80 {
				list = append(list, newTask(string(rune('a'+i%26))+string(rune('0'+i/26))))
			}
			d := mustNew(t, tasksOf(list...), xdag.WithMaxConcurrency(3))
			_, _ = d.Execute(context.Background())
		},
	}

	for name, run := range scenarios {
		t.Run(name, func(t *testing.T) {
			runtime.GC()
			time.Sleep(20 * time.Millisecond)
			base := runtime.NumGoroutine()
			run(t)
			if after := goroutinesSettled(t, base); after > base {
				t.Errorf("疑似泄漏 goroutine: 前 %d, 后 %d", base, after)
			}
		})
	}
}
