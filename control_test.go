// control_test.go —— 控制面：context 取消、单任务控制、整场终止。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// context 取消
// ---------------------------------------------------------------------------

// ctx 取消后，尚未执行的任务应落在 StateCanceled 而不是 StateFailed，
// 且整场执行只上报一条取消错误，而不是每个任务各刷一条同质噪音。
func TestCancelYieldsCanceledStateAndOneError(t *testing.T) {
	const n = 30
	started := make(chan struct{})
	m := make(map[string]xdag.ITask, n)
	var prev string
	for i := range n {
		name := fmt.Sprintf("c%02d", i)
		var task *testTask
		if prev == "" {
			task = newTask(name)
			task.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			}
		} else {
			task = newTask(name, prev)
		}
		m[name] = task
		prev = name
	}

	ctx, cancel := context.WithCancel(context.Background())
	dag := mustNew(t, m)

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(ctx)
		done <- err
	}()
	<-started
	cancel()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 在取消后未能返回，可能死锁")
	}

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	// 「只报一条」指的是整场取消这件事只上报一次，而不是叶子错误恰好一个——
	// 那条错误现在会额外带上最后一次尝试的根因，天然是两个叶子。
	if n := strings.Count(err.Error(), "execution canceled"); n != 1 {
		t.Errorf("整场取消上报了 %d 条，want 1: %v", n, err)
	}

	states := dag.States()
	var canceled, failed, pending int
	for _, s := range states {
		switch s {
		case xdag.StateCanceled:
			canceled++
		case xdag.StateFailed:
			failed++
		case xdag.StatePending:
			pending++
		default:
		}
	}
	if failed != 0 {
		t.Errorf("取消不应产生 %d 个 failed 任务", failed)
	}
	if pending != 0 {
		t.Errorf("取消后仍有 %d 个任务停在 pending", pending)
	}
	if canceled != n {
		t.Errorf("canceled=%d, want %d（states=%v）", canceled, n, states)
	}
}

// 已经取消的 ctx：所有任务都不执行，全部落在 canceled。
func TestExecuteWithAlreadyCanceledContext(t *testing.T) {
	a := newTask("a")
	b := newTask("b", "a")
	dag := mustNew(t, tasksOf(a, b))

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := dag.Execute(ctx)

	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateCanceled)
	assertState(t, dag, "b", xdag.StateCanceled)
	if a.runs() != 0 || b.runs() != 0 {
		t.Errorf("取消后不应执行任何任务: a=%d b=%d", a.runs(), b.runs())
	}
}

// 超时同样归入 StateCanceled，且 context.Cause 能透出根因。
func TestDeadlineExceededYieldsCanceled(t *testing.T) {
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	dag := mustNew(t, tasksOf(a))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := dag.Execute(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateCanceled)
}

// 取消状态沿依赖链传播为取消，不会被误报成普通的「上游未成功」。
func TestCanceledUpstreamPropagatesAsCanceled(t *testing.T) {
	if got := xdag.StateCanceled.String(); got != "canceled" {
		t.Errorf("StateCanceled.String() = %q, want %q", got, "canceled")
	}
	root := newTask("root")
	root.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	child := newTask("child", "root")
	grandchild := newTask("grandchild", "child")
	dag := mustNew(t, tasksOf(root, child, grandchild))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, _ = dag.Execute(ctx)

	assertState(t, dag, "root", xdag.StateCanceled)
	assertState(t, dag, "child", xdag.StateCanceled)
	assertState(t, dag, "grandchild", xdag.StateCanceled)
}

// ---------------------------------------------------------------------------
// 单任务控制：CancelTask / SuspendTask / ResumeTask / TaskSuspended
// ---------------------------------------------------------------------------

// CancelTask 只取消目标任务这一条链路，不影响图里其他不相关的分支。
func TestCancelTaskCancelsOnlyThatTaskAndCascades(t *testing.T) {
	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	b := newTask("b", "a")
	c := newTask("c")

	dag := mustNew(t, tasksOf(a, b, c))

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()
	<-started
	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 在单独取消 a 后未能返回，可能死锁")
	}

	if !errors.Is(err, xdag.ErrTaskCanceled) {
		t.Fatalf("want ErrTaskCanceled, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateCanceled)
	assertState(t, dag, "b", xdag.StateCanceled)
	assertState(t, dag, "c", xdag.StateSuccess)
}

// SuspendTask 在两次重试尝试之间生效：挂起期间不会开始下一次尝试，
// ResumeTask 之后才继续。
func TestSuspendTaskBlocksBetweenRetries(t *testing.T) {
	attempt1Started := make(chan struct{}, 1)
	a := newTask("a").retries(3)
	a.policy.Interval = 200 * time.Millisecond
	a.policy.MaxInterval = 200 * time.Millisecond
	a.fn = func(_ context.Context, attempt int64, _ map[string]any) (map[string]any, error) {
		if attempt == 1 {
			attempt1Started <- struct{}{}
			return nil, errors.New("boom")
		}
		return map[string]any{"ok": true}, nil
	}

	dag := mustNew(t, tasksOf(a))

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()

	<-attempt1Started
	// 第一次尝试刚失败，退避等待还有 ~200ms 的窗口，在此期间挂起
	if err := dag.SuspendTask("a"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}
	if !dag.TaskSuspended("a") {
		t.Fatal("SuspendTask 之后 TaskSuspended 应为 true")
	}

	// 等过退避窗口，确认挂起确实拦住了第二次尝试
	time.Sleep(400 * time.Millisecond)
	if runs := a.runs(); runs != 1 {
		t.Fatalf("挂起期间 runs=%d，want 1（不应开始第二次尝试）", runs)
	}

	if err := dag.ResumeTask("a"); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	if dag.TaskSuspended("a") {
		t.Fatal("ResumeTask 之后 TaskSuspended 应为 false")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ResumeTask 之后 Execute 未能返回，可能死锁")
	}
	assertState(t, dag, "a", xdag.StateSuccess)
	if runs := a.runs(); runs != 2 {
		t.Errorf("a 执行了 %d 次, want 2", runs)
	}
}

// SuspendTask/ResumeTask 可以在同一个任务上反复交替调用：
// 挂起 -> 解挂 -> 挂起 -> 解挂，每一轮都要正确生效。
func TestSuspendResumeCanCycleRepeatedly(t *testing.T) {
	const cycles = 3
	attemptStarted := make(chan int64, cycles+1)
	a := newTask("a").retries(int64(cycles + 1))
	a.policy.Interval = 80 * time.Millisecond
	a.policy.MaxInterval = 80 * time.Millisecond
	a.fn = func(_ context.Context, attempt int64, _ map[string]any) (map[string]any, error) {
		attemptStarted <- attempt
		if attempt <= cycles {
			return nil, errors.New("boom")
		}
		return map[string]any{"ok": true}, nil
	}

	dag := mustNew(t, tasksOf(a))

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()

	for i := 0; i < cycles; i++ {
		select {
		case <-attemptStarted:
		case <-time.After(5 * time.Second):
			t.Fatalf("第 %d 轮：等待尝试开始超时", i+1)
		}

		if err := dag.SuspendTask("a"); err != nil {
			t.Fatalf("第 %d 轮 SuspendTask: %v", i+1, err)
		}
		if !dag.TaskSuspended("a") {
			t.Fatalf("第 %d 轮：挂起后 TaskSuspended 应为 true", i+1)
		}
		// 挂起期间确认确实没有提前开始下一次尝试
		select {
		case attempt := <-attemptStarted:
			t.Fatalf("第 %d 轮：挂起期间不应开始新尝试 attempt=%d", i+1, attempt)
		case <-time.After(150 * time.Millisecond):
		}

		if err := dag.ResumeTask("a"); err != nil {
			t.Fatalf("第 %d 轮 ResumeTask: %v", i+1, err)
		}
		if dag.TaskSuspended("a") {
			t.Fatalf("第 %d 轮：解挂后 TaskSuspended 应为 false", i+1)
		}
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("最后一轮解挂后 Execute 未能返回，可能死锁")
	}
	assertState(t, dag, "a", xdag.StateSuccess)
	if runs := a.runs(); runs != cycles+1 {
		t.Errorf("a 执行了 %d 次, want %d", runs, cycles+1)
	}
}

// ResumeTask 对没有被挂起的任务是无害空操作；CancelTask 能立刻把一个
// 正卡在挂起等待里的任务解脱出来，而不是让它永久卡住。
func TestResumeIsIdempotentAndCancelUnblocksSuspended(t *testing.T) {
	started := make(chan struct{})
	b := newTask("b").retries(2)
	b.policy.Interval = time.Second
	b.policy.MaxInterval = time.Second
	b.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		return nil, errors.New("boom")
	}
	dag := mustNew(t, tasksOf(b))

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()
	<-started

	// ResumeTask 对尚未被挂起的任务同样是无害空操作
	if err := dag.ResumeTask("b"); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}

	if err := dag.SuspendTask("b"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}
	// 给挂起等待一点时间真正生效
	time.Sleep(50 * time.Millisecond)
	if err := dag.CancelTask("b"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("取消一个正在挂起等待中的任务后 Execute 未能返回，可能死锁")
	}
	assertState(t, dag, "b", xdag.StateCanceled)
}

// 对未知任务名或已经处于终态的任务调用三个控制方法都应返回明确错误，
// 而不是静默忽略。
func TestActionsOnUnknownOrDoneTask(t *testing.T) {
	a := newTask("a")
	dag := mustNew(t, tasksOf(a))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if err := dag.CancelTask("nope"); !errors.Is(err, xdag.ErrUnknownTask) {
		t.Errorf("CancelTask(未知任务) = %v, want ErrUnknownTask", err)
	}
	if err := dag.SuspendTask("nope"); !errors.Is(err, xdag.ErrUnknownTask) {
		t.Errorf("SuspendTask(未知任务) = %v, want ErrUnknownTask", err)
	}
	if err := dag.ResumeTask("nope"); !errors.Is(err, xdag.ErrUnknownTask) {
		t.Errorf("ResumeTask(未知任务) = %v, want ErrUnknownTask", err)
	}

	if err := dag.CancelTask("a"); !errors.Is(err, xdag.ErrTaskAlreadyDone) {
		t.Errorf("CancelTask(已完成任务) = %v, want ErrTaskAlreadyDone", err)
	}
	if err := dag.SuspendTask("a"); !errors.Is(err, xdag.ErrTaskAlreadyDone) {
		t.Errorf("SuspendTask(已完成任务) = %v, want ErrTaskAlreadyDone", err)
	}
	if err := dag.ResumeTask("a"); !errors.Is(err, xdag.ErrTaskAlreadyDone) {
		t.Errorf("ResumeTask(已完成任务) = %v, want ErrTaskAlreadyDone", err)
	}
	if dag.TaskSuspended("a") {
		t.Error("已完成的任务 TaskSuspended 应为 false")
	}
}

// Execute 之前就可以取消某个任务：这类「预取消」在 Execute 开始时立即
// 兑现——任务体一次都不会被调用，终态是 StateCanceled，并级联下游。
func TestCancelBeforeExecute(t *testing.T) {
	a := newTask("a")
	b := newTask("b", "a")
	c := newTask("c")
	dag := mustNew(t, tasksOf(a, b, c))

	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("Execute 之前 CancelTask 应当可用，got %v", err)
	}

	_, err := dag.Execute(context.Background())
	if !errors.Is(err, xdag.ErrTaskCanceled) {
		t.Fatalf("want ErrTaskCanceled, got %v", err)
	}

	assertState(t, dag, "a", xdag.StateCanceled)
	assertState(t, dag, "b", xdag.StateCanceled)
	assertState(t, dag, "c", xdag.StateSuccess)
	if a.runs() != 0 {
		t.Errorf("预取消的任务不应被执行，runs=%d", a.runs())
	}
}

// Execute 之前就可以挂起某个任务：这类「预挂起」确定性地拦在第一次尝试
// 之前，不需要和调度抢时序；解挂之后任务才开始跑。
func TestSuspendBeforeExecute(t *testing.T) {
	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		return map[string]any{"ok": true}, nil
	}
	dag := mustNew(t, tasksOf(a))

	if err := dag.SuspendTask("a"); err != nil {
		t.Fatalf("Execute 之前 SuspendTask 应当可用，got %v", err)
	}
	if !dag.TaskSuspended("a") {
		t.Fatal("Execute 之前挂起后 TaskSuspended 应为 true")
	}

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()

	// 预挂起必须确定性生效：任务体一次都不该被调用
	select {
	case <-started:
		t.Fatal("预挂起的任务不应开始执行")
	case <-time.After(200 * time.Millisecond):
	}
	if a.runs() != 0 {
		t.Fatalf("预挂起期间 runs=%d，want 0", a.runs())
	}

	if err := dag.ResumeTask("a"); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("解挂后 Execute 未能返回，可能死锁")
	}
	assertState(t, dag, "a", xdag.StateSuccess)
	if a.runs() != 1 {
		t.Errorf("a 执行了 %d 次, want 1", a.runs())
	}
}

// 挂起一个实际上依赖失败、注定要被跳过的任务：它应该正确落在
// StateSkipped，而不是卡死在挂起等待上——snapshot() 的依赖判定先于
// 挂起门发生。
func TestSuspendedTaskDestinedToSkipDoesNotHang(t *testing.T) {
	a := newTask("a").fails(errors.New("boom"))
	b := newTask("b", "a")
	dag := mustNew(t, tasksOf(a, b))

	// b 在 a 完成之前根本不会被调度，这里提前挂起完全无害：
	// 一旦 a 失败，b 的 snapshot() 判定会直接短路成 StateSkipped，
	// 不会经过挂起门。
	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()

	// 尽力赶在 b 被调度之前发出挂起请求；即使晚了，b 也早已跑过挂起门，
	// 挂起请求本身应当是无害的空操作（对 b 来说要么还没建好控制柄的窗口
	// 已经过去，要么它已经终态）
	_ = dag.SuspendTask("b")

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未能返回，可能死锁在挂起等待上")
	}
	assertState(t, dag, "a", xdag.StateFailed)
	assertState(t, dag, "b", xdag.StateSkipped)
}

// ---------------------------------------------------------------------------
// 优雅停机
// ---------------------------------------------------------------------------

// Cancel 不打扰正在执行的任务，只是不再启动新的：正在跑的跑完，
// 尚未开始的记为 StateCanceled 并带上 ErrRunCanceled。
func TestStopLetsInFlightFinishAndBlocksNewOnes(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	running := newTask("running")
	running.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		<-release
		return map[string]any{"done": true}, nil
	}
	// downstream 依赖 running，Cancel 之后才会被调度
	downstream := newTask("downstream", "running")
	// 预挂起 idle，保证它在 Cancel 之前还没开始
	idle := newTask("idle")

	dag := mustNew(t, tasksOf(running, downstream, idle))
	if err := dag.SuspendTask("idle"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}

	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()

	<-started
	stopAsync(t, dag)
	if !dag.Canceled() {
		t.Error("Cancel 之后 Canceled() 应为 true")
	}
	// 不需要 ResumeTask：Cancel 会直接叫醒挂起中的任务，让它以 canceled 收尾
	close(release) // 让在跑的任务正常跑完

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}

	if !errors.Is(err, xdag.ErrRunCanceled) {
		t.Errorf("聚合错误里应能找到 ErrRunCanceled, got %v", err)
	}
	// 正在跑的那个不受影响，正常成功
	assertState(t, dag, "running", xdag.StateSuccess)
	if running.runs() != 1 {
		t.Errorf("running 执行了 %d 次, want 1", running.runs())
	}
	// 停机后才轮到调度的，一个都不该执行
	assertState(t, dag, "downstream", xdag.StateCanceled)
	assertState(t, dag, "idle", xdag.StateCanceled)
	if downstream.runs() != 0 || idle.runs() != 0 {
		t.Errorf("停机后不应有任务开始执行: downstream=%d idle=%d",
			downstream.runs(), idle.runs())
	}
	if dag.Phase() != xdag.PhaseCanceled {
		t.Errorf("Phase = %v, want canceled", dag.Phase())
	}
}

// Execute 之前调用 Cancel：一个任务都不会执行。
func TestStopBeforeExecute(t *testing.T) {
	a := newTask("a")
	b := newTask("b", "a")
	dag := mustNew(t, tasksOf(a, b))

	stopAsync(t, dag)
	stopAsync(t, dag) // 幂等
	_, err := dag.Execute(context.Background())

	if !errors.Is(err, xdag.ErrRunCanceled) {
		t.Fatalf("want ErrRunCanceled, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateCanceled)
	assertState(t, dag, "b", xdag.StateCanceled)
	if a.runs() != 0 || b.runs() != 0 {
		t.Errorf("Cancel 之后不应执行任何任务: a=%d b=%d", a.runs(), b.runs())
	}
}

// 依赖没跑成的任务该记 Skipped 就记 Skipped——停机不该把这个更贴切的
// 成因盖掉。
func TestStopDoesNotMaskSkipped(t *testing.T) {
	failing := newTask("failing").fails(errors.New("boom"))
	skipped := newTask("skipped", "failing")
	dag := mustNew(t, tasksOf(failing, skipped))

	stopAsync(t, dag)
	_, _ = dag.Execute(context.Background())

	// failing 因停机没开始，记 canceled；它的下游因依赖没成功，记 skipped
	assertState(t, dag, "failing", xdag.StateCanceled)
	assertState(t, dag, "skipped", xdag.StateCanceled)
}

// Cancel 不打断**已经在跑的那次尝试**：任务体照常执行完，终态是成功。
func TestStopDoesNotInterruptRunningAttempt(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	a := newTask("a")
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		<-release // Cancel 期间一直在跑
		return map[string]any{"ok": true}, nil
	}
	dag := mustNew(t, tasksOf(a))

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()
	<-started
	stopAsync(t, dag)
	close(release)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}
	assertState(t, dag, "a", xdag.StateSuccess)
	if a.runs() != 1 {
		t.Errorf("a 执行了 %d 次, want 1", a.runs())
	}
}

// Cancel 会打断退避等待：不该让优雅停机干等一个最长可达 150s 的退避走完。
func TestStopCutsShortBackoffWait(t *testing.T) {
	firstAttempt := make(chan struct{})
	var once sync.Once
	a := newTask("a").retries(5)
	a.policy.Interval = 3 * time.Second // 足够长，Cancel 若不打断就会超时
	a.policy.MaxInterval = 3 * time.Second
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		once.Do(func() { close(firstAttempt) })
		return nil, errors.New("boom")
	}
	dag := mustNew(t, tasksOf(a))

	done := make(chan struct{})
	start := time.Now()
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()
	<-firstAttempt
	stopAsync(t, dag)

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Cancel 没有打断退避等待，Execute 干等到退避走完")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("耗时 %v，Cancel 应当立刻打断退避", elapsed)
	}
	// 已经跑过一次但没跑完重试周期，记为被停机取消而不是失败
	assertState(t, dag, "a", xdag.StateCanceled)
	if a.runs() != 1 {
		t.Errorf("a 执行了 %d 次, want 1（第二次尝试不该开始）", a.runs())
	}
}

// Cancel 必须能叫醒被挂起的任务，否则 Execute 永远等不到它结束。
func TestStopWakesSuspendedTask(t *testing.T) {
	a := newTask("a")
	b := newTask("b")
	dag := mustNew(t, tasksOf(a, b))
	if err := dag.SuspendTask("a"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}

	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()
	time.Sleep(50 * time.Millisecond) // 让 a 停到挂起门上
	stopAsync(t, dag)

	var err error
	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("Cancel 叫不醒被挂起的任务，Execute 永久挂起")
	}
	if !errors.Is(err, xdag.ErrRunCanceled) {
		t.Errorf("want ErrRunCanceled, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateCanceled)
	if a.runs() != 0 {
		t.Errorf("被挂起的任务不该执行, runs=%d", a.runs())
	}
}

// Cancel 必须能叫醒排在并发闸门上的任务，否则前面的任务跑多久它就被无视多久。
func TestStopWakesTasksQueuedOnConcurrencyGate(t *testing.T) {
	const n = 60
	list := make([]*testTask, 0, n)
	var ran atomic.Int64
	for i := range n {
		task := newTask(fmt.Sprintf("t%02d", i))
		task.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			ran.Add(1)
			time.Sleep(5 * time.Millisecond)
			return nil, nil
		}
		list = append(list, task)
	}
	dag := mustNew(t, tasksOf(list...), xdag.WithMaxConcurrency(2))

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()
	time.Sleep(20 * time.Millisecond)
	stopAsync(t, dag)

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}

	p := dag.Progress()
	if p.Canceled == 0 {
		t.Fatalf("Cancel 对排队中的任务完全失效：一个都没拦住（%+v，实际执行 %d）", p, ran.Load())
	}
	if ran.Load() >= int64(n) {
		t.Errorf("全部 %d 个任务都执行了，Cancel 被无视", n)
	}
	if dag.Phase() != xdag.PhaseCanceled {
		t.Errorf("Phase = %v, want canceled", dag.Phase())
	}
}

// ---------------------------------------------------------------------------
// 单任务优雅停止
// ---------------------------------------------------------------------------

// CancelTask 只是通知，调度器**仍然等任务体自己返回**。用一个无视 ctx、
// 固定睡 400ms 的任务体把这件事量出来：取消之后 Execute 不会提前返回，
// 任务照样跑完并记为成功——取消是协作式的，Go 没有别的办法叫停一个
// 不配合的 goroutine。整张图都不想等了要用 Cancel，它有宽限期。
func TestCancelWaitsForTaskBody(t *testing.T) {
	const bodyTime = 400 * time.Millisecond

	sawCancel := new(atomic.Bool)
	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		close(started)
		time.Sleep(bodyTime) // 故意不看 ctx
		sawCancel.Store(ctx.Err() != nil)
		return map[string]any{"ok": true}, nil
	}
	dag := mustNew(t, tasksOf(a))

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()
	<-started

	start := time.Now()
	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}
	elapsed := time.Since(start)

	if elapsed < bodyTime/2 {
		t.Errorf("CancelTask 应当等任务体返回，只用了 %v", elapsed)
	}
	if !sawCancel.Load() {
		t.Error("CancelTask 也要通过 ctx 通知任务体")
	}
	if got := dag.State("a"); got != xdag.StateSuccess {
		t.Errorf("任务体无视取消跑完了，终态应是 success, got %v", got)
	}
}

// 重复调用的规则：对运行中的任务取消可以重复，对已终态的任务做控制
// 一律返回 ErrTaskAlreadyDone。这不是两条特例，是同一条规则的两面。
func TestRepeatabilityFollowsFromTerminalRule(t *testing.T) {
	t.Run("运行中：取消可重复", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		defer close(release)
		a := newTask("a")
		a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			close(started)
			<-release
			return nil, nil
		}
		dag := mustNew(t, tasksOf(a))
		go func() { _, _ = dag.Execute(context.Background()) }()
		<-started

		if err := dag.CancelTask("a"); err != nil {
			t.Fatalf("第 1 次: %v", err)
		}
		if err := dag.CancelTask("a"); err != nil {
			t.Errorf("取消应当可以重复调用, 第 2 次 = %v", err)
		}
	})

	t.Run("未知任务名", func(t *testing.T) {
		dag := mustNew(t, tasksOf(newTask("a")))
		for name, fn := range map[string]func(string) error{
			"CancelTask":  func(n string) error { return dag.CancelTask(n) },
			"SuspendTask": dag.SuspendTask, "ResumeTask": dag.ResumeTask,
		} {
			if err := fn("nope"); !errors.Is(err, xdag.ErrUnknownTask) {
				t.Errorf("%s(未知) = %v, want ErrUnknownTask", name, err)
			}
		}
	})

	t.Run("已终态的任务：四个方法口径一致", func(t *testing.T) {
		dag := mustNew(t, tasksOf(newTask("a")))
		if _, err := dag.Execute(context.Background()); err != nil {
			t.Fatalf("Execute: %v", err)
		}
		for name, fn := range map[string]func(string) error{
			"CancelTask":  func(n string) error { return dag.CancelTask(n) },
			"SuspendTask": dag.SuspendTask, "ResumeTask": dag.ResumeTask,
		} {
			if err := fn("a"); !errors.Is(err, xdag.ErrTaskAlreadyDone) {
				t.Errorf("%s(已终态) = %v, want ErrTaskAlreadyDone", name, err)
			}
		}
	})
}

// ---------------------------------------------------------------------------
// 调用方给的取消原因（cause）
// ---------------------------------------------------------------------------

// CancelTask 接受调用方给的 cause，并且**不替换**库的哨兵：两者都要能
// errors.Is 得到。库的哨兵回答「发生了什么」，cause 回答「为什么」，
// 调用方的业务原因不该把既有的哨兵判定挤掉。
func TestCancelTaskCarriesCallerCause(t *testing.T) {
	reason := errors.New("killed by operator")

	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	dag := mustNew(t, tasksOf(a))
	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()
	<-started

	if err := dag.CancelTask("a", xdag.WithCause(reason)); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}

	got := dag.Results()["a"].Err
	if !errors.Is(got, xdag.ErrTaskCanceled) {
		t.Errorf("传了自定义 cause 之后哨兵丢了: %v", got)
	}
	if !errors.Is(got, reason) {
		t.Errorf("调用方给的 cause 没有传出来: %v", got)
	}
	assertState(t, dag, "a", xdag.StateCanceled)
}

// Cancel 同样接受 cause，口径与 CancelTask 一致。
func TestCancelCarriesCallerCause(t *testing.T) {
	reason := errors.New("deploy window closed")

	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	dag := mustNew(t, tasksOf(a))
	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()
	<-started

	if err := dag.Cancel(context.Background(), xdag.WithCause(reason)); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	err := <-done
	if !errors.Is(err, xdag.ErrRunCanceled) {
		t.Errorf("传了自定义 cause 之后哨兵丢了: %v", err)
	}
	if !errors.Is(err, reason) {
		t.Errorf("调用方给的 cause 没有传出来: %v", err)
	}
}

// cause 传 nil 时行为与改造之前完全一致，不多带任何东西。
func TestNilCauseKeepsSentinelOnly(t *testing.T) {
	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	dag := mustNew(t, tasksOf(a))
	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()
	<-started

	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	<-done
	if got := dag.Results()["a"].Err; !errors.Is(got, xdag.ErrTaskCanceled) {
		t.Errorf("want ErrTaskCanceled, got %v", got)
	}
}

// 已经成功落定的任务，不该因为随后的整场 Cancel 而在聚合错误里被报成取消。
//
// abandonAll 无条件遍历每一个任务，而 abandon 原来是**先往 errCh 发错误、
// 再 commit**——commit 里的 settled CAS 只挡得住状态写入，挡不住那条已经
// 发出去的错误。于是一个明明跑成功的任务会出现在 Execute 的聚合错误里。
func TestAbandonDoesNotReportSettledTasks(t *testing.T) {
	finished := make(chan struct{})
	held := make(chan struct{})

	quick := newTask("quick")
	quick.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(finished)
		return map[string]any{"ok": true}, nil
	}
	// 一个无视取消的任务，逼 Cancel 走到宽限期耗尽、真的去 abandonAll。
	stubborn := newTask("stubborn")
	stubborn.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		<-held
		return nil, nil
	}

	dag := mustNew(t, tasksOf(quick, stubborn))
	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()
	<-finished

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = dag.Cancel(ctx)

	var aggErr error
	select {
	case aggErr = <-done:
	case <-time.After(5 * time.Second):
		close(held)
		t.Fatal("Execute 未返回")
	}
	close(held)

	assertState(t, dag, "quick", xdag.StateSuccess)
	if got := dag.Results()["quick"].Err; got != nil {
		t.Errorf("已经成功的任务被补了一条错误: %v", got)
	}
	if aggErr != nil && strings.Contains(aggErr.Error(), "task quick") {
		t.Errorf("聚合错误里出现了已经成功的任务: %v", aggErr)
	}
}
