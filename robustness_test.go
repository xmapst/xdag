// robustness_test.go —— 健壮性：panic 兜底、input 隔离、Goexit、以及历次评审补漏的回归。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// panic 兜底
// ---------------------------------------------------------------------------

// 任务体 panic 必须降级为该任务失败，而不是带走整个进程；
// 下游照常推进，其它任务不受影响。
func TestTaskPanicBecomesFailure(t *testing.T) {
	boom := newTask("boom")
	boom.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		panic("任务体炸了")
	}
	child := newTask("child", "boom")
	sibling := newTask("sibling")

	dag := mustNew(t, tasksOf(boom, child, sibling))
	_, err := dag.Execute(context.Background())

	if !errors.Is(err, xdag.ErrTaskPanic) {
		t.Fatalf("want ErrTaskPanic, got %v", err)
	}
	if !strings.Contains(err.Error(), "任务体炸了") {
		t.Errorf("error should carry the panic value, got %v", err)
	}
	assertState(t, dag, "boom", xdag.StateFailed)
	assertState(t, dag, "child", xdag.StateSkipped)
	assertState(t, dag, "sibling", xdag.StateSuccess)
}

// PreExecution / PostExecution 里的 panic 同样要被接住。
func TestCallbackPanicBecomesFailure(t *testing.T) {
	a := &panicCallbackTask{testTask: newTask("a")}
	dag := mustNew(t, map[string]xdag.ITask{"a": a})

	_, err := dag.Execute(context.Background())
	if !errors.Is(err, xdag.ErrTaskPanic) {
		t.Fatalf("want ErrTaskPanic, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateFailed)
}

type panicCallbackTask struct{ *testTask }

func (t *panicCallbackTask) PreExecution(context.Context, int64, map[string]any) {
	panic("回调炸了")
}

// panic 后仍受重试策略约束：第一次 panic，第二次成功，最终应为成功。
func TestPanicIsRetriedLikeAnyOtherFailure(t *testing.T) {
	a := newTask("a").retries(3)
	var calls atomic.Int64
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		n := calls.Add(1)
		if n == 1 {
			panic("第一次炸")
		}
		return map[string]any{"ok": true}, nil
	}

	dag := mustNew(t, tasksOf(a))
	_, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertState(t, dag, "a", xdag.StateSuccess)
	if a.runs() != 2 {
		t.Errorf("a executed %d times, want 2", a.runs())
	}
}

// ---------------------------------------------------------------------------
// input 隔离 / Goexit 兜底 / 不可重试错误 / Results / Phase / Jitter
// ---------------------------------------------------------------------------

// 每个下游拿到的 input 必须是自己的一份拷贝：否则任一下游往里写一个 key，
// 就会污染上游的输出、Execute 的结果集，以及其他下游看到的内容。
func TestInputIsIsolatedPerDownstream(t *testing.T) {
	a := newTask("a").returns(map[string]any{"n": 1})

	var bSaw, cSaw int
	b := newTask("b", "a")
	b.fn = func(_ context.Context, _ int64, in map[string]any) (map[string]any, error) {
		m := in["a"].(map[string]any)
		m["written-by-b"] = true
		bSaw = len(m)
		return nil, nil
	}
	c := newTask("c", "a", "b") // 依赖 b，保证在 b 之后运行，从而能观察到污染
	c.fn = func(_ context.Context, _ int64, in map[string]any) (map[string]any, error) {
		cSaw = len(in["a"].(map[string]any))
		return nil, nil
	}

	dag := mustNew(t, tasksOf(a, b, c))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if bSaw != 2 {
		t.Errorf("b 自己写完后应看到 2 个 key, got %d", bSaw)
	}
	if cSaw != 1 {
		t.Errorf("c 不应看到 b 的写入，want 1 个 key, got %d", cSaw)
	}
	if got := len(results["a"]); got != 1 {
		t.Errorf("a 的输出被下游污染了：want 1 个 key, got %d (%v)", got, results["a"])
	}
}

// 任务里 runtime.Goexit（例如在任务里调用 t.Fatal）不是 panic，recover 拦不住。
// 它必须被识别成失败并上报，而不是让整棵下游子树被静默丢弃。
func TestGoexitIsReportedAndCascades(t *testing.T) {
	a := newTask("a")
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		runtime.Goexit()
		return nil, nil
	}
	b := newTask("b", "a")
	c := newTask("c")
	dag := mustNew(t, tasksOf(a, b, c))

	done := make(chan error, 1)
	go func() {
		_, err := dag.Execute(context.Background())
		done <- err
	}()

	var err error
	select {
	case err = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}

	if !errors.Is(err, xdag.ErrTaskAbandoned) {
		t.Fatalf("want ErrTaskAbandoned, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateFailed)
	assertState(t, dag, "b", xdag.StateSkipped)
	assertState(t, dag, "c", xdag.StateSuccess)
	if dag.Phase() != xdag.PhaseFailed {
		t.Errorf("Phase = %v, want failed", dag.Phase())
	}
}

// 包了 ErrNonRetryable 的错误应当立刻放弃，不消耗剩余尝试次数。
func TestNonRetryableErrorStopsRetrying(t *testing.T) {
	a := newTask("a").retries(5)
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		return nil, fmt.Errorf("bad request: %w", xdag.ErrNonRetryable)
	}
	dag := mustNew(t, tasksOf(a))

	_, err := dag.Execute(context.Background())
	if !errors.Is(err, xdag.ErrNonRetryable) {
		t.Fatalf("want ErrNonRetryable, got %v", err)
	}
	if runs := a.runs(); runs != 1 {
		t.Errorf("不可重试的错误应只执行 1 次, got %d", runs)
	}
	assertState(t, dag, "a", xdag.StateFailed)
}

// Results 要给出错误归属：哪个任务、什么终态、报了什么错、试了几次。
func TestResultsReportStateErrAndAttempts(t *testing.T) {
	ok := newTask("ok").returns(map[string]any{"v": 1})
	bad := newTask("bad").retries(3).fails(errors.New("boom"))
	skipped := newTask("skipped", "bad")

	dag := mustNew(t, tasksOf(ok, bad, skipped))
	_, _ = dag.Execute(context.Background())

	res := dag.Results()
	if len(res) != 3 {
		t.Fatalf("Results 应覆盖全部任务, got %d", len(res))
	}

	if r := res["ok"]; r.State != xdag.StateSuccess || r.Err != nil || r.Attempts != 1 {
		t.Errorf("ok = %+v, want {success, nil, 1}", r)
	}
	if r := res["bad"]; r.State != xdag.StateFailed || r.Err == nil || r.Attempts != 3 {
		t.Errorf("bad = %+v, want {failed, non-nil, 3}", r)
	}
	if r := res["skipped"]; r.State != xdag.StateSkipped || r.Err != nil || r.Attempts != 0 {
		t.Errorf("skipped = %+v, want {skipped, nil, 0}", r)
	}
	// 错误归属：能直接按任务名拿到那个 error，而不用去 errors.Join 里翻
	if !strings.Contains(res["bad"].Err.Error(), "bad") {
		t.Errorf("bad 的错误里应能定位到任务名: %v", res["bad"].Err)
	}
}

// Execute 之前 Results 也可用，全部是零值 Pending。
func TestResultsBeforeExecute(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))

	res := dag.Results()
	// 覆盖性断言：New 里的预填一旦被删，这里会拿到空 map
	if len(res) != 2 {
		t.Fatalf("Execute 之前 Results 应覆盖全部任务, got %d 条: %v", len(res), res)
	}
	for _, name := range []string{"a", "b"} {
		r, ok := res[name]
		if !ok {
			t.Fatalf("Results 缺少任务 %q", name)
		}
		if r.State != xdag.StatePending || r.Err != nil || r.Attempts != 0 {
			t.Errorf("%s = %+v, want 零值 pending", name, r)
		}
	}
}

// Phase 覆盖：未开始 / 运行中 / 三种终值，以及 Failed 压过 Canceled 的优先级。
func TestPhaseLifecycleAndPrecedence(t *testing.T) {
	t.Run("未调用 Execute", func(t *testing.T) {
		dag := mustNew(t, tasksOf(newTask("a")))
		if p := dag.Phase(); p != xdag.PhasePending || p.Done() {
			t.Errorf("Phase = %v (Done=%v), want pending/false", p, p.Done())
		}
	})

	t.Run("运行中", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		a := newTask("a")
		a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			close(started)
			<-release
			return nil, nil
		}
		dag := mustNew(t, tasksOf(a))
		done := make(chan struct{})
		go func() { _, _ = dag.Execute(context.Background()); close(done) }()

		<-started
		if p := dag.Phase(); p != xdag.PhaseRunning || p.Done() {
			t.Errorf("执行途中 Phase = %v (Done=%v), want running/false", p, p.Done())
		}
		close(release)
		<-done
		if !dag.Phase().Done() {
			t.Error("Execute 返回后 Phase 应当 Done")
		}
	})

	t.Run("全部成功", func(t *testing.T) {
		dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))
		_, _ = dag.Execute(context.Background())
		if p := dag.Phase(); p != xdag.PhaseSuccess {
			t.Errorf("Phase = %v, want success", p)
		}
	})

	t.Run("空任务表", func(t *testing.T) {
		dag := mustNew(t, map[string]xdag.ITask{})
		if _, err := dag.Execute(context.Background()); err != nil {
			t.Fatalf("空图不应报错: %v", err)
		}
		if p := dag.Phase(); p != xdag.PhaseSuccess {
			t.Errorf("空图 Phase = %v, want success", p)
		}
	})

	t.Run("有失败", func(t *testing.T) {
		dag := mustNew(t, tasksOf(newTask("a").fails(errors.New("boom")), newTask("b", "a")))
		_, _ = dag.Execute(context.Background())
		if p := dag.Phase(); p != xdag.PhaseFailed {
			t.Errorf("Phase = %v, want failed", p)
		}
	})

	t.Run("只有取消", func(t *testing.T) {
		dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))
		if err := dag.CancelTask("a"); err != nil {
			t.Fatalf("CancelTask: %v", err)
		}
		_, _ = dag.Execute(context.Background())
		if p := dag.Phase(); p != xdag.PhaseCanceled {
			t.Errorf("Phase = %v, want canceled", p)
		}
	})

	// 失败与取消并存时报 failed：取消是调用方自己发起的、本来就知道的事，
	// 残留的失败才是它不知道的真实故障。
	t.Run("失败压过取消", func(t *testing.T) {
		bad := newTask("bad").fails(errors.New("boom"))
		victim := newTask("victim")
		dag := mustNew(t, tasksOf(bad, victim))
		if err := dag.CancelTask("victim"); err != nil {
			t.Fatalf("CancelTask: %v", err)
		}
		_, _ = dag.Execute(context.Background())
		assertState(t, dag, "bad", xdag.StateFailed)
		assertState(t, dag, "victim", xdag.StateCanceled)
		if p := dag.Phase(); p != xdag.PhaseFailed {
			t.Errorf("失败与取消并存时 Phase = %v, want failed", p)
		}
	})
}

func TestPhaseStringAndDone(t *testing.T) {
	cases := []struct {
		p    xdag.Phase
		want string
		done bool
	}{
		{xdag.PhasePending, "pending", false},
		{xdag.PhaseRunning, "running", false},
		{xdag.PhaseSuccess, "success", true},
		{xdag.PhaseCanceled, "canceled", true},
		{xdag.PhaseFailed, "failed", true},
		{xdag.Phase(99), "unknown", false},
	}
	for _, c := range cases {
		if got := c.p.String(); got != c.want {
			t.Errorf("Phase(%d).String() = %q, want %q", c.p, got, c.want)
		}
		if got := c.p.Done(); got != c.done {
			t.Errorf("Phase(%d).Done() = %v, want %v", c.p, got, c.done)
		}
	}
}

// 钉住 Phase 判定所依赖的不变量：只要图里出现了 StateSkipped，
// 就必然存在至少一个 StateFailed。这条不变量成立，Phase 才可以完全无视
// StateSkipped——它一旦被破坏，Phase 会把一场有跳过的执行误报成 success。
func TestSkippedImpliesFailedInvariant(t *testing.T) {
	panicTask := newTask("a")
	panicTask.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		panic("boom")
	}

	shapes := []struct {
		name        string
		tasks       map[string]xdag.ITask
		wantSkipped int // 显式写死，避免断言在 skipped==0 时整体空转
	}{
		{"链式失败", tasksOf(
			newTask("a").fails(errors.New("x")), newTask("b", "a"), newTask("c", "b")), 2},
		{"菱形中一支失败", tasksOf(
			newTask("a"), newTask("b", "a").fails(errors.New("x")),
			newTask("c", "a"), newTask("d", "b", "c")), 1},
		{"panic 也算失败", tasksOf(panicTask, newTask("b", "a")), 1},
		{"重试耗尽", tasksOf(
			newTask("a").retries(2).fails(errors.New("x")), newTask("b", "a")), 1},
	}

	for _, s := range shapes {
		t.Run(s.name, func(t *testing.T) {
			dag := mustNew(t, s.tasks)
			_, _ = dag.Execute(context.Background())

			var skipped, failed int
			for _, st := range dag.States() {
				switch st {
				case xdag.StateSkipped:
					skipped++
				case xdag.StateFailed:
					failed++
				}
			}
			// 先确认这个形状真的产生了跳过，否则下面两条断言等于没跑
			if skipped != s.wantSkipped {
				t.Fatalf("skipped = %d, want %d（states=%v）", skipped, s.wantSkipped, dag.States())
			}
			if failed == 0 {
				t.Fatalf("不变量被破坏：有 %d 个 skipped 却没有任何 failed（states=%v）", skipped, dag.States())
			}
			if dag.Phase() != xdag.PhaseFailed {
				t.Errorf("有跳过时 Phase = %v, want failed", dag.Phase())
			}
		})
	}
}

// ---------------------------------------------------------------------------
// 评审补漏
// ---------------------------------------------------------------------------

// Goexit 兜底不能只在第一次尝试生效：发生在重试途中的 Goexit 同样必须
// 被识别，否则任务静默丢失而 Execute 不报错。
func TestGoexitOnLaterAttemptIsStillCaught(t *testing.T) {
	a := newTask("a").retries(5)
	a.fn = func(_ context.Context, attempt int64, _ map[string]any) (map[string]any, error) {
		if attempt == 1 {
			return nil, errors.New("boom")
		}
		runtime.Goexit()
		return nil, nil
	}
	b := newTask("b", "a")
	dag := mustNew(t, tasksOf(a, b))

	done := make(chan error, 1)
	go func() { _, err := dag.Execute(context.Background()); done <- err }()
	select {
	case err := <-done:
		if !errors.Is(err, xdag.ErrTaskAbandoned) {
			t.Fatalf("want ErrTaskAbandoned, got %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 卡死")
	}
	assertState(t, dag, "a", xdag.StateFailed)
	assertState(t, dag, "b", xdag.StateSkipped)
	if got := dag.Results()["a"].Attempts; got != 2 {
		t.Errorf("Attempts = %d, want 2", got)
	}
}

// 第二次 Execute 只能拿到 ErrAlreadyExecuted，且不得把已经落定的 Phase
// 打回 running——这钉住了 setPhase 必须在 CAS 之后。
func TestSecondExecuteDoesNotResetPhase(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a").fails(errors.New("boom"))))
	_, _ = dag.Execute(context.Background())
	before := dag.Phase()
	if before != xdag.PhaseFailed {
		t.Fatalf("首次执行后 Phase = %v, want failed", before)
	}

	if _, err := dag.Execute(context.Background()); !errors.Is(err, xdag.ErrAlreadyExecuted) {
		t.Fatalf("want ErrAlreadyExecuted, got %v", err)
	}
	if after := dag.Phase(); after != before {
		t.Errorf("第二次 Execute 把 Phase 从 %v 改成了 %v", before, after)
	}
	if !dag.Phase().Done() {
		t.Error("Phase 应保持 Done")
	}
}

// 执行途中被取消的任务，Attempts 会 >= 1——不能按「终态是 canceled」
// 反推 Attempts 一定是 0，这一点必须与 godoc 一致。
func TestAttemptsNonZeroWhenCanceledMidFlight(t *testing.T) {
	started := make(chan struct{})
	a := newTask("a")
	a.fn = func(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
		close(started)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	dag := mustNew(t, tasksOf(a))

	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()
	<-started
	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Execute 未返回")
	}

	r := dag.Results()["a"]
	if r.State != xdag.StateCanceled {
		t.Fatalf("State = %v, want canceled", r.State)
	}
	if r.Attempts != 1 {
		t.Errorf("执行途中被取消 Attempts = %d, want 1（godoc 明确说不是 0）", r.Attempts)
	}
}

// 预取消的任务一次尝试都没发起过，Attempts 必须是 0——与上一条构成对照。
func TestAttemptsZeroWhenPreCanceled(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")))
	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	_, _ = dag.Execute(context.Background())
	if r := dag.Results()["a"]; r.Attempts != 0 {
		t.Errorf("预取消 Attempts = %d, want 0", r.Attempts)
	}
}

// 级联取消的下游 Err 恒为 nil，带 error 的是最初被点名的那个任务。
func TestCascadedCancelHasNilErr(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))
	if err := dag.CancelTask("a"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	_, _ = dag.Execute(context.Background())

	res := dag.Results()
	if res["a"].Err == nil {
		t.Error("被点名取消的任务应带 error")
	}
	if res["b"].State != xdag.StateCanceled {
		t.Errorf("b State = %v, want canceled", res["b"].State)
	}
	if res["b"].Err != nil {
		t.Errorf("级联取消的下游 Err 应为 nil, got %v", res["b"].Err)
	}
}

// settle 是 defer 调用，panic 展开时同样会跑到。Execute(nil) 会在 bind 阶段
// panic，那时一个任务都还没派生、states 全是零值——照常结算会得出
// PhaseSuccess，谎报一个「成功」的终值，而 States() 其实全是 pending。
func TestPhaseDoesNotLieOnPanicUnwind(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("Execute(nil) 应当 panic")
			}
		}()
		//lint:ignore SA1012 故意传 nil 触发 panic 展开路径
		_, _ = dag.Execute(nil) //nolint:staticcheck
	}()

	if p := dag.Phase(); p.Done() {
		t.Errorf("非正常展开后 Phase = %v (Done=true)，不该给出终值——States 其实是 %v",
			p, dag.States())
	}
}

// ---------------------------------------------------------------------------
// 回归补漏（来自全面回归的发现）
// ---------------------------------------------------------------------------

// 三条「尝试之前的等待」被打断时，都必须带上最后一次尝试的真实原因，
// 否则被 ctx 掐断或被 Cancel 的任务只剩一句 deadline exceeded。
func TestWaitInterruptionPreservesLastError(t *testing.T) {
	sentinel := errors.New("db unreachable")

	t.Run("挂起门", func(t *testing.T) {
		a := newTask("a").retries(5)
		a.policy.Interval = 5 * time.Millisecond
		a.policy.MaxInterval = 5 * time.Millisecond
		// 挂起门上的等待被打断时，必须带回最后一次尝试的真实原因。
		// 打断源用 Cancel：挂起是无限期的，只有 resume 和 ctx 取消能放行它。
		var n atomic.Int64
		a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			n.Add(1)
			return nil, sentinel
		}
		dag := mustNew(t, tasksOf(a))
		done := make(chan error, 1)
		go func() { _, err := dag.Execute(context.Background()); done <- err }()
		// 等第一次尝试失败后挂起，让后续尝试停在挂起门上
		for n.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		_ = dag.SuspendTask("a")
		time.Sleep(20 * time.Millisecond)
		go func() { _ = dag.Cancel(context.Background()) }()

		select {
		case err := <-done:
			if !errors.Is(err, sentinel) {
				t.Errorf("挂起门路径丢了根因: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Execute 未返回")
		}
	})

	t.Run("退避等待", func(t *testing.T) {
		a := newTask("a").retries(int64(xdag.InfiniteAttempts))
		a.policy = &xdag.RetryPolicy{
			MaxAttempts: xdag.InfiniteAttempts,
			Interval:    10 * time.Millisecond,
			MaxInterval: 10 * time.Millisecond,
			Multiplier:  1,
		}
		a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			return nil, sentinel
		}
		dag := mustNew(t, tasksOf(a))
		ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
		defer cancel()
		_, err := dag.Execute(ctx)
		if !errors.Is(err, sentinel) {
			t.Errorf("退避路径丢了根因: %v", err)
		}
	})

	t.Run("Cancel打断退避", func(t *testing.T) {
		first := make(chan struct{})
		var once sync.Once
		a := newTask("a").retries(5)
		a.policy.Interval = 2 * time.Second
		a.policy.MaxInterval = 2 * time.Second
		a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
			once.Do(func() { close(first) })
			return nil, sentinel
		}
		dag := mustNew(t, tasksOf(a))
		done := make(chan error, 1)
		go func() { _, err := dag.Execute(context.Background()); done <- err }()
		<-first
		go func() { _ = dag.Cancel(context.Background()) }()

		select {
		case err := <-done:
			if !errors.Is(err, sentinel) {
				t.Errorf("Cancel 打断退避后丢了根因: %v", err)
			}
			if !errors.Is(err, xdag.ErrRunCanceled) {
				t.Errorf("want ErrRunCanceled, got %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Execute 未返回")
		}
	})
}

// 观察者 panic 不能让 errors.Is(err, ErrTaskPanic) 假阳性——那个任务其实是成功的。
func TestObserverPanicIsNotTaskPanic(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")),
		xdag.WithObserver(func(xdag.Event) { panic("observer boom") }))
	_, err := dag.Execute(context.Background())

	if !errors.Is(err, xdag.ErrObserverPanic) {
		t.Fatalf("want ErrObserverPanic, got %v", err)
	}
	if errors.Is(err, xdag.ErrTaskPanic) {
		t.Error("观察者 panic 被误报成任务 panic")
	}
	if tp, ok := errors.AsType[*xdag.PanicError](err); ok {
		t.Errorf("不该取到 *PanicError（那代表任务体炸了）: %+v", tp)
	}
	op, ok := errors.AsType[*xdag.ObserverPanicError](err)
	if !ok || op.Task != "a" || len(op.Stack) == 0 {
		t.Errorf("应当取到 *ObserverPanicError, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateSuccess)
}

// panic 里携带的 ErrNonRetryable 要和 return 回来的同一个 error 行为一致。
func TestPanicCarryingNonRetryableStopsRetrying(t *testing.T) {
	a := newTask("a").retries(5)
	a.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		panic(fmt.Errorf("bad input: %w", xdag.ErrNonRetryable))
	}
	dag := mustNew(t, tasksOf(a))
	_, err := dag.Execute(context.Background())

	if !errors.Is(err, xdag.ErrNonRetryable) {
		t.Fatalf("panic 携带的哨兵没生效: %v", err)
	}
	if a.runs() != 1 {
		t.Errorf("a 执行了 %d 次, want 1", a.runs())
	}
}

// Multiplier 的 NaN/Inf 要和 Jitter 一样被挡掉，不能让退避直接顶到上限。
func TestMultiplierNaNAndInfAreDefaulted(t *testing.T) {
	for _, m := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), 0, -3} {
		p := xdag.RetryPolicy{Interval: 100 * time.Millisecond, Multiplier: m, MaxInterval: time.Hour}
		r := xdag.NewRetryExecutorForTest(p)
		// 补默认值 2.0 之后，第 1、2 次退避应是 100ms / 200ms
		if got := r.CalculateBackoff(1, time.Hour); got != 100*time.Millisecond {
			t.Errorf("Multiplier=%v attempt1 退避 = %v, want 100ms", m, got)
		}
		if got := r.CalculateBackoff(2, time.Hour); got != 200*time.Millisecond {
			t.Errorf("Multiplier=%v attempt2 退避 = %v, want 200ms（NaN/Inf 未被挡会直接顶到上限）", m, got)
		}
	}
}
