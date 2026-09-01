// progress_test.go —— Phase 与 Progress 的取值。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// 进度
// ---------------------------------------------------------------------------

// 各状态计数正确，且各字段之和恒等于 Total。
func TestProgressCounts(t *testing.T) {
	ok1 := newTask("ok1")
	ok2 := newTask("ok2")
	bad := newTask("bad").fails(errors.New("boom"))
	skipped := newTask("skipped", "bad")
	canceled := newTask("canceled")

	dag := mustNew(t, tasksOf(ok1, ok2, bad, skipped, canceled))
	if err := dag.CancelTask("canceled"); err != nil {
		t.Fatalf("CancelTask: %v", err)
	}
	_, _ = dag.Execute(context.Background())

	p := dag.Progress()
	if p.Total != 5 {
		t.Errorf("Total = %d, want 5", p.Total)
	}
	if p.Success != 2 || p.Failed != 1 || p.Skipped != 1 || p.Canceled != 1 {
		t.Errorf("计数不对: %+v", p)
	}
	if p.Pending != 0 {
		t.Errorf("执行完毕后 Pending = %d, want 0", p.Pending)
	}
	if sum := p.Pending + p.Success + p.Skipped + p.Canceled + p.Failed; sum != p.Total {
		t.Errorf("各字段之和 %d != Total %d (%+v)", sum, p.Total, p)
	}
	if p.Done() != 5 {
		t.Errorf("Done() = %d, want 5", p.Done())
	}
	if p.Ratio() != 1 {
		t.Errorf("Ratio() = %v, want 1", p.Ratio())
	}
}

// Execute 之前：全部 Pending，Ratio 为 0。
func TestProgressBeforeExecute(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))
	p := dag.Progress()
	if p.Total != 2 || p.Pending != 2 || p.Done() != 0 {
		t.Errorf("Execute 之前 = %+v, want 2 个全 pending", p)
	}
	if p.Ratio() != 0 {
		t.Errorf("Ratio() = %v, want 0", p.Ratio())
	}
}

// 执行途中能看到部分完成，且与 States() 的口径一致。
func TestProgressDuringExecution(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	a := newTask("a").returns(map[string]any{"v": 1})
	b := newTask("b", "a")
	b.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		<-release
		return nil, nil
	}
	c := newTask("c", "b")

	dag := mustNew(t, tasksOf(a, b, c))
	done := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(done) }()

	<-started
	p := dag.Progress()
	if p.Total != 3 || p.Success != 1 || p.Pending != 2 {
		t.Errorf("途中 = %+v, want total=3 success=1 pending=2", p)
	}
	if got, want := p.Ratio(), 1.0/3.0; got != want {
		t.Errorf("Ratio() = %v, want %v", got, want)
	}
	// 与 States() 逐个统计的结果必须一致
	var success, pending int
	for _, s := range dag.States() {
		switch s {
		case xdag.StateSuccess:
			success++
		case xdag.StatePending:
			pending++
		}
	}
	if success != p.Success {
		t.Errorf("Progress.Success=%d 与 States() 统计的 %d 不一致", p.Success, success)
	}

	close(release)
	<-done
	if final := dag.Progress(); final.Done() != 3 {
		t.Errorf("结束后 = %+v, want 全部完成", final)
	}
}

// 空图：Total 为 0，Ratio 为 1，与 Phase 报 success 保持一致。
func TestProgressEmptyGraph(t *testing.T) {
	dag := mustNew(t, map[string]xdag.ITask{})
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	p := dag.Progress()
	if p.Total != 0 || p.Done() != 0 {
		t.Errorf("空图 = %+v", p)
	}
	if p.Ratio() != 1 {
		t.Errorf("空图 Ratio() = %v, want 1（与 Phase 报 success 一致）", p.Ratio())
	}
	if dag.Phase() != xdag.PhaseSuccess {
		t.Errorf("Phase = %v, want success", dag.Phase())
	}
}

func TestProgressString(t *testing.T) {
	p := xdag.Progress{Total: 5, Pending: 1, Success: 2, Skipped: 1, Failed: 1}
	want := "4/5 done (success 2, skipped 1, canceled 0, failed 1)"
	if got := p.String(); got != want {
		t.Errorf("String() = %q, want %q", got, want)
	}
}

// 执行期间高频轮询查询方法必须是安全的——这正是进度条的用法。
// -race 下运行：任何一个查询方法漏了锁，都会在这里被抓出来。
func TestQueriesAreSafeUnderConcurrentExecution(t *testing.T) {
	const n = 40
	list := make([]*testTask, 0, n)
	for i := range n {
		task := newTask(fmt.Sprintf("t%02d", i))
		if i%3 == 0 {
			task.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
				return nil, errors.New("boom")
			}
		}
		list = append(list, task)
	}
	dag := mustNew(t, tasksOf(list...))

	done := make(chan struct{})
	stopPolling := make(chan struct{})
	var polls atomic.Int64

	go func() {
		for {
			select {
			case <-stopPolling:
				return
			default:
			}
			p := dag.Progress()
			// 快照必须自洽：各字段之和恒等于 Total
			if sum := p.Pending + p.Success + p.Skipped + p.Canceled + p.Failed; sum != p.Total {
				t.Errorf("进度快照撕裂: 各字段和 %d != Total %d (%+v)", sum, p.Total, p)
				return
			}
			if r := p.Ratio(); r < 0 || r > 1 {
				t.Errorf("Ratio 越界: %v", r)
				return
			}
			_ = dag.States()
			_ = dag.Results()
			_ = dag.Phase()
			_ = dag.State("t00")
			polls.Add(1)
		}
	}()

	go func() { _, _ = dag.Execute(context.Background()); close(done) }()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Execute 未返回")
	}
	close(stopPolling)

	if polls.Load() == 0 {
		t.Error("轮询一次都没跑到，这个用例没起到作用")
	}
	final := dag.Progress()
	if final.Done() != n {
		t.Errorf("结束后 = %+v, want 全部完成", final)
	}
}

// 观察者 panic 也走结构化错误：栈在字段里而不是文案里，与任务 panic 一致。
func TestObserverPanicIsStructured(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")),
		xdag.WithObserver(func(xdag.Event) { panic("observer boom") }))
	_, err := dag.Execute(context.Background())

	if !errors.Is(err, xdag.ErrObserverPanic) {
		t.Fatalf("want ErrObserverPanic, got %v", err)
	}
	var pe *xdag.ObserverPanicError
	if !errors.As(err, &pe) {
		t.Fatalf("errors.As 取不到 *ObserverPanicError: %v", err)
	}
	if pe.Task != "a" || pe.Value != "observer boom" || len(pe.Stack) == 0 {
		t.Errorf("ObserverPanicError = %+v", pe)
	}
	// 观察者炸了不等于任务炸了——任务 a 其实是成功的
	if errors.Is(err, xdag.ErrTaskPanic) {
		t.Error("观察者 panic 不该让 errors.Is(err, ErrTaskPanic) 成立")
	}
	assertState(t, dag, "a", xdag.StateSuccess)
	if strings.Contains(err.Error(), "goroutine") {
		t.Error("错误文案里不该有调用栈")
	}
	if n := len(err.Error()); n > 200 {
		t.Errorf("错误文案 %d 字节，太长", n)
	}
}

// 观察者 panic 不该把任务自己的失败挤出聚合错误——errCh 容量要够两者共存。
func TestObserverPanicDoesNotEvictTaskErrors(t *testing.T) {
	const n = 8
	sentinel := errors.New("task-real-failure")
	list := make([]*testTask, 0, n)
	for i := range n {
		list = append(list, newTask(fmt.Sprintf("t%02d", i)).fails(sentinel))
	}
	dag := mustNew(t, tasksOf(list...),
		xdag.WithObserver(func(xdag.Event) { panic("observer boom") }))

	_, err := dag.Execute(context.Background())
	if !errors.Is(err, sentinel) {
		t.Fatalf("任务的真实失败被挤出了聚合错误: %v", err)
	}
	// n 个任务失败 + n 个观察者 panic，一条都不该被丢
	if got := countMatches(err, sentinel); got != n {
		t.Errorf("聚合错误里有 %d 条任务失败, want %d —— 有错误被静默丢弃", got, n)
	}
	if got := countMatches(err, xdag.ErrObserverPanic); got != n {
		t.Errorf("聚合错误里有 %d 条观察者 panic, want %d —— 有错误被静默丢弃", got, n)
	}
}

// Cancel(ctx) 会等到本次执行真正结束才返回，返回时状态已经是最终值。
func TestStopWaitsForDrain(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	slow := newTask("slow")
	slow.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		<-release
		return map[string]any{"ok": true}, nil
	}
	dag := mustNew(t, tasksOf(slow, newTask("other")))

	execDone := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(execDone) }()
	<-started

	stopReturned := make(chan error, 1)
	go func() { stopReturned <- dag.Cancel(context.Background()) }()

	// 在飞任务还没放行，Cancel 不该返回
	select {
	case err := <-stopReturned:
		t.Fatalf("在飞任务未结束，Cancel 就返回了: %v", err)
	case <-time.After(120 * time.Millisecond):
	}

	close(release)
	select {
	case err := <-stopReturned:
		if err != nil {
			t.Fatalf("Cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Cancel 未返回")
	}

	// 返回时 Execute 已经收尾，终态是最终值
	select {
	case <-execDone:
	case <-time.After(time.Second):
		t.Fatal("Cancel 返回了但 Execute 还没结束")
	}
	if !dag.Phase().Done() {
		t.Errorf("Cancel 返回时 Phase 应已落定, got %v", dag.Phase())
	}
	assertState(t, dag, "slow", xdag.StateSuccess)
}

// 等不及时 Cancel 返回 ctx.Err()，但停机请求本身已经生效、不会撤销。
func TestStopGivesUpWaitingOnContext(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	defer close(release)
	slow := newTask("slow")
	slow.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		close(started)
		<-release // 无视一切，一直占着
		return nil, nil
	}
	dag := mustNew(t, tasksOf(slow))

	execDone := make(chan struct{})
	go func() { _, _ = dag.Execute(context.Background()); close(execDone) }()
	<-started

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	err := dag.Cancel(ctx)

	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	// 「不等了」不等于「不停了」
	if !dag.Canceled() {
		t.Error("Cancel 超时返回后，停机请求本身仍应生效")
	}
	<-time.After(10 * time.Millisecond)
}

// Execute 之前调用：没有在飞任务要等，立即返回 nil，不会阻塞到 ctx 到期。
func TestStopBeforeExecuteReturnsImmediately(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")))
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()
	if err := dag.Cancel(ctx); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Errorf("Execute 之前调用不该阻塞，耗时 %v", elapsed)
	}
	if !dag.Canceled() {
		t.Error("Canceled() 应为 true")
	}
}

// panic 展开路径上也要放行等待方，不能把它永远晾着。
func TestStopUnblocksOnPanicUnwind(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")))
	go func() {
		defer func() { _ = recover() }()
		//nolint:staticcheck // 故意传 nil 触发 panic 展开路径
		_, _ = dag.Execute(nil)
	}()
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := dag.Cancel(ctx); err != nil {
		t.Errorf("Execute panic 之后 Cancel 应当能返回, got %v", err)
	}
}
