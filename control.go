// control.go —— Scheduler 的控制面：外部干预一次正在进行的执行。
//
// 两个维度、两个作用域：
//
// 	              单个任务                整张图
// 	终止          CancelTask(name)       Cancel(ctx)      ← 后者带宽限期，超时后 abandon
// 	挂起 / 恢复   SuspendTask/ResumeTask Suspend/Resume   ← 两路来源独立记账
//
// 每个任务的控制柄本身在 taskcontrol.go，这里只负责按名字找到它并转发。

package xdag

import (
	"context"
	"fmt"
)

// Cancel 强制停止整张图的执行。
//
// 它做三件事：取消每个任务专属的 context（正在执行的任务体因此会收到取消
// 通知）、不再让任何新的尝试开始、然后在 ctx 给的宽限期内等待排空。
//
// ctx 是**宽限期**，不是执行本身的 context：
//   - 在宽限期内全部任务落定 → 返回 nil，此时 Execute 已经返回，
//     States/Results/Phase 都是最终值
//   - 宽限期耗尽仍有任务没落定 → 替它们落终态、放行下游，让 Execute 得以
//     返回，然后返回 ctx.Err()
//
// **代价要认**：宽限期耗尽后被抛下的任务 goroutine 仍然活着——Go 没有办法
// 杀死一个不肯响应 ctx 的 goroutine。任务体如果既不检查 ctx 又永不返回，
// 那就是永久泄漏。想避免就给足宽限期，或者把任务体写成尊重 ctx 的。
//
//	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
//	defer cancel()
//	if err := dag.Cancel(ctx); err != nil {
//	    log.Printf("宽限期内没排空，已强制收尾: %v", err)
//	}
//
// Execute 还没开始就调用则立即返回 nil，并且整张图一个任务都不会执行。
// 可以重复调用，也可以并发调用。Canceled 用于查询是否已经发起过停止。
//
// 想只停一个任务，用 CancelTask。
func (d *Scheduler) Cancel(ctx context.Context, opts ...CancelOption) error {
	cause := causeOf(opts)
	d.cancelOnce.Do(func() {
		d.cancelRequested.Store(true)
		// 逐个取消每个任务的 context。用 own=false：整场停机是一次操作，
		// 走去重路径只上报一条错误，否则 N 个任务会刷出 N 条同质错误。
		for _, ctrl := range d.controls {
			ctrl.cancel(d.cancelCause("", ErrRunCanceled, cause), false)
		}
	})

	// 压根没开始执行，就没有要等的东西。不这么短路的话，
	// 「构造完直接 Cancel、从不 Execute」会一直阻塞到 ctx 到期。
	if !d.executed.Load() {
		return nil
	}

	select {
	case <-d.doneCh:
		return nil
	case <-ctx.Done():
		// 宽限期到了还没排空：剩下的任务体不肯响应取消，就替它们落终态、
		// 放行下游，让 Execute 得以返回。被抛下的 goroutine 仍在运行，
		// 见 abandon 的说明。
		d.abandonAll()
		<-d.doneCh
		return ctx.Err()
	}
}

// abandonAll 替所有还没落终态的任务收尾，供 Cancel 在宽限期耗尽时使用。
func (d *Scheduler) abandonAll() {
	for name := range d.controls {
		d.abandon(name, fmt.Errorf("task %s: %w", name, ErrRunCanceled))
	}
}

// Suspend 挂起**整场执行**：不再让任何任务开始新的尝试，已经在执行中的
// 那一次不受打扰，跑完为止。
//
// 它与 Cancel 是两件事：Cancel 是单向的，取消每个任务的 context 并收摊；
// Suspend 可以用 Resume 原样解除，执行继续往下走。想让整场立刻停下、
// 连在飞的那次尝试也一并收到取消，用 Cancel；想让在飞的那次干完、
// 之后还能松开继续，用 Suspend。
//
// 生效的位置与 SuspendTask 完全相同——调度器完全掌控的那两个时间点：
// 任务还没开始第一次尝试，以及两次重试尝试之间。它打不断已经在进行中的
// 单次 Execute 调用，Go 没有中途冻结一个正在运行的 goroutine 的机制。
//
// 与 SuspendTask **互相独立**：整场挂起不会清掉某个任务身上单独施加的挂起，
// 反之亦然。一个任务只要被任一路按住就停着，两路都松开才放行。
// 这一点很要紧：否则"暂停某个步骤 → 暂停整个任务 → 恢复整个任务"
// 会把那个步骤静默放行，而用户以为它还停着。
//
// New 之后、Execute 之前调用同样有效：那样整张图会停在起跑线上，
// 直到 Resume。可以重复调用，也可以并发调用。
func (d *Scheduler) Suspend() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runSuspended = true
	for _, ctrl := range d.controls {
		ctrl.setSuspended(sourceRun, true)
	}
}

// Resume 解除 Suspend 施加的整场挂起。
//
// 它只松开整场那一路：被 SuspendTask 单独挂起的任务仍然停着，
// 要用 ResumeTask 单独放行。对没有被整场挂起的执行调用是无害空操作。
func (d *Scheduler) Resume() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.runSuspended = false
	for _, ctrl := range d.controls {
		ctrl.setSuspended(sourceRun, false)
	}
}

// lookupControl 在 mu 保护下一次性查出任务的控制柄与当前状态，供
// CancelTask/SuspendTask/ResumeTask/SuspendedTask 共用。
func (d *Scheduler) lookupControl(name string) (*taskControl, State, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	ctrl, ok := d.controls[name]
	if !ok {
		return nil, StatePending, fmt.Errorf("%w: %q", ErrUnknownTask, name)
	}
	return ctrl, d.states[name], nil
}

// CancelTask 取消单个任务：通过取消它专属的 context 通知任务体，
// 并阻止它开始任何新的尝试。
//
// 生效范围：**待执行**（还没轮到它，或依赖刚满足）、**下一轮重试**
// （上一次失败、正在退避等待）、**挂起中**（停在挂起门上）。这三种情况下
// 取消 100% 有效，任务体一次都不会（再）被调用。
//
// 正在执行中的那次尝试也会收到 ctx 上的取消通知，但调度器**仍然等它自己
// 返回**——能不能及时停下取决于任务体检不检查 ctx。整张图都不想等了，
// 用 Cancel：它在宽限期耗尽时会替没落定的任务写终态、放行下游。
//
// 终态记为 StateCanceled，错误里带 ErrTaskCanceled，并按现有规则级联到
// 下游（下游同样记为 StateCanceled）。
//
// New 之后、Execute 之前就可以调用：这类「预取消」会在 Execute 开始时
// 立即兑现，任务一被调度就直接落在 StateCanceled，任务体一次都不会被调用。
//
// **可以重复调用**：取消不会让任务立刻终态（还要等任务体返回），所以第二次
// 调用依然有意义，依然返回 nil。
//
// 任务名不存在返回 ErrUnknownTask；任务已经处于终态返回 ErrTaskAlreadyDone。
func (d *Scheduler) CancelTask(name string, opts ...CancelOption) error {
	ctrl, state, err := d.lookupControl(name)
	if err != nil {
		return err
	}
	if state.Done() {
		return fmt.Errorf("%w: %q (%s)", ErrTaskAlreadyDone, name, state)
	}
	ctrl.cancel(d.cancelCause(fmt.Sprintf("task %s", name), ErrTaskCanceled, causeOf(opts)), true)
	return nil
}

// SuspendTask 挂起单个任务：如果它还没有开始第一次尝试，或者上一次尝试
// 失败正准备重试，调度器会让它停在这里，直到 ResumeTask 或取消（这个
// 任务被单独取消，或整场执行被取消）为止。
//
// 挂起打不断已经在进行中的单次 Execute 调用——Go 没有中途冻结一个正在
// 运行的 goroutine 的机制，这是刻意的取舍：挂起只承诺调度器完全掌控的
// 两个时间点，不假装能做到更多。
//
// New 之后、Execute 之前就可以调用：这类「预挂起」能确定性地拦在任务
// 的第一次尝试之前，不需要和调度抢时序。
//
// 任务名不存在返回 ErrUnknownTask；任务已经处于终态返回
// ErrTaskAlreadyDone。可以重复调用，是幂等操作；解挂之后还可以再次挂起。
func (d *Scheduler) SuspendTask(name string) error {
	ctrl, state, err := d.lookupControl(name)
	if err != nil {
		return err
	}
	if state.Done() {
		return fmt.Errorf("%w: %q (%s)", ErrTaskAlreadyDone, name, state)
	}
	ctrl.suspend()
	return nil
}

// ResumeTask 解除 SuspendTask 施加的挂起，放行正在等待的任务。
// 对没有被挂起的任务调用是无害的空操作，同样可以在 Execute 之前调用。
//
// 任务名不存在返回 ErrUnknownTask；任务已经处于终态返回
// ErrTaskAlreadyDone。
func (d *Scheduler) ResumeTask(name string) error {
	ctrl, state, err := d.lookupControl(name)
	if err != nil {
		return err
	}
	if state.Done() {
		return fmt.Errorf("%w: %q (%s)", ErrTaskAlreadyDone, name, state)
	}
	ctrl.resume()
	return nil
}

// cancelCause 把库的哨兵与调用方给的原因合成一条错误。
//
// 两个 %w 都要留：哨兵回答「发生了什么」（这个任务被取消 / 整场被取消），
// cause 回答「为什么」（那是调用方的业务语义，库不该替它发明哨兵）。
// 无论调用方传什么，errors.Is(err, 哨兵) 始终成立——自定义原因是**附加**
// 信息不是替换，否则每加一种业务原因都会悄悄破坏既有的哨兵判定。
func (d *Scheduler) cancelCause(prefix string, sentinel, cause error) error {
	switch {
	case cause == nil && prefix == "":
		return sentinel
	case cause == nil:
		return fmt.Errorf("%s: %w", prefix, sentinel)
	case prefix == "":
		return fmt.Errorf("%w: %w", sentinel, cause)
	default:
		return fmt.Errorf("%s: %w: %w", prefix, sentinel, cause)
	}
}

// CancelOption 调整一次取消的行为。目前只有 WithCause 一个。
//
// 做成可选参数而不是必填形参：绝大多数取消并没有什么特别的原因要说，
// 逼每个调用点写一个 nil 只是噪音。
type CancelOption func(*cancelOptions)

type cancelOptions struct{ cause error }

// WithCause 给这次取消附上调用方自己的原因。
//
// 它会被 context.Cause 透给任务体，也会进入这个任务终态的错误里，
// 调用方随后用 errors.Is 就能把自己的原因取回来。
//
// 库的哨兵（ErrTaskCanceled / ErrRunCanceled）**不会**被它替换掉，两者
// 同时成立：哨兵回答「发生了什么」，cause 回答「为什么」。「为什么」是
// 业务语义，不该让库为每一种原因发明一个哨兵——那样每加一种业务原因，
// 库就得跟着发一个版本。
//
// 传 nil 等同于没传。
func WithCause(cause error) CancelOption {
	return func(o *cancelOptions) { o.cause = cause }
}

func causeOf(opts []CancelOption) error {
	var o cancelOptions
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o.cause
}
