// taskcontrol.go —— 单个任务的控制柄：取消用的专属 context，与挂起用的那道门。
//
// 它在 New 阶段就为每个任务建好，因此 Execute 之前发出的控制指令同样有效。
// 对外的控制 API 在 control.go。

package xdag

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
)

// errTaskDone 是任务进入终态后用来回收其专属 context 的哨兵 cause。
// 它不会被任何调用方观察到——只有 resolve 判定任务被取消时才会去读
// context.Cause，而那时任务的终态早已经由别的路径决定完毕。
var errTaskDone = errors.New("task reached terminal state")

// suspendSource 标识一路挂起来源。两路互相独立，任一为真即挂起。
type suspendSource int

const (
	// sourceTask 是 SuspendTask 施加的、只针对这一个任务的挂起。
	sourceTask suspendSource = iota
	// sourceRun 是 Suspend 施加的、覆盖整场执行的挂起。
	sourceRun
	numSuspendSources
)

// taskControl 是单个任务的控制柄，支撑 CancelTask/SuspendTask/ResumeTask：
// 挂起/解挂只作用于「任务还没开始第一次尝试」与「两次重试尝试之间」这两个
// 调度器完全掌控的时间点，不会打断正在进行中的单次 Execute() 调用——Go
// 没有中途冻结一个正在运行的 goroutine 的机制。
//
// 取消复用同一个任务专属的 context：它是 Execute(ctx) 收到的 ctx 派生出
// 的子 context，父 context 取消时这个任务自动一并取消；单独取消这一个
// 任务，不影响图里的其他任务。
//
// 控制柄在 New 阶段就为每个任务建好，而这个专属 context 要等 Execute
// 拿到 ctx 之后才能派生出来（bind）。因此 Execute 之前发起的取消先记在
// pending 里，bind 时立即兑现——控制方法的可用窗口不该由 Execute 划定：
// 业务层完全可能在构图之后、启动之前就已经知道某个任务该取消。
type taskControl struct {
	// ---- 构建期确定，此后只读 ----

	name string

	// ---- 自带同步，不需要 mu ----

	// own 标记这次取消是不是专门针对这一个任务发起的（CancelTask），
	// 而不是整体 Cancel、父 context 取消/超时向下传播的结果——决定错误
	// 怎么上报，见 Scheduler.cancellation。
	own atomic.Bool
	// attempts 记录任务体实际被调用的次数，供 TaskResult.Attempts 报告。
	// 这个数字此前只存在于 retry.go 的错误文案里，程序拿不到。
	attempts atomic.Int64

	// ---- 以下全部由 mu 保护 ----

	mu sync.Mutex
	// ctx/cancelFn 由 bind 填入，Execute 之前都是 nil。
	ctx      context.Context
	cancelFn context.CancelCauseFunc
	// pending 是 bind 之前记下的取消原因，nil 表示没有待兑现的取消。
	//
	// Cancel 与 CancelTask 走的是同一个 cancel()，两者在 Execute 之前发起时
	// 都要在这里排队，等 bind 时一并兑现——控制方法的可用窗口不该由 Execute
	// 划定。
	pending error
	// pauseCh 非 nil 表示挂起中；resume 关闭它放行等待者。
	pauseCh chan struct{}
	// bySource 分别记住两路挂起来源的状态。pauseCh 是它们的或运算结果。
	bySource [numSuspendSources]bool

	// ---- 自带同步 ----

	// settled 保证一个任务的终态只被写入一次。
	//
	// Cancel 会在放弃等待时**从外部**替任务落终态，而那个任务的
	// goroutine 还活着、事后仍会走到自己的 commit。没有这道闸，状态会被
	// 写两次、wg 会被 Done 两次（直接 panic）、下游会被派生两次。
	settled atomic.Bool
}

func newTaskControl(name string) *taskControl {
	return &taskControl{name: name}
}

// bind 在 Execute 开始时把控制柄挂到本次执行的 context 上，并兑现
// Execute 之前已经记下的强制停止意图。它在任何任务 goroutine 派生之前调用，
// 因此调度路径上读 ctx 不需要再加锁。
func (c *taskControl) bind(parent context.Context) {
	ctx, cancel := context.WithCancelCause(parent)

	c.mu.Lock()
	c.ctx, c.cancelFn = ctx, cancel
	pending := c.pending
	c.pending = nil
	c.mu.Unlock()

	if pending != nil {
		cancel(pending)
	}
}

// release 在任务进入终态后调用一次，回收这个任务专属 context 占用的资源。
func (c *taskControl) release() {
	c.mu.Lock()
	cancelFn := c.cancelFn
	c.mu.Unlock()
	if cancelFn != nil {
		cancelFn(errTaskDone)
	}
}

// cancel 取消这一个任务专属的 context，cause 会被 context.Cause 透出。
//
// 这是唯一能伸进**正在执行的任务体**的通路：CancelTask 与 Cancel 都走它，
// 任务体能不能及时停下取决于它自己检不检查 ctx——与整场取消一样是
// 协作式的，Go 没有别的办法叫停一个不配合的 goroutine。
//
// 两者的区别不在这里，而在调用方随后等不等：CancelTask 照常等任务体返回，
// Cancel 在宽限期耗尽后不等了，直接替它落终态。
//
// own 为 true 表示这是针对单个任务发起的，错误单独上报；整体 Cancel 传
// false，走去重路径，避免一次操作刷出 N 条同质错误。
//
// 若此时还没有 bind（Execute 尚未开始），先把 cause 记下来，等 bind 兑现。
func (c *taskControl) cancel(cause error, own bool) {
	if own {
		c.own.Store(true)
	}

	c.mu.Lock()
	cancelFn := c.cancelFn
	if cancelFn == nil {
		c.pending = cause
	}
	c.mu.Unlock()

	if cancelFn != nil {
		cancelFn(cause)
	}
}

// suspend 让这个任务在下一次 wait 检查点处停住，直到 resume 或取消为止。
// 重复调用是无害的空操作。
func (c *taskControl) suspend() { c.setSuspended(sourceTask, true) }

// resume 放行正在等待的调用者。对没有被挂起的任务调用是无害空操作。
func (c *taskControl) resume() { c.setSuspended(sourceTask, false) }

func (c *taskControl) suspended() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.pauseCh != nil
}

// suspendedBy 报告某一路来源当前有没有把这个任务按住。
func (c *taskControl) suspendedBy(src suspendSource) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.bySource[src]
}

// setSuspended 打开或关掉某一路挂起来源，再按「任一来源为真即挂起」
// 重算那道门。
//
// 两路来源刻意共用同一个 pauseCh，而不是各给一个 channel 让 wait 去
// select——wait 的注释写明了那条设计原则：除了 resume，唤醒通路只有
// ctx.Done() 一条，「不存在新加了一种停止方式却忘了在这里补一路的可能」。
// 在这里做或运算，wait 一行都不用改，那条原则原样成立。
//
// 两路必须独立记账：整场挂起时逐个 suspend、恢复时逐个 resume 的做法，
// 会把「单独挂起的那个任务」一起放行——用户点了暂停某个步骤，
// 又暂停再恢复整个任务，那个步骤就被静默放行了。
func (c *taskControl) setSuspended(src suspendSource, on bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bySource[src] == on {
		return
	}
	c.bySource[src] = on
	want := c.bySource[sourceTask] || c.bySource[sourceRun]
	switch {
	case want && c.pauseCh == nil:
		c.pauseCh = make(chan struct{})
	case !want && c.pauseCh != nil:
		close(c.pauseCh)
		c.pauseCh = nil
	}
}

// wait 在任务即将开始新一次尝试之前调用：不在挂起中立即返回 nil；
// 在挂起中就阻塞，直到 resume 放行、ctx 被取消（这个任务被单独取消，
// 或父 context 整体取消/超时），或者整体优雅停机。
//
// 挂起是无限期的，没有别的东西会放行它，所以除了 resume 之外必须还有
// 一条唤醒通路——ctx.Done()。取消、强制停止、整场取消、超时全都汇到这一个
// 信号上，所以这里只需要 select 它一个：不存在「新加了一种停止方式却忘了
// 在这里补一路」的可能，而那正是历史上两个严重 bug 的成因。
func (c *taskControl) wait(ctx context.Context) error {
	c.mu.Lock()
	ch := c.pauseCh
	c.mu.Unlock()
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
