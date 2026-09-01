// observer.go —— 观察者：任务进入终态时的回调，以及回调自身 panic 的兜底。

package xdag

import (
	"fmt"
	"runtime/debug"
)

// Event 描述一次任务终态的落定。
type Event struct {
	// Task 是进入终态的**任务名**。
	//
	// 它是 string 不是 ITask。这里一度被机械重命名成 ITask——
	// 一个存名字的字段叫 ITask，读起来像装着接口，而接口重命名波及
	// 不到它。字段名跟着类型名走是错的，跟着语义走才对。
	Task string
	// TaskResult 是这个任务的结果摘要，字段含义与 Results() 返回的完全一致。
	TaskResult
}

// WithObserver 注册一个观察者，每当有任务进入终态时被调用一次。
//
// 它是执行期控制 API 的配套：既然可以在执行期间 CancelTask/SuspendTask，
// 就需要在执行期间看得见发生了什么——看不到任务 Y 刚失败，就无从决定要不要
// 取消任务 X。轮询 States() 拿不到「刚刚发生了什么」，只能看到当前快照。
//
// 契约（这几条都是硬约束，不是建议）：
//
//   - **回调绝对不能阻塞**。Execute 会等待每一个回调返回——commit 是 runTask
//     里最先执行的 defer，跑在 wg.Done 之前，所以 wg.Wait 必然覆盖它。回调
//     卡住就等于整场执行挂起，没有超时也没有兜底。尤其不要在回调里等任何
//     「要等 Execute 返回之后才会满足」的条件，下面这种写法会直接吊死：
//
//     events := make(chan xdag.Event)                       // 无缓冲
//     dag, _ := xdag.New(t, xdag.WithObserver(func(ev xdag.Event) {
//     events <- ev                                      // 没人在收
//     }))
//     dag.Execute(ctx)                                      // 永远不返回
//
//   - 在回调里**查询**是安全的（States/Results/Progress/Phase/State/
//     ExecutionOrder/TaskSuspended/Canceled/WriteGraph）：回调不持有 d.mu，
//     这些方法只是各自短暂取一次锁。
//
//   - **不等待的控制方法**（CancelTask/SuspendTask/ResumeTask）
//     在回调里调用同样安全，只是对刚落终态的这个任务会返回
//     ErrTaskAlreadyDone，对下游是否来得及生效取决于下面那条事件时序——
//     需要确定性就别在回调里做控制。
//
//   - **绝对不要在回调里直接调用 Cancel**——它会等 Execute 返回，而 Execute
//     在等这个回调，必然死锁。要从回调里发起停机就别等它：
//
//     go func() { _ = dag.Cancel(ctx) }()
//
//     或者只把决定投递出去，由外面那条主线去调 Cancel。
//
//   - **回调会被多个 goroutine 并发调用**，实现必须自己保证并发安全。多个
//     任务同时完成时，它们的回调是并发的。
//
//   - **事件顺序不保证与因果顺序一致**。事件在释放调度锁之后触发，而下游
//     任务在锁内就已经被派生，因此一个下游的事件完全可能先于它上游的事件
//     到达。需要确定的完成顺序请用 ExecutionOrder()。
//
//   - **回调只观察，不参与任何调度决策**。它没有返回值，做什么都不会改变
//     任务的终态或后续调度。
//
//   - **回调要快**。它不阻塞别的任务提交终态（那些在锁内已经完成），但
//     Execute 要等它返回。要做 I/O 请自己异步化，且异步化的那一端不能反过来
//     依赖 Execute 已经返回。
//
// 回调里的 panic 会被接住并转成一条 ErrObserverPanic 错误汇入 Execute 的
// 返回值——观察者是调用方代码，它出问题不该打垮整个进程，但也不该被静默吞掉。
// 错误里包着一个 *ObserverPanicError，用 errors.As 可以取到 panic 值与调用栈。
// 刻意不复用 PanicError，理由见 ObserverPanicError 的文档。
// 任务的终态不受影响，此时它早已落定。
//
// 每个任务恰好触发一次，包括被跳过、被取消、失败的任务。
func WithObserver(fn func(Event)) Option {
	return func(o *options) { o.observer = fn }
}

// ObserverPanicError 描述一次 WithObserver 回调里发生的 panic。
//
// 它与 PanicError 是两回事，刻意不共用类型：那个说的是「任务体炸了」，
// 这个说的是「把某个任务的事件告诉你时，你的回调炸了」——Task 字段指的是
// 触发这次回调的任务，而它本身完全可能是成功的。混用会让
// errors.Is(err, ErrTaskPanic) 出现假阳性。
type ObserverPanicError struct {
	// Task 是触发这次回调的任务名。注意它不是「出问题的任务」，
	// 那个任务的终态另见 Results()。
	Task string
	// Value 是 recover() 拿到的原始值。
	Value any
	// Stack 是 panic 现场的调用栈。
	Stack []byte
}

// Error 同样不含调用栈，理由见 PanicError.Error。
func (e *ObserverPanicError) Error() string {
	return fmt.Sprintf("observer panicked on event for task %s: %v", e.Task, e.Value)
}

// Is 让 errors.Is(err, ErrObserverPanic) 成立。
func (e *ObserverPanicError) Is(target error) bool { return target == ErrObserverPanic }

// notify 在**锁外**调用观察者。调用点见 commit：d.mu 是全图唯一一把锁且
// 不可重入，在持锁状态下回调，调用方只要碰一下 States() 就会自锁死。
func (d *Scheduler) notify(ev Event, errCh chan error) {
	if d.opts.observer == nil {
		return
	}
	defer func() {
		if r := recover(); r != nil {
			// 用独立类型而不是复用 PanicError：后者的 Is 认 ErrTaskPanic，
			// 会让「观察者炸了」被误报成「任务炸了」——而 ev.Task 那个任务
			// 很可能刚刚成功。栈同样进字段不进文案，与 PanicError 一致。
			err := &ObserverPanicError{Task: ev.Task, Value: r, Stack: debug.Stack()}
			select {
			case errCh <- err:
			default:
			}
		}
	}()
	d.opts.observer(ev)
}
