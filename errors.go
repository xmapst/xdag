// errors.go —— 执行与控制相关的哨兵错误，以及 PanicError。
//
// 图校验的 ErrCircularDependency/ErrUnknownDependency 在 check.go，重试的
// ErrNonRetryable 在 retry.go——它们跟着各自的逻辑走，不在这里。
//
// 单独成篇是因为它们是这个包的**对外契约面**：调用方用 errors.Is/As 去分辨
// 「任务自己失败了」「被单独取消」「整场被终止」「名字对不上」，
// 这些判断散落在调度代码里会很难一次看全。

package xdag

import (
	"errors"
	"fmt"
)

// ErrAlreadyExecuted 表示对同一个 Scheduler 调用了不止一次 Execute。
// 入度表等调度状态在执行过程中会被消费掉，因此不可重入。
var ErrAlreadyExecuted = errors.New("dag already executed")

// ErrTaskPanic 表示调度器接住了一次与该任务有关的 panic。
//
// 最常见的是任务体调用（PreExecution、Execute、PostExecution）里的 panic，
// 它按本次尝试的失败处理、交给重试策略。任务体之外的路径（依赖判定、
// RetryPolicy() 等）逃出来的 panic 同样归入它，那种情况 Attempt 为 0。
//
// 任务体运行在独立的 goroutine 中，panic 若不被拦截会直接
// 终止整个进程，因此调度器会把它 recover 下来，转换成本次尝试的失败。
var ErrTaskPanic = errors.New("task panicked")

// PanicError 描述一次被调度器接住的 panic，用 errors.AsType 取出即可拿到
// panic 值与调用栈：
//
//	if pe, ok := errors.AsType[*xdag.PanicError](err); ok {
//	    log.Printf("%s 第 %d 次尝试 panic: %v", pe.Task, pe.Attempt, pe.Value)
//	    log.Print(string(pe.Stack))
//	}
//
// errors.Is(err, ErrTaskPanic) 同样成立。
type PanicError struct {
	// Task 是发生 panic 的**任务名**，类型是 string 不是 ITask。
	//
	// 它一度被机械重命名成 ITask：字段名跟着类型名走是错的，跟着语义走才对。
	Task string
	// Attempt 是第几次尝试（从 1 开始）。panic 发生在任务体之外的路径
	// （例如依赖判定）时为 0。
	Attempt int64
	// Value 是 recover() 拿到的原始值，没有做任何转换。
	Value any
	// Stack 是 panic 现场的调用栈。
	Stack []byte
}

// Error 刻意**不含**调用栈：错误文案会被日志逐条打印，把上千字节的栈塞进去
// 会让一次 panic 淹掉整屏，聚合多个失败任务时更甚。要栈就读 Stack 字段。
func (e *PanicError) Error() string {
	if e.Attempt == 0 {
		return fmt.Sprintf("task %s panicked: %v", e.Task, e.Value)
	}
	return fmt.Sprintf("task %s panicked on attempt %d: %v", e.Task, e.Attempt, e.Value)
}

// Is 让 errors.Is(err, ErrTaskPanic) 对 PanicError 成立。
func (e *PanicError) Is(target error) bool { return target == ErrTaskPanic }

// Unwrap 在 panic 的值本身就是 error 时把它透出来，这样
// panic(fmt.Errorf("...: %w", xdag.ErrNonRetryable)) 里携带的哨兵
// 与 return 回来的同一个 error 行为一致，不会因为「换了个抛出方式」就失效。
func (e *PanicError) Unwrap() error {
	if err, ok := e.Value.(error); ok {
		return err
	}
	return nil
}

// ErrTaskAbandoned 表示任务的 goroutine 在没有产出任何结果的情况下终止了。
// 最常见的原因是任务体里调用了 runtime.Goexit——例如在任务里调用
// testing.T.Fatal。它不是 panic，recover 拦不住，因此调度器改用「终态仍是
// 零值」来识别这种情况，把任务记为 StateFailed 并上报，而不是让整棵下游
// 子树被静默丢弃。
var ErrTaskAbandoned = errors.New("task goroutine exited without a result")

// ErrObserverPanic 表示 WithObserver 注册的回调发生了 panic。观察者是
// 调用方代码，它出问题不该打垮整个进程，但也不该被静默吞掉，因此转成一条
// 错误汇入 Execute 的返回值。任务的终态不受影响——触发回调时它早已落定。
var ErrObserverPanic = errors.New("observer panicked")

// ErrRunCanceled 表示任务是因为**整场执行被 Cancel 终止**而结束的，
// 区别于 ErrTaskCanceled——那是 CancelTask 只针对这一个任务发起的。
// Cancel 与 CancelTask 都通过取消任务专属的 context 生效，那是唯一能伸进
// 正在执行的任务体的通路；两者的区别在于随后等不等任务体返回——
// Cancel 在宽限期耗尽后不等了。
var ErrRunCanceled = errors.New("run canceled")

// ErrTaskCanceled 表示任务被 CancelTask 取消，因而没有开始（或没有继续）
// 执行。取消会取消这个任务专属的 context，正在执行的那次尝试同样收到通知；
// 但调度器仍然等它自己返回——能不能及时停下取决于任务体检不检查 ctx。
var ErrTaskCanceled = errors.New("task canceled")

// ErrUnknownTask 表示 CancelTask/SuspendTask/ResumeTask/SuspendedTask
// 收到了一个不存在于本次构建的任务表中的任务名。
var ErrUnknownTask = errors.New("unknown task")

// ErrTaskNameMismatch 表示某个任务的 Name() 与它在任务表里的键不一致。
//
// 这两者必须相同：调度与控制 API 全都按键寻址，不一致会让按 Name() 发出的
// CancelTask/SuspendTask/ResumeTask 全部拿到 ErrUnknownTask，
// 而任务本身照常执行——表现是控制指令静默失效，极难定位。
var ErrTaskNameMismatch = errors.New("task name does not match its key")

// ErrTaskAlreadyDone 表示对一个已经处于终态（成功/跳过/取消/失败）的
// 任务调用了 CancelTask/SuspendTask/ResumeTask——这类操作只对尚未结束
// 的任务有意义，调度器不会静默忽略。
var ErrTaskAlreadyDone = errors.New("task already done")
