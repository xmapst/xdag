// query.go —— 只读查询面：执行期间与结束后都可以并发调用。
//
// 它们一律返回**快照**而不是内部结构的引用——调用方拿到之后随便怎么读、
// 怎么改都不会影响调度，也不会踩到并发。

package xdag

import "maps"

// States 返回全部任务当前状态的一份快照，可在执行过程中或结束后并发调用。
func (d *Scheduler) States() map[string]State {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]State, len(d.states))
	maps.Copy(out, d.states)
	return out
}

// TaskResult 是单个任务在本次执行中的结果摘要。
//
// 它刻意不包含任务的 output：那已经在 Execute 的返回值里了，存两份等于
// 两个真相来源。这里只放调度器独家掌握、调用方从 ITask 接口这一侧原理上
// 拿不到的信息——被跳过的任务、以及一次尝试都没发起过就被取消的任务，
// Pre/Execute/PostExecution 一次都不会触发，任务自己什么也看不到；
// 执行途中被取消的任务则已经触发过。
type TaskResult struct {
	// State 是任务的终态，与 State(name) 返回的一致。
	State State

	// Err 是这个任务上报进 Execute 聚合错误里的那个 error，为 nil 的情形
	// 比「成功」多：
	//   - StateSuccess：成功，没有错误
	//   - StateSkipped：依赖没跑成，任务本身没出错
	//   - StateCanceled 且取消是从上游级联下来的：Err 恒为 nil，
	//     带 error 的是最初被 CancelTask 点名的那个任务
	//   - StateCanceled 且源自整场取消：只有第一个命中的任务带 error，
	//     其余为 nil——这与 Execute 的聚合口径一致，避免同一个取消原因
	//     刷屏，见 cancellation
	//
	// 所以判断一个任务有没有被取消要看 State，不能靠 Err 是否为 nil。
	Err error

	// Attempts 是调度器**发起过**的尝试次数。计数发生在每次尝试调用
	// PreExecution 之前，因此 PreExecution 自己 panic 的那次尝试也计入。
	//
	// 一次尝试都没发起过的任务为 0，这类任务的终态可以是 StateSkipped
	// （被跳过）、StateCanceled（预取消，或在挂起等待、并发名额排队期间被
	// 取消），**也可以是 StateFailed**——任务体之外的路径 panic 时就是这样
	// ，那次 panic 记在 PanicError.Attempt=0 上。
	//
	// 反过来，**执行途中或退避等待中被取消**的任务 Attempts 会 >= 1。
	// 总之不要按终态反推 Attempts，无论是从 StateCanceled 还是 StateFailed。
	//
	// 这个数字此前只存在于 retry.go 的错误文案里，程序拿不到。
	Attempts int64
}

// Results 返回全部任务结果摘要的一份快照，执行过程中与结束后都可以并发调用。
//
// 它补上的是错误归属：Execute 返回的是 errors.Join 聚合，errors.As 在上面
// 只会命中 N 个失败中的一个，任务名此前只活在错误文案的字符串里。用这个
// 方法可以按任务名直接拿到「它是什么终态、报了什么错、试了几次」。
func (d *Scheduler) Results() map[string]TaskResult {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]TaskResult, len(d.taskResults))
	maps.Copy(out, d.taskResults)
	return out
}

// State 返回单个任务当前的状态；任务名不存在于本次构建的任务表中时
// 返回 StatePending。
func (d *Scheduler) State(name string) State {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.states[name]
}

// Suspended 报告整场执行当前是否处于挂起中。
//
// 它回答的是"有没有按下过整场暂停"，不是"当前有没有任务停着"——
// 后者要遍历 States() 的 key 逐个调 TaskSuspended。
func (d *Scheduler) Suspended() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.runSuspended
}

// Canceled 报告是否调用过 Cancel。
func (d *Scheduler) Canceled() bool { return d.cancelRequested.Load() }

// TaskSuspended 报告某个任务当前是否处于挂起等待中，Execute 之前调用同样
// 有效（反映预挂起）。任务名不存在或任务已经处于终态时返回 false。
func (d *Scheduler) TaskSuspended(name string) bool {
	ctrl, state, err := d.lookupControl(name)
	if err != nil || state.Done() {
		return false
	}
	return ctrl.suspended()
}
