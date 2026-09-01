// state.go —— State：单个任务的状态枚举。

package xdag

// State 表示一个任务在本次执行中的终态。
//
// 调度语义只有一条规则：全部依赖成功，任务才执行。因此「没有成功」有三种
// 成因：依赖没跑成（StateSkipped）、被取消（StateCanceled，可能来自
// CancelTask 的单任务取消、Cancel 的整场取消，或传给 Execute 的 ctx
// 取消/超时）、自身执行失败（StateFailed）。
type State uint8

const (
	// StatePending 任务还没有终态。它涵盖三种情况：还在等依赖完成、
	// 被 SuspendTask 单独挂起或被 Suspend 整场挂起、以及**正在执行中**——
	// 调度器不为「运行中」单独
	// 设值，因此不能用它区分「还没开始」和「跑到一半」。
	// 整场执行的阶段用 Phase 查询。
	StatePending State = iota

	// StateSuccess 任务执行成功，输出已写入结果集，也会作为 input 提供给
	// 依赖它的下游任务。
	StateSuccess

	// StateSkipped 至少一个直接依赖未成功，任务体没有被调用，
	// PreExecution/Execute/PostExecution 一次都不会触发。
	//
	// 成因只有两种：依赖自身失败（StateFailed），或依赖同样因为它的依赖
	// 未成功而被跳过（StateSkipped）。**依赖被取消不在其中**——那种情况
	// 下游记的是 StateCanceled 而不是 StateSkipped，取消沿依赖链原样传播。
	// 因此「图中出现 StateSkipped」必然蕴含「图中存在 StateFailed」，
	// Phase 的整场判定正是据此可以完全无视 StateSkipped。
	//
	// 没有依赖的根任务不受这条规则约束，除非被 CancelTask 单独取消、被
	// Cancel 整场取消，或传给 Execute 的 ctx 已经取消/超时，否则总会被执行。
	StateSkipped

	// StateCanceled 因传给 Execute(ctx, ...) 的 context 被取消或超时，
	// 任务未执行或执行途中被打断。
	//
	// 它与 StateFailed 是两回事：取消是调用方主动叫停，不是任务本身出了问题；
	// 也与 StateSkipped 不同：取消沿依赖链传播时同样记为 StateCanceled，
	// 不会被误报成“依赖没跑成”。
	StateCanceled

	// StateFailed 任务体执行失败——Execute 返回了 error、Execute 或前后
	// 回调发生了 panic，并且用尽了 RetryPolicy 允许的全部尝试次数。
	StateFailed
)

// String 实现 fmt.Stringer，用于日志与调试输出。
func (s State) String() string {
	switch s {
	case StatePending:
		return "pending"
	case StateSuccess:
		return "success"
	case StateSkipped:
		return "skipped"
	case StateCanceled:
		return "canceled"
	case StateFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Done 报告任务是否已经离开 StatePending，进入四种终态之一。
func (s State) Done() bool { return s != StatePending }
