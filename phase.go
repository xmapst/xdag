// phase.go —— Phase：一次执行整体所处的阶段，与描述单个任务的 State 是两个层级。

package xdag

// Phase 表示**一次执行整体**处在什么阶段，与描述单个任务的 State 是两个
// 层级：State(name)/States() 回答「这个任务怎么样」，Phase() 回答「这场
// 执行怎么样」。
//
// 两个枚举的取值集合刻意不同，也刻意用了不同的词：PhaseRunning 在任务级
// 不存在，StateSkipped 在整场级不可能出现（原因见 Scheduler.Phase 的文档）。
// 更实际的理由是 State 不能扩容——CancelTask/SuspendTask/ResumeTask 全都
// 靠 State.Done()（定义为 s != StatePending）拒绝已结束的任务，往 State 里
// 加一个「运行中」的值会让这几个方法对**活着**的任务返回 ErrTaskAlreadyDone。
type Phase uint8

const (
	// PhasePending 还没有调用过 Execute。
	PhasePending Phase = iota

	// PhaseRunning Execute 进行中，还有任务没有进入终态。
	PhaseRunning

	// PhaseSuccess 全部任务都是 StateSuccess。空任务表执行后同样是它。
	PhaseSuccess

	// PhaseCanceled 没有任务失败，但至少一个任务是 StateCanceled。
	PhaseCanceled

	// PhaseFailed 至少一个任务是 StateFailed。
	PhaseFailed
)

// String 实现 fmt.Stringer，用于日志与调试输出。
func (p Phase) String() string {
	switch p {
	case PhasePending:
		return "pending"
	case PhaseRunning:
		return "running"
	case PhaseSuccess:
		return "success"
	case PhaseCanceled:
		return "canceled"
	case PhaseFailed:
		return "failed"
	default:
		return "unknown"
	}
}

// Done 报告本次执行是否已经结束，即 Phase 是否落在三个终值之一。
func (p Phase) Done() bool {
	switch p {
	case PhaseSuccess, PhaseCanceled, PhaseFailed:
		return true
	default:
		return false
	}
}

// Phase 返回本次执行的整体阶段，执行期间与结束后都可以并发调用。
//
// 它只沿 PhasePending → PhaseRunning → 三个终值之一单向推进，永不回退，
// 也不会在执行途中提前给出终值：只要还有任务没进终态，它一律是
// PhaseRunning。一旦 Done()，States/Results/ExecutionOrder 与 Execute 的
// 返回值都已经定型，之后再查恒定不变。
//
// 终值的判定规则（这是库钉死的口径，写在这里就是承诺，不是实现细节）：
//   - 任一任务 StateFailed       → PhaseFailed
//   - 否则任一任务 StateCanceled → PhaseCanceled
//   - 否则                       → PhaseSuccess（空任务表同样落在这里，
//     与 Execute 对空图返回 nil error 一致）
//
// StateFailed 优先于 StateCanceled：resolve 已经把「执行途中被取消打断」
// 判成了 StateCanceled，所以残留下来的 StateFailed 是调用方事先并不知道
// 的真实故障，而取消是调用方自己发起的、本来就知道的事——先报调用方不
// 知道的那一件。注意这与 snapshot 里「依赖为 StateCanceled 时压过
// StateSkipped」的方向相反：那里回答「我为什么没跑」，取最近因；这里回答
// 「这场健不健康」，取最重症。
//
// StateSkipped 不参与判定，因为「有 Skipped 而没有 Failed」的终局不存在。
// 论证：跳过只可能由某个直接依赖处于 StateSkipped 或 StateFailed 引起——
// 依赖是 StateCanceled 时下游会被判成 StateCanceled 而不是 StateSkipped，
// 所以取消不会产生跳过。于是从任一 StateSkipped 出发沿依赖链上溯，每一步
// 都落在 {Skipped, Failed} 里；图无环保证这条链有限，而链的起点不可能还是
// Skipped（没有依赖就不会被跳过），因此必然终止在一个 StateFailed 上。
//
// 因为某个任务被 SuspendTask 挂起而停滞不动的执行报告 PhaseRunning：
// 调度器只知道「还没跑完」，无法知道「跑不完了」——任务体自己阻塞、配了
// 无限重试、和被挂起，这三者在调度器看来完全一样。要区分停滞原因，遍历
// States() 的 key 逐个调 TaskSuspended。
func (d *Scheduler) Phase() Phase {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.phase
}

// setPhase 在 mu 下推进阶段，只在 Execute 开头调用一次。
func (d *Scheduler) setPhase(p Phase) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.phase = p
}

// settle 汇总全部任务的终态，得出本次执行的终值。它在 Execute 的 defer 里
// 调用。正常路径下此时 wg.Wait 已经返回、states 不会再变；panic 展开时也会
// 跑到这里，那种情况由下面的 Pending 闸兜底。
func (d *Scheduler) settle() {
	d.mu.Lock()
	defer d.mu.Unlock()

	// 还有任务停在 Pending 就不结算。正常路径下 wg.Wait 返回后不可能有
	// Pending，但 settle 是 defer 调用，panic 展开时同样会跑到这里——例如
	// Execute(nil) 会在 bind 阶段 panic，那时一个任务都还没派生、states 全是
	// 零值，照常结算会得出 PhaseSuccess，谎报一个「成功」的终值。
	for _, s := range d.states {
		if s == StatePending {
			d.phase = PhaseRunning // 非正常展开：诚实地停在「还没有结论」
			return
		}
	}

	d.phase = PhaseSuccess
	for _, s := range d.states {
		switch s {
		case StateFailed:
			d.phase = PhaseFailed
			return
		case StateCanceled:
			d.phase = PhaseCanceled
		default:
		}
	}
}
