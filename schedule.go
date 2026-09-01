// schedule.go —— 调度引擎：一个任务从入度归零被派生，到终态落定、下游放行之间的全部环节。
//
// runTask ─ safeResolve ─ resolve ─┬─ snapshot   （依赖够不够、输入是什么）
//                                  └─ executeTask ─ acquire/release（并发闸门）
//                                                 └─ retryExecutor（见 retry.go）
//                                  ↓
//                                commit（写终态 + 派生下游）
//
// abandon 是旁路：Cancel 的宽限期耗尽时从外部替任务落终态，见 control.go。

package xdag

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"runtime/debug"
)

// runTask 判定单个任务的终态、按需真正执行它，并推进下游调度。
// 它总是在自己的 goroutine 里运行——调用方是 Execute 的根任务派发，
// 或 commit 里对下游的派发。
func (d *Scheduler) runTask(name string, errCh chan error) {
	// 注意这里没有 defer d.wg.Done()：计数在 commit 里减，且由 settled
	// 这道 CAS 闸保证恰好一次。Cancel 放弃等待时会从外部替这个任务
	// commit，那之后这个 goroutine 事后返回时的 commit 会被挡掉——若这里
	// 还各自 Done 一次，计数会变负，直接 panic。
	ctrl := d.controls[name]
	defer ctrl.release()

	var (
		state  State
		output map[string]any
		err    error
	)

	// commit 必须放在 defer 里。无论成功、跳过、取消还是失败都要走到它，
	// 否则下游入度永远减不到 0，整棵子树被静默丢弃——而且 Execute 连错都
	// 不会报。写在函数体里漏掉的是 runtime.Goexit 这条路径：它不是 panic，
	// recover 拦不住，但它会跑完全部 defer。任务体里调用 testing.T.Fatal
	// 就会走到这里。
	defer func() {
		// resolve 的任何正常出口都不会返回 StatePending，所以 state 还停在
		// 零值就说明 safeResolve 没有正常返回过
		if state == StatePending {
			state = StateFailed
			err = fmt.Errorf("task %s: %w", name, ErrTaskAbandoned)
			select {
			case errCh <- err:
			default:
			}
		}
		d.commit(name, state, output, err, errCh)
	}()

	state, output, err = d.safeResolve(name)
	if err != nil {
		select {
		case errCh <- err:
		default:
		}
	}
}

// safeResolve 包住 resolve，把任何逃逸出来的 panic 转成本任务的失败。
//
// 任务体与用户回调都在独立的 goroutine 中运行，panic 逃出去既无法被
// Execute 的调用方 recover，也会让 commit 永远执行不到、整棵下游子树
// 随之丢失。任务体自己以及 Pre/PostExecution 的 panic 由 executeTask
// 就地接住（因而受重试策略约束），这里兜的是其余路径（依赖判定等）。
func (d *Scheduler) safeResolve(name string) (state State, output map[string]any, err error) {
	defer func() {
		if r := recover(); r != nil {
			state, output = StateFailed, nil
			err = &PanicError{Task: name, Value: r, Stack: debug.Stack()}
		}
	}()
	return d.resolve(name)
}

// resolve 判定任务的终态：先看这个任务专属的 ctx 是否已被取消（可能源自
// 整场 Cancel、父 context 整体取消/超时，也可能是单独针对它的
// CancelTask），再看依赖是否
// 全部成功，都通过才真正执行任务体。
func (d *Scheduler) resolve(name string) (State, map[string]any, error) {
	ctrl := d.controls[name]

	select {
	case <-ctrl.ctx.Done():
		return d.cancellation(ctrl, nil)
	default:
	}

	task := d.tasks[name]

	blocked, inputs := d.snapshot(name)
	if blocked != StatePending {
		return blocked, nil, nil
	}

	output, err := d.executeTask(ctrl, task, inputs)
	if err != nil {
		// 执行途中被取消打断，不算任务自身的失败
		if ctrl.ctx.Err() != nil && d.isCancellation(err) {
			return d.cancellation(ctrl, err)
		}
		// 任务体自己返回了包着 ErrRunCanceled 的错误时，同样不算它自身的失败——
		// 整场 Cancel 现在走 ctx，绝大多数情况由上一条分支接走
		if errors.Is(err, ErrRunCanceled) {
			return StateCanceled, nil, err
		}
		return StateFailed, nil, err
	}
	return StateSuccess, output, nil
}

// cancellation 把取消统一成 StateCanceled。
//
// ctrl.own 为 true 时，这次取消是专门针对这一个任务发起的
// （CancelTask），单独生成一条错误，不去重——这类调用通常只是零星取消
// 个别任务，不是雪崩。否则说明这次取消不是针对单个任务发起的——整场
// Cancel，或父 context 整体取消/超时向下传播，沿用 cancelReported 的 CAS 保证整场执行只上报一条错误：取消会
// 让所有尚未完成的任务同时命中这条路径，逐个上报只会把真正的根因淹在
// 一堆同质噪音里。
//
// 两条路径都只需要 context.Cause(ctrl.ctx)：ctrl.ctx 是父 context 的
// 子 context，因父 context 取消而级联取消时，Cause 会透传父 context
// 的取消原因。
func (d *Scheduler) cancellation(ctrl *taskControl, underlying error) (State, map[string]any, error) {
	cause := context.Cause(ctrl.ctx)
	// underlying 是重试层一路带上来的「最后一次尝试到底错在哪」。这里必须
	// 转发出去：重新造一个只讲取消的错误，会把 PanicError、业务哨兵、
	// ErrNonRetryable 全部丢掉——而被取消打断的任务几乎总是从这条路退出。
	if ctrl.own.Load() {
		if underlying != nil {
			return StateCanceled, nil, fmt.Errorf("task %s canceled: %w (last attempt error: %w)", ctrl.name, cause, underlying)
		}
		return StateCanceled, nil, fmt.Errorf("task %s canceled: %w", ctrl.name, cause)
	}
	if d.cancelReported.CompareAndSwap(false, true) {
		if underlying != nil {
			return StateCanceled, nil, fmt.Errorf("execution canceled: %w (last attempt error: %w)", cause, underlying)
		}
		return StateCanceled, nil, fmt.Errorf("execution canceled: %w", cause)
	}
	// 已经报过一条了：这里仍然把根因带出去，否则整场取消时除第一个任务
	// 之外的失败原因会全部消失
	if underlying != nil {
		return StateCanceled, nil, underlying
	}
	return StateCanceled, nil, nil
}

// isCancellation 判断错误是否源自 context 取消或超时。
func (d *Scheduler) isCancellation(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// commit 写入任务终态，并推进下游调度：把每个下游的入度减一，
// 减到 0 的下游立即派生新的 goroutine 去调度。
//
// 这一步对全部四种终态一视同仁——不止 StateSuccess，StateSkipped/
// StateCanceled/StateFailed 同样会推进下游，否则下游的入度永远减不到 0，
// 会被静默挂起。
func (d *Scheduler) commit(name string, state State, output map[string]any, err error, errCh chan error) bool {
	// 终态只写一次。Cancel 放弃等待时会从外部先替任务落终态，
	// 被抛下的那个 goroutine 事后仍会走到这里——它的结果直接丢弃。
	if !d.controls[name].settled.CompareAndSwap(false, true) {
		return false
	}

	d.mu.Lock()

	d.states[name] = state
	result := TaskResult{
		State:    state,
		Err:      err,
		Attempts: d.controls[name].attempts.Load(),
	}
	d.taskResults[name] = result
	if state == StateSuccess {
		d.results.Store(name, output)
		d.executionOrder = append(d.executionOrder, name)
	}

	for _, child := range d.dependents[name] {
		d.inDegrees[child]--
		if d.inDegrees[child] == 0 {
			d.wg.Add(1)
			go d.runTask(child, errCh)
		}
	}
	d.mu.Unlock()

	// 观察者在锁外触发。代价是事件顺序不保证与因果顺序一致（下游在锁内就已
	// 派生，它的事件可能先到），换来的是一个慢回调只拖住自己这个 goroutine，
	// 不会卡住其他任务提交终态——而在锁内回调，调用方碰一下 States() 就死锁。
	d.notify(Event{Task: name, TaskResult: result}, errCh)

	// 放在最后：下游的 wg.Add 已经在锁内做完，这里再减自己这一份，
	// 计数不会提前归零
	d.wg.Done()
	return true
}

// executeTask 按任务的 RetryPolicy 反复调用一次任务体（PreExecution→
// Execute→PostExecution），直到成功或用尽重试次数。
func (d *Scheduler) executeTask(ctrl *taskControl, task ITask, inputs map[string]any) (map[string]any, error) {
	retryExecutor := d.newRetryExecutor(task.RetryPolicy(), ctrl)

	var result map[string]any
	err := retryExecutor.run(ctrl.ctx, ctrl.name, func(taskCtx context.Context, attempt int64) (err error) {
		// 并发闸门按单次尝试占用。收在这里而不是包住整个任务，是因为
		// 退避等待与挂起等待都发生在这个闭包之外——那两处占着槽位干等，
		// 一个 InfiniteAttempts 或者被挂起的任务就能永久霸占一个名额。
		//
		// 用 run 传进来的 taskCtx：它就是这个任务专属的 ctx，
		// 排队等名额期间被取消/超时能立刻退出，不会拿着一个早已失效的 ctx 开跑。
		if err := d.acquire(taskCtx); err != nil {
			return err
		}
		defer d.release()

		// 拿到名额、真正要开跑了才计数。放在 acquire 之前的话，一个卡在
		// 闸门上被取消的任务会报 Attempts=1，而它的 PreExecution 一次都没跑。
		ctrl.attempts.Store(attempt)

		// 任务体与前后回调的 panic 按「本次尝试失败」处理，交给重试策略正常走完，
		// 而不是让 panic 直接逃出这个 goroutine
		defer func() {
			if r := recover(); r != nil {
				err = &PanicError{
					Task:    ctrl.name,
					Attempt: attempt,
					Value:   r,
					Stack:   debug.Stack(),
				}
			}
		}()

		task.PreExecution(taskCtx, attempt, inputs)
		output, err := task.Execute(taskCtx, attempt, inputs)
		task.PostExecution(taskCtx, attempt, output, err)
		if err != nil {
			return err
		}
		result = output
		return nil
	})

	if err != nil {
		// 不在这里补包装：run 的文案已经带了任务名，也已经说清
		// 了结局（用尽重试 / 被取消 / 被停机）。再包一层既重复了任务名，
		// 又会在终态其实是 StateCanceled 时硬说成 failed。
		return nil, err
	}
	return result, nil
}

// abandon 从外部替一个任务落终态并放行下游，不再等它的 goroutine 返回。
//
// 这是 Cancel 与 CancelTask 的分界：取消只是通知，仍然等任务体
// 自己返回；强制停止通知之后就不等了。被抛下的 goroutine 还活着，它事后
// 的 commit 会被 settled 挡掉，结果丢弃——**这意味着 Execute 可能在那个
// goroutine 仍在运行时就返回**，任务体不尊重 ctx 的话就是永久泄漏。
// 这是「不等待」的固有代价，写在 Cancel 的文档里。
func (d *Scheduler) abandon(name string, err error) {
	d.mu.Lock()
	errCh := d.errCh
	d.mu.Unlock()
	if errCh == nil {
		return // Execute 还没开始，没有正在等待的东西可放弃
	}

	// 先 commit 再发错误，顺序不能反。
	//
	// commit 里的 settled CAS 是「这个任务的终态归谁写」的唯一裁决点，
	// 但它只挡得住状态写入——原来的顺序是先往 errCh 发、再 commit，
	// 于是一个已经成功落定的任务同样会被补上一条 ErrRunCanceled，
	// 最后出现在 Execute 的聚合错误里：明明跑成功了，却报它被取消。
	// 换成先 commit，抢不到终态就直接返回，连竞态窗口都没有。
	//
	// 非阻塞发送：errCh 满了就丢。这条错误是「被放弃」的补充说明，
	// 丢掉的代价远小于在这里把停机流程卡住。
	if !d.commit(name, StateCanceled, nil, err, errCh) {
		return
	}
	select {
	case errCh <- err:
	default:
	}
}

// acquire 取一个并发名额；不限制并发时直接放行。等待期间 ctx 被取消会
// 立即返回，避免整场取消之后还有任务干等在闸门上。
func (d *Scheduler) acquire(ctx context.Context) error {
	if d.sem == nil {
		return nil
	}
	// 先做一次非阻塞尝试：名额本来就空闲时不必进下面的 select，也避免
	// 在已经 Cancel 但名额充足的情况下被 ctx.Done() 抢先
	select {
	case d.sem <- struct{}{}:
		return nil
	default:
	}
	select {
	case d.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// release 归还一个并发名额，与 acquire 成对出现。
func (d *Scheduler) release() {
	if d.sem == nil {
		return
	}
	<-d.sem
}

// snapshot 在锁内判定某个任务的直接依赖是否已经全部成功，并在成功时
// 顺带取出任务执行所需的 input（依赖名 -> 依赖的输出）。
//
// 判定结果：
//   - 任一依赖处于 StateCanceled → 返回 StateCanceled，取消原样传播下去
//   - 否则只要有依赖未成功（自身失败，或同样因为它的依赖未成功而被跳过）
//     → 返回 StateSkipped
//   - 全部依赖都是 StateSuccess（或没有依赖）→ 返回 StatePending，
//     表示可以继续执行，并附带收集好的 input
func (d *Scheduler) snapshot(name string) (State, map[string]any) {
	deps := d.depOrder[name]

	// 这里不用 defer Unlock：下面的拷贝要在锁外做，解锁点是手写的
	d.mu.Lock()

	blocked := StatePending
	for _, dep := range deps {
		switch d.states[dep] {
		case StateSuccess:
			continue
		case StateCanceled:
			blocked = StateCanceled
		default:
			if blocked != StateCanceled {
				blocked = StateSkipped
			}
		}
	}
	if blocked != StatePending {
		d.mu.Unlock()
		return blocked, nil
	}

	// 锁内只把引用取出来，拷贝挪到锁外做。d.mu 是全图唯一一把锁，commit
	// 推进下游、States/Results/Phase 查询全都争它；把 O(下游数 × output
	// key 数) 的拷贝放在临界区里，会把本可并行的拷贝排成一队，顺带挡住
	// 整图的调度推进（实测宽扇出 + 大 output 时差出近一倍）。
	raw := make(map[string]any, len(deps))
	for _, dep := range deps {
		if value, ok := d.results.Load(dep); ok {
			raw[dep] = value
		}
	}
	d.mu.Unlock()

	// 顶层浅拷贝。同一个依赖的 output 会被喂给它的每一个下游，直接把那一个
	// map 分发出去的话，任一下游往 input 里写一个 key，就会同时污染上游的
	// 输出、Execute 返回的结果集，以及其他下游看到的内容；多个下游并发写
	// 还是数据竞争。
	//
	// 出锁做是安全的：output 只在 commit 里 Store 一次，之后调度器不再碰它，
	// 任务体也不得在返回后继续改（写在 ITask.Execute 的契约里）；results.Clear
	// 是 Execute 的 defer，必然在 wg.Wait 之后，而 snapshot 只跑在计数还没归零
	// 的任务 goroutine 里，两者不可能并发。
	//
	// 拷贝只做一层：嵌套的 map/slice 仍然共享，所以 input 里的值必须视为只读。
	inputs := make(map[string]any, len(deps))
	for dep, value := range raw {
		inputs[dep] = maps.Clone(value.(map[string]any))
	}
	return StatePending, inputs
}
