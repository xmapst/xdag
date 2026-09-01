// retry.go —— 重试策略（RetryPolicy）与驱动单个任务全部重试尝试的 retryExecutor。
//
// 退避、抖动、挂起门、不可重试错误的判定都在这里。任务级的时间预算不在
// 这里——这个库不提供，要给任务限时请在传进 Execute 的 ctx 上加 deadline。

package xdag

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"time"
)

// ErrNonRetryable 供任务声明「这次失败不必再重试」：把它包进返回的 error
// 里，调度器就会立刻放弃剩余的尝试次数，不再等待退避。
//
//	if resp.StatusCode == http.StatusBadRequest {
//	    return nil, fmt.Errorf("bad request: %w", xdag.ErrNonRetryable)
//	}
//
// 判据是「错误长什么样」而不是「已经试了几次」，所以判断权完整留在业务
// 侧——库只对返回的 error 做一次 errors.Is。重试一个永久性错误除了把
// 退避时间耗满，不会有别的结果。
var ErrNonRetryable = errors.New("non-retryable error")

// InfiniteAttempts 用作 RetryPolicy.MaxAttempts，表示一直重试到成功或
// context 被取消为止。
//
// 它必须显式写出来。零值不是无限重试——一个只填了 Interval 的策略
// （如 &RetryPolicy{Interval: time.Second}）只会执行一次，不会永不放弃。
const InfiniteAttempts int64 = -1

// RetryPolicy 描述单个任务失败后的重试行为：等待多久再试一次、最多试几次。
//
// 未显式设置的字段会在执行前补上默认值：Interval 1s、MaxInterval 30s、
// Multiplier 2.0；MaxAttempts 的零值等同于 1（只执行一次，不重试）。
// 因此零值 RetryPolicy{} 与 nil 效果相同。
type RetryPolicy struct {
	// MaxAttempts 是**总执行次数**而非重试次数：1 表示只执行一次、
	// 失败不重试；N 表示最多执行 N 次，即最多重试 N-1 次。
	// 零值等同于 1。
	//
	// 任意负数都表示无限重试，但请写 InfiniteAttempts 而不是随手写个负数——
	// 前者一眼看得出意图，后者读起来像笔误。
	MaxAttempts int64 `json:"maxAttempts" yaml:"maxAttempts"`
	// Interval 是第一次重试前的等待时间。
	Interval time.Duration `json:"interval" yaml:"interval"`
	// Multiplier 是每次重试后等待时间的放大倍数（指数退避），
	// 零值或负数等同于 2.0。取值在 (0, 1) 区间会让等待时间逐次收敛到 0
	// 而非发散，这同样是合法用法。
	Multiplier float64 `json:"multiplier" yaml:"multiplier"`
	// MaxInterval 是退避等待时间的上限，同时也受全局硬上限 150s 约束，
	// 取两者中更小的一个。
	MaxInterval time.Duration `json:"maxInterval" yaml:"maxInterval"`
	// Jitter 是退避时间的随机抖动幅度，取值 [0, 1]，超出范围会被钳制。
	// 零值（默认）表示不抖动，退避时间完全确定。
	//
	// 实际等待时间在 [backoff*(1-Jitter), backoff] 之间随机取值——只向下
	// 抖，因此绝不会突破 MaxInterval 与硬上限。
	//
	// 建议在扇出较宽的图里打开：同一层的任务会在同一毫秒被派生，失败后
	// 又按同一条确定性公式退避，于是整齐地在同一时刻重试，把下游打成
	// 尖峰。这种同步性是拓扑强加的，不是巧合。
	Jitter float64 `json:"jitter" yaml:"jitter"`
}

// hardMaxInterval 是退避等待时间的硬上限，任何策略配置都不会超过它。
const hardMaxInterval = 150 * time.Second

// retryExecutor 是单个任务一次执行（含全部重试尝试）的驱动器，
// 由 Scheduler 在每次调度任务时构造，调用方不需要直接使用它。
type retryExecutor struct {
	// policy 是补齐默认值后的**副本**，不是调用方传进来的那个指针。
	policy RetryPolicy
	// gate 支撑 SuspendTask/ResumeTask：每次尝试开始之前都会检查它，
	// 见 run。
	gate *taskControl
}

// newRetryExecutor 按值拷贝一份策略再补默认值，构造出一个可用的 retryExecutor。
//
// task.RetryPolicy() 返回的指针属于调用方，而且完全可能被多个任务共享
// （例如一个包级的默认策略对象）。就地改写这个指针既会在多个任务并发执行时
// 产生数据竞争，也会静默篡改调用方的配置，因此这里只读它一次、按值拷贝，
// 从不写回。
func (d *Scheduler) newRetryExecutor(policy *RetryPolicy, ctrl *taskControl) *retryExecutor {
	effective := RetryPolicy{MaxAttempts: 1} // 默认策略：只执行一次
	if policy != nil {
		effective = *policy
	}
	if effective.Interval <= 0 {
		effective.Interval = 1 * time.Second
	}
	if effective.MaxInterval <= 0 {
		effective.MaxInterval = 30 * time.Second
	}
	if effective.MaxInterval > hardMaxInterval {
		effective.MaxInterval = hardMaxInterval
	}
	// 与 Jitter 同理：NaN 和 <=0 的比较都是 false，不显式挡就会一路穿到
	// math.Pow，让退避从第二次起直接顶到 MaxInterval。+Inf 同理。
	if math.IsNaN(effective.Multiplier) || math.IsInf(effective.Multiplier, 0) || effective.Multiplier <= 0 {
		effective.Multiplier = 2.0
	}
	// NaN 必须显式挡掉：它与 0、1 的任何比较都是 false，会原样穿过下面两条
	// 钳制，也穿过 applyJitter 的 `<= 0` 短路，最终让 float64→Duration 的转换
	// 塌成负值——退避被完全跳过，配 InfiniteAttempts 就是一个无间隔的热循环。
	if math.IsNaN(effective.Jitter) || effective.Jitter < 0 {
		effective.Jitter = 0
	}
	if effective.Jitter > 1 {
		effective.Jitter = 1
	}
	if effective.MaxAttempts == 0 {
		// 零值按「只执行一次」处理。把未填写的字段解释成「永不放弃」是个陷阱：
		// &RetryPolicy{Interval: time.Second} 看起来只是想设个间隔，
		// 却会让任务在失败时永远重试下去。
		effective.MaxAttempts = 1
	}
	return &retryExecutor{policy: effective, gate: ctrl}
}

// run 按策略反复调用 fn，直到下列任一条件成立：
//   - fn 返回 nil（成功）
//   - fn 返回的 error 里包着 ErrNonRetryable（业务声明重试无用，立即放弃）
//   - 用尽 MaxAttempts 次尝试（配了 InfiniteAttempts 时只剩下面两条能终止它）
//   - ctx 被取消或超时——检查点在每次尝试之前、挂起等待期间、并发名额排队
//     期间、退避等待期间
//   - 整场取消（Scheduler.Cancel）——它取消每个任务专属的 context，因此同样
//     覆盖上述全部等待点。注意这一层返回的仍是包着 ctx.Err() 的错误，
//     ErrRunCanceled 只作为 context 的 cause 传递，由 schedule.go 的
//     cancellation 读出来
//
// fn 每次被调用都会收到本次执行的 ctx 与从 1 开始的 attempt 序号。
//
// 这里**没有**任务级的时间预算。想给任务限时就在传进 Execute 的 ctx 上加
// deadline：那条 deadline 会沿着 context 树传到每一个任务，语义与 Go 的其余
// 部分完全一致，也不需要库再发明一套。库里曾经有过一个 per-task 的预算，
// 但它带来的问题比解决的多——最典型的是它按墙钟计时，于是被 SuspendTask
// 按住的任务会在没人看的时候把预算耗光，解挂时任务体一次都没跑就判死，
// 而挂起恰恰是人在介入排查的时候。
func (r *retryExecutor) run(ctx context.Context, taskName string, fn func(ctx context.Context, attempt int64) error) error {
	maxAttempts := r.policy.MaxAttempts
	infiniteRetry := maxAttempts < 0 // 只有显式的 InfiniteAttempts（负数）才是无限重试

	var lastErr error
	var attempt int64
	for attempt = 1; infiniteRetry || attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return wrapWait(taskName, "canceled before attempt", ctx.Err(), lastErr)
		default:
		}

		// 每次尝试开始之前都要过一遍挂起门：不在挂起中立即放行；挂起中就
		// 停在这里，直到 ResumeTask/Resume 放行或 ctx 被取消。SuspendTask/
		// ResumeTask 可以在同一个任务上反复交替调用——每次挂起都是全新
		// 判定，不依赖之前是否挂起过。
		// 挂起等待必须能被 Cancel 叫醒，否则一个被挂起的任务会让 Execute
		// 永远等下去——挂起门是无限期的，没有别的东西会放行它。
		if err := r.gate.wait(ctx); err != nil {
			return wrapWait(taskName, "interrupted while suspended", err, lastErr)
		}

		if err := fn(ctx, attempt); err == nil {
			return nil // 成功
		} else if lastErr == nil || !isInterruption(err) {
			// 取消类错误不覆盖已经拿到的真实根因：一次尝试可能仅仅因为
			// 在并发闸门上排队时被取消而返回 ctx.Err()，那不是任务的失败，
			// 覆盖掉上一次的业务错误/panic 会让根因彻底消失。
			lastErr = err
		}

		// 业务显式声明这个错误重试也没用：立刻放弃，既不等待也不再尝试
		if errors.Is(lastErr, ErrNonRetryable) {
			return fmt.Errorf("task %s failed with non-retryable error on attempt %d: %w", taskName, attempt, lastErr)
		}

		// 有限重试且已经是最后一次尝试，不用再等待，直接结束循环上报
		if !infiniteRetry && attempt == maxAttempts {
			break
		}

		waitTime := r.calculateBackoff(attempt, r.policy.MaxInterval)
		timer := time.NewTimer(waitTime)
		select {
		case <-ctx.Done():
			timer.Stop()
			return wrapWait(taskName, "canceled during retry wait", ctx.Err(), lastErr)
		case <-timer.C:
			// 继续下一次尝试
		}
	}

	// 文案里不放次数。原来报的是配置的 maxAttempts，那根本不是任何计数器；
	// 改报循环变量也不对——「卡在并发闸门上从未越过」的那次迭代，循环变量
	// 算它一次，而 TaskResult.Attempts 不算（它在拿到名额之后才计数）。
	// 两个数字测量的本就是不同的东西，把其中一个塞进文案只会制造第二个
	// 真相来源。要次数请读 TaskResult.Attempts，那是文档承诺的权威值。
	return fmt.Errorf("task %s exhausted retries: %w", taskName, lastErr)
}

// isInterruption 判断错误是否只是「等待被打断」，而不是任务本身的失败。
func isInterruption(err error) bool {
	return errors.Is(err, context.Canceled) ||
		errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, ErrTaskCanceled) ||
		errors.Is(err, ErrRunCanceled)
}

// wrapWait 统一「尝试之前的等待被打断」这类错误的措辞：说明在哪一步被打断、
// 是被什么打断的，并且**始终带上**上一次尝试的真实失败原因。
//
// 少了 last 那一半，被 ctx 掐断或被 Cancel 的任务就只剩一句「deadline
// exceeded」，PanicError、业务哨兵、ErrNonRetryable 全都取不回来——而这几条
// 等待路径恰恰是这类任务最常见的退出口。
func wrapWait(taskName, where string, cause, last error) error {
	if last == nil {
		return fmt.Errorf("task %s %s: %w", taskName, where, cause)
	}
	return fmt.Errorf("task %s %s: %w (last attempt error: %w)", taskName, where, cause, last)
}

// calculateBackoff 按指数退避公式计算第 attempt 次失败后应等待的时间：
// Interval * Multiplier^(attempt-1)，钳制在 [0, maxInterval] 区间内，
// 最后按 Jitter 施加随机抖动。
func (r *retryExecutor) calculateBackoff(attempt int64, maxInterval time.Duration) time.Duration {
	backoff := float64(r.policy.Interval) * math.Pow(r.policy.Multiplier, float64(attempt-1))

	var wait time.Duration
	switch {
	// 钳制必须在浮点域完成。backoff 一旦超过 int64 上限（约 9.22e18），
	// time.Duration(backoff) 会绕回负数，让「result > maxInterval」恒为假，
	// 退避上限被静默绕过，重试退化成无间隔的忙循环。
	// 默认 Interval=1s、Multiplier=2 时约第 35 次尝试就会触发，无限重试场景下
	// 会一直保持。
	case math.IsNaN(backoff) || backoff >= float64(maxInterval):
		wait = maxInterval
	case backoff <= 0:
		// Multiplier < 1 时 backoff 会随 attempt 单调递减到 0，这是正常收敛，
		// 不是异常——不能借用 maxInterval 当下限，否则序列会在触底后突然跳回上限。
		wait = 0
	default:
		wait = time.Duration(backoff)
	}
	return r.applyJitter(wait)
}

// applyJitter 把等待时间随机下调到 [wait*(1-Jitter), wait] 区间内。
//
// 只向下抖不向上抖，是为了让 MaxInterval 与硬上限保持是真正的上限——
// 对称抖动会让实际等待时间越过那条线。抖动也要覆盖被钳到 maxInterval 的
// 情形：一批同时失败的任务恰恰最容易一起顶在上限上，那里正是最需要打散的
// 地方。
func (r *retryExecutor) applyJitter(wait time.Duration) time.Duration {
	if r.policy.Jitter <= 0 || wait <= 0 {
		return wait
	}
	return time.Duration(float64(wait) * (1 - r.policy.Jitter*rand.Float64()))
}
