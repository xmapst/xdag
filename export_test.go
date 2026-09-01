// export_test.go —— 只对同包测试开放的内部钩子。

package xdag

import "time"

// 本文件把若干内部实现暴露给外部测试包（package xdag_test）。
// _test.go 不参与正式构建，因此不会扩大公开 API。
//
// 退避计算尤其需要直接测：只看整场耗时的话，「对称抖动」和「删掉上钳制」
// 这两种改坏方式都能蒙混过关——前者均值不变，后者只是让等待变短。
// 逐次取值才能断言「抖动只向下、绝不越过基准值」。

// NewRetryExecutorForTest 按给定策略构造一个 retryExecutor（含默认值补齐与钳制）。
func NewRetryExecutorForTest(p RetryPolicy) *retryExecutor {
	return (&Scheduler{}).newRetryExecutor(&p, nil)
}

// CalculateBackoff 暴露单次退避时间的计算。
func (r *retryExecutor) CalculateBackoff(attempt int64, maxInterval time.Duration) time.Duration {
	return r.calculateBackoff(attempt, maxInterval)
}

// EffectiveJitter 返回补齐/钳制之后真正生效的 Jitter。
func (r *retryExecutor) EffectiveJitter() float64 { return r.policy.Jitter }
