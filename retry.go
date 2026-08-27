package xdag

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"
)

type RetryPolicy struct {
	Interval    time.Duration `json:"interval" yaml:"interval"`
	MaxInterval time.Duration `json:"maxInterval" yaml:"maxInterval"`
	MaxAttempts int64         `json:"maxAttempts" yaml:"maxAttempts"`
	Multiplier  float64       `json:"multiplier" yaml:"multiplier"`
	// RetryIf 是可选的重试条件表达式，在每次失败后、真正发起下一次尝试之前求值。
	// 求值为 false 时立即放弃剩余尝试；空串表示无条件重试，与旧行为一致。
	//
	// 表达式中可以使用 Env.Error（最近一次失败的错误信息）与 Env.Attempt，例如：
	//
	//	Error matches "timeout|connection reset"
	//	not (Error contains "invalid argument")
	//
	// 该表达式在 New 阶段编译，运行期修改 RetryPolicy 不会生效。
	RetryIf string `json:"retryIf" yaml:"retryIf"`
}

type RetryExecutor struct {
	policy  *RetryPolicy
	retryIf Program
	env     *Env
	condErr ConditionErrorPolicy
}

func (d *Dagcuter) newRetryExecutor(policy *RetryPolicy, retryIf Program, env *Env) *RetryExecutor {
	if policy == nil {
		// 默认策略：只执行一次
		policy = &RetryPolicy{
			MaxAttempts: 1,
		}
	}
	if policy.Interval <= 0 {
		policy.Interval = 1 * time.Second // 默认间隔1秒
	}
	if policy.MaxInterval <= 0 {
		policy.MaxInterval = 30 * time.Second // 默认最大间隔30秒
	}
	if policy.Multiplier <= 0 {
		policy.Multiplier = 2.0 // 默认乘数为2
	}
	return &RetryExecutor{
		policy:  policy,
		retryIf: retryIf,
		env:     env,
		condErr: d.opts.condErr,
	}
}

// ExecuteWithRetry 带重试的执行函数
func (r *RetryExecutor) ExecuteWithRetry(ctx context.Context, taskName string, fn func(attempt int64) error) error {
	if r.policy.MaxInterval > 150*time.Second {
		r.policy.MaxInterval = 150 * time.Second // 默认最大间隔150秒
	}

	maxAttempts := r.policy.MaxAttempts
	infiniteRetry := maxAttempts <= 0

	var lastErr error
	var attempt int64
	for attempt = 1; infiniteRetry || attempt <= maxAttempts; attempt++ {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry attempt %d: %w", attempt, ctx.Err())
		default:
		}

		// 执行任务
		if err := fn(attempt); err == nil {
			return nil // 成功执行
		} else {
			lastErr = err
		}

		// 有限重试且已达到最后一次尝试，不再等待
		if !infiniteRetry && attempt == maxAttempts {
			break
		}

		// 重试条件：仅在确实还要再试一次时求值
		if r.retryIf != nil {
			retry, cerr := r.shouldRetry(ctx, attempt, lastErr)
			if cerr != nil {
				if r.condErr == SkipOnConditionError {
					// 宽松策略：条件坏了不掩盖真正的业务错误，直接放弃重试
					return fmt.Errorf("task %s failed after %d attempts, last error: %w", taskName, attempt, lastErr)
				}
				return fmt.Errorf("task %s: retryIf: %w", taskName,
					errors.Join(cerr, lastErr))
			}
			if !retry {
				return fmt.Errorf("task %s aborted after %d attempts, retryIf evaluated false, last error: %w",
					taskName, attempt, lastErr)
			}
		}

		// 计算等待时间（指数退避）
		waitTime := r.calculateBackoff(attempt, r.policy.MaxInterval)

		// 等待重试
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled during retry wait: %w", ctx.Err())
		case <-time.After(waitTime):
			// 继续重试
		}
	}

	if infiniteRetry {
		return fmt.Errorf("task %s failed after infinite retry attempts, last error: %w", taskName, lastErr)
	}

	return fmt.Errorf("task %s failed after %d attempts, last error: %w",
		taskName, maxAttempts, lastErr)
}

// shouldRetry 求值重试条件。env 按次拷贝，避免多个任务共享同一份可变环境。
func (r *RetryExecutor) shouldRetry(ctx context.Context, attempt int64, lastErr error) (bool, error) {
	env := *r.env
	env.Attempt = attempt
	if lastErr != nil {
		env.Error = lastErr.Error()
	}
	return r.retryIf.RunBool(ctx, &env)
}

// calculateBackoff 计算指数退避时间，使用 math.Pow 处理浮点数
func (r *RetryExecutor) calculateBackoff(attempt int64, maxInterval time.Duration) time.Duration {
	// 使用 math.Pow 进行精确的浮点数幂运算
	// 公式: baseInterval * (multiplier ^ (attempt - 1))
	backoff := float64(r.policy.Interval) * math.Pow(r.policy.Multiplier, float64(attempt-1))

	result := time.Duration(backoff)

	// 确保不超过最大间隔
	if result > maxInterval {
		return maxInterval
	}

	return result
}
