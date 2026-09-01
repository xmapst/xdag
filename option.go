// option.go —— New 的构造选项。

package xdag

type options struct {
	maxConcurrency int
	observer       func(Event)
}

// Option 用于配置 Scheduler 的构建参数。
type Option func(*options)

// WithMaxConcurrency 限制同时执行的任务数，传 0 或负数表示不限制（默认）。
//
// xdag 的调度模型是「入度减到 0 就立即派生一个 goroutine」，唯一的既有约束
// 一张宽图会在 Execute 的第一瞬间同时派生出全部根任务——goroutine 本身
// 不是问题，问题在进程外：几十上百个并发请求瞬间打到同一个下游，连接池
// 耗尽、对端限流。
//
// 额度按**单次尝试**占用，因此：
//   - 因依赖没跑成而被跳过、以及被取消的任务不占额度，它们根本不会执行
//   - 两次重试之间的退避等待不占额度
//   - 被 SuspendTask 挂起、停在挂起门上的任务不占额度，不会饿死别的任务
//
// 注意嵌套：如果某个任务在自己的 Execute 里又构造了一个 Scheduler，外层任务
// 占着一个槽位的同时内层还有自己独立的上限，实际并发是两者相乘，不是外层
// 那个数。
func WithMaxConcurrency(n int) Option {
	return func(o *options) { o.maxConcurrency = n }
}
