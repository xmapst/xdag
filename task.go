// task.go —— ITask：参与调度的最小单元，调用方要实现的那个接口。

package xdag

import "context"

// ITask 是参与调度的最小单元。实现者描述“依赖谁”与“怎么执行”，
// 其余的一切——何时执行、要不要执行、失败了下游怎么办——都由 Scheduler 决定。
//
// xdag 对任务体的内容不做任何假设：Execute 的返回值只是一个不透明的
// map[string]any，会原样交给依赖它的下游任务，以及最终的结果集。
type ITask interface {
	// Name 返回任务名。它必须与传给 New 的 map 里对应的 key 一致——
	// 调度器内部只按 map 的 key 索引任务，控制 API（CancelTask/
	// SuspendTask/ResumeTask）也按 key 寻址。
	//
	// **New 会强制校验这一点**，不一致直接返回 ErrTaskNameMismatch。
	// 此前这里只写着"二者不一致不会被检测出来"，把成本留给了每一个调用方：
	// 拿 Name() 去调控制 API 会全部拿到 ErrUnknownTask，而任务本身跑得好好的，
	// 表现成"点了暂停没反应"，没有任何地方会告诉你为什么。
	Name() string

	// Dependencies 返回本任务的直接依赖任务名列表，元素必须都是同一张
	// 任务表里的 key。全部依赖成功后，本任务才会被调度执行；任一依赖未成功，
	// 本任务体不会被调用——依赖是失败或被跳过时终态记为 StateSkipped，
	// 依赖是被**取消**的则记为 StateCanceled，取消沿依赖链原样传播。
	//
	// New 只会调用一次 Dependencies() 并把返回值拷贝下来冻结，之后的校验、
	// 入度计算、依赖建边、调度判定全部使用这一份副本——构建完成后再通过
	// 任务自身状态改变它的返回值不会生效。
	Dependencies() []string

	// RetryPolicy 返回本任务的重试策略。返回 nil 等价于
	// &RetryPolicy{MaxAttempts: 1}，即失败后不重试。
	//
	// 返回的指针只会被读取、按值拷贝后再补默认值，不会被就地修改，
	// 因此可以安全地在多个任务间共享同一个策略对象。
	RetryPolicy() *RetryPolicy

	// PreExecution 在每次尝试调用 Execute 之前触发，典型用途是记录日志、
	// 上报指标。attempt 从 1 开始计数；input 与随后 Execute 收到的完全一致。
	//
	// panic 会被调度器接住，按本次尝试失败处理，效果与 Execute 返回
	// error 相同（但不会带回一个 output）。
	PreExecution(ctx context.Context, attempt int64, input map[string]any)

	// Execute 是任务体本身。input 以直接依赖的任务名为 key，值是该依赖
	// Execute 的返回值；未成功的依赖不会出现在 input 中。
	//
	// **input 必须当作只读**。调度器给每个下游的是一层浅拷贝，所以往
	// input[dep] 这一层写 key 不会串到别的任务；但嵌套的 map/slice 仍然与
	// 上游的输出、以及其他下游共享同一份底层数据，改它就是跨任务的
	// 数据竞争。需要修改就自己复制一份。
	//
	// 另外这份 input 在同一个任务的多次重试尝试之间、以及 PreExecution/
	// Execute/PostExecution 之间是**同一份**：往里写的东西会被后续尝试看到，
	// 不会随重试重置。
	//
	// 对称的另一半：Execute 返回的 output 交出去之后就不该再动。它会被
	// 分发给全部下游，也会进入 Execute 的结果集，在任务返回之后继续持有
	// 并写入它同样是数据竞争。
	//
	// 返回 error 视为本次尝试失败，是否重试由 RetryPolicy 决定；重试期间
	// PreExecution/Execute/PostExecution 会按 attempt 递增依次重新触发。
	// 把 ErrNonRetryable 包进返回的 error 里可以声明「这次失败重试也没用」，
	// 调度器会立刻放弃剩余次数。panic 与返回 error 等价，同样会被调度器
	// 接住并计入重试。
	Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error)

	// PostExecution 在每次尝试的 Execute 返回之后触发（无论成功还是失败），
	// output/err 就是 Execute 刚刚返回的那一对值。
	//
	// panic 的处理方式与 PreExecution 相同。
	PostExecution(ctx context.Context, attempt int64, output map[string]any, err error)
}
