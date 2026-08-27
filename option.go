package xdag

// ConditionErrorPolicy 决定条件表达式求值出错时任务的归宿。
type ConditionErrorPolicy uint8

const (
	// FailOnConditionError 求值出错视为任务失败（默认）。
	// 表达式写错与条件不成立是两回事，静默跳过会让问题难以定位。
	FailOnConditionError ConditionErrorPolicy = iota
	// SkipOnConditionError 求值出错视为条件不成立，任务被跳过。
	SkipOnConditionError
)

type options struct {
	evaluator Evaluator
	vars      map[string]any
	condErr   ConditionErrorPolicy
	maxTasks  int
}

// Option 用于配置 Dagcuter。
type Option func(*options)

// WithEvaluator 注入表达式引擎。只有配置了引擎，实现 Conditional 的任务
// 以及 RetryPolicy.RetryIf 才能使用表达式。
// 官方实现见子包 github.com/xmapst/xdag/xexpr。
func WithEvaluator(e Evaluator) Option {
	return func(o *options) { o.evaluator = e }
}

// WithVars 注入全局变量，在条件表达式中通过 Vars["key"] 访问。
func WithVars(vars map[string]any) Option {
	return func(o *options) { o.vars = vars }
}

// WithConditionErrorPolicy 设置条件求值出错时的处理策略，默认 FailOnConditionError。
func WithConditionErrorPolicy(p ConditionErrorPolicy) Option {
	return func(o *options) { o.condErr = p }
}

// WithMaxTasks 覆盖本次构建的任务数量上限，默认取包级变量 MaxTasks。
func WithMaxTasks(n int) Option {
	return func(o *options) { o.maxTasks = n }
}
