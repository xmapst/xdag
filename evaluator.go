package xdag

import (
	"context"
	"fmt"
)

// Conditional 是 Task 的可选扩展接口，也是 xdag 中唯一的分支判断入口。
//
// 边条件回答的是「这条依赖要不要参与门禁」：求值为 false 时该边失活，
// 即本任务不再要求这个依赖成功；求值为 true 或未声明时该边生效，要求依赖必须成功。
//
// 由此派生出两种终态：
//   - 所有入边都失活 → StateSkipped，没有任何依赖构成执行本任务的理由
//   - 存在生效的边但其上游未成功 → StateUpstreamSkipped
//
// 边条件求值时 Env.Dep 为该边的上游任务名，因此一条 Succeeded(Dep) 可以复用到所有边上。
//
// 注意：分支判断挂在边上，因此没有依赖的根任务无法附加条件——它没有入边。
// 需要按开关控制入口时，请在任务自身的 Execute 中处理，或引入一个显式的前置任务。
type Conditional interface {
	// Condition 返回依赖边 dep -> 当前任务 的条件表达式。
	// 返回空串表示该边无条件生效。
	Condition(dep string) string
}

// Evaluator 把表达式文本编译成可复用的 Program。
// 实现由 New 在构建阶段调用，因此表达式的语法与类型错误会在 New 中一次性暴露。
type Evaluator interface {
	Compile(expression string) (Program, error)
}

// Program 是编译后的表达式。同一个 Program 会被多个 goroutine 并发求值，
// 实现必须保证并发安全。
type Program interface {
	RunBool(ctx context.Context, env *Env) (bool, error)
}

// RefKind 区分静态引用所指向的可见范围。
type RefKind uint8

const (
	// RefAncestor 通过 Output/State/Succeeded 等函数引用，要求被引用者是当前任务的祖先。
	RefAncestor RefKind = iota
	// RefInput 通过 Inputs["x"] 引用，要求被引用者是当前任务的直接依赖。
	RefInput
)

func (k RefKind) String() string {
	if k == RefInput {
		return "Inputs"
	}
	return "ancestor"
}

// Reference 是表达式中以字面量形式出现的一处任务名引用。
type Reference struct {
	Kind RefKind
	Task string
}

// Referencer 是 Program 的可选扩展接口。实现了它的 Program 会在 New 阶段接受
// 引用范围校验：越界引用从运行期错误提前为构建期错误。
//
// 只有字面量参数能被静态分析，Output(Dep) 这类动态引用仍由运行期检查兜底。
type Referencer interface {
	References() []Reference
}

// InputsField 是 Env 中按依赖名索引的字段名，供 Evaluator 做静态引用分析。
const InputsField = "Inputs"

// ancestorFuncs 是 Env 上以任务名为唯一参数的函数集合。
var ancestorFuncs = map[string]struct{}{
	"Output": {}, "State": {}, "Succeeded": {}, "Skipped": {}, "Failed": {},
}

// IsAncestorFunc 报告 name 是否为以任务名为参数的 Env 函数，供 Evaluator 做静态引用分析。
func IsAncestorFunc(name string) bool {
	_, ok := ancestorFuncs[name]
	return ok
}

// Env 是条件表达式的求值环境。
//
// 导出字段与导出方法可以直接在表达式中书写，例如：
//
//	Output("check").code == 200
//	Inputs["fetch-user"]["vip"] == true and Vars["env"] == "prod"
//	Succeeded(Dep)
//
// 可见范围被限制在当前任务的祖先集合内：访问非祖先任务会返回错误。
// 这是刻意的约束——非祖先任务在本任务被调度时不保证已经完成，读取它的输出
// 会让求值结果随调度时序变化。
type Env struct {
	// Task 是当前任务名。
	Task string
	// Attempt 是当前尝试次数，从 1 开始。
	// 分支条件在重试之前求值一次，因此恒为 0；只有 RetryPolicy.RetryIf 会用到它。
	Attempt int64
	// Dep 是当前正在求值的依赖边的上游任务名。边条件中恒为非空，RetryIf 中为空。
	Dep string
	// Error 是最近一次失败的错误信息，仅在 RetryPolicy.RetryIf 中非空。
	Error string
	// Vars 是通过 WithVars 注入的全局变量。
	Vars map[string]any
	// Inputs 是直接依赖的输出，key 为依赖任务名。
	// 被跳过或失败的依赖不会出现在其中。
	Inputs map[string]map[string]any

	scope   map[string]struct{}
	outputs map[string]map[string]any
	states  map[string]State
}

// Output 返回祖先任务 name 的输出。任务被跳过或失败时返回 nil。
func (e Env) Output(name string) (map[string]any, error) {
	if err := e.inScope(name); err != nil {
		return nil, err
	}
	return e.outputs[name], nil
}

// State 返回祖先任务 name 的状态字符串，取值见 State.String。
func (e Env) State(name string) (string, error) {
	if err := e.inScope(name); err != nil {
		return "", err
	}
	return e.states[name].String(), nil
}

// Succeeded 报告祖先任务 name 是否执行成功。
func (e Env) Succeeded(name string) (bool, error) { return e.stateIs(name, StateSuccess) }

// Failed 报告祖先任务 name 是否执行失败。
func (e Env) Failed(name string) (bool, error) { return e.stateIs(name, StateFailed) }

// Skipped 报告祖先任务 name 是否被跳过（含因上游未成功而跳过）。
func (e Env) Skipped(name string) (bool, error) {
	if err := e.inScope(name); err != nil {
		return false, err
	}
	s := e.states[name]
	return s == StateSkipped || s == StateUpstreamSkipped, nil
}

func (e Env) stateIs(name string, want State) (bool, error) {
	if err := e.inScope(name); err != nil {
		return false, err
	}
	return e.states[name] == want, nil
}

func (e Env) inScope(name string) error {
	if _, ok := e.scope[name]; !ok {
		return fmt.Errorf("task %q is not an ancestor of %q: only ancestors are visible in conditions", name, e.Task)
	}
	return nil
}
