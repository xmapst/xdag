// Package xexpr 基于 github.com/expr-lang/expr 实现 xdag.Evaluator。
//
// 它被拆成独立子包，是为了让根包 xdag 保持零第三方依赖：
// 不需要条件判断的使用者不会把 expr 编进二进制。
//
//	dag, err := xdag.New(tasks, xdag.WithEvaluator(xexpr.New()))
package xexpr

import (
	"context"
	"fmt"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/vm"

	"github.com/xmapst/xdag"
)

// DefaultMaxNodes 是表达式 AST 的默认节点上限。
// expr 自身默认为 1e4，分支条件用不到这个量级，收紧一些可以挡住畸形输入。
const DefaultMaxNodes = 1000

// Engine 实现 xdag.Evaluator。零值不可用，请使用 New 构造。
type Engine struct {
	opts []expr.Option
}

// New 构造表达式引擎。传入的 opts 会追加在默认选项之后，因此可以覆盖默认行为。
//
// 默认选项：
//   - expr.Env(xdag.Env{})：以 Env 结构体做编译期类型检查
//   - expr.AsBool()：编译期强制表达式结果为 bool
//   - expr.MaxNodes(DefaultMaxNodes)：限制 AST 规模
func New(opts ...expr.Option) *Engine {
	base := []expr.Option{
		expr.Env(xdag.Env{}),
		expr.AsBool(),
		expr.MaxNodes(DefaultMaxNodes),
	}
	return &Engine{opts: append(base, opts...)}
}

// Compile 实现 xdag.Evaluator。
func (e *Engine) Compile(expression string) (xdag.Program, error) {
	compiled, err := expr.Compile(expression, e.opts...)
	if err != nil {
		return nil, err
	}
	return &program{compiled: compiled, refs: e.analyze(compiled)}, nil
}

// analyze 遍历 AST，收集以字面量形式出现的任务名引用，供 xdag 在构建期做范围校验。
func (e *Engine) analyze(compiled *vm.Program) []xdag.Reference {
	node := compiled.Node()
	if node == nil {
		return nil
	}
	visitor := &refVisitor{seen: make(map[xdag.Reference]struct{})}
	ast.Walk(&node, visitor)
	return visitor.refs
}

type refVisitor struct {
	refs []xdag.Reference
	seen map[xdag.Reference]struct{}
}

func (v *refVisitor) Visit(node *ast.Node) {
	switch n := (*node).(type) {
	case *ast.CallNode:
		// Output("a") / Succeeded("a") / ...
		callee, ok := n.Callee.(*ast.IdentifierNode)
		if !ok || !xdag.IsAncestorFunc(callee.Value) || len(n.Arguments) != 1 {
			return
		}
		if arg, ok := n.Arguments[0].(*ast.StringNode); ok {
			v.add(xdag.RefAncestor, arg.Value)
		}
	case *ast.MemberNode:
		// Inputs["a"]
		owner, ok := n.Node.(*ast.IdentifierNode)
		if !ok || owner.Value != xdag.InputsField {
			return
		}
		if key, ok := n.Property.(*ast.StringNode); ok {
			v.add(xdag.RefInput, key.Value)
		}
	}
	// 非字面量参数（如 Output(Dep)）无法静态判定，交给运行期的 Env 检查
}

func (v *refVisitor) add(kind xdag.RefKind, task string) {
	ref := xdag.Reference{Kind: kind, Task: task}
	if _, ok := v.seen[ref]; ok {
		return
	}
	v.seen[ref] = struct{}{}
	v.refs = append(v.refs, ref)
}

// vmPool 复用 vm.VM 实例。vm.Run 在入口会完整重置内部状态，且对求值过程中的
// panic 做了 recover，因此复用是安全的——但同一个 VM 绝不能被并发使用，
// 所以这里必须走 Get/Put 而不是共享一个实例。
var vmPool = sync.Pool{New: func() any { return new(vm.VM) }}

type program struct {
	compiled *vm.Program
	refs     []xdag.Reference
}

// References 实现 xdag.Referencer。返回的切片只读，调用方不应修改。
func (p *program) References() []xdag.Reference { return p.refs }

// RunBool 实现 xdag.Program。*vm.Program 本身可并发复用。
func (p *program) RunBool(ctx context.Context, env *xdag.Env) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}

	machine := vmPool.Get().(*vm.VM)
	out, err := machine.Run(p.compiled, *env)
	vmPool.Put(machine)
	if err != nil {
		return false, err
	}

	result, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("condition must evaluate to bool, got %T", out)
	}
	return result, nil
}
