package xdag

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
)

// MaxTasks 是默认的任务数量上限。可用 WithMaxTasks 按实例覆盖。
var MaxTasks = 150

// ErrAlreadyExecuted Dagcuter 的入度表在执行过程中被消费，不能重复执行。
var ErrAlreadyExecuted = errors.New("dag already executed")

// ErrNoEvaluator 任务声明了条件表达式，但没有通过 WithEvaluator 配置表达式引擎。
var ErrNoEvaluator = errors.New("task declares a condition but no evaluator is configured")

type Dagcuter struct {
	Tasks          map[string]Task
	results        *sync.Map
	inDegrees      map[string]int
	dependents     map[string][]string
	executionOrder []string
	mu             *sync.Mutex
	wg             *sync.WaitGroup

	opts          options
	states        map[string]State
	edgePrograms  map[string]map[string]Program
	retryPrograms map[string]Program
	deps          map[string]map[string]struct{}
	ancestors     map[string]map[string]struct{}
	executed      atomic.Bool
}

func New(tasks map[string]Task, opts ...Option) (*Dagcuter, error) {
	o := options{maxTasks: MaxTasks}
	for _, opt := range opts {
		opt(&o)
	}

	if o.maxTasks > 0 && len(tasks) > o.maxTasks {
		return nil, fmt.Errorf("too many tasks: %d > %d", len(tasks), o.maxTasks)
	}
	if err := Validate(tasks); err != nil {
		return nil, err
	}

	dag := &Dagcuter{
		mu:            new(sync.Mutex),
		wg:            new(sync.WaitGroup),
		results:       new(sync.Map),
		inDegrees:     make(map[string]int, len(tasks)),
		dependents:    make(map[string][]string, len(tasks)),
		Tasks:         tasks,
		opts:          o,
		states:        make(map[string]State, len(tasks)),
		edgePrograms:  make(map[string]map[string]Program),
		retryPrograms: make(map[string]Program),
		deps:          make(map[string]map[string]struct{}, len(tasks)),
		ancestors:     computeAncestors(tasks),
	}

	for name, task := range dag.Tasks {
		dag.states[name] = StatePending
		dag.inDegrees[name] = len(task.Dependencies())
		dag.deps[name] = make(map[string]struct{}, len(task.Dependencies()))
		for _, dep := range task.Dependencies() {
			dag.dependents[dep] = append(dag.dependents[dep], name)
			dag.deps[name][dep] = struct{}{}
		}
	}

	// 预编译全部表达式：语法错误、类型错误、越界的任务名引用都在构建期一次性暴露，
	// 而不是跑到一半才炸
	if err := dag.compileConditions(); err != nil {
		return nil, err
	}

	return dag, nil
}

func (d *Dagcuter) compileConditions() error {
	var errs error
	for name, task := range d.Tasks {
		if policy := task.RetryPolicy(); policy != nil {
			if cond := strings.TrimSpace(policy.RetryIf); cond != "" {
				if err := d.compileInto(d.retryPrograms, name, cond, "retryIf"); err != nil {
					errs = errors.Join(errs, err)
				}
			}
		}

		edges, ok := task.(Conditional)
		if !ok {
			continue
		}
		for _, dep := range task.Dependencies() {
			cond := strings.TrimSpace(edges.Condition(dep))
			if cond == "" {
				continue
			}
			program, err := d.compile(name, cond, fmt.Sprintf("edge condition %s -> %s", dep, name))
			if err != nil {
				errs = errors.Join(errs, err)
				continue
			}
			if d.edgePrograms[name] == nil {
				d.edgePrograms[name] = make(map[string]Program, len(task.Dependencies()))
			}
			d.edgePrograms[name][dep] = program
		}
	}
	return errs
}

func (d *Dagcuter) compileInto(dst map[string]Program, name, cond, what string) error {
	program, err := d.compile(name, cond, what)
	if err != nil {
		return err
	}
	dst[name] = program
	return nil
}

func (d *Dagcuter) compile(name, cond, what string) (Program, error) {
	if d.opts.evaluator == nil {
		return nil, fmt.Errorf("%w: task %s (%s)", ErrNoEvaluator, name, what)
	}
	program, err := d.opts.evaluator.Compile(cond)
	if err != nil {
		return nil, fmt.Errorf("task %s: compile %s %q: %w", name, what, cond, err)
	}
	if err = d.checkReferences(name, program, what, cond); err != nil {
		return nil, err
	}
	return program, nil
}

// checkReferences 校验表达式中以字面量出现的任务名引用是否越界。
// Output/Succeeded 等函数只能指向祖先，Inputs["x"] 只能指向直接依赖。
// 非字面量引用无法静态判定，由 Env 在运行期兜底。
func (d *Dagcuter) checkReferences(name string, program Program, what, cond string) error {
	referencer, ok := program.(Referencer)
	if !ok {
		return nil
	}

	var errs error
	for _, ref := range referencer.References() {
		switch ref.Kind {
		case RefInput:
			if _, ok := d.deps[name][ref.Task]; !ok {
				errs = errors.Join(errs, fmt.Errorf("task %s: %s %q uses Inputs[%q], but %q is not a direct dependency of %s",
					name, what, cond, ref.Task, ref.Task, name))
			}
		default:
			if _, ok := d.ancestors[name][ref.Task]; !ok {
				errs = errors.Join(errs, fmt.Errorf("task %s: %s %q references %q, which is not an ancestor of %s",
					name, what, cond, ref.Task, name))
			}
		}
	}
	return errs
}

func (d *Dagcuter) Execute(ctx context.Context) (map[string]map[string]any, error) {
	if !d.executed.CompareAndSwap(false, true) {
		return nil, ErrAlreadyExecuted
	}
	defer d.results.Clear()
	errCh := make(chan error, len(d.Tasks)+1)

	// 先收集根节点再派生：runTask 会并发写 inDegrees，边 range 边派生会触发
	// "concurrent map iteration and map write" 的 fatal error
	var roots []string
	for name, deg := range d.inDegrees {
		if deg == 0 {
			roots = append(roots, name)
		}
	}
	for _, name := range roots {
		d.wg.Add(1)
		go d.runTask(ctx, name, errCh)
	}

	d.wg.Wait()
	close(errCh)
	var err error

	for _err := range errCh {
		err = errors.Join(err, _err)
	}

	results := make(map[string]map[string]any)
	d.results.Range(func(key, value any) bool {
		results[key.(string)] = value.(map[string]any)
		return true
	})
	return results, err
}

func (d *Dagcuter) runTask(ctx context.Context, name string, errCh chan error) {
	defer d.wg.Done()

	state, output, err := d.resolve(ctx, name)
	if err != nil {
		select {
		case errCh <- err:
		default:
		}
	}
	// 无论成功、跳过还是失败都必须走 commit，否则下游入度永远减不到 0，
	// 整个子树会被静默丢弃
	d.commit(ctx, name, state, output, errCh)
}

// gateResult 是入边门禁的判定结果。
type gateResult uint8

const (
	gatePass    gateResult = iota // 放行
	gateSkipped                   // 所有入边失活：没有任何依赖构成执行本任务的理由
	gateBlocked                   // 存在生效的边，但其上游未成功
)

func (g gateResult) state() State {
	if g == gateSkipped {
		return StateSkipped
	}
	return StateUpstreamSkipped
}

// resolve 判定任务的终态：先过入边门禁，再决定是否执行任务体。
//
// 门禁规则：
//   - 声明了边条件 → 逐条求值，失活的边不参与门禁，生效的边要求上游成功
//   - 未声明边条件 → 要求所有依赖成功
//
// 没有依赖的根任务没有入边，因此不受门禁约束，总是执行。
func (d *Dagcuter) resolve(ctx context.Context, name string) (State, map[string]any, error) {
	select {
	case <-ctx.Done():
		return StateFailed, nil, fmt.Errorf("task %s: %w", name, ctx.Err())
	default:
	}

	task := d.Tasks[name]
	inputs, env := d.snapshot(name)

	if edges := d.edgePrograms[name]; len(edges) > 0 {
		result, err := d.edgeGate(ctx, name, env, edges)
		if err != nil {
			return d.conditionFailure(name, err)
		}
		if result != gatePass {
			return result.state(), nil, nil
		}
	} else if !allSucceeded(env.states, task.Dependencies()) {
		return StateUpstreamSkipped, nil, nil
	}

	output, err := d.executeTask(ctx, name, task, inputs, env)
	if err != nil {
		return StateFailed, nil, err
	}
	return StateSuccess, output, nil
}

// edgeGate 逐条求值入边：失活的边不参与门禁，生效的边要求上游成功。
func (d *Dagcuter) edgeGate(ctx context.Context, name string, env *Env, edges map[string]Program) (gateResult, error) {
	var active int
	for _, dep := range d.Tasks[name].Dependencies() {
		if program, ok := edges[dep]; ok {
			scoped := *env
			scoped.Dep = dep
			pass, err := program.RunBool(ctx, &scoped)
			if err != nil {
				return gateBlocked, fmt.Errorf("edge condition %s -> %s: %w", dep, name, err)
			}
			if !pass {
				continue // 边失活
			}
		}
		active++
		if env.states[dep] != StateSuccess {
			return gateBlocked, nil
		}
	}
	if active == 0 {
		return gateSkipped, nil
	}
	return gatePass, nil
}

func (d *Dagcuter) conditionFailure(name string, err error) (State, map[string]any, error) {
	if d.opts.condErr == SkipOnConditionError {
		return StateSkipped, nil, nil
	}
	return StateFailed, nil, fmt.Errorf("task %s: %w", name, err)
}

func allSucceeded(states map[string]State, deps []string) bool {
	for _, dep := range deps {
		if states[dep] != StateSuccess {
			return false
		}
	}
	return true
}

// commit 写入终态并推进下游调度。
func (d *Dagcuter) commit(ctx context.Context, name string, state State, output map[string]any, errCh chan error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.states[name] = state
	if state == StateSuccess {
		d.results.Store(name, output)
		d.executionOrder = append(d.executionOrder, name)
	}

	for _, child := range d.dependents[name] {
		d.inDegrees[child]--
		if d.inDegrees[child] == 0 {
			d.wg.Add(1)
			go d.runTask(ctx, child, errCh)
		}
	}
}

func (d *Dagcuter) executeTask(ctx context.Context, name string, task Task, inputs map[string]any, env *Env) (map[string]any, error) {
	// 获取任务的重试策略与重试条件
	retryExecutor := d.newRetryExecutor(task.RetryPolicy(), d.retryPrograms[name], env)

	var result map[string]any

	// 使用重试机制执行任务
	err := retryExecutor.ExecuteWithRetry(ctx, name, func(attempt int64) error {
		// PreExecution
		task.PreExecution(ctx, attempt, inputs)

		// Execute
		output, err := task.Execute(ctx, attempt, inputs)
		// PostExecution
		task.PostExecution(ctx, attempt, output, err)
		if err != nil {
			// 不做额外包装：RetryPolicy.RetryIf 通过 Env.Error 读到的应当是任务原始错误
			return err
		}
		result = output
		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("task %s failed: %w", name, err)
	}

	return result, nil
}

// snapshot 在锁内取出任务执行所需的输入，以及条件表达式的求值环境。
// 可见范围限制在祖先集合内——祖先在本任务被调度时必然已进入终态，读取它们的结果是确定的。
func (d *Dagcuter) snapshot(name string) (map[string]any, *Env) {
	deps := d.Tasks[name].Dependencies()
	ancestors := d.ancestors[name]

	d.mu.Lock()
	defer d.mu.Unlock()

	inputs := make(map[string]any, len(deps))
	envInputs := make(map[string]map[string]any, len(deps))
	for _, dep := range deps {
		if value, ok := d.results.Load(dep); ok {
			inputs[dep] = value
			if m, ok := value.(map[string]any); ok {
				envInputs[dep] = m
			}
		}
	}

	outputs := make(map[string]map[string]any, len(ancestors))
	states := make(map[string]State, len(ancestors))
	for a := range ancestors {
		states[a] = d.states[a]
		if value, ok := d.results.Load(a); ok {
			if m, ok := value.(map[string]any); ok {
				outputs[a] = m
			}
		}
	}

	return inputs, &Env{
		Task:    name,
		Vars:    d.opts.vars,
		Inputs:  envInputs,
		scope:   ancestors,
		outputs: outputs,
		states:  states,
	}
}

// States 返回所有任务的状态快照。
func (d *Dagcuter) States() map[string]State {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make(map[string]State, len(d.states))
	for name, state := range d.states {
		out[name] = state
	}
	return out
}

// State 返回单个任务的状态。任务不存在时返回 StatePending。
func (d *Dagcuter) State(name string) State {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.states[name]
}

// ExecutionOrder 返回成功执行的任务的完成顺序。
// 被跳过与失败的任务不在其中，需要完整视图请使用 States。
func (d *Dagcuter) ExecutionOrder() string {
	d.mu.Lock()
	order := append([]string(nil), d.executionOrder...)
	d.mu.Unlock()

	var sb = strings.Builder{}
	sb.WriteString("\n")
	for i, step := range order {
		_, _ = fmt.Fprintf(&sb, "%d. %s\n", i+1, step)
	}
	return sb.String()
}

// PrintGraph 输出链式依赖
func (d *Dagcuter) PrintGraph() {
	// 1. 找到所有根节点（无依赖）。这里不读 inDegrees——它在执行过程中会被消费掉
	var roots []string
	for name, task := range d.Tasks {
		if len(task.Dependencies()) == 0 {
			roots = append(roots, name)
		}
	}
	// 2. 分别从每个根节点开始打印
	for _, root := range roots {
		fmt.Println(root)        // 先打印根
		d.printChain(root, "  ") // 从根的下一层开始缩进两格
		fmt.Println()            // 不同根之间空行
	}
}

// printChain 递归打印子依赖，
// name: 当前节点；
// prefix: 当前缩进前缀（已经包含了箭头前需要的空格）
func (d *Dagcuter) printChain(name, prefix string) {
	children := d.dependents[name]
	for _, child := range children {
		// 打印箭头和子节点
		fmt.Printf("%s└─> %s\n", prefix, child)
		// 递归打印子节点的子依赖，缩进再多四格
		d.printChain(child, prefix+"    ")
	}
}
