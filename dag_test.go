package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/xmapst/xdag"
	"github.com/xmapst/xdag/xexpr"
)

// testTask 是一个可配置的 Task 实现，同时实现了 xdag.Conditional。
type testTask struct {
	name      string
	deps      []string
	edgeConds map[string]string
	policy    *xdag.RetryPolicy
	fn        func(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error)

	mu       sync.Mutex
	runCount int
}

func (t *testTask) Name() string                { return t.name }
func (t *testTask) Dependencies() []string      { return t.deps }
func (t *testTask) Condition(dep string) string { return t.edgeConds[dep] }

func (t *testTask) RetryPolicy() *xdag.RetryPolicy {
	if t.policy != nil {
		return t.policy
	}
	return &xdag.RetryPolicy{MaxAttempts: 1}
}
func (t *testTask) PreExecution(context.Context, int64, map[string]any)         {}
func (t *testTask) PostExecution(context.Context, int64, map[string]any, error) {}

func (t *testTask) Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error) {
	t.mu.Lock()
	t.runCount++
	t.mu.Unlock()
	if t.fn != nil {
		return t.fn(ctx, attempt, input)
	}
	return map[string]any{"task": t.name}, nil
}

func (t *testTask) runs() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.runCount
}

func newTask(name string, deps ...string) *testTask {
	return &testTask{name: name, deps: deps}
}

func (t *testTask) edge(dep, cond string) *testTask {
	if t.edgeConds == nil {
		t.edgeConds = make(map[string]string)
	}
	t.edgeConds[dep] = cond
	return t
}

// retries 配置重试策略。间隔取 1ms，避免拖慢测试。
func (t *testTask) retries(maxAttempts int64, retryIf string) *testTask {
	t.policy = &xdag.RetryPolicy{
		Interval:    time.Millisecond,
		MaxInterval: time.Millisecond,
		MaxAttempts: maxAttempts,
		Multiplier:  1,
		RetryIf:     retryIf,
	}
	return t
}

func (t *testTask) returns(out map[string]any) *testTask {
	t.fn = func(context.Context, int64, map[string]any) (map[string]any, error) { return out, nil }
	return t
}

func (t *testTask) fails(err error) *testTask {
	t.fn = func(context.Context, int64, map[string]any) (map[string]any, error) { return nil, err }
	return t
}

func tasksOf(list ...*testTask) map[string]xdag.Task {
	m := make(map[string]xdag.Task, len(list))
	for _, task := range list {
		m[task.name] = task
	}
	return m
}

func assertState(t *testing.T, dag *xdag.Dagcuter, name string, want xdag.State) {
	t.Helper()
	if got := dag.State(name); got != want {
		t.Errorf("state of %q = %v, want %v", name, got, want)
	}
}

func mustNew(t *testing.T, tasks map[string]xdag.Task, opts ...xdag.Option) *xdag.Dagcuter {
	t.Helper()
	dag, err := xdag.New(tasks, opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return dag
}

// ---------------------------------------------------------------------------
// Phase 1：条件分支
// ---------------------------------------------------------------------------

// 典型的条件分支图，分支与汇聚都由边条件表达：
//
//	         ┌─ notify-ok   (edge: code == 200) ─┐
//	check ───┤                                   ├──> report (edge: Succeeded(Dep))
//	         └─ notify-fail (edge: code != 200) ─┘
func diamond(code int) (map[string]xdag.Task, map[string]*testTask) {
	check := newTask("check").returns(map[string]any{"code": code})
	ok := newTask("notify-ok", "check").edge("check", `Output("check").code == 200`)
	fail := newTask("notify-fail", "check").edge("check", `Output("check").code != 200`)
	report := newTask("report", "notify-ok", "notify-fail").
		edge("notify-ok", `Succeeded(Dep)`).
		edge("notify-fail", `Succeeded(Dep)`)

	index := map[string]*testTask{
		"check": check, "notify-ok": ok, "notify-fail": fail, "report": report,
	}
	return tasksOf(check, ok, fail, report), index
}

func TestConditionalDiamond(t *testing.T) {
	cases := []struct {
		name    string
		code    int
		taken   string
		skipped string
	}{
		{"success branch", 200, "notify-ok", "notify-fail"},
		{"failure branch", 500, "notify-fail", "notify-ok"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tasks, index := diamond(tc.code)
			dag := mustNew(t, tasks, xdag.WithEvaluator(xexpr.New()))

			results, err := dag.Execute(context.Background())
			if err != nil {
				t.Fatalf("Execute: %v", err)
			}

			assertState(t, dag, "check", xdag.StateSuccess)
			assertState(t, dag, tc.taken, xdag.StateSuccess)
			assertState(t, dag, tc.skipped, xdag.StateSkipped)
			// 关键：一条分支被跳过，汇聚节点仍然要执行
			assertState(t, dag, "report", xdag.StateSuccess)

			if got := index[tc.skipped].runs(); got != 0 {
				t.Errorf("skipped task %q executed %d times, want 0", tc.skipped, got)
			}
			if got := index[tc.taken].runs(); got != 1 {
				t.Errorf("taken task %q executed %d times, want 1", tc.taken, got)
			}
			if _, ok := results[tc.skipped]; ok {
				t.Errorf("skipped task %q should not appear in results", tc.skipped)
			}
			if _, ok := results["report"]; !ok {
				t.Error("report missing from results")
			}
		})
	}
}

func TestSkipPropagatesToUnconditionalDownstream(t *testing.T) {
	a := newTask("a").returns(map[string]any{"go": false})
	b := newTask("b", "a").edge("a", `Output("a").go == true`)
	c := newTask("c", "b") // 无条件：默认要求所有依赖成功

	dag := mustNew(t, tasksOf(a, b, c), xdag.WithEvaluator(xexpr.New()))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	assertState(t, dag, "a", xdag.StateSuccess)
	assertState(t, dag, "b", xdag.StateSkipped)
	assertState(t, dag, "c", xdag.StateUpstreamSkipped)
	if c.runs() != 0 {
		t.Errorf("c executed %d times, want 0", c.runs())
	}
}

func TestInputsAndVarsInCondition(t *testing.T) {
	a := newTask("a").returns(map[string]any{"count": 5})
	b := newTask("b", "a").edge("a", `Inputs["a"]["count"] > 3 and Vars["env"] == "prod"`)

	dag := mustNew(t, tasksOf(a, b),
		xdag.WithEvaluator(xexpr.New()),
		xdag.WithVars(map[string]any{"env": "prod"}),
	)
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertState(t, dag, "b", xdag.StateSuccess)
}

func TestStateAndSkippedHelpers(t *testing.T) {
	a := newTask("a").returns(map[string]any{"go": false})
	b := newTask("b", "a").edge("a", `false`)
	c := newTask("c", "a", "b").
		edge("a", `State("a") == "success"`).
		edge("b", `not Skipped("b")`)

	dag := mustNew(t, tasksOf(a, b, c), xdag.WithEvaluator(xexpr.New()))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertState(t, dag, "b", xdag.StateSkipped)
	assertState(t, dag, "c", xdag.StateSuccess)
}

func TestNonAncestorReferenceRejectedAtNew(t *testing.T) {
	a := newTask("a").returns(map[string]any{"x": 1})
	b := newTask("b").returns(map[string]any{"x": 2})
	// b 不是 c 的祖先，读取它的输出结果不确定，静态分析应在 New 阶段拦下
	c := newTask("c", "a").edge("a", `Output("b").x == 2`)

	_, err := xdag.New(tasksOf(a, b, c), xdag.WithEvaluator(xexpr.New()))
	if err == nil {
		t.Fatal("expected New to reject non-ancestor reference")
	}
	if !strings.Contains(err.Error(), "not an ancestor of c") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNonDependencyInputsRejectedAtNew(t *testing.T) {
	a := newTask("a").returns(map[string]any{"x": 1})
	b := newTask("b", "a").returns(map[string]any{"x": 2})
	// a 是 c 的祖先但不是直接依赖，Inputs["a"] 运行期恒为 nil
	c := newTask("c", "b").edge("b", `Inputs["a"]["x"] == 1`)

	_, err := xdag.New(tasksOf(a, b, c), xdag.WithEvaluator(xexpr.New()))
	if err == nil {
		t.Fatal("expected New to reject non-dependency Inputs access")
	}
	if !strings.Contains(err.Error(), "not a direct dependency of c") {
		t.Errorf("unexpected error: %v", err)
	}
}

// 非字面量引用无法静态判定，由 Env 在运行期兜底。
// Output(Task) 引用任务自身，而任务不是自己的祖先。
func TestDynamicReferenceCaughtAtRuntime(t *testing.T) {
	a := newTask("a").returns(map[string]any{"x": 1})
	b := newTask("b", "a").edge("a", `Output(Task).x == 1`)

	dag := mustNew(t, tasksOf(a, b), xdag.WithEvaluator(xexpr.New()))
	_, err := dag.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "not an ancestor") {
		t.Fatalf("want ancestor scope error, got %v", err)
	}
	assertState(t, dag, "b", xdag.StateFailed)
}

func TestConditionErrorPolicySkip(t *testing.T) {
	a := newTask("a").returns(map[string]any{"x": 1})
	b := newTask("b", "a").edge("a", `Output(Task).x == 1`)

	dag := mustNew(t, tasksOf(a, b),
		xdag.WithEvaluator(xexpr.New()),
		xdag.WithConditionErrorPolicy(xdag.SkipOnConditionError),
	)
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertState(t, dag, "b", xdag.StateSkipped)
}

func TestCompileErrorSurfacesAtNew(t *testing.T) {
	cases := map[string]string{
		"syntax error":  `Output("a").code ==`,
		"not a bool":    `1 + 1`,
		"unknown field": `NoSuchField == 1`,
		"wrong arity":   `Succeeded()`,
	}
	for name, cond := range cases {
		t.Run(name, func(t *testing.T) {
			a := newTask("a")
			b := newTask("b", "a").edge("a", cond)
			_, err := xdag.New(tasksOf(a, b), xdag.WithEvaluator(xexpr.New()))
			if err == nil {
				t.Fatalf("expected compile error for %q", cond)
			}
			if !strings.Contains(err.Error(), "task b") {
				t.Errorf("error should name the task, got: %v", err)
			}
		})
	}
}

// Output 返回 map[string]any，取字段后是动态类型，expr.AsBool 在编译期无法判定，
// 但会插入一次运行期强制转换，求值时报错。
func TestNonBoolResultCaughtAtRuntime(t *testing.T) {
	a := newTask("a").returns(map[string]any{"code": 200})
	b := newTask("b", "a").edge("a", `Output("a").code`)

	dag := mustNew(t, tasksOf(a, b), xdag.WithEvaluator(xexpr.New()))
	_, err := dag.Execute(context.Background())
	if err == nil || !strings.Contains(err.Error(), "bool") {
		t.Fatalf("want bool type error, got %v", err)
	}
	if !strings.Contains(err.Error(), `task b: edge condition a -> b`) {
		t.Errorf("error should name the task and edge, got: %v", err)
	}
	assertState(t, dag, "b", xdag.StateFailed)
}

func TestConditionWithoutEvaluator(t *testing.T) {
	a := newTask("a")
	b := newTask("b", "a").edge("a", `true`)
	_, err := xdag.New(tasksOf(a, b))
	if !errors.Is(err, xdag.ErrNoEvaluator) {
		t.Fatalf("want ErrNoEvaluator, got %v", err)
	}
}

// ---------------------------------------------------------------------------
// Phase 0：地基修复
// ---------------------------------------------------------------------------

func TestFailureDoesNotStallDownstream(t *testing.T) {
	boom := errors.New("boom")
	a := newTask("a").fails(boom)
	b := newTask("b", "a")
	c := newTask("c", "b")

	dag := mustNew(t, tasksOf(a, b, c))
	// 修复前：a 失败后不推进入度，b/c 永远不会被调度，也无从得知原因
	_, err := dag.Execute(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateFailed)
	assertState(t, dag, "b", xdag.StateUpstreamSkipped)
	assertState(t, dag, "c", xdag.StateUpstreamSkipped)
	if b.runs() != 0 || c.runs() != 0 {
		t.Errorf("downstream should not execute: b=%d c=%d", b.runs(), c.runs())
	}
}

func TestUnknownDependencyReturnsError(t *testing.T) {
	// 修复前：check.go 对不存在的 key 调用方法，直接 nil 解引用 panic
	a := newTask("a", "ghost")
	_, err := xdag.New(tasksOf(a))
	if !errors.Is(err, xdag.ErrUnknownDependency) {
		t.Fatalf("want ErrUnknownDependency, got %v", err)
	}
	if !strings.Contains(err.Error(), "a -> ghost") {
		t.Errorf("error should name both ends, got: %v", err)
	}
}

func TestCycleDetection(t *testing.T) {
	a := newTask("a", "c")
	b := newTask("b", "a")
	c := newTask("c", "b")

	_, err := xdag.New(tasksOf(a, b, c))
	if !errors.Is(err, xdag.ErrCircularDependency) {
		t.Fatalf("want ErrCircularDependency, got %v", err)
	}
	if !xdag.HasCycle(tasksOf(a, b, c)) {
		t.Error("HasCycle should report true")
	}
}

func TestHasCycleWithUnknownDependencyDoesNotPanic(t *testing.T) {
	a := newTask("a", "ghost")
	if xdag.HasCycle(tasksOf(a)) {
		t.Error("dangling dependency is not a cycle")
	}
}

// 修复前：Execute 一边 range inDegrees 一边派生会写同一张 map，
// 触发 fatal error: concurrent map iteration and map write。
// 该用例需要 -race 才能稳定暴露。
func TestManyRootsNoRace(t *testing.T) {
	const n = 60
	list := make([]*testTask, 0, 2*n)
	for i := 0; i < n; i++ {
		root := fmt.Sprintf("root-%02d", i)
		list = append(list, newTask(root), newTask(fmt.Sprintf("leaf-%02d", i), root))
	}

	dag := mustNew(t, tasksOf(list...), xdag.WithMaxTasks(0))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(results) != 2*n {
		t.Fatalf("got %d results, want %d", len(results), 2*n)
	}
}

func TestExecuteTwiceIsRejected(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := dag.Execute(context.Background()); !errors.Is(err, xdag.ErrAlreadyExecuted) {
		t.Fatalf("want ErrAlreadyExecuted, got %v", err)
	}
}

func TestBackwardCompatibleWithoutConditions(t *testing.T) {
	// 未实现 Conditional、未配置 Evaluator 的老用法必须原样工作
	user := newTask("fetch-user").returns(map[string]any{"id": 1001})
	order := newTask("fetch-order").returns(map[string]any{"amount": 199})
	summary := newTask("summary", "fetch-user", "fetch-order")
	summary.fn = func(_ context.Context, _ int64, input map[string]any) (map[string]any, error) {
		return map[string]any{"user": input["fetch-user"], "order": input["fetch-order"]}, nil
	}

	dag := mustNew(t, tasksOf(user, order, summary))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("got %d results, want 3", len(results))
	}
	if results["summary"]["user"] == nil {
		t.Error("summary did not receive upstream output")
	}
	if lines := strings.Count(dag.ExecutionOrder(), "\n"); lines != 4 {
		t.Errorf("ExecutionOrder has %d newlines, want 4", lines)
	}
}

// ---------------------------------------------------------------------------
// Phase 2：retryIf
// ---------------------------------------------------------------------------

func TestRetryIfStopsEarly(t *testing.T) {
	a := newTask("a").
		fails(errors.New("invalid argument: field id")).
		retries(5, `not (Error contains "invalid argument")`)

	dag := mustNew(t, tasksOf(a), xdag.WithEvaluator(xexpr.New()))
	_, err := dag.Execute(context.Background())
	if err == nil {
		t.Fatal("expected failure")
	}
	if !strings.Contains(err.Error(), "retryIf evaluated false") {
		t.Errorf("unexpected error: %v", err)
	}
	// 永久性错误不该被重试 5 次
	if a.runs() != 1 {
		t.Errorf("a executed %d times, want 1", a.runs())
	}
	assertState(t, dag, "a", xdag.StateFailed)
}

func TestRetryIfAllowsRetryUntilSuccess(t *testing.T) {
	a := newTask("a").retries(5, `Error contains "timeout"`)
	a.fn = func(_ context.Context, attempt int64, _ map[string]any) (map[string]any, error) {
		if attempt < 3 {
			return nil, errors.New("i/o timeout")
		}
		return map[string]any{"attempt": attempt}, nil
	}

	dag := mustNew(t, tasksOf(a), xdag.WithEvaluator(xexpr.New()))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if a.runs() != 3 {
		t.Errorf("a executed %d times, want 3", a.runs())
	}
	if results["a"]["attempt"] != int64(3) {
		t.Errorf("attempt = %v, want 3", results["a"]["attempt"])
	}
}

// README 中给出的正则示例必须真的可用。
func TestRetryIfWithRegex(t *testing.T) {
	a := newTask("a").
		fails(errors.New("upstream returned 503")).
		retries(3, `Error matches "timeout|connection reset|5\\d\\d"`)
	b := newTask("b").
		fails(errors.New("bad request 400")).
		retries(3, `Error matches "timeout|connection reset|5\\d\\d"`)

	dag := mustNew(t, tasksOf(a, b), xdag.WithEvaluator(xexpr.New()))
	if _, err := dag.Execute(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if a.runs() != 3 {
		t.Errorf("a (503, retryable) executed %d times, want 3", a.runs())
	}
	if b.runs() != 1 {
		t.Errorf("b (400, permanent) executed %d times, want 1", b.runs())
	}
}

func TestRetryIfCanUseAttempt(t *testing.T) {
	a := newTask("a").fails(errors.New("boom")).retries(10, `Attempt < 3`)

	dag := mustNew(t, tasksOf(a), xdag.WithEvaluator(xexpr.New()))
	if _, err := dag.Execute(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	// attempt=1,2 允许重试；attempt=3 时条件为假，停在第 3 次
	if a.runs() != 3 {
		t.Errorf("a executed %d times, want 3", a.runs())
	}
}

func TestRetryIfNotEvaluatedOnLastAttempt(t *testing.T) {
	// 最后一次尝试后不会再重试，此时不应浪费一次求值，
	// 错误信息也应是「失败」而不是「retryIf 为假而中止」
	a := newTask("a").fails(errors.New("boom")).retries(2, `true`)

	dag := mustNew(t, tasksOf(a), xdag.WithEvaluator(xexpr.New()))
	_, err := dag.Execute(context.Background())
	if err == nil {
		t.Fatal("expected failure")
	}
	if strings.Contains(err.Error(), "retryIf") {
		t.Errorf("unexpected retryIf in error: %v", err)
	}
	if !strings.Contains(err.Error(), "failed after 2 attempts") {
		t.Errorf("unexpected error: %v", err)
	}
	if a.runs() != 2 {
		t.Errorf("a executed %d times, want 2", a.runs())
	}
}

func TestRetryIfErrorsAreValidatedAtNew(t *testing.T) {
	t.Run("compile error", func(t *testing.T) {
		a := newTask("a").retries(3, `Error contains`)
		_, err := xdag.New(tasksOf(a), xdag.WithEvaluator(xexpr.New()))
		if err == nil || !strings.Contains(err.Error(), "retryIf") {
			t.Fatalf("want retryIf compile error, got %v", err)
		}
	})

	t.Run("non-ancestor reference", func(t *testing.T) {
		a := newTask("a")
		b := newTask("b").retries(3, `Succeeded("a")`)
		_, err := xdag.New(tasksOf(a, b), xdag.WithEvaluator(xexpr.New()))
		if err == nil || !strings.Contains(err.Error(), "not an ancestor of b") {
			t.Fatalf("want ancestor scope error, got %v", err)
		}
	})

	t.Run("no evaluator", func(t *testing.T) {
		a := newTask("a").retries(3, `true`)
		_, err := xdag.New(tasksOf(a))
		if !errors.Is(err, xdag.ErrNoEvaluator) {
			t.Fatalf("want ErrNoEvaluator, got %v", err)
		}
	})
}

// ---------------------------------------------------------------------------
// Phase 2：边条件
// ---------------------------------------------------------------------------

// 可选依赖：notify 这条边由全局变量控制是否参与门禁。
func TestConditionOptionalDependency(t *testing.T) {
	cases := []struct {
		notify bool
		want   xdag.State
	}{
		{false, xdag.StateSuccess},        // 边失活，b 失败也不影响 d
		{true, xdag.StateUpstreamSkipped}, // 边生效，b 失败则 d 不执行
	}

	for _, tc := range cases {
		t.Run(fmt.Sprintf("notify=%v", tc.notify), func(t *testing.T) {
			a := newTask("a").returns(map[string]any{"ok": true})
			b := newTask("b").fails(errors.New("notify service down"))
			d := newTask("d", "a", "b").edge("b", `Vars["notify"] == true`)

			dag := mustNew(t, tasksOf(a, b, d),
				xdag.WithEvaluator(xexpr.New()),
				xdag.WithVars(map[string]any{"notify": tc.notify}),
			)
			_, _ = dag.Execute(context.Background()) // b 必然失败，忽略聚合错误

			assertState(t, dag, "b", xdag.StateFailed)
			assertState(t, dag, "d", tc.want)
		})
	}
}

// 用边条件表达 OR 汇聚：Dep 是当前边的上游任务名。
func TestConditionOrJoin(t *testing.T) {
	cases := []struct {
		name   string
		bFails bool
		aFails bool
		want   xdag.State
	}{
		{"both ok", false, false, xdag.StateSuccess},
		{"one fails", true, false, xdag.StateSuccess},
		{"all fail", true, true, xdag.StateSkipped}, // 两条边都失活
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			a := newTask("a")
			b := newTask("b")
			if tc.aFails {
				a.fails(errors.New("a down"))
			}
			if tc.bFails {
				b.fails(errors.New("b down"))
			}
			d := newTask("d", "a", "b").
				edge("a", `Succeeded(Dep)`).
				edge("b", `Succeeded(Dep)`)

			dag := mustNew(t, tasksOf(a, b, d), xdag.WithEvaluator(xexpr.New()))
			_, _ = dag.Execute(context.Background())
			assertState(t, dag, "d", tc.want)
		})
	}
}

func TestConditionAllInactiveSkips(t *testing.T) {
	a := newTask("a")
	b := newTask("b")
	d := newTask("d", "a", "b").edge("a", `false`).edge("b", `false`)

	dag := mustNew(t, tasksOf(a, b, d), xdag.WithEvaluator(xexpr.New()))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	// 没有任何依赖构成执行 d 的理由
	assertState(t, dag, "d", xdag.StateSkipped)
	if d.runs() != 0 {
		t.Errorf("d executed %d times, want 0", d.runs())
	}
}

// 已知限制：分支判断挂在边上，没有依赖的根任务没有入边，因此无法附加条件。
// 这里把该行为固化下来，避免日后误以为它是 bug。
func TestRootTaskCannotBeConditioned(t *testing.T) {
	root := newTask("root").edge("nobody", `false`) // 没有这条依赖，条件不会被编译
	leaf := newTask("leaf", "root")

	dag := mustNew(t, tasksOf(root, leaf), xdag.WithEvaluator(xexpr.New()))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	assertState(t, dag, "root", xdag.StateSuccess)
	assertState(t, dag, "leaf", xdag.StateSuccess)
	if root.runs() != 1 {
		t.Errorf("root executed %d times, want 1", root.runs())
	}
}

func TestConditionErrorsAreValidatedAtNew(t *testing.T) {
	t.Run("compile error", func(t *testing.T) {
		a := newTask("a")
		b := newTask("b", "a").edge("a", `Succeeded(`)
		_, err := xdag.New(tasksOf(a, b), xdag.WithEvaluator(xexpr.New()))
		if err == nil || !strings.Contains(err.Error(), "edge condition a -> b") {
			t.Fatalf("want edge compile error, got %v", err)
		}
	})

	t.Run("non-ancestor reference", func(t *testing.T) {
		a := newTask("a")
		b := newTask("b")
		c := newTask("c", "a").edge("a", `Succeeded("b")`)
		_, err := xdag.New(tasksOf(a, b, c), xdag.WithEvaluator(xexpr.New()))
		if err == nil || !strings.Contains(err.Error(), "not an ancestor of c") {
			t.Fatalf("want ancestor scope error, got %v", err)
		}
	})
}
