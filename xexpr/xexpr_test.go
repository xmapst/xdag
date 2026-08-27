package xexpr_test

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/expr-lang/expr"

	"github.com/xmapst/xdag"
	"github.com/xmapst/xdag/xexpr"
)

func TestCompileRejectsBadExpressions(t *testing.T) {
	engine := xexpr.New()
	for _, expression := range []string{
		`1 +`,          // 语法错误
		`42`,           // 非 bool
		`Unknown == 1`, // 环境中不存在的标识符
	} {
		if _, err := engine.Compile(expression); err == nil {
			t.Errorf("Compile(%q) should fail", expression)
		}
	}
}

// 同一个 Program 会被多个 goroutine 并发求值，必须并发安全。
func TestProgramIsConcurrencySafe(t *testing.T) {
	program, err := engineCompile(t, `Vars["n"] > 10`)
	if err != nil {
		t.Fatal(err)
	}

	const workers = 64
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(i int) {
			defer wg.Done()
			env := &xdag.Env{Task: "t", Vars: map[string]any{"n": i}}
			got, err := program.RunBool(context.Background(), env)
			if err != nil {
				t.Errorf("RunBool: %v", err)
				return
			}
			if want := i > 10; got != want {
				t.Errorf("n=%d: got %v, want %v", i, got, want)
			}
		}(i)
	}
	wg.Wait()
}

func TestCanceledContextIsRejected(t *testing.T) {
	program, err := engineCompile(t, `true`)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := program.RunBool(ctx, &xdag.Env{Task: "t"}); err == nil {
		t.Fatal("want context error")
	}
}

// MaxNodes 可以被调用方覆盖。
func TestMaxNodesIsOverridable(t *testing.T) {
	engine := xexpr.New(expr.MaxNodes(3))
	if _, err := engine.Compile(`Vars["a"] == 1 and Vars["b"] == 2 and Vars["c"] == 3`); err == nil {
		t.Fatal("want max nodes error")
	} else if !strings.Contains(err.Error(), "nodes") {
		t.Errorf("unexpected error: %v", err)
	}
}

// 固化 README「编译期校验」小节里的双包共存示例，防止文档与实现漂移。
func TestReadmeDualImportExample(t *testing.T) {
	engine := xexpr.New(
		expr.MaxNodes(200),
		expr.DisableBuiltin("now"),
	)
	if _, err := engine.Compile(`Succeeded(Dep)`); err != nil {
		t.Fatalf("Compile: %v", err)
	}
	// 覆盖生效：200 个节点的上限应当拦下超长表达式
	long := `Vars["a"] == 1`
	for i := 0; i < 200; i++ {
		long += ` and Vars["a"] == 1`
	}
	if _, err := engine.Compile(long); err == nil {
		t.Error("expected MaxNodes override to reject the long expression")
	}
}

func engineCompile(t *testing.T, expression string) (xdag.Program, error) {
	t.Helper()
	return xexpr.New().Compile(expression)
}

func TestReferencesAreExtracted(t *testing.T) {
	program, err := engineCompile(t,
		`Output("a").code == 200 and Succeeded("b") and Inputs["c"]["x"] > 0 and State("a") == "success"`)
	if err != nil {
		t.Fatal(err)
	}

	referencer, ok := program.(xdag.Referencer)
	if !ok {
		t.Fatal("program should implement xdag.Referencer")
	}

	got := make(map[xdag.Reference]bool)
	for _, ref := range referencer.References() {
		got[ref] = true
	}

	want := []xdag.Reference{
		{Kind: xdag.RefAncestor, Task: "a"}, // Output("a") 与 State("a") 去重后只留一条
		{Kind: xdag.RefAncestor, Task: "b"},
		{Kind: xdag.RefInput, Task: "c"},
	}
	for _, ref := range want {
		if !got[ref] {
			t.Errorf("missing reference %v %q", ref.Kind, ref.Task)
		}
	}
	if len(got) != len(want) {
		t.Errorf("got %d references, want %d: %v", len(got), len(want), referencer.References())
	}
}

// 非字面量引用无法静态判定，不应出现在 References 中，否则会误报。
func TestDynamicReferencesAreIgnored(t *testing.T) {
	for _, expression := range []string{
		`Succeeded(Task)`,
		`Succeeded(Dep)`,
		`Vars["env"] == "prod"`,
	} {
		program, err := engineCompile(t, expression)
		if err != nil {
			t.Fatalf("Compile(%q): %v", expression, err)
		}
		if refs := program.(xdag.Referencer).References(); len(refs) != 0 {
			t.Errorf("Compile(%q) reported %v, want none", expression, refs)
		}
	}
}
