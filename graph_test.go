// graph_test.go —— 构图校验与依赖图输出。

package xdag_test

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// 图校验
// ---------------------------------------------------------------------------

func TestUnknownDependencyReturnsError(t *testing.T) {
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
}

func TestSelfDependencyIsCycle(t *testing.T) {
	a := newTask("a", "a")
	_, err := xdag.New(tasksOf(a))
	if !errors.Is(err, xdag.ErrCircularDependency) {
		t.Fatalf("want ErrCircularDependency, got %v", err)
	}
}

func TestValidateAcceptsWellFormedGraph(t *testing.T) {
	a := newTask("a")
	b := newTask("b", "a")
	if err := xdag.Validate(tasksOf(a, b)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
}

func TestValidateRejectsNilTask(t *testing.T) {
	err := xdag.Validate(map[string]xdag.ITask{"a": nil})
	if err == nil {
		t.Fatal("want error for nil task")
	}
}

// ---------------------------------------------------------------------------
// 依赖图输出
// ---------------------------------------------------------------------------

// 菱形图里的汇聚点只展开一次：重复展开会让输出随菱形层数指数膨胀。
func TestWriteGraphDeduplicatesJoinPoints(t *testing.T) {
	dag := mustNew(t, tasksOf(
		newTask("a"), newTask("b", "a"), newTask("c", "a"), newTask("d", "b", "c")))

	var sb strings.Builder
	if err := dag.WriteGraph(&sb); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}
	out := sb.String()

	if n := strings.Count(out, "d"); n != 2 {
		t.Errorf("汇聚点 d 出现 %d 次，want 2（一次展开 + 一次带 ... 的引用）\n%s", n, out)
	}
	if !strings.Contains(out, "d ...") {
		t.Errorf("第二次出现的 d 应以 ... 收尾，表示子树已在上方展开\n%s", out)
	}
}

// 多层叠加菱形：输出必须是线性规模，不能指数膨胀。
func TestWriteGraphDoesNotBlowUpOnStackedDiamonds(t *testing.T) {
	const layers = 12
	list := []*testTask{newTask("n0")}
	for i := range layers {
		l := fmt.Sprintf("l%d", i)
		r := fmt.Sprintf("r%d", i)
		prev := fmt.Sprintf("n%d", i)
		next := fmt.Sprintf("n%d", i+1)
		list = append(list, newTask(l, prev), newTask(r, prev), newTask(next, l, r))
	}
	dag := mustNew(t, tasksOf(list...))

	var sb strings.Builder
	if err := dag.WriteGraph(&sb); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}
	// 去重后每个任务最多出现常数次；不去重的话 12 层菱形是 2^12 量级
	if lines := strings.Count(sb.String(), "\n"); lines > 4*len(list) {
		t.Errorf("输出 %d 行，任务数 %d —— 看起来汇聚点被重复展开了", lines, len(list))
	}
}

// 根节点识别必须读 New 阶段冻结的依赖快照：任务在 New 之后改口说自己没有
// 依赖时，它不能既出现在某条链的下游、又被当成根打印一遍。
func TestWriteGraphUsesFrozenDeps(t *testing.T) {
	drift := &driftRoot{testTask: newTask("b")}
	dag := mustNew(t, map[string]xdag.ITask{"a": newTask("a"), "b": drift})

	var sb strings.Builder
	if err := dag.WriteGraph(&sb); err != nil {
		t.Fatalf("WriteGraph: %v", err)
	}
	out := sb.String()

	// b 只应作为 a 的下游出现一次，不应另起一行当根
	if n := strings.Count(out, "b"); n != 1 {
		t.Errorf("b 出现 %d 次，want 1（只作为 a 的下游）\n%s", n, out)
	}
}

// driftRoot 在 New 之后改口说自己没有依赖。
type driftRoot struct {
	*testTask
	calls atomic.Int64
}

func (t *driftRoot) Dependencies() []string {
	if t.calls.Add(1) > 1 {
		return nil
	}
	return []string{"a"}
}

// 同一张图多次输出必须完全一致（根与每层下游都按名字排序）。
func TestWriteGraphIsDeterministic(t *testing.T) {
	build := func() *xdag.Scheduler {
		return mustNew(t, tasksOf(
			newTask("zeta"), newTask("alpha"), newTask("mid", "alpha", "zeta"),
			newTask("beta"), newTask("leaf", "mid", "beta")))
	}
	var first string
	for i := range 5 {
		var sb strings.Builder
		if err := build().WriteGraph(&sb); err != nil {
			t.Fatalf("WriteGraph: %v", err)
		}
		if i == 0 {
			first = sb.String()
			continue
		}
		if sb.String() != first {
			t.Fatalf("第 %d 次输出与第一次不同：\n--- 第一次 ---\n%s\n--- 本次 ---\n%s", i+1, first, sb.String())
		}
	}
}

// WriteGraph 要如实上报 io.Writer 的错误。
func TestWriteGraphPropagatesWriteError(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a"), newTask("b", "a")))
	want := errors.New("disk full")
	if err := dag.WriteGraph(failingWriter{want}); !errors.Is(err, want) {
		t.Errorf("WriteGraph = %v, want %v", err, want)
	}
}

type failingWriter struct{ err error }

func (w failingWriter) Write([]byte) (int, error) { return 0, w.err }
