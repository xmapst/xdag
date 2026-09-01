// execute_test.go —— 基础执行、失败传播、重复执行与状态查询、Dependencies 的稳定性。

package xdag_test

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xmapst/xdag"
)

// ---------------------------------------------------------------------------
// 基础执行
// ---------------------------------------------------------------------------

func TestLinearChainExecution(t *testing.T) {
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
	assertState(t, dag, "fetch-user", xdag.StateSuccess)
	assertState(t, dag, "fetch-order", xdag.StateSuccess)
	assertState(t, dag, "summary", xdag.StateSuccess)
	if lines := strings.Count(dag.ExecutionOrder(), "\n"); lines != 4 {
		t.Errorf("ExecutionOrder has %d newlines, want 4", lines)
	}
}

// 菱形依赖：a 分叉到 b、c，再汇聚到 d。
func TestDiamondExecution(t *testing.T) {
	a := newTask("a").returns(map[string]any{"v": 1})
	b := newTask("b", "a")
	c := newTask("c", "a")
	d := newTask("d", "b", "c")

	dag := mustNew(t, tasksOf(a, b, c, d))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	for _, name := range []string{"a", "b", "c", "d"} {
		assertState(t, dag, name, xdag.StateSuccess)
	}
	if len(results) != 4 {
		t.Fatalf("got %d results, want 4", len(results))
	}
}

// 依赖的输出必须原样出现在下游的 input 参数里。
func TestDependencyOutputsVisibleAsInputs(t *testing.T) {
	a := newTask("a").returns(map[string]any{"x": 1})
	b := newTask("b").returns(map[string]any{"y": 2})
	c := newTask("c", "a", "b")

	var gotA, gotB map[string]any
	c.fn = func(_ context.Context, _ int64, input map[string]any) (map[string]any, error) {
		gotA, _ = input["a"].(map[string]any)
		gotB, _ = input["b"].(map[string]any)
		return map[string]any{}, nil
	}

	dag := mustNew(t, tasksOf(a, b, c))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotA["x"] != 1 {
		t.Errorf("input[a][x] = %v, want 1", gotA["x"])
	}
	if gotB["y"] != 2 {
		t.Errorf("input[b][y] = %v, want 2", gotB["y"])
	}
}

// 修复前：Execute 一边 range inDegrees 一边派生会写同一张 map，
// 触发 fatal error: concurrent map iteration and map write。
// 该用例需要 -race 才能稳定暴露。
func TestManyRootsNoRace(t *testing.T) {
	const n = 60
	list := make([]*testTask, 0, 2*n)
	for i := range n {
		root := fmt.Sprintf("root-%02d", i)
		list = append(list, newTask(root), newTask(fmt.Sprintf("leaf-%02d", i), root))
	}

	dag := mustNew(t, tasksOf(list...))
	results, err := dag.Execute(context.Background())
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(results) != 2*n {
		t.Fatalf("got %d results, want %d", len(results), 2*n)
	}
}

// ---------------------------------------------------------------------------
// 失败传播
// ---------------------------------------------------------------------------

func TestFailureDoesNotStallDownstream(t *testing.T) {
	boom := errors.New("boom")
	a := newTask("a").fails(boom)
	b := newTask("b", "a")
	c := newTask("c", "b")

	dag := mustNew(t, tasksOf(a, b, c))
	_, err := dag.Execute(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
	assertState(t, dag, "a", xdag.StateFailed)
	assertState(t, dag, "b", xdag.StateSkipped)
	assertState(t, dag, "c", xdag.StateSkipped)
	if b.runs() != 0 || c.runs() != 0 {
		t.Errorf("downstream should not execute: b=%d c=%d", b.runs(), c.runs())
	}
}

// 一条依赖失败不影响与它无关的兄弟分支；只有真正依赖它的子树才会被跳过。
func TestFailureIsScopedToItsOwnSubtree(t *testing.T) {
	bad := newTask("bad").fails(errors.New("bad"))
	good := newTask("good").returns(map[string]any{"ok": true})
	downOfBad := newTask("down-of-bad", "bad")
	downOfGood := newTask("down-of-good", "good")
	joined := newTask("joined", "bad", "good")

	dag := mustNew(t, tasksOf(bad, good, downOfBad, downOfGood, joined))
	_, _ = dag.Execute(context.Background())

	assertState(t, dag, "bad", xdag.StateFailed)
	assertState(t, dag, "good", xdag.StateSuccess)
	assertState(t, dag, "down-of-bad", xdag.StateSkipped)
	assertState(t, dag, "down-of-good", xdag.StateSuccess)
	assertState(t, dag, "joined", xdag.StateSkipped)
}

func TestResultsOnlyContainSuccessfulTasks(t *testing.T) {
	a := newTask("a").fails(errors.New("boom"))
	b := newTask("b").returns(map[string]any{"ok": true})
	c := newTask("c", "a")

	dag := mustNew(t, tasksOf(a, b, c))
	results, _ := dag.Execute(context.Background())
	if _, ok := results["a"]; ok {
		t.Error("failed task should not appear in results")
	}
	if _, ok := results["c"]; ok {
		t.Error("skipped task should not appear in results")
	}
	if _, ok := results["b"]; !ok {
		t.Error("successful task missing from results")
	}
	if lines := strings.Count(dag.ExecutionOrder(), "\n"); lines != 2 {
		t.Errorf("ExecutionOrder has %d newlines, want 2 (leading newline + only b succeeded)", lines)
	}
}

// ---------------------------------------------------------------------------
// 重复执行 / 状态查询
// ---------------------------------------------------------------------------

func TestExecuteTwiceIsRejected(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")))
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if _, err := dag.Execute(context.Background()); !errors.Is(err, xdag.ErrAlreadyExecuted) {
		t.Fatalf("want ErrAlreadyExecuted, got %v", err)
	}
}

func TestStateOfUnknownTaskIsPending(t *testing.T) {
	dag := mustNew(t, tasksOf(newTask("a")))
	if got := dag.State("no-such-task"); got != xdag.StatePending {
		t.Errorf("State(unknown) = %v, want StatePending", got)
	}
}

func TestPrintGraphDoesNotPanic(t *testing.T) {
	a := newTask("a")
	b := newTask("b", "a")
	c := newTask("c", "a")
	dag := mustNew(t, tasksOf(a, b, c))
	dag.PrintGraph() // 仅要求不 panic、不死锁
}

// ---------------------------------------------------------------------------
// Dependencies() 的稳定性
// ---------------------------------------------------------------------------

// driftTask 的 Dependencies() 每次调用返回的结果都不同，
// 用于验证构建期只快照一次、调度不会因此错乱。
type driftTask struct {
	*testTask
	calls atomic.Int64
}

func (t *driftTask) Dependencies() []string {
	if t.calls.Add(1)%2 == 0 {
		return []string{"fast"}
	}
	return []string{"fast", "slow"}
}

// Dependencies() 不稳定时，入度、建边、调度判定必须仍然基于同一张图，
// 否则任务会永久 pending 或在依赖尚未完成时被判定。
func TestUnstableDependenciesAreSnapshotAtNew(t *testing.T) {
	fast := newTask("fast")
	slow := newTask("slow")
	slow.fn = func(context.Context, int64, map[string]any) (map[string]any, error) {
		time.Sleep(30 * time.Millisecond)
		return map[string]any{"slow": true}, nil
	}
	child := &driftTask{testTask: newTask("child")}

	dag := mustNew(t, map[string]xdag.ITask{"fast": fast, "slow": slow, "child": child})
	if _, err := dag.Execute(context.Background()); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if state := dag.State("child"); state != xdag.StateSuccess {
		t.Fatalf("child 未被正常调度，state=%v", state)
	}
	assertState(t, dag, "fast", xdag.StateSuccess)
	assertState(t, dag, "slow", xdag.StateSuccess)
}
