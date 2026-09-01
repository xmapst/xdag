// run_suspend_test.go —— 整场挂起／恢复，及其与单任务挂起的独立性。

package xdag

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// gateTask 记录自己被调用了几次，并可选地阻塞。
type gateTask struct {
	name  string
	deps  []string
	mu    sync.Mutex
	calls int
	block chan struct{}
}

func (g *gateTask) Name() string                                                { return g.name }
func (g *gateTask) Dependencies() []string                                      { return g.deps }
func (g *gateTask) RetryPolicy() *RetryPolicy                                   { return nil }
func (g *gateTask) PreExecution(context.Context, int64, map[string]any)         {}
func (g *gateTask) PostExecution(context.Context, int64, map[string]any, error) {}
func (g *gateTask) Execute(ctx context.Context, _ int64, _ map[string]any) (map[string]any, error) {
	g.mu.Lock()
	g.calls++
	g.mu.Unlock()
	if g.block != nil {
		select {
		case <-g.block:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, nil
}
func (g *gateTask) ran() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.calls
}

// TestSuspendHoldsWholeRun 验证整场挂起真的把所有任务按在起跑线上。
func TestSuspendHoldsWholeRun(t *testing.T) {
	a := &gateTask{name: "a"}
	b := &gateTask{name: "b"}
	dag, err := New(map[string]ITask{"a": a, "b": b})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	dag.Suspend()
	if !dag.Suspended() {
		t.Fatal("Suspend 之后 Suspended 仍报 false")
	}

	done := make(chan error, 1)
	go func() { _, e := dag.Execute(context.Background()); done <- e }()

	time.Sleep(80 * time.Millisecond)
	if a.ran() != 0 || b.ran() != 0 {
		t.Fatalf("整场挂起没拦住：a 跑了 %d 次，b 跑了 %d 次", a.ran(), b.ran())
	}

	dag.Resume()
	if dag.Suspended() {
		t.Fatal("Resume 之后 Suspended 仍报 true")
	}
	select {
	case e := <-done:
		if e != nil {
			t.Fatalf("恢复后执行报错: %v", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Resume 之后执行没有推进")
	}
	if a.ran() != 1 || b.ran() != 1 {
		t.Fatalf("恢复后没有正常执行：a=%d b=%d", a.ran(), b.ran())
	}
}

// TestRunAndTaskSuspendAreIndependent 守住这次设计里最要紧的一条。
//
// 整场挂起与单任务挂起必须互相独立。用「整场挂起时逐个 suspend、
// 恢复时逐个 resume」实现的话，
// 「暂停某个步骤 → 暂停整个任务 → 恢复整个任务」会把那个步骤**静默放行**，
// 而用户以为它还停着。
func TestRunAndTaskSuspendAreIndependent(t *testing.T) {
	held := &gateTask{name: "held"}
	free := &gateTask{name: "free"}
	dag, err := New(map[string]ITask{"held": held, "free": free})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 先单独挂起 held，再整场挂起、整场恢复。
	if err = dag.SuspendTask("held"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}
	dag.Suspend()
	dag.Resume()

	go func() { _, _ = dag.Execute(context.Background()) }()
	time.Sleep(120 * time.Millisecond)

	if free.ran() != 1 {
		t.Fatalf("没有被单独挂起的任务应当已经跑完，实际跑了 %d 次", free.ran())
	}
	if held.ran() != 0 {
		t.Fatal("单独挂起的任务被整场恢复静默放行了——用户以为它还停着")
	}
	if !dag.SuspendedTask("held") {
		t.Fatal("单独挂起的状态被整场恢复清掉了")
	}

	// 单独放行它才该跑。
	if err = dag.ResumeTask("held"); err != nil {
		t.Fatalf("ResumeTask: %v", err)
	}
	deadline := time.After(3 * time.Second)
	for held.ran() == 0 {
		select {
		case <-deadline:
			t.Fatal("单独恢复之后仍未执行")
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// TestRunSuspendDoesNotUnstickTaskSuspend 是反方向的同一条不变式。
func TestRunSuspendDoesNotUnstickTaskSuspend(t *testing.T) {
	a := &gateTask{name: "a"}
	dag, err := New(map[string]ITask{"a": a})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	dag.Suspend()
	if err = dag.SuspendTask("a"); err != nil {
		t.Fatalf("SuspendTask: %v", err)
	}
	// 只松开整场那一路，单任务那一路还按着。
	dag.Resume()
	if !dag.SuspendedTask("a") {
		t.Fatal("整场恢复把单独挂起也解掉了")
	}
}

// TestTaskNameMustMatchKey 守住构造期校验。
//
// 调度与控制 API 全都按 map 键寻址。Name() 与键不一致时，按 Name() 发出的
// 控制指令会全部拿到 ErrUnknownTask，而任务本身跑得好好的——表现是
// 「点了暂停没反应」，且没有任何地方会告诉你为什么。
//
// 这个洞此前只写在文档里。检查是免费的：New 本来就在遍历这张 map。
func TestTaskNameMustMatchKey(t *testing.T) {
	bad := &gateTask{name: "mytask/build"} // 键会写成 "build"
	_, err := New(map[string]ITask{"build": bad})
	if !errors.Is(err, ErrTaskNameMismatch) {
		t.Fatalf("Name() 与键不一致却构造成功了：err=%v", err)
	}

	good := &gateTask{name: "build"}
	if _, err = New(map[string]ITask{"build": good}); err != nil {
		t.Fatalf("一致的图被误拒: %v", err)
	}
}
