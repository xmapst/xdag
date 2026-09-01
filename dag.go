// dag.go —— 接口定义、Scheduler 结构，以及一次执行的生命周期（New / Execute）。
//
// 这里只管「一场执行怎么开始、怎么结束」。任务被派生之后发生的事在
// schedule.go，外部干预在 control.go，只读查询在 query.go。

package xdag

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
)

// IScheduler 是 Scheduler 的完整能力：调度、查询、控制、可视化。
// 调用方想在测试里替换掉真实调度器时按它写签名。
type IScheduler interface {
	IControl
	Execute(ctx context.Context) (map[string]map[string]any, error)
	States() map[string]State
	Results() map[string]TaskResult
	ExecutionOrder() string
	PrintGraph()
	WriteGraph(w io.Writer) error
}

// IControl 是控制面：外部干预一次正在进行的执行。
// 两个维度（终止 / 挂起）× 两个作用域（单任务 / 整张图），
// 外加两个状态查询。实现见 control.go。
type IControl interface {
	Cancel(ctx context.Context, opts ...CancelOption) error
	Suspend()
	Resume()
	Suspended() bool
	Canceled() bool
	CancelTask(name string, opts ...CancelOption) error
	SuspendTask(name string) error
	ResumeTask(name string) error
	TaskSuspended(name string) bool
}

// Scheduler 是一次 DAG 执行的调度器实例。用 New 构造，调用一次 Execute
// 驱动全部任务跑完，执行期间与执行结束后都可以并发调用 States/State/
// Results/Phase/Progress/ExecutionOrder/WriteGraph/PrintGraph/Canceled
// 查询状态，也可以并发调用 CancelTask/SuspendTask/ResumeTask/Cancel
// 控制执行。
//
// 一个 Scheduler 只能 Execute 一次；需要重新跑同一张图，请用同一份
// tasks/opts 再次调用 New 构造一个新实例。
// 编译期断言：Scheduler 必须始终满足这两个接口。此前它们只是写在那里，
// 没有任何东西保证实现不会悄悄漂移——加一个方法忘了同步，或者删一个
// 方法忘了从接口里摘掉，都不会有人告诉你。
var (
	_ IScheduler = (*Scheduler)(nil)
	_ IControl   = (*Scheduler)(nil)
)

type Scheduler struct {
	// ---- 构建期确定，此后只读，可无锁并发访问 ----

	tasks map[string]ITask
	opts  options
	// depOrder 是 New 阶段冻结的依赖快照，调度全程只依据它，
	// 不再重复调用 ITask.Dependencies()，详见 snapshotDeps。
	depOrder map[string][]string
	// dependents 是 depOrder 的反向索引：任务名 -> 依赖它的下游任务。
	dependents map[string][]string
	// controls 每个任务一份，支撑 CancelTask/SuspendTask/ResumeTask。
	// 这张 map 本身在 New 之后不再变动，每个控制柄内部自带同步；
	// 这些方法在 Execute 之前就可以调用，见 taskControl.bind。
	controls map[string]*taskControl

	// ---- 调度期可变状态，全部由 mu 保护 ----

	mu             *sync.Mutex
	phase          Phase
	states         map[string]State
	taskResults    map[string]TaskResult
	inDegrees      map[string]int
	executionOrder []string
	// runSuspended 记录是否按下过整场暂停，供 Suspended() 查询。
	// 真正按住任务的是每个 taskControl 上的 sourceRun 那一路。
	runSuspended bool

	// ---- 自带同步，不需要 mu ----

	wg      *sync.WaitGroup
	results *sync.Map
	// executed 保证 Execute 只能被调用一次。
	executed atomic.Bool
	// cancelReported 保证整场级别的取消——父 context 取消/超时，以及
	// Scheduler.Cancel——只上报一条错误，见 cancellation；单独取消某一个
	// 任务（own=true）不受它影响。
	// 见 cancellation；单独取消某一个任务不受它影响。
	cancelReported atomic.Bool
	// sem 是 WithMaxConcurrency 的并发闸门，不限制时为 nil。
	// 容量即上限，按单次尝试获取/释放，见 acquire。
	sem chan struct{}
	// cancelRequested 记录是否调用过 Cancel，仅供 Canceled() 查询。
	// 真正的停机信号走每个任务自己的 context——Cancel 会逐个取消它们，
	// 于是所有等待点只需要 select 一个 ctx.Done() 就够了，不必再各自
	// 记住一条额外的全局通路（历史上两个严重 bug 都是某个等待点漏了它）。
	cancelRequested atomic.Bool
	cancelOnce      sync.Once
	// errCh 是本次执行的错误汇集通道，Execute 开头写入一次，之后不再变动。
	// Execute 之前它是 nil，abandon 据此判定「执行还没开始，没有要放弃的东西」。
	// Cancel 放弃等待、从外部替任务落终态时需要它。
	errCh chan error
	// doneCh 在 Execute 的最后一个 defer 里关闭，供 Cancel 等待排空。
	// 它比 wg 更适合做这件事：wg 的计数在 Execute 开头才开始加，
	// 从别的 goroutine 去 Wait 会撞上「还没 Add 就已经 Wait 到 0」的窗口。
	doneCh chan struct{}
}

// New 构造一个 Scheduler：校验任务图（依赖必须存在、不能成环），
// 并按依赖关系建好调度所需的内部结构。
//
// tasks 的 key 应当与对应任务的 Name() 一致（调度器本身只按 key 索引）。
// 图的规模本身不设上限——需要限制同时执行的任务数请用 WithMaxConcurrency，
// 那才是真正会压垮下游的东西。
func New(tasks map[string]ITask, opts ...Option) (*Scheduler, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}

	// 依赖列表在这里冻结一次，之后校验、入度、建边、调度判定全部用这一份，
	// 免得 Dependencies() 的多次调用返回不同结果、把调度器的入度算错
	depOrder := snapshotDeps(tasks)
	if err := validateGraph(tasks, depOrder); err != nil {
		return nil, err
	}

	dag := &Scheduler{
		mu:          new(sync.Mutex),
		wg:          new(sync.WaitGroup),
		results:     new(sync.Map),
		inDegrees:   make(map[string]int, len(tasks)),
		dependents:  make(map[string][]string, len(tasks)),
		tasks:       tasks,
		opts:        o,
		states:      make(map[string]State, len(tasks)),
		taskResults: make(map[string]TaskResult, len(tasks)),
		depOrder:    depOrder,
		controls:    make(map[string]*taskControl, len(tasks)),
	}
	dag.doneCh = make(chan struct{})
	if o.maxConcurrency > 0 {
		dag.sem = make(chan struct{}, o.maxConcurrency)
	}

	// 校验 Name() 与 map 键一致。
	//
	// 调度器内部只按 map 的键索引任务，而 CancelTask/SuspendTask/ResumeTask
	// 这一整套控制 API 也按键寻址。调用方如果拿 Name() 去调它们，
	// 会全部拿到 ErrUnknownTask——而任务本身跑得好好的，于是表现成
	// 「控制指令静默失效」：点了暂停没反应，日志里只有一句"未知任务"。
	//
	// 这个洞此前只写在文档里（"二者不一致不会被检测出来"），把成本留给了
	// 每一个调用方。而这里本来就在遍历这张 map，顺手一比就能把整类 bug
	// 消灭在构造期——检查是免费的，代价全在不做检查那一侧。
	for name, task := range dag.tasks {
		if got := task.Name(); got != name {
			return nil, fmt.Errorf("%w: map 键 %q 与 Name() 返回的 %q 不一致；"+
				"调度器只按键索引，不一致会让按 Name() 发出的控制指令全部失效",
				ErrTaskNameMismatch, name, got)
		}
	}

	for name := range dag.tasks {
		deps := depOrder[name]
		dag.states[name] = StatePending
		dag.taskResults[name] = TaskResult{State: StatePending}
		dag.inDegrees[name] = len(deps)
		// 控制柄在这里就位，调用方因此可以在 Execute 之前就取消或挂起
		// 某个任务；专属 context 要等 Execute 拿到 ctx 才 bind
		dag.controls[name] = newTaskControl(name)
		for _, dep := range deps {
			dag.dependents[dep] = append(dag.dependents[dep], name)
		}
	}

	return dag, nil
}

// Execute 驱动全部任务跑完：从没有依赖的根任务开始，一个任务**进入终态**后
// 立即尝试调度它的下游，直到所有任务都进入终态。成功与否只决定下游是真正
// 执行还是被判成 StateSkipped/StateCanceled，不决定要不要派生它。
//
// 返回值是全部**成功**任务的输出，key 为任务名；未成功的任务不会出现
// 在其中，需要完整状态请用 States。返回的 error 是本次执行中全部任务
// 错误的聚合（errors.Join），可用 errors.Is/As 检查具体原因。
//
// 同一个 Scheduler 只能调用一次 Execute，第二次调用返回 ErrAlreadyExecuted。
func (d *Scheduler) Execute(ctx context.Context) (map[string]map[string]any, error) {
	if !d.executed.CompareAndSwap(false, true) {
		return nil, ErrAlreadyExecuted
	}
	d.setPhase(PhaseRunning)
	// defer 是 LIFO：这里最先注册，因而最后执行——等 Cancel 的调用方被放行时，
	// settle 已经跑完，它看到的 Phase/States/Results 都是最终值。
	// panic 展开时同样会执行到，不会把等待方永远晾在那里。
	defer close(d.doneCh)
	// settle 用 defer 而不是在 return 前直接调用：这样「Phase().Done() 为真」
	// 严格蕴含「Execute 的返回值已经组装完毕」，中间不留假阳性窗口
	defer d.settle()
	defer d.results.Clear()

	// 把每个任务的控制柄挂到本次执行的 ctx 上。Execute 之前发起的取消
	// 会在这里立即兑现，因此那些任务一被调度就直接落在 StateCanceled。
	for _, ctrl := range d.controls {
		ctrl.bind(ctx)
	}

	// 每个任务最多贡献两条：自身的失败（或 ErrTaskAbandoned，二者互斥）
	// 各一条，加上观察者回调 panic 一条。容量不够时 select-default 会静默
	// 丢弃后到的错误——丢的正是跑得慢的那些任务，最值得看的那批。
	errCh := make(chan error, 2*len(d.tasks)+1)
	d.mu.Lock()
	d.errCh = errCh
	d.mu.Unlock()

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
		go d.runTask(name, errCh)
	}

	d.wg.Wait()

	// 排空而不是 close(errCh)。Cancel 放弃等待之后，被抛下的
	// goroutine 仍然活着、事后还会往 errCh 发——通道一旦关闭，那就是
	// 一个 panic: send on closed channel，而且发生在库自己的 goroutine 里，
	// 调用方连 recover 的机会都没有。不关闭的代价只是那些迟到的错误
	// 落进一个没人读的缓冲里被丢弃，而它们本来就是要丢弃的。
	var err error
	for {
		select {
		case _err := <-errCh:
			err = errors.Join(err, _err)
			continue
		default:
		}
		break
	}

	results := make(map[string]map[string]any)
	d.results.Range(func(key, value any) bool {
		results[key.(string)] = value.(map[string]any)
		return true
	})
	return results, err
}
