# xdag

`xdag` 是一个并发执行有依赖关系的任务图（DAG）的 Go 调度器。

- 声明式地描述任务与依赖，调度器负责按依赖关系并发调度，不需要手写 goroutine 编排
- 调度语义只有一条规则：**一个任务的全部直接依赖都成功，它才会被执行**；没有依赖的根任务不受这条规则约束——除非被取消（`CancelTask`/`Cancel`，或传给 `Execute` 的 ctx 已取消/超时），否则总会执行
- 失败、跳过、取消都会沿依赖链自动级联传播，不会出现「上游没跑成，下游却被静默执行」的情况
- 任务级重试，支持指数退避与无限重试
- 零第三方依赖，只用 Go 标准库

xdag 本身不提供条件分支、表达式引擎这类能力——它不对「要不要执行」做任何业务判断。需要按业务状态决定某个任务实际要不要「做事」，属于任务自身的职责，见[业务层条件判断](#业务层条件判断)。

## 目录

- [适用场景](#适用场景)
- [安装方式](#安装方式)
- [快速开始](#快速开始)
- [核心概念](#核心概念)
- [执行机制说明](#执行机制说明)
- [单任务控制](#单任务控制)
- [并发上限](#并发上限)
- [整场取消](#整场取消)
- [执行期观察](#执行期观察)
- [整体执行状态](#整体执行状态)
- [进度](#进度)
- [结构化结果与错误归属](#结构化结果与错误归属)
- [业务层条件判断](#业务层条件判断)
- [重试机制说明](#重试机制说明)
- [结果与执行顺序](#结果与执行顺序)
- [公开 API](#公开-api)
- [项目结构](#项目结构)
- [许可证](#许可证)

## 功能特性

### 1. DAG 依赖编排

每个任务通过 [`ITask.Dependencies()`](task.go) 声明依赖项。调度器根据依赖关系自动构建图，并确保任务只在其所有直接依赖成功完成后才开始执行。

### 2. 初始化图校验

调用 [`New()`](dag.go) 构造执行器时，内部会先冻结每个任务的依赖列表，再据此调用 [`Validate()`](check.go) 校验任务图：依赖必须真实存在，依赖关系不能成环，且每个任务的 `Name()` 必须与它在 map 里的键一致（不一致返回 `ErrTaskNameMismatch`）。任一条件不满足，初始化立即失败并给出具体的任务名/路径。

### 3. 自动并发执行

所有没有依赖的根任务会首先启动。后续任务在其直接依赖全部进入终态后就会被派生到新的 goroutine 上；依赖是否都成功只决定它是真正执行，还是直接落成 `StateSkipped`/`StateCanceled`，无需手动编排。

### 4. 失败/取消级联传播

任一任务失败或被取消，不会让整场执行崩溃，但会让依赖它的下游任务不再执行——**依赖失败时下游记为 `StateSkipped`，依赖被取消时下游记为 `StateCanceled`**。两者都会继续沿依赖链向后传播，直到抵达不受影响的分支为止。

### 5. 任务级重试

任务执行失败后，可通过 [`ITask.RetryPolicy()`](task.go) 返回的 [`RetryPolicy`](retry.go) 指定最大尝试次数、初始等待间隔、退避倍率与上限，支持显式配置为无限重试。

### 6. 生命周期钩子

任务在每次尝试执行前后分别会调用：

- [`ITask.PreExecution()`](task.go)
- [`ITask.PostExecution()`](task.go)

方便注入日志、监控、埋点、审计等横切逻辑。

### 7. panic 兜底

任务体或前后回调发生 panic 不会打垮整个进程，调度器会将其 recover 并转换为本次尝试的失败（`ErrTaskPanic`），按正常重试策略处理。

### 8. 统一结果汇总与状态查询

所有成功任务的输出会汇总到 [`Execute()`](dag.go) 的返回值中；[`States()`](query.go) / [`State()`](query.go) 则提供包含跳过、取消、失败在内的完整状态视图。

### 9. 可追踪执行顺序与依赖图可视化

[`ExecutionOrder()`](graph.go) 给出任务的实际完成顺序；[`WriteGraph(w)`](graph.go) 把依赖图写入任意 `io.Writer`（`PrintGraph()` 是它写 stdout 的薄包装），输出按名字排序因而确定，菱形结构的汇聚点只展开一次、之后以 `...` 引用，不会随层数指数膨胀。

### 10. 单任务控制

[`CancelTask()`](control.go)/[`SuspendTask()`](control.go)/[`ResumeTask()`](control.go) 可以在执行期间对某一个任务单独发起取消或挂起，不影响图中其他任务；[`Suspend()`](control.go)/[`Resume()`](control.go) 则挂起/恢复整张图，两路挂起来源互相独立，详见[单任务控制](#单任务控制)。

### 11. 整体执行状态

[`Phase()`](phase.go) 用一个值回答「这场执行怎么样」——未开始 / 运行中 / 成功 / 取消 / 失败，运行途中也能问，详见[整体执行状态](#整体执行状态)。

### 12. 并发上限

[`WithMaxConcurrency(n)`](option.go) 限制同时执行的任务数，默认不限。额度按单次尝试占用——被跳过、被取消、在退避等待、被挂起的任务都不占名额，详见[并发上限](#并发上限)。

### 13. 整场取消

[`Cancel(ctx)`](control.go) 取消整张图：取消每个任务的 context 通知任务体，并在 `ctx` 给的宽限期内等待排空，详见[整场取消](#整场取消)。

### 14. 执行期观察

[`WithObserver(fn)`](observer.go) 注册一个回调，每当有任务进入终态时触发一次——它是执行期控制 API 的配套：看不到任务 Y 刚失败，就无从决定要不要取消任务 X。详见[执行期观察](#执行期观察)。

### 15. 进度

[`Progress()`](progress.go) 一次取锁给出各状态的任务计数，适合进度条这类高频轮询，详见[进度](#进度)。

### 16. 结构化结果与错误归属

[`Results()`](query.go) 按任务名给出终态、错误与实际尝试次数。`Execute()` 返回的是 `errors.Join` 聚合，`errors.As` 在上面只会命中 N 个失败中的一个，任务名此前只活在错误文案的字符串里。

---

## 适用场景

- 有明确先后依赖关系的多步骤业务流程（数据拉取 → 加工 → 汇总 → 通知）
- 需要部分并发、部分串行执行，且希望框架自动处理并发编排的场景
- 需要在某个环节失败时，自动跳过下游而不是继续跑出错误结果的场景
- 需要对外部依赖（网络请求、第三方服务）做统一重试与退避的场景

## 安装方式

```bash
go get github.com/xmapst/xdag
```

要求 Go 1.27.0 及以上版本——go.mod 里的 `go 1.27.0` 是硬性下限，不是开发环境说明。

## 快速开始

```go
package main

import (
	"context"
	"fmt"

	"github.com/xmapst/xdag"
)

// demoTask 是一个最小的 ITask 实现。
type demoTask struct {
	name string
	deps []string
	run  func(input map[string]any) (map[string]any, error)
}

func (t *demoTask) Name() string           { return t.name }
func (t *demoTask) Dependencies() []string { return t.deps }
func (t *demoTask) RetryPolicy() *xdag.RetryPolicy {
	return &xdag.RetryPolicy{MaxAttempts: 1}
}
func (t *demoTask) PreExecution(context.Context, int64, map[string]any)         {}
func (t *demoTask) PostExecution(context.Context, int64, map[string]any, error) {}
func (t *demoTask) Execute(_ context.Context, _ int64, input map[string]any) (map[string]any, error) {
	return t.run(input)
}

func main() {
	tasks := map[string]xdag.ITask{
		"fetch-user": &demoTask{
			name: "fetch-user",
			run: func(map[string]any) (map[string]any, error) {
				return map[string]any{"id": 1001, "name": "Tom"}, nil
			},
		},
		"fetch-order": &demoTask{
			name: "fetch-order",
			run: func(map[string]any) (map[string]any, error) {
				return map[string]any{"amount": 199}, nil
			},
		},
		"summary": &demoTask{
			name: "summary",
			deps: []string{"fetch-user", "fetch-order"},
			run: func(input map[string]any) (map[string]any, error) {
				return map[string]any{
					"user":  input["fetch-user"],
					"order": input["fetch-order"],
				}, nil
			},
		},
	}

	dag, err := xdag.New(tasks)
	if err != nil {
		panic(err)
	}

	results, err := dag.Execute(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println(results["summary"])
}
```

`fetch-user`、`fetch-order` 没有依赖，会作为根任务并发启动；`summary` 依赖二者，会在两个依赖都成功后才执行，收到的 `input` 中包含它们各自的输出。

## 核心概念

### 1. ITask 接口

调度所需的一切都通过 [`ITask`](task.go) 接口描述：

```go
type ITask interface {
	Name() string
	Dependencies() []string
	RetryPolicy() *RetryPolicy
	PreExecution(ctx context.Context, attempt int64, input map[string]any)
	Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error)
	PostExecution(ctx context.Context, attempt int64, output map[string]any, err error)
}
```

- `Name()` **必须**与传给 `New()` 的 map 中对应的 key 一致；调度器与全部控制 API 只按 map 的 key 寻址，不一致时 `New()` 直接返回 `ErrTaskNameMismatch`
- `Dependencies()` 只会在 `New()` 阶段被调用一次并冻结，之后改变其返回值不会生效
- `RetryPolicy()` 返回 `nil` 等价于 `&RetryPolicy{MaxAttempts: 1}`（失败不重试）
- `Execute()` 的返回值是一个不透明的 `map[string]any`，原样交给下游任务的 `input`，以及最终的结果集

### 2. 任务状态

调度语义只有一条规则：**全部直接依赖成功，任务才会被执行**。因此「没有成功」只有三种成因，分别对应三种终态：

| 状态 | 含义 |
| --- | --- |
| `StatePending` | 还没有终态。涵盖「在等依赖」「被挂起」和**「正在执行中」**三种情况——调度器不为「运行中」单独设值 |
| `StateSuccess` | 执行成功，输出已写入结果集，也会作为 input 提供给下游 |
| `StateSkipped` | 至少一个直接依赖**失败**，或依赖同样因它的依赖未成功而被跳过；任务体完全没有被调用。注意依赖被**取消**时下游记的是 `StateCanceled` 而不是它 |
| `StateCanceled` | 被取消：整场 `ctx` 取消/超时、`CancelTask`、`Cancel()`。取消沿依赖链原样传播——依赖被取消时下游记的也是它，不是 `StateSkipped` |
| `StateFailed` | 任务体执行失败——`Execute` 返回了 error，或 `Execute`/前后回调发生 panic，且用尽了 `RetryPolicy` 允许的尝试次数（或错误里包了 `ErrNonRetryable`，重试被提前放弃）。另有两条不经任务体走完的路径也记为它：任务体之外的调度路径 panic（这种 `Attempts` 为 0），以及任务 goroutine 没产出任何结果就退出（`ErrTaskAbandoned`，典型是任务体里 `runtime.Goexit`，`Attempts` 是已经发起过的尝试次数，可能 ≥ 1） |

没有依赖的根任务不受「依赖必须成功」这条规则约束，除非被 `CancelTask` 单独取消、被 `Cancel()` 整场取消，或传给 `Execute` 的 `ctx` 已经取消/超时，否则总会被执行。

由此可得一条性质：**图中出现 `StateSkipped`，就必然存在 `StateFailed`**。因为跳过只可能由失败或跳过引起（取消不产生跳过），沿依赖链上溯每一步都落在这两者里，图无环保证链有限，而链的起点不可能还是跳过——没有依赖就不会被跳过。[`Phase()`](phase.go) 的整场判定正是据此可以完全无视 `StateSkipped`。

### 3. 输入参数约定

任务的 `input` 以直接依赖的任务名为 key，值是该依赖 `Execute()` 的返回值；未成功的依赖（跳过/取消/失败）不会出现在 `input` 中。任务只能看到自己的直接依赖，看不到更上游的祖先——需要传递的数据要由中间任务自己在输出里逐层转发。

**`input` 必须当作只读。** 调度器给每个下游的是一层浅拷贝，所以往 `input[dep]` 这一层写 key 不会串到别人；但嵌套的 `map`/`slice` 仍然与上游的输出、以及其他下游共享同一份底层数据，改它就是跨任务的数据竞争。需要修改就自己复制一份。

对称的另一半：`Execute()` 返回的 `output` 交出去之后就不该再动——它会被分发给全部下游，也会进入 `Execute()` 的结果集。

### 只排序、不传数据的依赖

依赖同时表达两件事：**执行顺序**和**数据流向**。只要不去读 `input` 里那个 key，声明依赖就退化成纯粹的顺序约束——「我不要你的输出，只要你先跑完」。

```go
func (t *cleanupTask) Dependencies() []string { return []string{"migrate", "warmup"} }

func (t *cleanupTask) Execute(ctx context.Context, _ int64, input map[string]any) (map[string]any, error) {
	// 压根不碰 input：这里只是要求 migrate 和 warmup 都成功之后才轮到我
	return nil, t.cleanup(ctx)
}
```

这也意味着「依赖必须全部成功」这条规则照常适用：上面这个任务在 `migrate` 失败时会被跳过，正是想要的效果。

### 4. 结果缓存

只有成功任务的输出会被缓存，既用于填充下游的 `input`，也用于 `Execute()` 最终返回的结果集。跳过、取消、失败的任务没有输出可言。

### 5. 入度与依赖关系

`New()` 会为每个任务计算入度（直接依赖数量），并建好反向的「下游」索引。任务进入终态后，调度器会把其全部下游的入度减一，减到 0 的下游立即被调度——这就是并发执行的驱动方式，整个过程不需要额外的调度线程或轮询。

## 执行机制说明

### 单任务调度流程

1. 检查这个任务专属的 context（由 `Execute(ctx, ...)` 的 ctx 派生，`Cancel`/`CancelTask` 同样取消它）是否已被取消/超时，是则终态记为 `StateCanceled`——这一步先于依赖判定，所以被点名取消的任务即使依赖也没跑成，记的仍是 `StateCanceled`
2. 检查全部直接依赖的终态：任一依赖是 `StateCanceled` → 本任务同样记为 `StateCanceled`；否则只要有依赖未成功 → 记为 `StateSkipped`；全部依赖都成功（或没有依赖）→ 进入第 3 步
3. 按 `RetryPolicy` 反复执行「过挂起门 → 取并发名额 → `PreExecution` → `Execute` → `PostExecution`」，直到成功、用尽尝试次数，或 ctx 被取消（单任务取消/整场取消/父 ctx 取消/超时全部汇到这一个信号）
4. 写入终态（`StateSuccess`/`StateFailed`/`StateCanceled`），推进下游：把每个下游的入度减一，减到 0 的立即派生新的 goroutine 调度

终态只写入一次：`Cancel` 宽限期耗尽、放弃等待时会从外部替任务落终态，被抛下的 goroutine 事后返回时的写入会被挡掉。

最后一步对全部终态一视同仁——不止成功，跳过、取消、失败同样会推进下游，否则下游的入度永远减不到 0，会被静默挂起。

### 并发方式

每个可以开始调度的任务（根任务，或入度刚好减到 0 的下游）都会在独立的 goroutine 中运行，不需要额外的调度线程；`Execute()` 会阻塞直到全部任务都进入终态。默认不限制同时执行的任务数，需要限制见[并发上限](#并发上限)。

### 取消

`Execute(ctx, ...)` 期间 `ctx` 被取消或超时，尚未完成的任务会陆续以 `StateCanceled` 收尾；整场执行只会上报一条形如 `execution canceled: <cause>` 的错误，避免大量同质噪音淹没真正的根因（那条错误仍会带上最后一次尝试的真实根因）。

想在执行期间主动叫停，见[整场取消](#整场取消)与[单任务控制](#单任务控制)——它们同样通过取消任务的 context 生效，与这里是同一条通路。

## 单任务控制

除了整场执行级别的取消（传给 `Execute(ctx)` 的 `ctx`），`Scheduler` 还提供了在执行期间对**某一个任务**单独操作的方法：

```go
func (d *Scheduler) CancelTask(name string, opts ...CancelOption) error
func (d *Scheduler) SuspendTask(name string) error
func (d *Scheduler) ResumeTask(name string) error
func (d *Scheduler) SuspendedTask(name string) bool
```

### 一条通路：全部走 context

取消单个任务、取消整张图、父 ctx 取消/超时——**全部通过取消任务专属的 context 生效**，不再有任何旁路信号。这意味着调度器里每一个「尝试之前的等待」（挂起门、并发名额排队、重试退避）都只需要 `select` 一个 `ctx.Done()`。

这不只是好看。此前停止信号分散在三条独立通路上，每加一个等待点就要记得把三条都 select 一遍——历史上两个严重 bug 恰恰都是漏了其中一条：整场停止对排在并发闸门上的任务完全失效（200 个任务一个都没拦住），以及被挂起的任务排不空、`Execute` 永久挂起。收敛到一条通路之后，**这个 bug 类别从结构上消失了**。

### 取消是协作式的

`CancelTask` 取消这个任务专属的 context，正在执行的那次尝试同样收到通知——但调度器**仍然等它自己返回**。能不能及时停下取决于任务体检不检查 `ctx`；Go 没有办法叫停一个不配合的 goroutine。

| 任务当前状态 | `CancelTask` 的效果 |
| --- | --- |
| 待执行 / 下轮重试 / 挂起中 | 100% 有效，任务体一次都不会（再）被调用 |
| 正在执行中 | 通知到，但等它返回 |

只要这次尝试确实因取消而中止，终态就记为 `StateCanceled`、错误带 `ErrTaskCanceled`，并按现有规则级联到下游。任务体无视 `ctx` 把活干完并返回成功时，终态仍是 `StateSuccess`——这正是协作式取消的含义。

想让整张图**不等**在飞的任务、到点就替它们落终态放行下游，用 `Cancel(ctx)`——`ctx` 就是那个宽限期，见[整场取消](#整场取消)。库不提供「只强杀一个任务」：那需要抛下一个仍然活着的 goroutine，而它如果既不检查 `ctx` 又永不返回，就是永久泄漏。

### 附上自己的取消原因

`CancelTask` 与 `Cancel` 都接受可选的 `WithCause`：

```go
var errKilledByUser = errors.New("killed by user")

dag.CancelTask("build", xdag.WithCause(errKilledByUser))
```

cause 会被 `context.Cause` 透给任务体，也会进入这个任务终态的错误里。库的哨兵**不会**被它替换掉，两者同时成立：

```go
err := dag.Results()["build"].Err
errors.Is(err, xdag.ErrTaskCanceled) // true —— 发生了什么
errors.Is(err, errKilledByUser)      // true —— 为什么
```

哨兵回答「发生了什么」，cause 回答「为什么」。「为什么」是调用方的业务语义，不该让库为每一种原因发明一个哨兵——那样每加一种业务原因，库就得跟着发一个版本。不传 `WithCause` 时行为与不带它完全一致。

### 重复调用

对运行中的任务取消可以重复；对已经处于终态的任务做任何控制，一律返回 `ErrTaskAlreadyDone`：

```go
// 运行中
dag.CancelTask("a")  // nil
dag.CancelTask("a")  // nil ——取消不让任务立刻终态，第二次依然有意义

// 已经跑完
dag.CancelTask("done")  // ErrTaskAlreadyDone
```

三个控制方法对未知任务名一律返回 `ErrUnknownTask`。

### 挂起

`SuspendTask` 让任务停在下一个「尝试之前」的检查点上——**任务还没开始第一次尝试**，或**上一次尝试失败、正准备重试**。它打不断已经在进行中的单次 `Execute()` 调用：Go 没有中途冻结一个正在运行的 goroutine 的机制，这是刻意的取舍，而不是遗漏。

挂起是**无限期**的，除了 `ResumeTask` 之外只有取消能叫醒它（都走 `ctx`）。`SuspendTask`/`ResumeTask` 可以在同一个任务上反复交替调用。

还有一路**整场挂起**：

```go
func (d *Scheduler) Suspend()
func (d *Scheduler) Resume()
func (d *Scheduler) Suspended() bool
```

它按住整张图里所有尚未开跑的任务，生效位置与 `SuspendTask` 完全相同。

两路来源**互相独立**，任一为真即挂起：用户单独挂起了步骤 B，随后又挂起整场，之后恢复整场——**B 必须还挂着**。用一个布尔量表示「挂没挂」做不到这一点，整场恢复会顺手把 B 也放行，用户会看见一个自己明明按住了的步骤莫名跑了起来。

一个已经注定要被跳过的任务（它的某个依赖失败了）即使被挂起过，也会正常落在 `StateSkipped`，不会卡在挂起等待上——依赖判定发生在挂起检查之前。

### 可用窗口从 New 就开始

这些方法在 `New()` 之后、`Execute()` 之前就可以调用。「预取消」会在 `Execute()` 开始时立即兑现，任务一被调度就直接落在 `StateCanceled`，任务体一次都不会被调用；「预挂起」则确定性地拦在第一次尝试之前，不需要和调度抢时序。

业务层在构图之后、启动之前就已经知道某一步该取消（配置关掉了、用户点了取消），不必等 `Execute()` 跑起来才能表达。

### 示例

```go
dag, err := xdag.New(tasks)
if err != nil {
	return err
}

// Execute 之前：此刻就知道这一步该跳过
if cfg.SkipNotify {
	_ = dag.CancelTask("notify")
}

go func() { results, err = dag.Execute(ctx) }()

// 执行期间：从另一个 goroutine 控制
_ = dag.SuspendTask("slow-step")
// ... 稍后 ...
_ = dag.ResumeTask("slow-step")

// 掐掉某个任务的重试风暴，但等它手头这次跑完
_ = dag.CancelTask("flaky-step")

// 整张图都不等了：给一个宽限期，到点替没落定的任务判终态、放行下游
gctx, gcancel := context.WithTimeout(context.Background(), 5*time.Second)
defer gcancel()
_ = dag.Cancel(gctx)
```

## 并发上限

xdag 的调度模型是「入度减到 0 就立即派生一个 goroutine」，默认不限制同时执行的任务数。一张宽图会在 `Execute()` 的第一瞬间同时派生出全部根任务——goroutine 本身不是问题（几百个对 Go 运行时微不足道），问题在**进程外**：几十上百个并发请求瞬间打到同一个下游，连接池耗尽、对端限流。

```go
dag, err := xdag.New(tasks, xdag.WithMaxConcurrency(8))
```

传 0 或负数表示不限制，也就是默认行为。

### 额度按单次尝试占用

这是落点上的关键取舍，直接决定会不会把自己饿死：

| 情形 | 占名额吗 |
| --- | --- |
| 正在执行任务体（含 `PreExecution`/`PostExecution`） | 占 |
| 因依赖没跑成而被跳过、或被取消 | 不占——它们根本不会执行 |
| 两次重试之间的退避等待 | 不占 |
| 被 `SuspendTask` 挂起、停在挂起门上 | 不占 |
| 等待名额期间整场被取消 | 立即放弃等待，不会死等 |

如果闸门包住的是「整个任务」而不是「单次尝试」，一个配了 `InfiniteAttempts` 的任务会在退避期间永久霸占一个名额，一个被挂起的任务同样——上限设成 1 时整张图就此停摆。

### 嵌套的乘法效应

如果某个任务在自己的 `Execute()` 里又构造了一个 `Scheduler`，外层任务占着一个名额的同时内层还有自己独立的上限，实际并发是**两者相乘**，不是外层那个数。

## 整场取消

[`Cancel(ctx)`](control.go) 取消整张图的执行：

```go
func (d *Scheduler) Cancel(ctx context.Context, opts ...CancelOption) error
```

它做三件事——取消每个任务专属的 context（正在执行的任务体因此会收到取消通知）、不再让任何新的尝试开始、然后在 `ctx` 给的宽限期内等待排空。

### ctx 是宽限期，不是执行本身的 context

- 宽限期内全部任务落定 → 返回 `nil`，此时 `Execute()` 已经返回，`States()`/`Results()`/`Phase()` 都是最终值
- 宽限期耗尽仍有任务没落定 → 替它们落终态、放行下游，让 `Execute()` 得以返回，然后返回 `ctx.Err()`

```go
go func() { results, err = dag.Execute(execCtx) }()

<-shutdownSignal
ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
defer cancel()
if err := dag.Cancel(ctx); err != nil {
	log.Printf("宽限期内没排空，已强制收尾: %v", err)
}
```

⚠️ **代价要认**：宽限期耗尽后被抛下的任务 goroutine 仍然活着——Go 没有办法杀死一个不肯响应 `ctx` 的 goroutine。任务体如果既不检查 `ctx` 又永不返回，那就是永久泄漏。想避免就给足宽限期，或者把任务体写成尊重 `ctx` 的。

`Execute()` 还没开始就调用则立即返回 `nil`，并且整张图一个任务都不会执行。可以重复调用，也可以并发调用。`Canceled()` 用于查询是否已经发起过取消。`WithCause` 同样适用，见[附上自己的取消原因](#附上自己的取消原因)。

### 与其他叫停方式的关系

| 想要的效果 | 用什么 |
| --- | --- |
| 停整张图，等在飞的收尾，超时强制收 | `Cancel(ctx)` |
| 停整张图，一直等到在飞的自己返回 | `Cancel(context.Background())` |
| 只取消一个任务，等它自己返回 | `CancelTask(name)` |
| 按住整张图，之后还要松开继续 | `Suspend()` / `Resume()` |
| 停整张图，原因走 context 自己那套（同样等在飞的自己返回） | 取消传给 `Execute()` 的那个 `ctx` |

⚠️ **绝对不要在观察者回调里直接调用 `Cancel`**：它要等 `Execute()` 返回，而 `Execute()` 在等这个回调，**必然死锁**。从回调里发起停止就别等它——`go func() { _ = dag.Cancel(ctx) }()`。

## 执行期观察

轮询 `States()` 只能看到当前快照，看不到「刚刚发生了什么」。[`WithObserver(fn)`](observer.go) 在每个任务进入终态时触发一次回调：

```go
dag, err := xdag.New(tasks, xdag.WithObserver(func(ev xdag.Event) {
	metrics.Observe(ev.Task, ev.State.String(), ev.Attempts)
}))
```

`Event` 携带任务名与该任务的 `TaskResult`（`State`/`Err`/`Attempts`）。每个任务恰好触发一次，包括被跳过、被取消、失败的。

### 契约

这几条都是硬约束，不是建议：

- **回调绝对不能阻塞。** `Execute()` 会等待每一个回调返回，回调卡住等于整场执行挂起，没有超时也没有兜底。尤其不要在回调里等任何「要等 `Execute()` 返回之后才会满足」的条件——往一个无缓冲 channel 里发事件、再在 `Execute()` 之后去收，会直接吊死。
- **查询是安全的**（`States`/`Results`/`Progress`/`Phase`/`State`/`ExecutionOrder`/`SuspendedTask`/`Suspended`/`Canceled`/`WriteGraph`）：回调不持有 `d.mu`，这些方法只是各自短暂取一次锁。
- **不等待的控制方法**在回调里调用也安全：`CancelTask`/`SuspendTask`/`ResumeTask` 对刚落终态的这个任务会返回 `ErrTaskAlreadyDone`；`Suspend`/`Resume` 是整场作用域、没有返回值，不受单个任务的终态影响，对下游是否来得及生效取决于下面那条事件时序——需要确定性就别在回调里做控制。
- ⚠️ **绝对不要在回调里直接调用 `Cancel`**：它要等 `Execute()` 返回，而 `Execute()` 在等这个回调，**必然死锁**。要从回调里发起取消就别等它——`go func() { _ = dag.Cancel(ctx) }()`，或者只把决定投递出去、由外面那条主线去调。
- **回调会被并发调用**，实现必须自己保证并发安全。
- **事件顺序不保证与因果顺序一致。** 事件在释放调度锁之后触发，而下游任务在锁内就已经被派生——一个下游的事件完全可能先于它上游的事件到达。需要确定的完成顺序请用 `ExecutionOrder()`。
- **回调只观察，不参与调度决策。** 它没有返回值，做什么都不改变任务终态。
- **回调要快。** 它不阻塞别的任务提交终态（那些在锁内已经完成），但 `Execute()` 要等它返回。要做 I/O 请自己异步化，且异步化的那一端不能反过来依赖 `Execute()` 已经返回。

回调里的 panic 会被接住并转成 `ErrObserverPanic` 汇入 `Execute()` 的返回值（错误是一个 `*ObserverPanicError`，`errors.As` 可取到 panic 值与调用栈）。它与任务体 panic 的 `*PanicError` **刻意不共用类型**：后者的 `errors.Is` 认 `ErrTaskPanic`，混用会让「观察者炸了」被误报成「任务炸了」——而那个任务往往刚刚成功——观察者是调用方代码，出问题不该打垮进程，也不该被静默吞掉。任务终态不受影响，触发回调时它早已落定。

为什么是「锁外触发」而不是「锁内触发」：`d.mu` 是全图唯一一把锁且不可重入，锁内回调时调用方只要碰一下 `States()` 就会自锁死。代价是上面那条顺序不保证，换来的是一个慢回调只拖住自己这个 goroutine。

## 整体执行状态

`State(name)`/`States()` 回答「这个任务怎么样」，[`Phase()`](phase.go) 回答「这场执行怎么样」：

```go
func (d *Scheduler) Phase() Phase
```

| Phase | 含义 |
| --- | --- |
| `PhasePending` | 还没有调用过 `Execute` |
| `PhaseRunning` | `Execute` 进行中且尚未收尾。注意它并不等价于「还有任务没进终态」：最后一个任务落定之后、`Execute` 组装完返回值之前有一个窗口，此时全部任务已终态而 `Phase` 仍是 `running`——这是刻意的，见下方「不提前给出终值」 |
| `PhaseSuccess` | 全部任务都成功（空任务表同样落在这里） |
| `PhaseCanceled` | 没有任务失败，但至少一个任务被取消 |
| `PhaseFailed` | 至少一个任务失败 |

`Phase` 只沿 `PhasePending → PhaseRunning → 三个终值之一` 单向推进，永不回退。

**不提前给出终值**：终值在 `Execute()` 的 defer 里统一结算，因此「最后一个任务落定」到「`Phase` 变成终值」之间存在一个窗口，期间 `Progress().Done() == Total` 而 `Phase()` 仍是 `running`。这是刻意的取舍——它换来的保证是：一旦 `Phase().Done()` 为真，`States()`/`Results()`/`ExecutionOrder()` 与 `Execute()` 的返回值就全都已经定型，中间不留假阳性。要判断「任务是不是都跑完了」请用 `Progress()`，`Phase()` 回答的是「这场执行结束了没有、结果如何」。

### 为什么是独立类型而不是复用 State

`State.Done()` 定义为 `s != StatePending`，而 `CancelTask`/`SuspendTask`/`ResumeTask` 全都靠它拒绝已结束的任务。往 `State` 里加一个「运行中」的值，会让这几个方法对**活着的**任务返回 `ErrTaskAlreadyDone`。两个枚举的取值集合本来也不同：`PhaseRunning` 在任务级不存在，`StateSkipped` 在整场级不可能出现。

### 判定口径

这是库钉死的承诺，不是实现细节：

- 任一任务 `StateFailed` → `PhaseFailed`
- 否则任一任务 `StateCanceled` → `PhaseCanceled`
- 否则 → `PhaseSuccess`

**失败优先于取消**：调度器已经把「执行途中被取消打断」判成了 `StateCanceled`，所以残留下来的 `StateFailed` 是调用方事先并不知道的真实故障，而取消是调用方自己发起的、本来就知道的事——先报它不知道的那一件。注意这与依赖判定里「依赖为 `StateCanceled` 时压过 `StateSkipped`」方向相反：那里回答「我为什么没跑」取最近因，这里回答「这场健不健康」取最重症。

**`StateSkipped` 不参与判定**，因为「有跳过而没有失败」的终局不存在：跳过只可能由某个直接依赖处于 `StateSkipped` 或 `StateFailed` 引起（依赖是 `StateCanceled` 时会被判成 `StateCanceled`），而根任务永远执行、图又无环，沿依赖链上溯必然终止在一个 `StateFailed` 上。

**被挂起而停滞的执行报告 `PhaseRunning`**：调度器只知道「还没跑完」，无法知道「跑不完了」——任务体自己阻塞、配了无限重试、和被挂起，这三者在调度器看来完全一样。要区分停滞原因，遍历 `States()` 的 key 逐个调 `SuspendedTask`。

## 进度

[`Progress()`](progress.go) 返回一份进度快照：

```go
type Progress struct {
	Total    int // 任务总数，执行全程不变
	Pending  int // 尚未进入终态：在等依赖、被挂起、或正在执行中
	Success  int
	Skipped  int
	Canceled int
	Failed   int
}

func (p Progress) Done() int      // Total - Pending
func (p Progress) Ratio() float64 // 已进入终态的占比 [0,1]；空图为 1
func (p Progress) String() string // "4/5 done (success 2, skipped 1, canceled 0, failed 1)"
```

各字段之和恒等于 `Total`，快照在取锁那一瞬间自洽。

### 为什么给计数而不是一个百分比

计数是**无观点的原始事实**。「什么算完成」各家口径不同——跳过算不算数？取消算不算失败？给出计数，任何口径调用方都能自己推导；只暴露一个百分比，就等于替调用方把口径定死了。`Done()`/`Ratio()` 只是最常用那一种口径的便利方法。

### 为什么不用 States() 自己数

能数，但 `States()` 每次都要复制一整张 map。进度条 10Hz 刷新一张 150 个任务的图，那是纯粹的浪费——`Progress()` 只取一次锁、遍历一遍状态表，不分配。需要逐个任务的状态才用 `States()`。

执行期间高频轮询是安全的，这正是它的设计用途。

## 结构化结果与错误归属

`Execute()` 返回的 error 是 `errors.Join` 聚合，`errors.As` 在上面只会命中 N 个失败中的一个，任务名只活在错误文案的字符串里。[`Results()`](query.go) 按任务名给出结构化结果：

```go
type TaskResult struct {
	State    State // 终态
	Err      error // 上报进聚合错误的那个 error；成功与被跳过的任务为 nil
	Attempts int64 // 调度器发起过的尝试次数；一次都没发起过的任务为 0
}

func (d *Scheduler) Results() map[string]TaskResult
```

它刻意**不包含 output**——那已经在 `Execute()` 的返回值里了，存两份等于两个真相来源。这里只放调度器独家掌握、调用方从 `ITask` 接口这一侧原理上拿不到的信息：被跳过的任务、以及一次尝试都没发起过就被取消的任务，`Pre`/`Execute`/`PostExecution` 一次都不会触发，任务自己什么也看不到；执行途中被取消的任务则已经触发过；`Attempts` 此前也只存在于错误文案里。

`Err` 为 `nil` 的情形比「成功」多：被跳过的任务、被上游级联取消的任务（带 `Err` 的是最初被 `CancelTask` 点名的那个），以及整场取消时除第一个命中者之外、且最后一次尝试没留下自身错误的任务——去重的只是「execution canceled」这层取消原因，任务自己最后一次的错误仍会原样带出；被 `Cancel` 宽限期耗尽强制收尾的任务则一律带 `ErrRunCanceled`。**判断一个任务有没有被取消要看 `State`，不能靠 `Err` 是否为 `nil`。**

`Attempts` 的计数发生在每次尝试调用 `PreExecution` 之前，所以 `PreExecution` 自己 panic 的那次也计入。一次尝试都没发起过的任务为 `0`（被跳过、以及还没开始第一次尝试就被取消的）；但**执行途中或退避等待中被取消**的任务 `Attempts` 会 ≥ 1——不要按「终态是 canceled」反推它一定是 0。

## 业务层条件判断

xdag 不提供条件分支、表达式求值这类能力，也不会替任务判断「要不要做」——它的调度语义只有一条规则：全部依赖成功，任务才执行。

需要按业务状态决定某个任务是否真的要执行某个动作（例如「上游返回失败码就不发通知」），请在任务自身的 `Execute()` 里处理：读取依赖的输出、按业务逻辑判断，并把判断结果写进自己的输出，供下游按需读取。这样调度器眼里这一步仍然是**成功**的，不会被误判为「跳过」，业务层的分支逻辑也完全在任务代码里，可以用 Go 本身写、调试、测试，不需要额外学习一套表达式语言。

```go
tasks := map[string]xdag.ITask{
	"check": &demoTask{
		name: "check",
		run: func(map[string]any) (map[string]any, error) {
			return map[string]any{"code": 500}, nil
		},
	},
	"notify": &demoTask{
		name: "notify",
		deps: []string{"check"},
		run: func(input map[string]any) (map[string]any, error) {
			check, _ := input["check"].(map[string]any)
			if check["code"] != 200 {
				// 业务层的分支判断：这一步“不做”，但对调度器而言仍然是成功的
				return map[string]any{"sent": false, "reason": "check failed"}, nil
			}
			return map[string]any{"sent": true}, nil
		},
	},
}
```

完整可运行版本见 [`example_test.go`](example_test.go) 中的 `Example_businessCondition`。

## 重试机制说明

### 重试策略结构

```go
type RetryPolicy struct {
	MaxAttempts int64         // 总执行次数（不是重试次数），零值等同于 1
	Interval    time.Duration // 第一次重试前的等待时间
	Multiplier  float64       // 每次重试后等待时间的放大倍数（指数退避）
	MaxInterval time.Duration // 退避等待时间的上限（同时受全局硬上限 150s 约束）
	Jitter      float64       // 退避时间的随机抖动幅度 [0,1]，零值=不抖动
}
```

### 默认策略与零值语义

`RetryPolicy()` 返回 `nil`，或字段留空，都会在执行前补上默认值：

- `MaxAttempts` 的零值等同于 `1`（只执行一次，失败不重试）
- `Interval` 默认 `1s`
- `Multiplier` 默认 `2.0`；取值在 `(0, 1)` 区间会让等待时间逐次收敛到 `0` 而非发散，这也是合法用法
- `MaxInterval` 默认 `30s`，且始终不超过全局硬上限 `150s`
- `Jitter` 默认 `0`（不抖动）

注意 **`MaxAttempts` 的零值不是无限重试**。需要无限重试必须显式填 [`xdag.InfiniteAttempts`](retry.go)（值为 `-1`）：

```go
policy := &xdag.RetryPolicy{
	Interval:    time.Second,
	MaxAttempts: xdag.InfiniteAttempts,
}
```

把「只填了 `Interval`」的策略误当成「会一直重试到成功」是常见的陷阱——实际效果是只执行一次。

### 指数退避

第 `attempt` 次失败后的等待时间为 `Interval * Multiplier^(attempt-1)`，并钳制在 `[0, MaxInterval]` 区间内，最后按 `Jitter` 施加抖动。

### 抖动

`Jitter` 取值 `[0,1]`，实际等待时间在 `[backoff*(1-Jitter), backoff]` 之间随机取值。**只向下抖不向上抖**，因此 `MaxInterval` 与硬上限始终是真正的上限；抖动也覆盖被钳到上限的情形——一批同时失败的任务恰恰最容易一起顶在上限上，那里正是最需要打散的地方。

建议在扇出较宽的图里打开：同一层的任务会在同一毫秒被派生，失败后又按同一条确定性公式退避，于是整齐地在同一时刻重试，把下游打成尖峰。这种同步性是拓扑强加的，不是巧合。

### 想给任务限时？用 ctx

库**不提供**任务级的时间预算。要限时就在传给 `Execute()` 的 `ctx` 上加 deadline——它沿 context 树传到每一个任务，语义与 Go 的其余部分完全一致：

```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
defer cancel()
results, err := dag.Execute(ctx)
```

这里曾经有过一个 per-task 的 `RetryPolicy.Timeout`，已经删掉。它按墙钟计时，于是**被 `SuspendTask` 按住的任务会在没人看的时候把预算耗光**——用户暂停一个配了 30 分钟预算的步骤去查问题，二十分钟后解挂，步骤直接超时失败、任务体一次都没跑。用户视角那不叫超时，叫「我点了暂停，任务就死了」。而暂停的唯一用途恰恰就是让人介入。

### 不可重试的错误

默认行为是「失败就重试到次数用尽」，但有些错误重试多少次都不会变——参数错误、401、业务规则拒绝。把 [`ErrNonRetryable`](retry.go) 包进返回的 error 里，调度器会立刻放弃剩余次数，既不等待也不再尝试：

```go
func (t *fetchTask) Execute(ctx context.Context, attempt int64, input map[string]any) (map[string]any, error) {
	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err // 网络错误，值得重试
	}
	if resp.StatusCode == http.StatusBadRequest {
		return nil, fmt.Errorf("bad request: %w", xdag.ErrNonRetryable) // 重试也没用
	}
	...
}
```

判据是「错误长什么样」而不是「已经试了几次」，判断权因此完整留在业务侧——库只对返回的 error 做一次 `errors.Is`。

### 执行过程

1. 每次尝试开始前先过挂起门：被 `SuspendTask` 挂起就停在这里，直到 `ResumeTask` 或取消
2. 取一个并发名额（配了 `WithMaxConcurrency` 时），再调用 `PreExecution` → `Execute` → `PostExecution`；名额按单次尝试占用，排队期间被取消会立即退出
3. `Execute` 返回 `nil` 即成功，结束
4. 返回的 error 里包着 `ErrNonRetryable` 就立刻放弃，既不等待也不再尝试
5. 否则按退避策略等待后再次尝试，等待期间 context 被取消会立即中断
6. 用尽尝试次数后返回聚合错误，包含最后一次失败的原因

## 结果与执行顺序

### 返回结果

`Execute()` 返回全部**成功**任务的输出（`map[string]map[string]any`，key 为任务名）。跳过、取消、失败的任务不会出现在其中——需要完整视图请用 `States()`。

### 执行顺序

`ExecutionOrder()` 返回全部成功任务按**完成先后**排列的编号列表（文本形式）。这是完成顺序，不是静态拓扑序：并发任务之间谁先谁后并不确定，多次执行同一张图，顺序完全可能不同。

### 错误聚合

`Execute()` 返回的 `error` 是 `errors.Join` 聚合：全部**失败**任务的错误、取消的**代表**错误（整场取消只上报一条，级联取消的下游不带错误），以及观察者回调的 panic。可用 `errors.Is`/`errors.As` 检查具体原因，但要按任务名逐个拿错误请用 `Results()`，例如 `errors.Is(err, xdag.ErrTaskPanic)` 判断是否有任务发生过 panic。

## 公开 API

### 类型

- [`IScheduler`](dag.go)：`*Scheduler` 的**导出方法全集**（调度 + 查询 + 控制 + 可视化）。它不是一层抽象——库内部一处都不引用它，`New()` 返回的是 `*Scheduler`；留着它只为给测试替身一个现成的签名。要实现请**内嵌**（`struct{ xdag.IScheduler }`）而不是平铺 18 个方法：本接口承诺恒等于全集，因此 `Scheduler` 新增方法时它会同步扩容，**不**按破坏性变更对待，内嵌的实现不受影响。这条不变式由 `TestISchedulerMirrorsScheduler` 守着
- [`IControl`](dag.go)：控制面接口，`IScheduler` 内嵌了它。六个命令（终止/挂起 × 单任务/整张图）外加三个状态回读（`Canceled`/`Suspended`/`SuspendedTask`）——回读跟命令放在一起是有意的，它们读的正是同一格命令写下的东西
- [`Scheduler`](dag.go)：DAG 执行器实例，由 `New()` 构造
- [`ITask`](task.go)：参与调度的最小单元，调用方实现的接口
- [`RetryPolicy`](retry.go)：任务级重试策略
- [`State`](state.go)：任务终态（`StatePending`/`StateSuccess`/`StateSkipped`/`StateCanceled`/`StateFailed`）
- [`Progress`](progress.go)：进度快照（各状态计数 + `Done`/`Ratio`）
- [`Phase`](phase.go)：整场执行的阶段（`PhasePending`/`PhaseRunning`/`PhaseSuccess`/`PhaseCanceled`/`PhaseFailed`）
- [`TaskResult`](query.go)：单个任务的结果摘要（`State`/`Err`/`Attempts`）
- [`ObserverPanicError`](observer.go)：观察者回调里的 panic（`Task` 指触发回调的任务，它本身可能是成功的）
- [`PanicError`](errors.go)：被接住的 panic，用 `errors.As` 取出 `Task`/`Attempt`/`Value`/`Stack`；`Error()` 刻意不含调用栈
- [`Option`](option.go)：构造 `Scheduler` 时的可选配置项
- [`Event`](observer.go)：一次任务终态落定的事件（任务名 + `TaskResult`）

### 构造与执行

- [`New(tasks, opts...)`](dag.go)：校验任务图并构造 `Scheduler`
- [`(*Scheduler).Execute(ctx)`](dag.go)：驱动全部任务跑完，只能调用一次
- [`(*Scheduler).States()`](query.go) / [`(*Scheduler).State(name)`](query.go)：查询任务状态
- [`(*Scheduler).Phase()`](phase.go)：查询整场执行的阶段，见[整体执行状态](#整体执行状态)
- [`(*Scheduler).Progress()`](progress.go)：查询各状态的任务计数，见[进度](#进度)
- [`(*Scheduler).Results()`](query.go)：按任务名查询终态/错误/尝试次数，见[结构化结果与错误归属](#结构化结果与错误归属)
- [`(*Scheduler).ExecutionOrder()`](graph.go)：查询实际完成顺序
- [`(*Scheduler).WriteGraph(w)`](graph.go)：把依赖图写入 `io.Writer`，输出确定、汇聚点不重复展开
- [`(*Scheduler).PrintGraph()`](graph.go)：等价于 `WriteGraph(os.Stdout)`
- [`(*Scheduler).Cancel(ctx, opts...)`](control.go) / [`(*Scheduler).Canceled()`](query.go)：取消整张图（ctx 是宽限期）与查询，见[整场取消](#整场取消)
- [`(*Scheduler).Suspend()`](control.go) / [`(*Scheduler).Resume()`](control.go) / [`(*Scheduler).Suspended()`](query.go)：挂起/恢复整张图与查询
- [`(*Scheduler).CancelTask(name, opts...)`](control.go) / [`(*Scheduler).SuspendTask(name)`](control.go) / [`(*Scheduler).ResumeTask(name)`](control.go) / [`(*Scheduler).SuspendedTask(name)`](query.go)：执行期间对单个任务发起取消/挂起/解挂/查询挂起状态，见[单任务控制](#单任务控制)

### 配置项

- [`WithMaxConcurrency(n)`](option.go)：限制同时执行的任务数，传 0 或负数表示不限制（默认）
- [`WithObserver(fn)`](observer.go)：注册任务终态事件的回调，见[执行期观察](#执行期观察)
- [`WithCause(err)`](control.go)：给 `Cancel`/`CancelTask` 附上调用方自己的取消原因，见[附上自己的取消原因](#附上自己的取消原因)

### 常量与错误

- [`InfiniteAttempts`](retry.go)：`RetryPolicy.MaxAttempts` 的无限重试标记
- `ErrCircularDependency` / `ErrUnknownDependency`：来自 [`Validate()`](check.go) 的图校验错误
- `ErrTaskNameMismatch`：任务的 `Name()` 与它在 tasks 里的键不一致，`New()` 直接拒绝构造
- `ErrAlreadyExecuted`：对同一个 `Scheduler` 调用了不止一次 `Execute`
- `ErrTaskPanic`：调度器接住了一次与该任务有关的 panic（配合 `PanicError` 使用）
- `ErrRunCanceled`：任务是因为整张图被 `Cancel()` 取消而结束的
- `ErrTaskCanceled`：任务被 `CancelTask()` 取消
- `ErrObserverPanic`：`WithObserver` 注册的回调发生了 panic
- `ErrTaskAbandoned`：任务 goroutine 没产出结果就终止了（多为任务里调用了 `runtime.Goexit`，例如 `t.Fatal`）
- `ErrNonRetryable`：由业务包进返回的 error，声明这次失败不必再重试
- `ErrUnknownTask`：`CancelTask`/`SuspendTask`/`ResumeTask`/`SuspendedTask` 收到了不存在的任务名
- `ErrTaskAlreadyDone`：对一个已经处于终态的任务调用了 `CancelTask`/`SuspendTask`/`ResumeTask`

### 辅助函数

- [`Validate(tasks)`](check.go)：独立于 `New()` 单独校验任务图

## 项目结构

```
.
├── doc.go            # 包文档
├── dag.go            # 接口定义、Scheduler 结构，以及 New / Execute 的生命周期
├── schedule.go       # 调度引擎：派生 → 判定 → 执行 → 落终态 → 放行下游
├── control.go        # 控制面：Cancel / CancelTask / Suspend / Resume / WithCause
├── taskcontrol.go    # 单任务控制柄：取消用的专属 context 与挂起用的那道门
├── query.go          # 只读查询面：States / Results / State / Suspended / Canceled / SuspendedTask
├── graph.go          # 依赖图的可视化输出
├── errors.go         # 执行与控制相关的哨兵错误，以及 PanicError
├── task.go           # ITask 接口定义
├── state.go          # State 类型与终态定义
├── phase.go          # Phase 类型与整场执行状态的汇总
├── progress.go       # Progress 类型与各状态计数
├── retry.go          # RetryPolicy 与重试驱动
├── option.go         # 构造期可选配置
├── check.go          # 依赖图校验（悬空依赖、环检测）
├── observer.go       # Event 类型与 WithObserver
├── *_test.go         # 按主题拆分：execute / control / run_suspend / retry /
│                     #   concurrency / observer / progress / graph /
│                     #   robustness；共用脚手架在 helpers_test.go，
│                     #   内部钩子在 export_test.go
├── example_test.go   # godoc Example，兼作用法示例
├── go.mod
└── README.md
```

每个 `.go` 文件开头都有一行说明它装什么。按职责找代码时可以先看那一行，而不必通读整个文件。

xdag 只依赖 Go 标准库，没有任何第三方依赖。

## 许可证

许可证内容见 [`LICENSE`](LICENSE)。
