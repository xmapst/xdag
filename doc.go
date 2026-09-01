// Package xdag 是一个并发执行有依赖关系的任务图（DAG）的调度器。
//
// 核心规则只有一条：一个任务的全部直接依赖都成功，它才会被执行；没有依赖
// 的根任务不受这条规则约束，总是执行。任一依赖未成功（失败、被取消，或
// 同样因为它自己的依赖未成功），下游任务体不会被调用，终态记为
// StateSkipped——但依赖是被**取消**的话，下游记的是 StateCanceled 而不是
// StateSkipped，取消沿依赖链原样传播。无论哪种，调度都不会因此停摆，
// 其余不受影响的分支照常推进。
//
//	tasks := map[string]xdag.ITask{
//	    "fetch-user":  fetchUserTask,
//	    "fetch-order": fetchOrderTask,
//	    "summary":     summaryTask, // Dependencies() 返回 ["fetch-user", "fetch-order"]
//	}
//	dag, err := xdag.New(tasks)
//	results, err := dag.Execute(context.Background())
//
// xdag 本身不对“要不要执行”做任何业务判断——它只负责按依赖关系调度。
// 需要按业务状态决定某个任务实际要不要“做事”，请在该任务自身的 Execute()
// 里处理：读取依赖的输出、自行决定要不要跳过真正的工作，并把决定写进
// 输出，供下游任务按需读取。
//
// 执行期间还可以用 CancelTask/SuspendTask/ResumeTask 对某一个任务单独
// 取消/挂起/解挂，不影响图里的其他任务；挂起只保证在任务还没开始第一次
// 尝试、或两次重试尝试之间生效，不会打断正在进行中的单次 Execute() 调用。
// Cancel 则停掉整张图：取消每个任务专属的 context（正在执行的任务体因此会
// 收到取消通知）、不再让任何新的尝试开始，然后在传给它的 ctx 给出的**宽限期**
// 内等待排空——那个 ctx 是宽限期，不是执行本身的 context。宽限期耗尽仍有任务
// 没落定，调度器会替它们落终态、放行下游，好让 Execute 得以返回，而被抛下的
// goroutine 仍然活着。想让在飞的那次尝试干完、之后还能松开继续，用
// Suspend/Resume 而不是 Cancel。
//
// 没有「嵌套 DAG」这个概念，也不需要——ITask 只是一个接口，它的 Execute
// 里跑什么都行，包括再构造并运行一个 Scheduler。见 Example_subgraph。
package xdag
