# 同步 Pregel 运行时

## 目标

实现编译后图的同步执行主线：按 superstep 调度节点，执行本轮任务，收集写入，并在下一轮应用可见。

## 源项目参考

- `study/chapter_4/README.md`：Pregel runtime 的同步执行主线。
- `libs/langgraph/langgraph/pregel/main.py`
- `libs/langgraph/langgraph/pregel/_loop.py`
- `libs/langgraph/langgraph/pregel/_runner.py`
- `libs/langgraph/langgraph/pregel/_algo.py`
- `libs/langgraph/langgraph/pregel/_read.py`
- `libs/langgraph/langgraph/pregel/_write.py`

## 应实现

- `invoke(input)`：执行完整图并返回最终状态。
- `stream(input)`：至少支持输出每轮节点更新或状态快照的一种稳定模式。
- superstep 三阶段模型：Plan、Execution、Update。
- 同一 superstep 内的写入对其他节点不可见，必须到下一轮才可见。
- 基于触发信号和版本信息避免节点重复执行。
- 基础递归/步数限制，防止无限循环。
- 明确的运行时错误类型，例如缺失节点、重复节点、非法边、无入口、无终点、非法更新。

## 当前代码状态

- `src/runtime/mod.rs` 已开始定义 `RuntimeContext<ContextT>`，当前只包含用户运行上下文 `context: ContextT`。该上下文按源项目 `Runtime.context` 的定位作为运行期只读依赖视图使用，图状态修改必须通过节点输出和 channel writes 表达。
- 节点函数签名已经预留 `&RuntimeContext<ContextT>` 参数，用于后续承载 runtime、config、writer、store、执行元数据等运行时信息；当前不提供隐式可变共享上下文。
- `src/pregel/node.rs` 已实现 `PregelNode` MVP，作为后续组装可执行 task 的运行时节点容器；当前保存输入 `channels: Vec<String>`、触发 `triggers`、可选输入 `mapper`、输出 `writers` 和主逻辑 `bound`。
- `PregelNodeBound<StateT, UpdateT, ContextT>` 的节点主逻辑签名对齐 `graph::node::NodeFn`，返回 `NodeOutput<UpdateT>`；当前不预置源项目 `DEFAULT_BOUND` 等默认执行逻辑。
- `src/pregel/task.rs` 已保留任务结构边界：`PregelExecutableTask` 保存任务名、输入、待执行 `bound` 引用、pending writes、writers 引用、triggers、id 和 path；`PregelTaskWrites` 对齐源项目传给 `apply_writes` 的轻量写入批次，只保存 `name`、`writes`、`triggers` 和 `path`。`PregelTaskManager::submit_task` 会按 task id 暂存任务，`prepare_task` / `prepare_tasks` 已实现源项目 PULL 任务准备主线：根据可用 trigger channel 选择节点，读取普通 channel 输入，应用节点 mapper，并生成待执行任务。单任务执行逻辑收敛在内部 helper 中：先执行 `bound`，再把节点输出按顺序交给 writers 的 `assemble`，将产生的 writes 追加到 task pending writes。`execute_pending_tasks` 是当前 MVP 的批量执行入口：空任务直接返回，单任务走同步 fast path，多任务使用 scoped threads 并发执行并按稳定顺序返回 `PregelTaskWrites`。
- `src/pregel/loops.rs` 已保留最小同步 Pregel loop 构造边界。`PregelLoop::new` 会从 `Pregel.channels` 和 `Pregel.managed` 通过 `copy_box()` 复制出本次运行专用 channel map 与 managed map；nodes、input/output/stream channels、stream mode、trigger 索引和 name 等图规格字段在 loop 中使用引用。`PregelLoop::enter` 对齐源项目 `SyncPregelLoop.__enter__` 的进入边界，并调用 `first` 执行 fresh input 的最小初始化：`enter` 将 `self.input_channels` 传给 `first(input_channels)`；`first` 把初始输入映射为 input writes，构造无 triggers 的 `PregelTaskWrites` 并直接调用 `apply_writes` 应用到 input channel，记录 `updated_channels`，再将状态切到 `Pending`。`execute` 已接入当前 superstep 已准备任务的执行和 pending writes 收集；`tick`、`after_tick` 和 `is_stream_closed` 当前暂不实现真实调度、更新或发送逻辑。
- `src/pregel/pregel.rs` 已实现 `Pregel` MVP 容器，保存 `nodes`、`channels`、`managed`、`input_channels`、`output_channels`、`stream_channels`、`stream_mode`、`recursion_limit`、`trigger_to_nodes` 和 `name`。
- `Pregel::validate` 已实现源项目 `validate_graph` 的最小 Rust 版校验：检查节点读取 channel、trigger channel、input/output/stream channel 是否存在，要求至少一个 input channel 被节点订阅，并重建 `trigger_to_nodes`。
- `CompiledStateGraph` 已能由 `StateGraph::compile()` 生成，并持有可校验的 `Pregel` 容器；当前已提供 `stream` 转发到内部 `Pregel::stream`，但仍未实现 `invoke`。
- `CompiledStateGraph::attach_node` 已接入 `START` 入口节点和用户节点：`START` 节点订阅 `START` input channel，用户节点读取所有 state channel 和 managed value，安装最小 state writer，并创建和订阅 `branch:to:{node}` trigger channel；`attach_edge` 已接入普通边控制流 writer 和多起点 join barrier channel；`attach_branch` 已把单目标条件分支封装为可执行 `ChannelWriter`，用于按路由结果写入 `branch:to:{target}`。
- `CompiledStateGraph` 当前将 `stream_channels` 默认设置为与 `output_channels` 相同的 state channel 集合；这对应源项目 `StateGraph.compile()` 会显式传入 stream channels 的路径。
- `src/error.rs` 已定义公共 `GraphError` 类型，当前覆盖 channel 空读、分支解析错误和构图阶段的基础结构错误。
- channel 写入层已具备 `ChannelWriter::assemble`，当前 `CompiledStateGraph::attach_edge` 会为普通边注册控制流 writer，`attach_branch` 会注册可执行条件分支 writer。task 执行节点后会把节点输出、当前 state 和 `RuntimeContext` 一起传给节点 writers，把组装出的 `(channel, StateValue)` 追加到 task writes。runtime 的 Update 阶段再统一按 channel 聚合同轮 writes，调用对应 channel 的 `update(values)`，并依据返回值维护 changed channel 集合。`PregelLoop::apply_writes` 已实现这一 Update 阶段核心原语：按 task path 排序、消费已读 trigger channel、聚合同轮 writes、对未更新的可用 channel 发送空更新，并在没有后续触发节点时调用 `finish()`。
- `Pregel::stream(input)` 已使用 `tokio::sync::mpsc` 返回管道 receiver，并由后台 task 建立 `PregelLoop`，随后立即调用 `enter()`，贴近源项目 `with SyncPregelLoop(...) as loop:` 的调用顺序。`tick` 保持返回是否继续的接口，`execute` 和 `after_tick` 不返回 stream chunk；真实调度和发送语义后续再补。`CompiledStateGraph` 通过 `Arc<Pregel>` 让后台 task 持有图规格，实际 loop 仍借用规格字段；每次运行复制 channels 和 managed，避免不同 stream 调用共享可变运行态。
- 已为 `RuntimeContext` 用户上下文字段和 `GraphError` 展示文本补充基础单元测试。

## 当前未完成

- `invoke` 尚未实现；`stream()` 当前只暴露管道和 loop 构造边界，尚未提供同步 Pregel 主循环的真实执行结果。
- `PregelLoop::tick` 尚未接入任务准备，因此完整 `stream()` 主循环仍不能端到端执行图。
- `PregelLoop::after_tick` 尚未调用 `apply_writes` 汇总 task writes；因此 Update 阶段原语和 Execution 阶段原语已可单测，但完整 stream/invoke 主循环仍未贯通。
- `first(input_channels)` 当前只支持 fresh input 初始化，并已对齐源项目 `_first(input_keys=...)` 的显式 input channel 参数和 fresh input 路径调用写入应用原语；但源项目 `_first` 中的 discard tasks、checkpoint 恢复、resume、Command 输入、time travel、delta channel 持久化和 interrupt 传播仍未迁移。因此 `None` 输入会作为空输入错误返回，而不是被解释为 resume。
- `PregelNode.channels` 已由 `CompiledStateGraph::attach_node` 填入所有 state channel 和 managed value；`prepare_task` 会把可用普通 channel 组装为 `StateValue::Object` 并调用节点 mapper。managed value 当前只有 `copy_box()`，尚无 `get()` 读取能力，因此任务输入暂不注入 managed value。
- `NodeOutput::Command` 的运行时解释尚未实现。
- 条件边和 waiting edge 已能编译为 writer / trigger / barrier channel，但还没有完整运行时调度验证；checkpoint、interrupt/resume 尚未接入运行时。
- 当前 `stream()` 的类型边界要求 `StateT: From<StateValue>`、`UpdateT: Into<StateValue>`、`ContextT: Default`，后续需要接入更完整的 typed state mapper 和 runtime context 传入机制。

## 暂缓

- async 节点执行接口。
- 远程图、子进程执行器或线程池优化。
- streaming transformer 与多种输出协议。
- retry、cache、timeout 的完整运行时策略。






