# Rust 版相对源项目的改进记录

本文记录 `mini-langgraph-rs` 在参考 Python `langgraph` 时已经确认或计划采用的 Rust 化改进。这里的“改进”不是追求完整兼容，而是在保持核心语义的前提下，利用 Rust 的类型系统、所有权和模块边界减少运行时歧义。

当前代码仍处于骨架阶段，本文只记录已经体现在源码或已明确作为本项目取舍的设计方向。

## 1. 用所有权限制 builder 编译后继续修改

源项目通过 `compiled: bool` 记录 builder 是否已经编译。编译后继续调用 `add_node`、`add_edge` 等方法时，Python 版本通常只能发出 warning。

Rust 版不保留 `compiled: bool`，而是让 `compile(self)` 消费 `StateGraph`：

```rust
pub fn compile(self) -> Result<CompiledStateGraph<...>, GraphError>
```

这样 builder 在编译后会被 move，调用方无法再继续修改同一个 builder。这个约束由编译器保证，不需要额外运行时状态位。

## 2. 节点执行签名统一，不复制 Python 多签名注入

源项目的 `StateNode` 支持多种 Python callable 形态，例如 `node(state)`、`node(state, config)`、`node(state, *, runtime)`、`node(state, *, writer)`、`Runnable` 等，并通过 `RunnableCallable` 做参数识别和包装。

Rust 版当前采用统一节点函数类型：

```rust
dyn Fn(&NodeInputT, &RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>
```

所有运行时依赖统一放入 `RuntimeContext<ContextT>`，避免在运行时识别函数参数。该上下文按源项目 `Runtime.context` 的定位作为只读运行依赖视图，不作为节点之间共享可变状态；需要可变共享资源时由调用方显式放入线程安全类型。后续如果需要适配不使用 context 的闭包，可以在 `add_node` 层做轻量 adapter，而不是复制完整 `Runnable` 生态。

## 3. 区分局部更新和节点完整输出

源项目节点可以返回普通 update，也可以返回 `Command(update=..., goto=...)` 或多个 command。Rust 版当前用泛型 `UpdateT` 表示局部状态更新，用 `NodeOutput<UpdateT>` 表示节点完整返回值。

当前骨架包含以下方向：

```rust
enum NodeOutput<UpdateT> {
    Update(UpdateT),
    Command(Command<UpdateT>),
    Commands(Vec<Command<UpdateT>>),
    None,
}
```

这样 `UpdateT` 不承担控制流职责，后续 runtime 可以分别处理状态写入和 command 跳转。

## 4. 条件边目标解析单独建模

源项目 `BranchSpec._finish()` 同时负责把 route 返回值统一成列表、根据 `ends` 映射目标节点、校验非法目标，并写入 Pregel channel。

Rust 版当前把目标解析逻辑收敛到 `BranchSpec::resolve`：

```rust
pub fn resolve(&self, output: BranchOutput) -> Result<Vec<String>, GraphError>
```

这让“路由结果到目标节点列表”的映射可以先独立测试，后续再接入 Pregel 写入和调度。

## 5. Join 边规范化，避免等价 waiting edge 重复

源项目 `waiting_edges` 直接保存 `tuple(start_key)`，因此 `['a', 'b'] -> 'c'` 和 `['b', 'a'] -> 'c'` 会成为两条不同 waiting edge，并在编译后生成不同 join channel。

Rust 版新增 `WaitingEdgeSpec`：

```rust
pub struct WaitingEdgeSpec {
    pub starts: Vec<String>,
    pub end: String,
}
```

构造时会对 `starts` 排序并去重：

```rust
starts.sort();
starts.dedup();
```

因此 `a,b -> c` 与 `b,a -> c` 会归一为同一条 join 边。这个行为比源项目更贴近 join/barrier 的集合语义。

## 6. Channel 抽象使用关联类型表达固定类型关系

源项目 `BaseChannel(Generic[Value, Update, Checkpoint])` 用 Python 泛型表达 channel 的当前值、更新值和 checkpoint 类型。

Rust 版迁移为 trait associated types：

```rust
trait BaseChannel {
    type Value;
    type Update;
    type Checkpoint;
}
```

这表达“某个具体 channel 自身固定一组 `Value/Update/Checkpoint` 类型”，比把这些类型作为调用方传入的 trait 泛型更贴合 Rust 模型。

Rust 版还把 `from_checkpoint` 设计为接收 `&self`：

```rust
fn from_checkpoint(&self, checkpoint: Option<Self::Checkpoint>) -> Result<Self, GraphError>
```

这让 `BinaryOperatorAggregate` 这类带 reducer 配置的 channel 能复用原实例中的 reducer，再恢复 checkpoint 值；也让 barrier channel 能保留预设名称集合。源项目依赖 Python 对象实例保存这些配置，Rust 版显式体现在 trait 方法签名中。

## 7. 动态 channel map 显式擦除为 StateValue

Python 源项目可以直接保存：

```python
channels: dict[str, BaseChannel]
```

因为 Python dict 可以容纳不同 `Value/Update/Checkpoint` 类型的 channel。Rust 的 `HashMap` value 类型必须统一，因此当前引入动态值：

```rust
enum StateValue {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    List(Vec<StateValue>),
    Object(HashMap<String, StateValue>),
}
```

并定义：

```rust
type DynChannel = dyn BaseChannel<
    Value = StateValue,
    Update = StateValue,
    Checkpoint = StateValue,
>;
```

这样 `StateGraph` 可以保存：

```rust
channels: HashMap<String, Box<DynChannel>>
```

这是对 Python 动态 channel 总表的显式 Rust 化。

## 8. Managed value spec 用 trait 表达计算规格

源项目中：

```python
ManagedValueSpec = type[ManagedValue]
managed: dict[str, ManagedValueSpec]
```

字段名保存在 dict key 中，value 是 managed value 的计算规格，例如 `IsLastStepManager`。

Rust 版当前用 trait object 方向表达：

```rust
trait ManagedValueSpec: Send + Sync {
    fn copy_box(&self) -> Box<dyn ManagedValueSpec>;
}
```

并在 `StateGraph` 中保存：

```rust
managed: HashMap<String, Box<dyn ManagedValueSpec>>
```

这避免把字段名错误地放进 spec 内部。后续等 `PregelScratchpad` 建立后，再给 trait 增加类似 `get(scratchpad)` 的方法。

## 9. 用 StateSchema trait 替代 Python 动态 schema 反射

源项目 `schemas: dict[type[Any], dict[str, BaseChannel | ManagedValueSpec]]` 用于缓存每个 Python schema 类型对应的字段视图，服务于 `state_schema`、`input_schema`、`output_schema`、节点级 input schema 和 branch input schema。

Rust 版当前所有节点和 branch 仍围绕 `StateT` 工作，还没有独立 input/output projection，也没有 schema derive 宏。因此不复制 Python 的动态 `schemas` 缓存，先引入 crate 内部手写 `StateSchema` trait：

```rust
trait StateSchema {
    fn channels() -> HashMap<String, Box<DynChannel>>;
    fn managed() -> HashMap<String, Box<dyn ManagedValueSpec>>;
}
```

`StateGraph::with_schema()` 会调用 `StateT::channels()` 和 `StateT::managed()`，把 state schema 的普通 channel 与 managed value 填入 builder；`StateGraph::new()` 仍保留为空 builder，方便测试和后续手动装配。由于 channel 和 managed 类型当前仍是 crate 内部骨架，`StateSchema` 和 `with_schema()` 暂不作为外部公共 API 暴露。

这个取舍先达成源项目 `_add_schema(self.state_schema)` 的最小效果，但字段名和 channel 类型仍由用户显式声明。等后续出现独立 `InputT`、`OutputT`、节点输入投影或宏生成 schema 时，再扩展 schema trait 或追加 derive 宏。

## 10. ChannelWriter 先收敛为同步字段写入层

源项目 Pregel 写入路径包含 `ChannelWriteEntry`、`ChannelWriteTupleEntry`、`Send`、`TASKS`、`RunnableCallable` 包装、async writer 和静态写入分析等能力，用于同时覆盖状态字段写入、任务发送和复杂 runnable 组合。

Rust 版当前先实现 `ChannelWriter` MVP：

```rust
struct ChannelWriter {
    entries: Vec<ChannelWriterEntry>,
}
```

它只负责把节点输出 `StateValue`、固定值或 mapper 结果组装为 `(channel, StateValue)` pending writes，不直接更新 `HashMap<String, Box<DynChannel>>`。单 channel 写入由 `ChannelWriteEntry` 表达，多 channel 展开由 `ChannelWriteTupleEntry` 表达；后者不保存单独的 `value`，而是直接把节点返回值交给 mapper，对应源项目中 `_get_updates`、`_control_branch` 这类把节点返回值展开为多条 writes 的 mapper。

这个取舍保留了源项目“writer 产出 task writes，Update 阶段再统一应用到 channel”的核心语义，但不复制 Python 的 `RunnableCallable`、config side effect、async writer、`Send` / `TASKS` 和静态写入分析。后续 runtime 接入时应让 task 调用节点 writers 的 `assemble`，再由独立 Update 算法按 channel 聚合并调用 `BaseChannel::update(values)`。

## 11. 错误类型集中到 GraphError

源项目在不同位置抛出 `ValueError`、`InvalidUpdateError`、`EmptyChannelError` 等异常。Rust 版当前统一预留 `GraphError` 作为图构建、channel 和运行时错误边界。

当前已有的错误覆盖 channel、branch 和构图阶段，例如：

```rust
EmptyChannel
InvalidBranchTarget(String)
MultipleUpdatesWithoutReducer { count }
InvalidChannelUpdate(String)
InvalidBarrierValue(String)
DuplicateNode(String)
MissingEntrypoint
```

后续新增运行时调度、状态合并或 checkpoint 错误时，也应继续集中到 `GraphError`，让 API 返回 `Result<_, GraphError>`。

## 12. PregelNode 只保留任务装配核心字段

源项目 `PregelNode` 同时保存 Pregel 调度所需字段和 LangChain 生态相关策略字段，例如 `channels`、`triggers`、`mapper`、`writers`、`bound`、`retry_policy`、`cache_policy`、`timeout`、`tags`、`metadata`、`subgraphs`、`is_error_handler` 和 `error_handler_node`。

Rust 版当前只迁移后续组装可执行 task 必需的核心字段：

```rust
struct PregelNode<ContextT> {
    channels: Vec<String>,
    triggers: Vec<String>,
    mapper: Option<PregelNodeMapper<StateT>>,
    writers: Vec<ChannelWriter>,
    bound: PregelNodeBound<StateT, UpdateT, ContextT>,
}
```

其中 `channels` 先统一保存为 `Vec<String>`，不额外建模源项目 `channels: str | list[str]` 的单值分支；`PregelNodeBound` 的签名对齐 `graph::node::NodeFn`，返回 `NodeOutput<UpdateT>`；当前不预置源项目 `DEFAULT_BOUND` 等默认执行逻辑。`retry`、`cache`、`timeout`、tracing metadata、subgraph 发现和 error handler 暂不迁移，避免在同步 Pregel 主线尚未完成前引入策略层复杂度。

## 13. PregelExecutableTask 先对齐核心执行字段

源项目 `PregelExecutableTask` 同时保存执行本体和多种运行时策略字段：`name`、`input`、`proc`、`writes`、`config`、`triggers`、`retry_policy`、`cache_key`、`id`、`path`、`writers`、`subgraphs` 和 `timeout`。

Rust 版当前只保留后续同步 Pregel 主循环直接需要的核心字段：

```rust
struct PregelExecutableTask<StateT, UpdateT, ContextT> {
    name: String,
    input: StateT,
    bound: PregelNodeBound<StateT, UpdateT, ContextT>,
    writes: Vec<(String, StateValue)>,
    writers: Vec<ChannelWriter<StateT, ContextT>>,
    triggers: Vec<String>,
    id: String,
    path: Vec<String>,
}
```

其中 `bound` 保存节点主逻辑，`writers` 保存节点写入器，`writes` 保存执行期间产生的 pending writes；三者在任务结构中显式拆开，不再通过 `proc: PregelNode` 间接承载。`id`、`path` 和 `writes` 直接使用标准集合类型，不额外定义类型别名。`PregelTaskWrites` 对齐源项目中可传给 `apply_writes` 的写入批次，让 fresh input 和已执行 task 都能进入同一个 Update 阶段入口；`PregelExecutableTask::to_writes()` 用于把执行完成的任务投影为写入批次。`PregelTaskManager` 内部用 `HashMap<String, PregelExecutableTask<...>>` 按任务 id 索引任务，当前已能提交、稳定取出、构造并执行任务。`config`、`retry_policy`、`cache_key`、`subgraphs` 和 `timeout` 暂不迁移，避免在基本调度循环完成前引入策略层复杂度。

## 14. Pregel 先实现容器和校验，不提前复制运行时生态

源项目 `Pregel` 同时承载核心运行时状态和大量平台/生态能力，例如 checkpoint、store、cache、retry、timeout、interrupt、debug event、schema/jsonschema、subgraph 和 stream transformer。

Rust 版当前只保留同步运行时主线后续需要的最小容器字段：

```rust
struct Pregel<StateT, UpdateT, ContextT> {
    nodes: HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
    channels: HashMap<String, Box<DynChannel>>,
    managed: HashMap<String, Box<dyn ManagedValueSpec>>,
    input_channels: Vec<String>,
    output_channels: Vec<String>,
    stream_channels: Option<Vec<String>>,
    stream_mode: StreamMode,
    recursion_limit: usize,
    trigger_to_nodes: HashMap<String, Vec<String>>,
    name: String,
}
```

源项目的 `channels: dict[str, BaseChannel | ManagedValueSpec]` 在 Rust 版拆成 `channels` 和 `managed` 两张表，以保留动态 channel map 的同时避免把 managed value 当作普通 channel 更新。当前 `Pregel::validate` 只迁移 `validate_graph` 的最小结构校验，并重建 `trigger_to_nodes`；`invoke` 已接入同步 loop 主线，`stream` 已接入最小 Tokio mpsc 管道和 loop 构造边界，checkpoint、interrupt/resume 等能力仍暂缓。

## 15. CompiledStateGraph 先固定编译装配边界

源项目 `CompiledStateGraph.compile()` 会在编译期完整接入 `attach_node`、`attach_edge`、`attach_branch`、state update writer、branch writer、join barrier channel、schema mapper 和 Pregel 运行时配置。

Rust 版当前实现最小编译装配边界：

```rust
pub fn compile(self) -> Result<CompiledStateGraph<...>, GraphError>
```

`compile(self)` 消费 builder，调用 `StateGraph::validate()`，创建 `CompiledStateGraph`，再通过 `attach_node`、`attach_edge` 和 `attach_branch` 装配 `Pregel` 容器。Rust 版对齐源项目，先以 `attach_node(START, None)` 创建入口节点并订阅 `START` input channel；用户节点读取所有 state channel 和 managed value，安装最小 state writer，并为自己创建和订阅 `branch:to:{node}` trigger channel；`attach_edge` 会把 `START -> a` 和普通边 `a -> b` 都编译为起点节点 writer 写入 `branch:to:{target}`，不重复追加目标节点已有的 `branch:to:{target}` trigger；`attach_branch` 会把单目标条件分支封装为可执行 `ChannelWriter`，按 route 结果写入 `branch:to:{target}`。

源项目 `StateGraph.compile()` 会分别计算 `output_channels` 与 `stream_channels` 并传入 `CompiledStateGraph`。Rust 版当前还没有独立 input/output schema projection，因此先让 `stream_channels` 默认等于 `output_channels`，后续引入 schema 后再拆分两者。

当前不复制源项目的完整 checkpoint 或 stream 路径；`PregelNode.bound` 直接复用 builder 中的节点函数，`mapper` 暂不接入真实状态投影协议。源项目通过 `ChannelWrite.register_writer(branch.run(...))` 把分支 runnable 标记为 writer；Rust 版用泛型可执行 `ChannelWriter` 表达同一编译边界，让 writer 能访问 `&StateT` 和 `RuntimeContext` 后返回 `ChannelWriteEntry`。该实现暂不迁移 schema reader、`Send`、async 或 Runnable 生态。waiting edge 已按源项目 `attach_edge(starts, end)` 路径编译为 `NamedBarrierValue` join channel，目标节点订阅 join channel，各起点节点写入自己的节点名。`CompiledStateGraph` 当前公开 `invoke`、`stream` 和 `stream_with_mode`，但一次 stream 调用仍只接受一个 `StreamMode`，不复制源项目多 stream mode 的复合输出协议。

## 16. PregelLoop 持有每次运行的独立状态

源项目 `PregelLoop` / `SyncPregelLoop` 同时承载运行时核心状态和 checkpoint、cache、store、interrupt、debug stream、retry、async executor、生命周期事件等平台能力。Rust 版当前只实现同步 Pregel 主线需要的运行态，并把图规格与运行状态分开：`Pregel` 保存 nodes、channel 原型和配置，`PregelLoop` 持有本次运行复制出的 channels、step、pending writes、updated channels 和输出缓存。

当前 `PregelLoop::new` 从 `Pregel.channels` 和 `Pregel.managed` 复制本次运行专用 map，不再借用或共享这两类运行态。`PregelLoop::enter` 对齐源项目 `SyncPregelLoop.__enter__` 的调用边界，并调用 Rust 版 `first` 执行 fresh input 初始化；`first` 只迁移源项目 `_first` 中“输入映射为 channel 写入并产生 `updated_channels`”的主线。`tick` 已接入源项目 Plan 阶段的最小主线：检查递归限制，根据上一轮 `updated_channels` 准备 PULL tasks，无任务时标记 `Done`。`execute` 已接入当前 superstep 已准备任务的执行和 pending writes 收集，并保持源项目“执行阶段只产出 writes，Update 阶段再应用 channel”的边界。`after_tick` 已接入最小 Update 阶段：应用 pending writes、刷新 `updated_channels`、清空 pending writes 并推进 `step`。`output` 读取最终 output channels：单 channel 返回该值，多 channel 返回对象并跳过不可用字段。`is_stream_closed` 目前只反映 Tokio receiver 是否关闭。

源项目 `_first` 还处理 checkpoint、resume、Command、time travel、delta channel、interrupt 等路径。Rust 版当前没有这些运行时能力，因此 `first` 不写入 pending writes，也不把 `None` 输入解释为 resume；`None` 或无法映射到 input channel 的输入会返回明确错误。

源项目的 `tasks: dict[str, PregelExecutableTask]` 在 Rust 版映射为 `PregelTaskManager`，让任务集合、准备和执行边界集中在 task manager 中。当前暂不迁移 retry、cache、timeout、interrupt、debug stream 和 checkpoint 语义。

## 17. Tokio mpsc stream 采用每次运行独立 channel map

源项目 `Pregel` 上的 `channels` 更接近图规格，真正执行时由 `SyncPregelLoop` 为本次运行准备独立 channel 和 managed 状态。Rust 版对齐这个边界：`Pregel.channels` 保存 channel 原型，`Pregel.managed` 保存 managed 规格，`PregelLoop::new` 通过 `copy_box()` 为每次 `stream()` 创建新的 `HashMap<String, Box<DynChannel>>` 和 `HashMap<String, Box<dyn ManagedValueSpec>>`。nodes、input/output/stream channels、stream mode、trigger 索引和 name 等图规格字段在 loop 中只通过引用使用，不为每次 loop 克隆。

channel 的 `copy_box()` 是现有 `BaseChannel::copy()` 的 trait object 版本，具体复制语义仍来自 channel 自身的 `checkpoint()` + `from_checkpoint()`。因此 `LastValue`、`EphemeralValue`、`BinaryOperatorAggregate` 和 `NamedBarrierValue` 都能复用已有 checkpoint 恢复逻辑。managed 的 `copy_box()` 暂只复制 managed spec object，scratchpad 读取语义后续再补。

Rust 版 `stream()` 使用 `tokio::sync::mpsc` 返回 receiver，并由后台 task 创建 `PregelLoop` 后立即调用 `enter()`。为了让后台 task 持有图规格，`CompiledStateGraph` 内部保存 `Arc<Pregel>`；`stream(&self)` 只 clone `Arc` 指针，后台 task 内部创建的 `PregelLoop` 再借用 `Pregel` 中的规格字段。`PregelNode` 和 `ChannelWriter` 本身不需要实现 `Clone`。当前不使用 `Arc<Mutex<channels>>`，连续或并发的 `stream()` 调用不会互相污染 channel 或 managed 状态。

当前节点执行仍是同步闭包，Tokio 只负责后台任务和管道发送。`stream()` 的类型边界暂时要求 `StateT: From<StateValue>`、`UpdateT: Into<StateValue>`、`ContextT: Default`，后续接入 typed state mapper 后再放宽或替换这组 MVP 约束。

## 18. `prepare_task(s)` 先迁移 PULL 任务准备主线

源项目 `prepare_next_tasks` 会合并 PUSH/Send 和 PULL 节点任务，并依赖 checkpoint 版本、versions_seen、scratchpad、Runnable config、cache/retry/timeout、interrupt/resume 等运行时设施判断任务是否可执行。

Rust 版当前只实现 PULL 节点任务准备：`prepare_tasks` 根据 `updated_channels + trigger_to_nodes` 缩小候选节点，没有增量信息时按节点名扫描全部节点；`prepare_task` 在任一 trigger channel 可用时读取普通 channel 输入，组装为 `StateValue::Object`，再交给节点 mapper 或 `StateT::from` 生成任务输入。任务 id 使用确定性字符串，path 使用 `["pull", node_name]` 表达源项目 `(PULL, node_name)`。

由于 Rust 版尚未实现 checkpoint 版本表，当前不复制源项目“channel version 大于 versions_seen 才触发”的完整语义，而是以 channel `is_available()` 作为可执行条件。`ManagedValueSpec` 目前只有 `copy_box()`，没有 scratchpad 相关 `get()`，因此 managed value 只作为合法读取项保留在 node channels 中，暂不注入任务输入。PUSH/Send、cache、retry、timeout、subgraph、interrupt/resume 和 Runnable config 仍暂缓。

## 19. `execute_pending_tasks` 迁移执行阶段主线

源项目由 `PregelRunner` 并发调度 task，并通过 `run_with_retry` / `arun_with_retry` 执行节点 runnable；节点执行期间 writer 会把 writes 追加到 task，随后由 loop 的 apply/update 阶段统一更新 channel。

Rust 版当前把对外执行入口收敛为 `PregelTaskManager::execute_pending_tasks`：空任务直接返回，单任务走同步 fast path，多任务使用 `std::thread::scope` 并发执行，并按稳定 task path/id 顺序返回 `PregelTaskWrites`。单任务执行逻辑只作为内部 helper 存在：调用 task 的 `bound` 得到 `NodeOutput`，把 `Update` 转成 `StateValue`、把 `None` 转成 `StateValue::Null`，再按顺序调用每个 `ChannelWriter::assemble`，将组装出的 `(channel, StateValue)` 追加到 task 的 `writes`。

为贴近源项目 `Runtime.context` 的只读运行依赖约定，Rust 版节点、branch 和 writer 均接收 `&RuntimeContext<ContextT>`，并要求并发执行路径上的 `ContextT: Sync`。runtime 不为用户 context 提供隐式锁；如果调用方需要共享可变依赖，应在 `ContextT` 内显式使用 `Arc<Mutex<_>>` 等线程安全类型。这个取舍保留源项目“节点执行和 writer 只产出 pending writes，channel 更新由后续阶段统一处理”的核心语义，同时避免过早引入 retry、timeout、cache、error handler 和 checkpoint 写入策略。

## 20. `apply_writes` 先迁移无 checkpoint 的 Update 原语

源项目 `pregel/_algo.py::apply_writes` 同时负责 channel 写入应用、checkpoint `versions_seen` 更新、channel version 递增、reserved control write 过滤、未更新 channel 的 step 通知以及最后 superstep 的 `finish()` 通知。

Rust 版当前没有 checkpoint、channel version、pending writes 持久化和 reserved control channel 表，因此 `PregelLoop::apply_writes` 只迁移当前运行时能表达的核心语义：按 task path 前 3 段排序，消费 task triggers 读过的 channel，按 channel 聚合同轮 writes，调用 `BaseChannel::update(values)`，对未更新但可用的 channel 调用空更新，并在本轮更新无法触发后续节点时调用 `finish()`。

未知 channel 写入沿用源项目 warning 后忽略的方向；mini 版暂不接入日志系统，因此实现为静默忽略。返回值只包含更新后仍 `is_available()` 的 channel，用于后续调度判断。`first(input_channels)` 对齐源项目 `_first(input_keys=...)` 的显式输入 channel 参数；fresh input 路径已复用同一写入应用原语：输入先按传入的 input channels 映射为 input writes，再构造无 triggers 的 `PregelTaskWrites` 并直接调用 `apply_writes` 写入 channel。但仍不提前引入源项目 `_first` 中 discard task、checkpoint resume 或 Command 输入路径。

## 21. `tick -> execute -> after_tick` 形成最小同步闭环

源项目 `PregelLoop.tick()` 负责检查步数、调用 `prepare_next_tasks`、处理中断/调试/缓存写入恢复，并把是否继续执行返回给外层 runner；`after_tick()` 则在任务执行完成后调用 `apply_writes`、输出 stream values、清理 checkpoint pending writes、保存 checkpoint 并推进运行状态。

Rust 版当前只迁移无 checkpoint 的同步主线：`tick()` 清理上一轮 task 集合，根据 `updated_channels` 准备本轮 PULL tasks，空任务时进入 `Done`，超过递归限制时进入 `OutOfSteps` 并返回 `PregelRecursionLimitReached`。`execute()` 是同步执行入口，`invoke` 与 `stream` 都复用它；它运行已准备任务并收集 `PregelTaskWrites`，在 `StreamMode::Updates` 下把命中 stream/output channels 的 task writes 映射为 `StateValue::Object({ node: update })` 后通过 `try_send` 非阻塞发送给调用方。`after_tick()` 也是同步入口，使用 `apply_writes` 应用本轮 pending writes，把返回的 channel 集合作为下一轮 `updated_channels`，并在 `StreamMode::Values` 下读取 stream/output channels 的可用快照后通过 `try_send` 非阻塞发送给调用方，然后递增 `step`。

由于没有 checkpoint 版本表，Rust 版不会复制源项目基于 `versions_seen` 的精确重复执行判断；当前仍以 channel `is_available()` 和上一轮 `updated_channels` 的 trigger 索引作为最小调度条件。stream 输出也只保留源项目 `map_output_updates` / `map_output_values` 的核心形状：`updates` 不暴露控制流 trigger channel，`values` 不读取不可用 channel，多输出或多节点更新通过 `StateValue::Object` / `StateValue::List` 表达。多 stream mode 列表、checkpoint pending writes、interrupt、debug/tasks/messages/custom stream、retry、cache、timeout、PUSH/Send task 和 error handler 仍暂缓。
## 当前仍需谨慎的地方

- 当前源码仍是骨架，很多类型未公开或未使用，warning 是预期状态。
- `StateValue` 是动态路线，后续如果要强化类型安全，可以增加宏生成的强类型 partial update。
- `DynChannel` 解决了异构 channel 存储，但会把字段类型检查推迟到运行时。
- `ManagedValueSpec` 目前只是空 trait，还没有 scratchpad 读取能力。
- `schemas` 暂不迁移是有意取舍，不代表后续永远不需要。






