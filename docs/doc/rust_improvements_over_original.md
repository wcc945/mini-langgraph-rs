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
dyn Fn(&NodeInputT, &mut RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>
```

所有运行时依赖统一放入 `RuntimeContext<ContextT>`，避免在运行时识别函数参数。后续如果需要适配不使用 context 的闭包，可以在 `add_node` 层做轻量 adapter，而不是复制完整 `Runnable` 生态。

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
trait ManagedValueSpec: Send + Sync {}
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

其中 `bound` 保存节点主逻辑，`writers` 保存节点写入器，`writes` 保存执行期间产生的 pending writes；三者在任务结构中显式拆开，不再通过 `proc: PregelNode` 间接承载。`id`、`path` 和 `writes` 直接使用标准集合类型，不额外定义类型别名。`PregelTaskManager` 内部用 `HashMap<String, PregelExecutableTask<...>>` 按任务 id 索引任务；当前只提供提交任务、准备任务、准备单个任务和执行任务的方法桩，后续实现 `stream()` 时再填充具体行为。`config`、`retry_policy`、`cache_key`、`subgraphs` 和 `timeout` 暂不迁移，避免在基本调度循环完成前引入策略层复杂度。

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

源项目的 `channels: dict[str, BaseChannel | ManagedValueSpec]` 在 Rust 版拆成 `channels` 和 `managed` 两张表，以保留动态 channel map 的同时避免把 managed value 当作普通 channel 更新。当前 `Pregel::validate` 只迁移 `validate_graph` 的最小结构校验，并重建 `trigger_to_nodes`；`invoke`、`stream`、checkpoint、interrupt/resume 等能力仍暂缓。

## 15. CompiledStateGraph 先固定编译装配边界

源项目 `CompiledStateGraph.compile()` 会在编译期完整接入 `attach_node`、`attach_edge`、`attach_branch`、state update writer、branch writer、join barrier channel、schema mapper 和 Pregel 运行时配置。

Rust 版当前实现最小编译装配边界：

```rust
pub fn compile(self) -> Result<CompiledStateGraph<...>, GraphError>
```

`compile(self)` 消费 builder，调用 `StateGraph::validate()`，创建 `CompiledStateGraph`，再通过 `attach_node`、`attach_edge` 和 `attach_branch` 装配 `Pregel` 容器。Rust 版对齐源项目，先以 `attach_node(START, None)` 创建入口节点并订阅 `START` input channel；用户节点读取所有 state channel 和 managed value，安装最小 state writer，并为自己创建和订阅 `branch:to:{node}` trigger channel；`attach_edge` 会把 `START -> a` 和普通边 `a -> b` 都编译为起点节点 writer 写入 `branch:to:{target}`，不重复追加目标节点已有的 `branch:to:{target}` trigger；`attach_branch` 会把单目标条件分支封装为可执行 `ChannelWriter`，按 route 结果写入 `branch:to:{target}`。

源项目 `StateGraph.compile()` 会分别计算 `output_channels` 与 `stream_channels` 并传入 `CompiledStateGraph`。Rust 版当前还没有独立 input/output schema projection，因此先让 `stream_channels` 默认等于 `output_channels`，后续引入 schema 后再拆分两者。

当前不复制源项目的完整 checkpoint 或 stream 路径；`PregelNode.bound` 直接复用 builder 中的节点函数，`mapper` 暂不接入真实状态投影协议。源项目通过 `ChannelWrite.register_writer(branch.run(...))` 把分支 runnable 标记为 writer；Rust 版用泛型可执行 `ChannelWriter` 表达同一编译边界，让 writer 能访问 `&StateT` 和 `RuntimeContext` 后返回 `ChannelWriteEntry`。该实现暂不迁移 schema reader、`Send`、async 或 Runnable 生态。waiting edge 已按源项目 `attach_edge(starts, end)` 路径编译为 `NamedBarrierValue` join channel，目标节点订阅 join channel，各起点节点写入自己的节点名。

## 16. PregelLoop 先保留同步主循环核心状态

源项目 `PregelLoop` / `SyncPregelLoop` 同时承载运行时核心状态和 checkpoint、cache、store、interrupt、debug stream、retry、async executor、生命周期事件等平台能力。Rust 版当前只建立同步 `PregelLoop` 骨架，并从已有 `Pregel` 容器拆分借用 `nodes`、`channels`、`managed`、input/output/stream channels 和 trigger 索引等字段，贴近源项目向 `SyncPregelLoop` 传入 `nodes=self.nodes`、`specs=self.channels` 等字段的方式。

Rust 版当前字段收敛为：

```rust
struct PregelLoop<'a, StateT, UpdateT, ContextT> {
    nodes: &'a HashMap<String, PregelNode<StateT, UpdateT, ContextT>>,
    channels: &'a mut HashMap<String, Box<DynChannel>>,
    managed: &'a mut HashMap<String, Box<dyn ManagedValueSpec>>,
    input_channels: &'a [String],
    output_channels: &'a [String],
    stream_channels: Option<&'a [String]>,
    stream_mode: StreamMode,
    recursion_limit: usize,
    trigger_to_nodes: &'a HashMap<String, Vec<String>>,
    name: &'a str,
    input: Option<StateValue>,
    step: usize,
    stop: usize,
    status: PregelLoopStatus,
    task_manager: PregelTaskManager<StateT, UpdateT, ContextT>,
    updated_channels: Option<HashSet<String>>,
    output: Option<StateValue>,
}
```

源项目的 `tasks: dict[str, PregelExecutableTask]` 在 Rust 版先映射为 `PregelTaskManager`，让任务集合、准备和执行边界集中在 task manager 中。`tick`、`execute`、`after_tick` 目前只是空方法，后续再分别接入准备任务、执行任务和应用 writes 的逻辑。

## 当前仍需谨慎的地方

- 当前源码仍是骨架，很多类型未公开或未使用，warning 是预期状态。
- `StateValue` 是动态路线，后续如果要强化类型安全，可以增加宏生成的强类型 partial update。
- `DynChannel` 解决了异构 channel 存储，但会把字段类型检查推迟到运行时。
- `ManagedValueSpec` 目前只是空 trait，还没有 scratchpad 读取能力。
- `schemas` 暂不迁移是有意取舍，不代表后续永远不需要。
