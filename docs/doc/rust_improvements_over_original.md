# Rust 版相对源项目的改进记录

本文记录 `mini-langgraph-rs` 在参考 Python `langgraph` 时已经确认或计划采用的 Rust 化改进。这里的“改进”不是追求完整兼容，而是在保持核心语义的前提下，利用 Rust 的类型系统、所有权和模块边界减少运行时歧义。

当前代码仍处于骨架阶段，本文只记录已经体现在源码或已明确作为本项目取舍的设计方向。

## 1. 用所有权限制 builder 编译后继续修改

源项目通过 `compiled: bool` 记录 builder 是否已经编译。编译后继续调用 `add_node`、`add_edge` 等方法时，Python 版本通常只能发出 warning。

Rust 版计划不保留 `compiled: bool`，而是让后续 `compile(self)` 消费 `StateGraph`：

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

## 9. 暂不迁移 schemas 字段，避免复制 Python 动态 schema 缓存

源项目 `schemas: dict[type[Any], dict[str, BaseChannel | ManagedValueSpec]]` 用于缓存每个 Python schema 类型对应的字段视图，服务于 `state_schema`、`input_schema`、`output_schema`、节点级 input schema 和 branch input schema。

Rust 版当前所有节点和 branch 仍围绕 `StateT` 工作，还没有独立 input/output projection，也没有 schema derive 宏。因此暂不迁移 `schemas` 字段，只保留：

```rust
channels
managed
```

等后续出现独立 `InputT`、`OutputT`、节点输入投影或宏生成 schema 时，再设计 Rust 版 `SchemaSpec` 或 `StateSchema` trait。

## 10. 错误类型集中到 GraphError

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

## 当前仍需谨慎的地方

- 当前源码仍是骨架，很多类型未公开或未使用，warning 是预期状态。
- `StateValue` 是动态路线，后续如果要强化类型安全，可以增加宏生成的强类型 partial update。
- `DynChannel` 解决了异构 channel 存储，但会把字段类型检查推迟到运行时。
- `ManagedValueSpec` 目前只是空 trait，还没有 scratchpad 读取能力。
- `schemas` 暂不迁移是有意取舍，不代表后续永远不需要。
