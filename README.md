# mini-langgraph-rs

`mini-langgraph-rs` 是 [LangGraph](https://github.com/langchain-ai/langgraph) 的 Rust 精简实现——一个受 Google Pregel 启发的有状态图执行框架。

本项目定位为可读、可测试的核心执行引擎，不追求完整兼容 Python 生态或平台能力。

## 快速开始

```toml
[dependencies]
mini-langgraph-rs = { path = "." }
tokio = { version = "1", features = ["rt", "rt-multi-thread", "sync", "macros"] }
```

### 构建并运行最简单的图

```rust
use std::collections::HashMap;
use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph: StateGraph<StateValue, StateValue> =
        StateGraph::with_channels(["value"]);

    graph.add_node(
        "write",
        Box::new(|_, _| {
            Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
                "value".to_string(),
                StateValue::String("done".to_string()),
            )]))))
        }),
    )?;
    graph.set_entry_point("write")?;
    graph.set_finish_point("write")?;

    let compiled = graph.compile()?;
    let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;

    assert_eq!(output, StateValue::String("done".to_string()));
    Ok(())
}
```

### 流式执行

```rust
use mini_langgraph_rs::{RuntimeContext, StateValue, StreamMode};

async fn example(
    compiled: mini_langgraph_rs::graph::CompiledStateGraph<StateValue, StateValue>,
) -> Result<(), Box<dyn std::error::Error>> {
    let ctx = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);
    let mut receiver = compiled.stream(Some(StateValue::Null), ctx)?;

    while let Some(item) = receiver.recv().await {
        let item = item?;
        println!("step={} mode={:?} data={:?}", item.step, item.mode, item.data);
    }
    Ok(())
}
```

### 条件边

```rust
use std::collections::HashMap;
use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};

let mut graph = StateGraph::with_channels(["value"]);

graph.add_node("route", Box::new(|_, _| Ok(NodeOutput::None)))?;
graph.add_node("next", Box::new(|_, _| {
    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
        "value".to_string(),
        StateValue::String("routed".to_string()),
    )]))))
}))?;

graph.set_entry_point("route")?;
graph.add_conditional_edges(
    "route",
    "choose",
    Box::new(|_, _| Some("next".to_string())),
    HashMap::from([("next".to_string(), "next".to_string())]),
)?;
graph.set_finish_point("next")?;

let compiled = graph.compile()?;
let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;
assert_eq!(output, StateValue::String("routed".to_string()));
```

### 并行 Fan-out + Join (Waiting Edge)

```rust
use std::collections::HashMap;
use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};

let mut graph: StateGraph<StateValue, StateValue> =
    StateGraph::with_channels(["a_out", "b_out", "merged"]);

graph.add_node("split", Box::new(|_, _| Ok(NodeOutput::None)))?;
graph.add_node("worker_a", Box::new(|_, _| {
    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
        "a_out".to_string(), StateValue::String("A".to_string()),
    )]))))
}))?;
graph.add_node("worker_b", Box::new(|_, _| {
    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
        "b_out".to_string(), StateValue::String("B".to_string()),
    )]))))
}))?;
graph.add_node("join", Box::new(|_, _| {
    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
        "merged".to_string(), StateValue::String("done".to_string()),
    )]))))
}))?;

graph.set_entry_point("split")?;
// Fan-out: split -> 两路并行
graph.add_edge("split", "worker_a")?;
graph.add_edge("split", "worker_b")?;
// Fan-in: 两路都完成后触发 join (waiting edge)
graph.add_edge(["worker_a", "worker_b"], "join")?;
graph.set_finish_point("join")?;

let compiled = graph.compile()?;
let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;
assert_eq!(
    output,
    StateValue::Object(HashMap::from([
        ("a_out".to_string(), StateValue::String("A".to_string())),
        ("b_out".to_string(), StateValue::String("B".to_string())),
        ("merged".to_string(), StateValue::String("done".to_string())),
    ]))
);
```

### 自定义 Reducer (BinaryOperatorAggregate)

```rust
use std::collections::HashMap;
use mini_langgraph_rs::{
    BinaryOperatorAggregate, DynChannel, GraphError, LastValue, NodeOutput,
    RuntimeContext, StateGraph, StateValue,
};

// 自定义 reducer：累加数字
fn sum_reducer(left: StateValue, right: StateValue) -> Result<StateValue, GraphError> {
    match (left, right) {
        (StateValue::Number(a), StateValue::Number(b)) => Ok(StateValue::Number(a + b)),
        _ => Err(GraphError::InvalidChannelUpdate("expected numbers".to_string())),
    }
}

// 手动装配 channels：counter 使用 BinaryOperatorAggregate
let mut channels: HashMap<String, Box<DynChannel>> = HashMap::new();
channels.insert("counter".to_string(), Box::new(BinaryOperatorAggregate::new(sum_reducer)));

let mut graph: StateGraph<StateValue, StateValue> = StateGraph::new();
graph.channels = channels;

graph.add_node("add_1", Box::new(|_, _| {
    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
        "counter".to_string(), StateValue::Number(1.0),
    )]))))
}))?;
graph.add_node("add_2", Box::new(|_, _| {
    Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
        "counter".to_string(), StateValue::Number(2.0),
    )]))))
}))?;

graph.set_entry_point("add_1")?;
graph.add_edge("add_1", "add_2")?;
graph.set_finish_point("add_2")?;

let compiled = graph.compile()?;
let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;
// counter 的两次写入被 reducer 合并：1 + 2 = 3
assert_eq!(
    output,
    StateValue::Object(HashMap::from([(
        "counter".to_string(), StateValue::Number(3.0)
    )]))
);
```

### 带 checkpoint 的执行

```rust
use mini_langgraph_rs::checkpoint::MemorySaver;
use mini_langgraph_rs::{RuntimeContext, StateValue};

let ctx = RuntimeContext::new(())
    .with_checkpointer(MemorySaver::new())
    .with_stream_mode(StreamMode::Values);

let output = compiled.invoke(Some(StateValue::Null), ctx)?;
// 状态已保存在 MemorySaver 中，可通过相同 thread_id 恢复
```

### 注入运行时上下文

```rust
use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};

let mut graph: StateGraph<StateValue, StateValue, i64> =
    StateGraph::with_channels(["value"]);

graph.add_node(
    "use_context",
    Box::new(|_, ctx: &RuntimeContext<i64>| {
        Ok(NodeOutput::Update(StateValue::Object(HashMap::from([(
            "value".to_string(),
            StateValue::Number(ctx.context as f64),
        )]))))
    }),
)?;
graph.set_entry_point("use_context")?;
graph.set_finish_point("use_context")?;

let compiled = graph.compile()?;
let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::new(42))?;
assert_eq!(output, StateValue::Number(42.0));
```

## 性能基准

项目使用 [criterion](https://github.com/bheisler/criterion.rs) 进行微基准测试，覆盖 5 个场景，并与 Python 源项目（LangGraph）同图结构对比。

### 场景设计

| group | 图结构 | 测量方式 |
| --- | --- | --- |
| `single_node` | START, write, END，单节点写入 `{"value": "done"}` | `invoke()` 单次耗时 |
| `linear_chain` | START, n0, n1, ..., END，5/10/20 节点链，每节点写入递增序号 | `invoke()` 吞吐 vs 节点数 |
| `conditional_edge` | START, route, (a 或 b 或 c), END，路由至 b 分支 | `invoke()` 路由开销 |
| `stream_values` | 同上 10 节点链，`stream(Values)` 模式 | 流式吞吐（drain 全部 chunk） |
| `checkpoint` | 同上 10 节点链 + MemorySaver | `invoke()` 含 checkpoint I/O |

- **Rust 端**: criterion `--sample-size 100`，`--significance-level 0.1`，release profile
- **Python 端**: 每场景 500 次 invoke（checkpoint 为 200 次），取中位数
- **对比方式**: Python 先运行产出 `python_bench_results.json`，Rust benchmark 启动时读取该文件并行打印基线

### 运行

```powershell
# 1. 生成 Python 基线数据
& "E:\codes\Python\langgraph\.venv\Scripts\python.exe" benches/python_bench.py

# 2. 运行 Rust criterion benchmark
cargo bench
```

### 对比结果

| group | Python (ms) | Rust (ms) | 加速比 |
| --- | ---: | ---: | ---: |
| `single_node` | 0.252 | 0.073 | **3.5x** |
| `linear_chain/5` | 0.608 | 0.160 | **3.8x** |
| `linear_chain/10` | 1.049 | 0.156 | **6.7x** |
| `linear_chain/20` | 1.964 | 0.165 | **11.9x** |
| `conditional_edge` | 0.366 | 0.134 | **2.7x** |
| `stream_values` | 0.971 | 0.037 | **26.2x** |
| `checkpoint` | 3.573 | 0.163 | **21.9x** |

Rust 端 invoke 耗时基本恒定（73-165 us），不随节点数线性增长——每个 superstep 内部开销远大于状态拷贝开销。Python 端 invoke 随节点数近似线性增长（解释执行 + 动态类型开销），因此节点越多加速越明显。stream 和 checkpoint 场景中 Rust 的内存分配与 I/O 优势进一步放大差距。

## Example: Code Review Pipeline Agent

`examples/plan_execute_review.rs` 是一个代码审查流水线 agent，同时演示三个核心功能：checkpoint 持久化、waiting edge（join）并行汇聚、以及 BinaryOperatorAggregate 自定义 reducer。

### 图拓扑

```
                         +-- security_check --+
START --> receive_pr ----+-- style_check    --+-- aggregate_report --> END
                         +-- perf_check     --+
```

- `receive_pr` 接收 PR 内容，fan-out 到三路并行审查
- 三路审查（安全/风格/性能）并行执行，各自产出 findings 列表
- findings channel 使用 `BinaryOperatorAggregate`，自定义 reducer 将各路的列表合并为一个
- 三路都完成后（waiting edge），`aggregate_report` 汇总最终报告
- 全程启用 `MemorySaver` checkpoint

### 状态字段

| 字段 | Channel 类型 | 说明 |
| --- | --- | --- |
| `pr_content` | LastValue | 待审查的 PR 源代码 |
| `findings` | BinaryOperatorAggregate | 各审查节点发现的问题列表（reducer: 列表拼接） |
| `report` | LastValue | 最终汇总报告 |

### 运行

```powershell
cargo run --example plan_execute_review
```

输出展示：
- superstep 2 中三路审查在同一轮并行执行（fan-out + waiting edge 生效）
- superstep 3 中 aggregate_report 触发（join 生效）
- 最终 findings 列表包含 3 个检查器的共 7 条发现（reducer 合并生效）
- checkpoint 在 stream 和 invoke 两次执行中均启用

## API 概览

### 图构建

| 方法 | 说明 |
| --- | --- |
| `StateGraph::new()` | 创建空 builder，后续手动添加 channel |
| `StateGraph::with_channels(["a", "b"])` | 用字段名创建，自动装配 `LastValue` channel |
| `StateGraph::with_schema()` | 从 `StateSchema` trait 自动生成 channels 和 managed values |
| `add_node(name, func)` | 注册节点，函数签名 `(&StateT, &RuntimeContext<ContextT>) -> Result<NodeOutput<UpdateT>, GraphError>` |
| `add_edge(from, to)` | 添加普通边。`from` 支持 `"a"`（fan-out）、`["a", "b"]`（join / waiting edge） |
| `add_conditional_edges(from, name, path_fn, ends)` | 添加条件边，按路由结果分发到不同目标 |
| `add_sequence([(name, func), ...])` | 顺序注册节点并自动连接 |
| `set_entry_point(key)` | 从 `START` 连接到节点 |
| `set_finish_point(key)` | 从节点连接到 `END` |
| `set_conditional_entry_point(name, path_fn, ends)` | 在入口处使用条件路由 |
| `compile()` | 消费 builder，返回 `CompiledStateGraph`（编译后不可再修改） |

### Channel 装配

| 方式 | 说明 |
| --- | --- |
| `StateGraph::with_channels(["a", "b"])` | 每个字段自动装配 `LastValue` |
| `graph.channels = custom_map` | 手动注入 `HashMap<String, Box<DynChannel>>`，可使用任意 channel 类型 |
| `BinaryOperatorAggregate::new(reducer)` | 创建带自定义 reducer 的聚合 channel |
| `LastValue::new()` | 创建标准的 last-value-wins channel |
| `StateSchema` trait | 实现 trait 自动装配 channels + managed values |

### 运行时执行

| 方法 | 说明 |
| --- | --- |
| `compiled.invoke(input, ctx)` | 同步执行完整图，返回最终输出 |
| `compiled.stream(input, ctx)` | 返回 `mpsc::Receiver`，异步流式输出每轮状态 |

### RuntimeContext

```rust
let ctx = RuntimeContext::new(user_context)       // 注入用户上下文
    .with_stream_mode(StreamMode::Updates)        // 覆盖默认 stream mode
    .with_checkpointer(MemorySaver::new());       // 附加内存 checkpoint
```

- `context: ContextT` --- 节点可读取的用户上下文（只读）
- `stream_mode: Option<StreamMode>` --- `Values`（状态快照）或 `Updates`（节点更新）
- `checkpointer: Option<MemorySaver>` --- 内存 checkpoint 存储

### 核心类型

- `StateValue` --- 动态值枚举：`Null` / `Bool` / `Number` / `String` / `List` / `Object`
- `NodeOutput<UpdateT>` --- 节点返回值：`Update` / `Command` / `Commands` / `None`
- `PregelStreamItem` --- 流式输出项：`{ step, mode, data }`
- `GraphError` --- 统一错误类型，覆盖构图、channel、运行时三类错误
- `BinaryOperatorAggregate` --- 自定义 reducer channel（多写入自动合并）
- `LastValue` --- last-value-wins channel
- `DynChannel` / `BaseChannel` --- channel trait object 和 trait 定义
- `StateSchema` --- 类型化 state schema trait（自动生成 channels + managed values）

## 功能对比

按 LangGraph 源项目功能域深入排列。勾选表示已有可用实现，三角表示部分实现或仅骨架，叉号表示暂未实现。

| 功能 | 源项目 | mini-langgraph-rs |
| --- | --- | --- |
| **图构建** | | |
| `StateGraph<StateT, ContextT, InputT, OutputT>` builder | ✓ | ✓（`StateT` / `ContextT`，无独立 InputT/OutputT） |
| `START` / `END` 虚拟节点 | ✓ | ✓ |
| 显式节点注册 `add_node(name, func)` | ✓ | ✓ |
| 自动推断节点名 `add_node(func)` | ✓ | ✗ |
| `state_schema` / `context_schema` / `input_schema` / `output_schema` | ✓ | △（`with_channels` 字段名入口；`StateSchema` trait 已公开，无 derive 宏） |
| 普通边 `add_edge(from, to)` | ✓ | ✓（`into()` 重载：`"a"` 或 `["a","b"]`） |
| 多起点 join 边 `add_edge([a, b], c)` | ✓ | ✓ |
| 条件边 `add_conditional_edges(from, name, path_fn, ends)` | ✓ | ✓ |
| `add_sequence` 顺序构图 | ✓ | ✓ |
| `set_entry_point` / `set_finish_point` | ✓ | ✓ |
| `set_conditional_entry_point` 条件入口 | ✓ | ✓ |
| `set_node_defaults(retry/cache/error/timeout)` | ✓ | ✗ |
| `compile(checkpointer, cache, store, interrupt_*, debug, transformers)` | ✓ | △（仅 `compile()` 无参） |
| **编译与执行** | | |
| `compile()` 生成可执行图 | ✓ | ✓ |
| `invoke(input, config)` 同步执行 | ✓ | ✓ |
| `stream(input, config)` 流式执行 | ✓ | ✓（`Values` / `Updates`，单 mode） |
| `ainvoke()` / `astream()` 异步执行 | ✓ | ✗ |
| `stream_events(version="v3")` / `astream_events()` | ✓ | ✗ |
| `batch()` / `abatch()` 批处理 | ✓ | ✗ |
| `get_graph()` / `.draw_mermaid()` 图可视化 | ✓ | ✗ |
| `GraphOutput(value, interrupts)` 类型化返回值 | ✓ | ✗ |
| **运行时调度** | | |
| Pregel superstep 调度循环 | ✓ | ✓ |
| Plan, Execution, Update 三阶段 | ✓ | ✓ |
| 同轮写入对同轮节点不可见 | ✓ | ✓ |
| 每次运行独立复制 channel / managed（`PregelLoop` vs `Pregel`） | ✓ | ✓ |
| `RuntimeContext` 运行时上下文注入 | ✓ | ✓（`RuntimeContext<ContextT>`） |
| `Runtime(context, store, previous, execution_info)` | ✓ | △（仅 `RuntimeContext.context`，无 store/previous/info） |
| 递归限制 | ✓ | ✓（默认 25，可配置） |
| `Durability` (sync / async / exit) | ✓ | ✗ |
| **状态管理** | | |
| `LastValue` 默认 reducer | ✓ | ✓ |
| `BinaryOperatorAggregate` 自定义聚合 | ✓ | ✓（已公开，可通过 `graph.channels` 手动装配） |
| `EphemeralValue` 调度信号 | ✓ | ✓ |
| `NamedBarrierValue` / `NamedBarrierValueAfterFinish` join 屏障 | ✓ | ✓（无 AfterFinish 变体） |
| `LastValueAfterFinish` | ✓ | ✗ |
| `ChannelWriter` 写入组装（entry / tuple entry） | ✓ | ✓ |
| `StateSchema` trait 类型推导 | ✓ | ✓（已公开，无 derive 宏） |
| 从 annotated type 字段自动推断 schema | ✓ | ✗ |
| `Overwrite` 绕过 reducer 直接写入 | ✓ | ✗ |
| `StateUpdate` / `StateSnapshot` 状态快照 | ✓ | ✗ |
| managed value 运行时读取 (`IsLastStep`, `RemainingSteps`) | ✓ | ✗（仅 `copy_box`） |
| **Channel 类型** | | |
| `LastValue` | ✓ | ✓ |
| `BinaryOperatorAggregate` (function reducer) | ✓ | ✓ |
| `EphemeralValue` | ✓ | ✓ |
| `NamedBarrierValue` | ✓ | ✓ |
| `Topic` (PubSub, accumulate/non-accumulate) | ✓ | ✗ |
| `AnyValue` (多写入相等断言) | ✓ | ✗ |
| `DeltaChannel` (delta snapshot, batch-replay) | ✓ | ✗ |
| `UntrackedValue` (不参与 checkpoint) | ✓ | ✗ |
| **Checkpoint 持久化** | | |
| checkpoint 数据结构（id/versions/values/seen/writes） | ✓ | ✓（`channel_versions` 用 `u64`，`channel_values` 直存无 blobs 分层） |
| `CheckpointSaver` trait | ✓ | ✓（同步） |
| `MemorySaver` 内存存储 | ✓ | ✓ |
| `put` / `get_tuple` / `put_writes` / `delete_thread` | ✓ | ✓ |
| PregelLoop 中 enter 恢复 / after_tick 保存 | ✓ | ✓ |
| `channel_versions` / `versions_seen` 版本追踪 | ✓ | ✓ |
| `create_checkpoint` / `empty_checkpoint` / `copy_checkpoint` | ✓ | ✓ |
| `PendingWrite(task_id, channel, value)` 结构体 | ✓ | ✓（命名结构体替代三元组） |
| sqlite / postgres 持久化后端 | ✓ | ✗ |
| checkpoint migration (v1,v2,v3) | ✓ | ✗ |
| **子图** | | |
| `CompiledStateGraph` 作为节点（嵌套图） | ✓ | ✗ |
| `checkpointer: Checkpointer` 继承控制 (`True`/`False`/`None`) | ✓ | ✗ |
| 子图 checkpoint namespace 隔离 | ✓ | ✗ |
| `Command(graph=Command.PARENT)` 导航到父图 | ✓ | ✗ |
| 子图 replay state 恢复 | ✓ | ✗ |
| **流式模式** | | |
| `values` 每步全量状态快照 | ✓ | ✓ |
| `updates` 每步节点输出增量 | ✓ | ✓ |
| `messages` LLM token 级流式 + metadata | ✓ | ✗ |
| `checkpoints` checkpoint 创建事件 | ✓ | ✗ |
| `tasks` 任务开始/结束事件 | ✓ | ✗ |
| `debug` checkpoints + tasks 联合事件 | ✓ | ✗ |
| `custom` 自定义流式（`StreamWriter`） | ✓ | ✗ |
| 复合 stream mode（同时多个 mode） | ✓ | ✗ |
| **高级特性** | | |
| `Command(update/goto/resume/graph)` | ✓ | ✗（类型已定义，语义未实现） |
| `Send(node, arg, timeout)` 动态并行分发（map-reduce） | ✓ | ✗ |
| `interrupt()` / resume（human-in-the-loop） | ✓ | ✗ |
| time travel / replay / fork | ✓ | ✗ |
| `RetryPolicy` 节点重试 | ✓ | ✗ |
| `CachePolicy` 节点结果缓存 | ✓ | ✗ |
| `TimeoutPolicy` 节点超时 | ✓ | ✗ |
| node-level `error_handler` / graph-level default error handler | ✓ | ✗ |
| `MessagesState` / `add_messages` (append-only messages) | ✓ | ✗ |
| `push_message` 手动写入消息流 | ✓ | ✗ |
| prebuilt agent / tool node / React agent | ✓ | ✗ |
| `Store` 长期记忆 / `BaseStore` | ✓ | ✗ |
| **功能 API** | | |
| `@entrypoint` / `@task` 装饰器式构图 | ✓ | ✗ |
| **平台生态** | | |
| LangGraph Platform / CLI / 远程 SDK | ✓ | ✗ |
| LangSmith tracing / 可观测性 | ✓ | ✗ |
| LangChain 模型 / prompt / tool 适配 | ✓ | ✗ |
| graph lifecycle events (`on_enter`/`on_interrupt`/`on_resume`) | ✓ | ✗ |
| serde / strict msgpack 序列化白名单 | ✓ | ✗ |

## 开发命令

```powershell
# 编译检查
cargo check

# 运行测试
cargo test

# 格式化代码
cargo fmt

# Lint 检查
cargo clippy --all-targets --all-features
```

所有测试（含集成测试）均不需要外部依赖，可直接在本地运行。

## 文档

| 文档 | 说明 |
| --- | --- |
| [项目背景](docs/doc/project_background.md) | 项目定位、参考依据与实现原则 |
| [核心构图 API](docs/doc/implementation_scope_core_graph.md) | StateGraph builder、节点、边、条件分支 |
| [状态、channel 与 reducer](docs/doc/implementation_scope_state_channels.md) | 状态字段合并、channel 抽象与 reducer 协议 |
| [同步 Pregel 运行时](docs/doc/implementation_scope_runtime.md) | superstep 调度、invoke/stream、任务执行 |
| [可恢复执行与持久化](docs/doc/implementation_scope_persistence.md) | checkpoint 数据结构、MemorySaver、PregelLoop 集成 |
| [暂不实现范围](docs/doc/implementation_scope_out_of_scope.md) | 不在当前范围的平台与高级能力 |
| [Rust 版改进记录](docs/doc/rust_improvements_over_original.md) | 相对源项目的设计取舍与 Rust 化改造 |
| [Code Review Pipeline Agent](examples/plan_execute_review.rs) | 完整 agent example：checkpoint + waiting edge + reducer |

## 参考项目

原始参考项目为 [LangGraph](https://github.com/langchain-ai/langgraph)。本地参考路径：`E:\codes\Python\langgraph`。