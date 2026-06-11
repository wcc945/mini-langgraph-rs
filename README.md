# mini-langgraph-rs

`mini-langgraph-rs` 是一个用 Rust 编写的 `langgraph` mini 实现，目标是构建一个精简、可读、可测试的图执行框架。

本项目当前处于早期阶段，优先复刻 `langgraph` 的核心执行主线，而不是完整兼容 Python 生态或平台能力。

## 项目目标

核心主线：

```text
StateGraph builder
  -> nodes / edges / branches
  -> compile()
  -> CompiledStateGraph / Pregel
  -> invoke() / stream()
```

第一阶段关注：

- 图结构定义
- 节点注册与执行
- 普通边与条件边
- 状态字段更新与 reducer
- 同步 Pregel 风格运行时
- 清晰的错误类型与测试覆盖

## 当前状态

项目目前是纯 Rust library crate。源码入口位于：

```text
src/lib.rs
```

当前已经开始按职责拆分模块骨架：

```text
src/graph/      # StateGraph、节点、条件边、waiting edge 与 START / END 常量
src/channel/    # BaseChannel、StateValue、基础 channel 与 ChannelWriter 写入组装
src/managed/    # ManagedValueSpec 规格与每次运行复制边界
src/pregel/     # Pregel 容器、loop 骨架、task 骨架与 stream 管道边界
src/runtime/    # 节点执行上下文 RuntimeContext
src/error.rs    # 图构建与运行时错误类型边界
```

后续仍会继续补充：

```text
src/state/      # 状态 update、字段合并和 reducer 协议
src/checkpoint/ # 可恢复执行能力的边界预留
```

当前已达到可验证 MVP：外部调用方可以通过 `StateGraph::with_channels([...])` 构图，注册节点、普通边、条件边、waiting edge 或顺序链路，随后 `compile()` 得到 `CompiledStateGraph`，并通过 `invoke(input, runtime_context)` 获取一次性输出，或通过 `stream(input, runtime_context)` 接收运行过程输出。

运行时已经接入最小 Pregel superstep 主线：每次运行复制 `channels` 与 `managed`，按 `enter -> tick -> execute -> after_tick` 推进，节点输出经 `ChannelWriter` 组装为 writes，再按 channel 聚合并调用 `update(values)`。`invoke` 当前贴近源项目行为，内部通过 `stream` 收集结果：`StreamMode::Values` 返回最后一个 values payload，`StreamMode::Updates` 返回 `StateValue::List` chunk 列表。`stream` 使用 `tokio::sync::mpsc` 和后台 task，一次调用只支持一个 `StreamMode`，通过 `RuntimeContext.stream_mode` 指定。

channel 侧已具备 `LastValue`、`BinaryOperatorAggregate`、`EphemeralValue`、`NamedBarrierValue` 和 `ChannelWriter::assemble` 的 MVP。`tests/mvp_runtime.rs` 已从公开 API 视角覆盖 24 条端到端运行时行为，包括多 state/output channel、顺序链路、多步 stream、updates chunk 收集、条件入口、条件边、waiting edge、多次 invoke/stream 隔离以及常见错误路径。

暂不支持 checkpoint、resume、interrupt、`Command` 动态跳转、`Send` 动态分发、managed value 读取注入、多 stream mode 列表、公开 typed schema 投影或 async 节点接口。

## 快速示例

```rust
use std::collections::HashMap;

use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};

fn update_value(value: impl Into<StateValue>) -> StateValue {
    StateValue::Object(HashMap::from([("value".to_string(), value.into())]))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut graph: StateGraph<StateValue, StateValue> = StateGraph::with_channels(["value"]);

    graph.add_node(
        "write",
        Box::new(|_, _| Ok(NodeOutput::Update(update_value("done")))),
    )?;
    graph.set_entry_point("write")?;
    graph.set_finish_point("write")?;

    let compiled = graph.compile()?;
    let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;

    assert_eq!(output, StateValue::String("done".to_string()));
    Ok(())
}
```

选择 updates stream mode：

```rust
use mini_langgraph_rs::{RuntimeContext, StateValue, StreamMode};

# async fn example(compiled: mini_langgraph_rs::graph::CompiledStateGraph<StateValue, StateValue>) -> Result<(), Box<dyn std::error::Error>> {
let context = RuntimeContext::new(()).with_stream_mode(StreamMode::Updates);
let mut receiver = compiled.stream(Some(StateValue::Null), context)?;

while let Some(item) = receiver.recv().await {
    let item = item?;
    println!("step={} mode={:?} data={:?}", item.step, item.mode, item.data);
}
# Ok(())
# }
```

## 功能对比

说明：`√` 表示当前项目已有可用实现或明确的编译装配能力；`×` 表示尚未实现，或还没有达到端到端可用状态。

| 源项目功能 | LangGraph 源项目 | mini-langgraph-rs |
| --- | --- | --- |
| `StateGraph` 构图器 | √ | √ |
| `START` / `END` 虚拟节点 | √ | √ |
| 显式节点注册 `add_node(name, func)` | √ | √ |
| 自动从函数或 runnable 推断节点名 | √ | × |
| 普通边 `add_edge(from, to)` | √ | √ |
| 多起点 join 边 `add_edge([a, b], c)` | √ | √ |
| 条件边定义 `add_conditional_edges` | √ | √ |
| 条件边路由到多个目标 | √ | √ |
| `add_sequence` 顺序构图辅助 | √ | √ |
| `set_entry_point` / `set_finish_point` | √ | √ |
| `compile()` 生成可执行图容器 | √ | √ |
| `invoke()` 一次性执行图 | √ | √ |
| `stream()` 流式执行图 | √ | √（`Values` / `Updates` 单 mode） |
| 异步执行 `ainvoke()` / `astream()` | √ | × |
| Pregel superstep 调度循环 | √ | √（同步 MVP） |
| 节点局部状态更新 `State -> Partial<State>` | √ | √（`StateValue::Object` update） |
| 默认 `LastValue` 字段合并 | √ | √ |
| reducer 聚合 `BinaryOperatorAggregate` | √ | √ |
| 调度信号 `EphemeralValue` | √ | √ |
| join barrier `NamedBarrierValue` | √ | √ |
| `ChannelWriter` 写入组装 | √ | √ |
| `StateSchema` 推导 state channel / managed value | √ | √ |
| 从 Python/Rust 类型字段自动推断 schema | √ | × |
| managed value 运行时读取 | √ | × |
| 每次运行复制 channel / managed 运行态 | √ | √ |
| 运行时上下文注入 | √ | √（`RuntimeContext<ContextT>`） |
| `Command(update/goto/resume/graph)` 执行语义 | √ | × |
| `Send` 动态并行分发 | √ | × |
| checkpoint 持久化 | √ | × |
| interrupt / resume | √ | × |
| time travel / replay | √ | × |
| retry / cache / timeout 节点策略 | √ | × |
| 多种 stream mode | √ | √（单次一个 mode，不支持 mode 列表） |
| `MessagesState` / `add_messages` | √ | × |
| prebuilt agent / tool node / React agent | √ | × |
| LangGraph Platform、CLI、远程 SDK | √ | × |
| LangSmith tracing / 可观测性集成 | √ | × |

## 开发命令

```powershell
cargo check
cargo test
cargo fmt
cargo clippy --all-targets --all-features
```

- `cargo check`：快速验证编译。
- `cargo test`：运行单元测试和集成测试。
- `cargo fmt`：格式化代码。
- `cargo clippy --all-targets --all-features`：运行 lint 检查。

## 文档

项目背景与实现范围位于 `docs/doc`：

- [项目背景](docs/doc/project_background.md)
- [核心构图 API](docs/doc/implementation_scope_core_graph.md)
- [状态、channel 与 reducer](docs/doc/implementation_scope_state_channels.md)
- [同步 Pregel 运行时](docs/doc/implementation_scope_runtime.md)
- [可恢复执行与持久化取舍](docs/doc/implementation_scope_persistence.md)
- [暂不实现范围](docs/doc/implementation_scope_out_of_scope.md)
- [Rust 版相对源项目的改进记录](docs/doc/rust_improvements_over_original.md)

## 参考项目

原始参考项目为 `langgraph`。本地参考路径：

```text
E:\codes\Python\langgraph
```

实现时应参考其核心行为和命名，但 Rust 版本应保持符合 Rust 习惯，避免为了兼容完整 Python API 而过早引入复杂抽象。
