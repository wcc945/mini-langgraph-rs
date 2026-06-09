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
src/runtime/    # 节点执行上下文 RuntimeContext
src/error.rs    # 图构建与运行时错误类型边界
```

后续仍会继续补充：

```text
src/state/      # 状态 update、字段合并和 reducer 协议
src/checkpoint/ # 可恢复执行能力的边界预留
```

当前代码仍处于骨架阶段，`add_node`、`add_edge`、`add_conditional_edges`、`add_sequence`、`compile` 和 Pregel 容器校验已具备 MVP；`invoke`、`stream` 和完整 superstep 调度尚未实现。channel 侧已具备 `LastValue`、`BinaryOperatorAggregate`、`EphemeralValue`、`NamedBarrierValue` 和 `ChannelWriter::assemble` 的 MVP；后续 runtime 仍需把 task writes 按 channel 聚合并调用 channel `update(values)`。

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
| `invoke()` 一次性执行图 | √ | × |
| `stream()` 流式执行图 | √ | × |
| 异步执行 `ainvoke()` / `astream()` | √ | × |
| Pregel superstep 调度循环 | √ | × |
| 节点局部状态更新 `State -> Partial<State>` | √ | × |
| 默认 `LastValue` 字段合并 | √ | √ |
| reducer 聚合 `BinaryOperatorAggregate` | √ | √ |
| 调度信号 `EphemeralValue` | √ | √ |
| join barrier `NamedBarrierValue` | √ | √ |
| `ChannelWriter` 写入组装 | √ | √ |
| `StateSchema` 推导 state channel / managed value | √ | √ |
| 从 Python/Rust 类型字段自动推断 schema | √ | × |
| managed value 运行时读取 | √ | × |
| 运行时上下文注入 | √ | × |
| `Command(update/goto/resume/graph)` 执行语义 | √ | × |
| `Send` 动态并行分发 | √ | × |
| checkpoint 持久化 | √ | × |
| interrupt / resume | √ | × |
| time travel / replay | √ | × |
| retry / cache / timeout 节点策略 | √ | × |
| 多种 stream mode | √ | × |
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
