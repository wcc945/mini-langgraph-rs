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
src/runtime/    # 节点执行上下文 RuntimeContext
src/error.rs    # 图构建与运行时错误类型边界
```

后续仍会继续补充：

```text
src/state/      # 状态 update、字段合并和 reducer 协议
src/checkpoint/ # 可恢复执行能力的边界预留
```

当前代码仍处于骨架阶段，`add_node`、`compile`、`invoke`、`stream` 和 channel 合并逻辑尚未实现。

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
