# 实现 `PregelNode` v1 计划

## Summary

本次只实现 Rust 版 `PregelNode`，不实现 `CompiledStateGraph`、`compile()`、调度、`invoke()` 或 `stream()`。

参考源项目后，Rust v1 的 `PregelNode` 只承载后续组装 `PregelExecutableTask` 必需的运行时字段：`channels`、`triggers`、`mapper`、`writers` 和 `bound`。

暂不实现源项目中的 `retry_policy`、`cache_policy`、`timeout`、`tags`、`metadata`、`subgraphs`、`is_error_handler`、`error_handler_node`。

## Key Changes

- 在 `src/pregel/node.rs` 定义运行时层 `PregelNode`，输入 channel 先统一保存为 `Vec<String>`。
- 用 `PregelNodeMapper<StateT>` 表达从 channel 动态值到节点输入的可选映射。
- 用 `PregelNodeBound<StateT, UpdateT, ContextT>` 表达节点主逻辑，签名对齐 `graph::node::NodeFn`。
- `PregelNode<StateT, UpdateT, ContextT>` 保存 `channels`、`triggers`、`mapper`、`writers`、`bound` 五个字段。
- 提供显式 `new` 构造函数，不预置默认执行逻辑。
- 仅调整 `src/pregel/mod.rs` 的 crate 内部导出，不实现 `CompiledStateGraph` 或 runtime 调度。

## Test Plan

- 添加 `PregelNode` 单元测试，覆盖字段保存、mapper 到 bound 的未来执行形状、`Vec<String>` channel 输入表达。
- 执行：`cargo test` 和 `cargo check`。

## Docs

- 同步更新 `docs/doc/implementation_scope_runtime.md`，记录 `PregelNode` 已作为运行时任务装配容器实现。
- 同步更新 `docs/doc/rust_improvements_over_original.md`，记录 Rust 版只保留核心 PregelNode 字段、暂不迁移 retry/cache/timeout/tracing/subgraph/error handler。

## Assumptions

- `PregelNode` 是 Pregel runtime 层结构，`bound` 泛型签名与 `StateNodeSpec<StateT, UpdateT, ContextT>` 中的节点函数保持一致。
- 后续 `CompiledStateGraph.attach_node` 再负责把 graph node 适配成动态 `StateValue` 运行时节点。
- 本次不解决 `NodeOutput<UpdateT>` 到 `StateValue` 的转换协议，也不实现节点输出如何拆成字段级 writes。
- `ChannelWriter` 与 `PregelNode` 均保持 `pub(crate)`。
