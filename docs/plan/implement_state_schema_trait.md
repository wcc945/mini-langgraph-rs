# 增加手写 `StateSchema` 自动填充计划

## 涉及文件

- `src/graph/schema.rs`
- `src/graph/mod.rs`
- `src/graph/state.rs`
- `docs/doc/implementation_scope_state_channels.md`
- `docs/doc/rust_improvements_over_original.md`
- `docs/plan/implement_state_schema_trait.md`

## Summary

先用手写 trait 达成源项目 `_add_schema(self.state_schema)` 的 Rust 版最小效果，不引入 derive 宏。新增独立 `src/graph/schema.rs` 定义 crate 内部 `StateSchema`，并通过 crate 内部 `StateGraph::with_schema()` 在创建 builder 时填充 state channels 和 managed values。

## Key Changes

- 新增 crate 内部 `StateSchema` trait，提供 `channels()` 和默认空实现的 `managed()`。
- `StateGraph::with_schema()` 要求 `StateT: StateSchema`，调用 trait 方法填充 builder。
- `StateGraph::new()` 保持空 builder 语义，不自动解析 schema。
- `compile()` 不额外解析 schema，只消费 builder 中已有的 `channels` 和 `managed`。
- 暂不实现 `derive(StateSchema)`、Python 版 `schemas` 缓存、input/output schema 和节点级 input projection。
- 暂不作为外部公共 API 暴露；后续公开时需要同步公开 channel 和 managed 类型。

## Test Plan

- `with_schema_adds_state_channels`：确认 `with_schema()` 自动注册 state channels，并让编译后节点读取这些 channel。
- `with_schema_adds_managed_values`：确认 `with_schema()` 自动注册 managed values，并让编译后节点读取 managed key。
- `new_keeps_empty_schema_tables`：确认 `new()` 不触发 schema 自动填充。
- 验证命令：`cargo fmt`、`cargo test`、`cargo clippy --all-targets --all-features`。

## Result

已完成：新增 `StateSchema` trait 与 `StateGraph::with_schema()`，并补充自动填充 state channels、managed values 以及 `new()` 保持空 builder 的测试。已同步相关说明文档。

验证已通过：`cargo fmt`、`cargo test`、`cargo clippy --all-targets --all-features`。当前仍有项目骨架阶段既有 warning，例如未使用运行时字段、`Command` 可见性和 clippy 建议；本次新增的 schema 功能测试通过。
