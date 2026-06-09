# 实现 `CompiledStateGraph` 编译装配层计划

## 涉及文件

- `docs/plan/implement_compiled_state_graph.md`
- `src/graph/compiled.rs`
- `src/graph/state.rs`
- `src/graph/mod.rs`
- `src/lib.rs`
- `src/error.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/implementation_scope_state_channels.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

实现 `StateGraph -> compile() -> CompiledStateGraph` 的编译装配层，贴近源项目中 `CompiledStateGraph` 自身的职责：装配 Pregel 节点、普通边 trigger 和控制流 writer，并委托 `Pregel::validate()` 完成运行时容器校验。

本次不实现 `invoke`、`stream`、superstep 调度、状态合并、checkpoint、interrupt/resume 或 `Command` 动态跳转；这些后续应放在 Pregel runtime 主线中实现。

## Key Changes

- `CompiledStateGraph` 内部持有 `Pregel<StateT, UpdateT, ContextT>`，不暴露运行时执行方法。
- 新增 `CompiledStateGraph::attach_node`，为用户节点注册 `PregelNode`，读取当前所有 state channel 和 managed value，安装最小 state writer，并创建 `branch:to:{node}` trigger channel。
- 新增 `CompiledStateGraph::attach_edge`，接收起点集合：单起点边将 `START -> node` 接入 `START` trigger，并将普通边 `a -> b` 编译为源节点 writer 写入 `branch:to:b`；多起点边按源项目语义生成 `join:{starts}:{end}` barrier channel。
- 新增 `CompiledStateGraph::attach_branch` 骨架，当前返回 `UnsupportedCompiledBranches`。
- `StateGraph::compile(self)` 恢复无参签名，消费 builder，调用 `validate()`，创建 `CompiledStateGraph`，依次 attach node / edge / branch，最后 validate Pregel 容器。
- `CompiledStateGraph` 默认将 `stream_channels` 设为与 `output_channels` 相同的 state channel 集合；后续有独立 schema 后再区分 output projection 与 stream projection。
- `StateGraph::compile(self)` 会像源项目一样遍历 `waiting_edges` 并传入 `attach_edge(starts, end)`；多起点 join 使用 `NamedBarrierValue`，目标节点订阅 join channel，各起点节点向 join channel 写入自己的节点名。

## Test Plan

- 合法单节点图可编译，用户节点被注册为 `PregelNode`。
- 无入口图编译失败并返回 `MissingEntrypoint`。
- `attach_node` 为节点创建 `branch:to:{node}` trigger channel，设置 state/managed 读取 channels，并安装 state writer。
- `START -> a` 会让节点 `a` 订阅 `START` trigger。
- `a -> b` 会让节点 `b` 订阅 `branch:to:b`，并创建对应 channel。
- `a -> END` 不生成 `END` 节点或 `END` trigger。
- 条件边当前返回明确不支持错误；waiting edge 会编译为 join barrier channel。
- `stream_channels` 默认等于 `output_channels`。

验证命令：`cargo fmt`、`cargo test`、`cargo check`、`cargo clippy --all-targets --all-features`。

## Result

已按本计划完成：`CompiledStateGraph` 只保留编译装配职责，不再引入 `StateCodec`、`InvokeResult` 或 `invoke` 执行循环；`StateGraph::compile(self)` 已恢复为无参接口，并通过 `attach_node` / `attach_edge` / `attach_branch` 装配 Pregel 容器。`attach_node` 当前会写入 state/managed 读取 channels，并传入最小 state writer；state writer 的 tuple mapper 直接接收节点返回值并解析出更新字段。普通边控制流 writer 和 waiting edge join barrier 都由 `attach_edge` 追加。

## Assumptions

- 本项目当前仍处于骨架阶段，`CompiledStateGraph` 采用组合持有 `Pregel`，而不是像 Python 源项目那样继承 `Pregel`。
- `invoke` / `stream` 后续应在 Pregel runtime 层实现；如需为了 API 便利在 `CompiledStateGraph` 暴露同名方法，也应只做薄转发。
- 状态字段写入、schema mapper、条件边和 checkpoint 仍是后续工作。
