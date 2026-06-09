# 在 `ChannelWriter` 中实现可执行分支写入

## 涉及文件

- `docs/plan/implement_attach_branch.md`
- `src/channel/channel_writer.rs`
- `src/graph/compiled.rs`
- `src/pregel/node.rs`
- `src/graph/state.rs`
- `src/error.rs`
- `docs/doc/implementation_scope_core_graph.md`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/implementation_scope_state_channels.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

参照源项目 `ChannelWrite.register_writer(branch.run(...))` 的思路，把条件分支编译为一种可执行 `ChannelWriter` entry。`attach_branch` 不再单独维护 branch writer 表，而是把路由函数封装进 `ChannelWriter`，追加到起点节点的 `writers` 中。

现阶段只实现当前 Rust API 已表达的单目标路由：`BranchPathFn<StateT, ContextT> -> Option<String>`。`Some(key)` 通过 `ends` 映射到目标节点并写入 `branch:to:{target}`；目标是 `END` 或返回 `None` 时不写 trigger。

## Key Changes

- 将 `ChannelWriter` 泛型化为 `ChannelWriter<StateT, ContextT>`，使 writer 的动态 entry 能访问 `&StateT` 和 `&mut RuntimeContext<ContextT>`。
- 在 `ChannelWriterEntry` 增加可执行变体，例如 `Executable(ChannelExecutable<StateT, ContextT>)`。
- 调整 `ChannelWriter::assemble`：接收当前节点输入 state 与 runtime context，执行动态 entry 得到 `ChannelWriteEntry` 后复用现有组装逻辑。
- 保留现有 fixed channel writer、tuple state writer、passthrough、mapper、skip_none 语义；只扩展 writer 能力，不改 channel 更新模型。
- 实现 `CompiledStateGraph::attach_branch`：把 branch 路由函数封装为可执行 writer 挂到普通起点节点或 `START` 入口节点上；目标节点的 `branch:to:{node}` channel 由 `attach_node` 统一创建。
- 更新 `PregelNode` 中 writer 类型为 `Vec<ChannelWriter<StateT, ContextT>>`。
- `GraphError::UnsupportedCompiledBranches` 暂时保留，但 `attach_branch` 不再返回它，避免扩大无关删除 diff。

## Test Plan

- `ChannelWriter` 单元测试覆盖可执行 entry 返回写入、返回空写入、错误透传，并保持原有 fixed/tuple/mapper/skip_none/passthrough 测试通过。
- `attach_branch` / compile 测试覆盖普通节点条件边、条件入口 `START`、目标 trigger channel 创建、`END` 跳过和未知 key 错误。
- 验证命令：`cargo fmt`、`cargo test`、`cargo clippy --all-targets --all-features`。

## Docs

- 更新 `docs/doc/implementation_scope_core_graph.md`：条件边从“编译不支持”改为“通过可执行 `ChannelWriter` 编译为 trigger writes”。
- 更新 `docs/doc/implementation_scope_runtime.md`：说明 runtime 后续执行节点 writer 时需要把 state/context 传给 `ChannelWriter::assemble`。
- 更新 `docs/doc/implementation_scope_state_channels.md`：说明 `ChannelWriter::assemble` 支持可执行 entry。
- 更新 `docs/doc/rust_improvements_over_original.md`：记录 Rust 版对源项目 `ChannelWrite.register_writer` 的取舍，即用泛型可执行 writer 表达 branch runnable，但暂不迁移 schema reader、`Send`、async 和 Runnable 生态。
