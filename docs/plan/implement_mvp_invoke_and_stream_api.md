# mini-langgraph-rs MVP invoke / stream API 收尾计划

## 涉及文件

- `src/lib.rs`
- `src/channel/mod.rs`
- `src/pregel/loops.rs`
- `src/pregel/pregel.rs`
- `src/graph/compiled.rs`
- `src/graph/state.rs`
- `docs/plan/implement_mvp_invoke_and_stream_api.md`
- `docs/doc/implementation_scope_core_graph.md`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## 目标

把当前“运行时主线已可跑”的状态收口成可验证 MVP：用户通过 `StateGraph` 构图、`compile()` 编译后，可以调用 `CompiledStateGraph::invoke()` 获取最终输出，也可以调用 `stream()` / `stream_with_mode()` 接收 `values` 或 `updates` 输出。

本轮不实现 checkpoint、resume、interrupt、`Command` 动态跳转、managed value 读取注入、多 stream mode、typed schema 投影或 async 节点接口。

## 执行步骤

1. 增加 loop 最终输出读取能力。
   - 验证：单 output channel 返回该 channel 值；多 output channels 返回 `StateValue::Object`，并跳过不可用 channel。
2. 增加同步 `Pregel::invoke(input)` 主线。
   - 验证：复用 `enter -> tick -> execute -> after_tick` 跑到 `Done`，并传播 enter / execute / update 阶段错误。
3. 增加按调用选择 stream mode 的入口。
   - 验证：默认 `stream()` 仍使用 `Values`；`stream_with_mode(..., StreamMode::Updates)` 返回 updates item。
4. 在 `CompiledStateGraph` 上暴露运行入口并补 crate root 导出。
   - 验证：`StateValue`、`StreamMode`、`PregelStreamItem` 可由外部调用方命名；`CompiledStateGraph::invoke` / `stream` / `stream_with_mode` 作为薄转发工作。
5. 补端到端运行测试。
   - 验证：`StateGraph -> compile -> invoke` 返回预期状态；`stream_with_mode(Updates)` 能收到节点更新；条件边和 waiting edge 编译后可实际调度执行。
6. 同步文档。
   - 验证：runtime、core graph 和 Rust 化取舍文档反映 MVP 已完成能力和仍暂缓能力。

## 当前结果

- 已实现 `PregelLoop::output()`，用于读取最终 output channels。
- 已实现 `Pregel::invoke()`，同步执行完整 Pregel loop 并返回最终输出。
- 已实现 `Pregel::stream_with_mode()`，单次调用可覆盖默认 stream mode。
- 已实现 `CompiledStateGraph::invoke()`、`stream()`、`stream_with_mode()` 转发。
- 已公开并 re-export `StateValue`、`StreamMode`、`PregelStreamItem`。
- 已补充 loop、Pregel 和 StateGraph 端到端测试。

## 验证命令

- `cargo fmt`
- `cargo test`
- `cargo clippy --all-targets --all-features`
- `git diff --check`

