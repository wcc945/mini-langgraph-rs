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

把当前“运行时主线已可跑”的状态收口成可验证 MVP：用户通过 `StateGraph` 构图、`compile()` 编译后，可以调用 `CompiledStateGraph::invoke(input, runtime_context)` 获取输出，也可以调用 `stream(input, runtime_context)` 接收 `values` 或 `updates` 输出。`RuntimeContext` 承载用户 context，并可通过可选 `stream_mode` 覆盖本次运行的 stream mode；`invoke` 贴近源项目，通过 `stream` 收集结果，`Values` 返回最后一个 payload，`Updates` 返回 chunk 列表。

本轮不实现 checkpoint、resume、interrupt、`Command` 动态跳转、managed value 读取注入、多 stream mode、typed schema 投影或 async 节点接口。

## 执行步骤

1. 增加 loop 最终输出读取能力。
   - 验证：单 output channel 返回该 channel 值；多 output channels 返回 `StateValue::Object`，并跳过不可用 channel。
2. 增加同步 `Pregel::invoke(input)` 主线。
   - 验证：复用 `enter -> tick -> execute -> after_tick` 跑到 `Done`，并传播 enter / execute / update 阶段错误。
3. 增加按调用选择 stream mode 的入口。
- 验证：默认 `stream()` 仍使用 `Values`；`RuntimeContext::with_stream_mode(StreamMode::Updates)` 返回 updates item。
4. 在 `CompiledStateGraph` 上暴露运行入口并补 crate root 导出。
- 验证：`StateValue`、`StreamMode`、`PregelStreamItem` 可由外部调用方命名；`CompiledStateGraph::invoke` / `stream` 作为薄转发工作。
5. 补端到端运行测试。
- 验证：`StateGraph -> compile -> invoke` 返回预期状态；`stream(Updates)` 能收到节点更新；节点能读取 `RuntimeContext.context`；多 state/output channel、顺序链路、条件入口、条件边和 waiting edge 编译后可实际调度执行；公开 API 错误路径有集成覆盖。
6. 同步文档。
   - 验证：runtime、core graph 和 Rust 化取舍文档反映 MVP 已完成能力和仍暂缓能力。

## 当前结果

- 已实现 `PregelLoop::output()`，用于读取最终 output channels。
- 已实现 `Pregel::invoke(input, runtime_context)`，内部调用 `stream` 并按 stream mode 收集输出：`Values` 返回最后一个 payload，`Updates` 返回 `StateValue::List` chunk 列表。
- 已实现 `Pregel::stream(input, runtime_context)`，单次调用可通过 `RuntimeContext.stream_mode` 覆盖默认 stream mode。
- 已实现 `CompiledStateGraph::invoke(input, runtime_context)`、`stream(input, runtime_context)` 转发。
- 已公开并 re-export `StateValue`、`StreamMode`、`PregelStreamItem`。
- 已补充 loop、Pregel 和 StateGraph 端到端测试；`tests/mvp_runtime.rs` 当前从公开 API 视角覆盖 24 条 MVP 运行时行为。

## 验证命令

- `cargo fmt`
- `cargo test`
- `cargo clippy --all-targets --all-features`
- `git diff --check`
