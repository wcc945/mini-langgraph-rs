# 用 Tokio 管道实现 `Pregel::stream()`

## 涉及文件

- `Cargo.toml`
- `src/channel/mod.rs`
- `src/channel/*`
- `src/channel/channel_writer.rs`
- `src/pregel/node.rs`
- `src/pregel/pregel.rs`
- `src/pregel/loops.rs`
- `src/pregel/task.rs`
- `src/graph/compiled.rs`
- `src/error.rs`
- `docs/plan/implement_pregel_stream.md`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## Summary

- 引入 `tokio`，用 `tokio::sync::mpsc` 实现管道式 `stream()`。
- 对齐源项目 Pregel 语义：`Pregel` 保存图规格；每次创建 loop 时只为本次运行复制 `channels` 和 `managed`，运行写入不会污染下一次运行。
- Channel 副本通过现有 `checkpoint()` + `from_checkpoint()` 机制创建。
- `stream()` 返回 `mpsc::Receiver<Result<PregelStreamItem, GraphError>>`，后台 task 先建立 Pregel loop；实际逐轮调度和发送逻辑暂时保留为方法桩。

## Key Changes

- `Cargo.toml` 增加 `tokio`，节点函数仍同步执行。
- `Pregel.channels` 和 `Pregel.managed` 作为规格；`PregelLoop` 持有本次运行专用 channel map 和 managed map。
- 为 `DynChannel` 增加对象安全 `copy_box()`，内部沿用 `checkpoint()` + `from_checkpoint()`；为 `ManagedValueSpec` 增加对象安全 `copy_box()`，用于每次 loop 创建 managed 运行态副本。
- `CompiledStateGraph` 持有 `Arc<Pregel>`；后台 task 只 clone `Arc` 指针，`PregelLoop` 借用 nodes、input/output/stream channels、trigger 索引和 name 等图规格字段。
- `Pregel::stream(self: Arc<Self>, input)` 创建 `mpsc` channel、启动后台任务；`PregelLoop::new` 复制 channels、managed 并接收 sender。
- `PregelLoop` 目前只保留 `new` 的真实构造逻辑；`tick`、`execute`、`after_tick` 和 `is_stream_closed` 暂为无运行语义的方法桩。
- `PregelTaskManager` 目前只保留 `new` 的真实初始化逻辑；提交、准备和执行任务的方法暂不实现真实运行语义。
- `CompiledStateGraph::stream()` 转发到内部 `Pregel::stream()`。

## Test Plan

- `#[tokio::test]` 当前只覆盖 `stream()` 能返回 receiver，并在 loop 逻辑尚未实现时正常关闭。
- 验证命令：`cargo fmt`、`cargo test`、`cargo clippy --all-targets --all-features`。

## Assumptions

- 默认异步运行时采用 `tokio`。
- 真正需要按 run 复制的是 `channels` 和 `managed`；channel 复制方式使用 channel 自身的 `checkpoint()` + `from_checkpoint()`。
- 节点、writer、trigger 索引、输入输出配置属于图规格，loop 中使用引用，不为准备任务克隆这些描述。
- 不使用共享可变 channel，也不通过 `Arc<Mutex<channels>>` 复用运行态。
- 本轮暂不实现 `values` 和 `updates` 的真实发送语义。
