# 可恢复执行与持久化取舍

## 目标

理解源项目 checkpoint、interrupt、resume 的语义，但 mini 版本先以清晰边界为主，不急于实现完整持久化系统。

## 源项目参考

- `study/chapter_5/README.md`：checkpoint / interrupt / resume 主线。
- `study/chapter_5/checkpoint.md`
- `study/chapter_5/pending_writes.md`
- `study/chapter_5/interrupt_resume.md`
- `study/chapter_5/durability.md`
- `libs/langgraph/langgraph/types.py`
- `libs/langgraph/langgraph/pregel/_checkpoint.py`

## 第一阶段不强制实现

- checkpoint saver 接口。
- pending writes 日志。
- `interrupt()` / `Command(resume=...)`。
- 线程级 `thread_id` 和跨 run 恢复。
- sqlite、postgres、memory saver 等后端。

## 后续可选最小实现

如果需要支持恢复能力，应先实现内存级 checkpoint，并只保存 step 边界上的运行时状态：

- channel 当前值。
- channel 版本。
- 节点已经见过的 trigger 版本。
- 最近更新的 channel。
- 当前 step 元数据。

恢复语义应明确为“从 checkpoint 重新调度 task”，而不是恢复函数调用栈。
## 第二阶段已实现：内存级 Checkpoint

以下内容已在本轮实现中完成：

- `Checkpoint` / `CheckpointMetadata` / `PendingWrite` / `CheckpointTuple` / `CheckpointConfig` 数据结构
- `CheckpointSaver` trait（同步版本，含 `get_tuple/put/put_writes/delete_thread/get_next_version`）
- `MemorySaver` 内存实现（含 `put + get_tuple` 往返、无 `checkpoint_id` 取最新、`put_writes` 覆盖语义、`delete_thread` 清空）
- 辅助函数 `empty_checkpoint` / `create_checkpoint` / `copy_checkpoint`
- `PregelLoop` 在 `enter()` 时从 checkpointer 加载或创建空 checkpoint
- `PregelLoop` 在 `after_tick()` 时保存 loop checkpoint
- `PregelLoop` 维护 `channel_versions` 和 `versions_seen` 字段
- `checkpoint/mod.rs` / `checkpoint/saver.rs` / `checkpoint/memory.rs` 三个模块

尚未实现：

- `interrupt()` / `Command(resume=...)` 恢复链路
- pending_writes 在 PregelLoop 中的持久化（当前只在 task 执行期间存在）
- thread_id 从 config 传入的完整路径
- sqlite / postgres 等持久化 backend
- time travel / fork 分支