# 实现 PregelLoop tick 闭环

## 涉及文件

- `docs/plan/implement_pregel_loop_tick.md`
- `src/pregel/task.rs`
- `src/pregel/loops.rs`
- `src/pregel/pregel.rs`
- `docs/doc/implementation_scope_runtime.md`
- `docs/doc/rust_improvements_over_original.md`

## 目标

参照源项目 `langgraph/pregel/_loop.py::PregelLoop.tick`，补齐 mini Rust 版同步 Pregel 主循环的最小闭环：`tick` 负责按上一轮 `updated_channels` 准备当前 superstep tasks，`execute` 只执行任务并收集 pending writes，`after_tick` 应用 writes、推进 step，并为下一轮更新 `updated_channels`。

## 实施步骤

1. 在 `PregelTaskManager` 增加清空当前 task 集合的内部入口。
   - 验证：loop 每轮只执行本轮准备出的 tasks，不重复执行上一轮已完成任务。
2. 实现 `PregelLoop::tick`。
   - 验证：超过递归限制时报错并设置 `OutOfSteps`；无任务时设置 `Done` 并停止；有任务时返回 `true`。
3. 实现 `PregelLoop::after_tick` 的最小 Update 阶段。
   - 验证：pending writes 被 `apply_writes` 应用，`updated_channels` 刷新，`pending_writes` 清空，`step` 增加。
4. 更新 stream 和 loop 测试。
   - 验证：`stream()` 能自然执行到结束；empty input 错误路径仍通过 receiver 返回。
5. 同步运行时文档和 Rust 化取舍文档。
   - 验证：文档说明 `tick/after_tick` 已形成最小闭环，同时列明未迁移能力。

## 不实现范围

- 不实现 `invoke`。
- 不实现 values/updates stream item 输出协议。
- 不迁移 checkpoint、pending writes 持久化、interrupt、debug stream、cache、retry、timeout、PUSH/Send task 或 error handler。
