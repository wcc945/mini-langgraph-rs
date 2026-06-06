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