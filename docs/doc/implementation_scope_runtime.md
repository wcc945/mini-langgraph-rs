# 同步 Pregel 运行时

## 目标

实现编译后图的同步执行主线：按 superstep 调度节点，执行本轮任务，收集写入，并在下一轮应用可见。

## 源项目参考

- `study/chapter_4/README.md`：Pregel runtime 的同步执行主线。
- `libs/langgraph/langgraph/pregel/main.py`
- `libs/langgraph/langgraph/pregel/_loop.py`
- `libs/langgraph/langgraph/pregel/_runner.py`
- `libs/langgraph/langgraph/pregel/_algo.py`
- `libs/langgraph/langgraph/pregel/_read.py`
- `libs/langgraph/langgraph/pregel/_write.py`

## 应实现

- `invoke(input)`：执行完整图并返回最终状态。
- `stream(input)`：至少支持输出每轮节点更新或状态快照的一种稳定模式。
- superstep 三阶段模型：Plan、Execution、Update。
- 同一 superstep 内的写入对其他节点不可见，必须到下一轮才可见。
- 基于触发信号和版本信息避免节点重复执行。
- 基础递归/步数限制，防止无限循环。
- 明确的运行时错误类型，例如缺失节点、重复节点、非法边、无入口、无终点、非法更新。

## 暂缓

- async 执行接口。
- 远程图、子进程执行器或线程池优化。
- streaming transformer 与多种输出协议。
- retry、cache、timeout 的完整运行时策略。