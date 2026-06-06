# 核心构图 API

## 目标

实现 `langgraph` 最小可用主线：用户先定义共享状态，再注册节点和边，最后编译成可执行图。

## 源项目参考

- `study/chapter_0/README.md`：最小心智模型与 `StateGraph -> compile -> invoke/stream` 链路。
- `study/chapter_2/README.md`：`StateGraph` 如何记录节点、普通边和条件边。
- `study/chapter_2_5/README.md`：`StateNodeSpec`、`BranchSpec` 与 `CompiledStateGraph`。
- `libs/langgraph/langgraph/graph/state.py`
- `libs/langgraph/langgraph/graph/_node.py`
- `libs/langgraph/langgraph/graph/_branch.py`

## 应实现

- `START` / `END` 虚拟节点常量。
- `StateGraph` builder，负责保存图定义，不直接执行节点。
- `add_node(name, func)`，先支持显式节点名和同步函数。
- `add_edge(from, to)`，支持固定控制流。
- `add_conditional_edges(source, route, path_map)`，支持路由函数返回 key 后映射到目标节点。
- `compile()`，完成基本校验并生成可执行图。
- 节点签名采用 Rust 习惯表达，但语义保持 `State -> Partial<State>`。

## 暂缓

- 自动从函数名推断节点名。
- 多种 Runnable 适配层。
- 节点级 `retry_policy`、`cache_policy`、`timeout`、`metadata`。
- `Command(goto=...)` 与节点动态跳转目标展示。