# Benchmark 与 Agent Example 实现计划

## 涉及文件

- `Cargo.toml` — 添加 criterion dev-dependency，配置 `[[bench]]`
- `benches/throughput.rs` — invoke/stream/checkpoint 综合 benchmark（5 组）
- `benches/python_bench.py` — Python 源项目同场景对比脚本
- `examples/plan_execute_review.rs` — Plan→Execute→Review agent
- `readme.md` — 新增性能基准和 Example 章节（含实际对比数据）
- `docs/plan/benchmark_and_example.md` — 本计划存档

## Benchmark

### 场景设计

| group | 场景 | 图结构 | 测量方式 |
| --- | --- | --- | --- |
| `single_node` | 1 节点 1 字段 | START,write,END | invoke() 单次耗时 |
| `linear_chain` | 5/10/20 节点线性链 | START,n0,n1,...,END | invoke() 吞吐 vs 节点数 |
| `conditional_edge` | 1 路由 + 3 分支 | START,route,(a|b|c),END | invoke() 路由开销 |
| `stream_values` | 10 节点流式 | 同上链，stream(Values) | 流式吞吐 |
| `checkpoint` | 10 节点 + MemorySaver | 同上链 + MemorySaver | invoke() 含 checkpoint I/O |

### 对比数据

| group | Python (ms) | Rust (ms) | 加速比 |
| --- | ---: | ---: | ---: |
| `single_node` | 0.252 | 0.073 | **3.5x** |
| `linear_chain/5` | 0.608 | 0.160 | **3.8x** |
| `linear_chain/10` | 1.049 | 0.156 | **6.7x** |
| `linear_chain/20` | 1.964 | 0.165 | **11.9x** |
| `conditional_edge` | 0.366 | 0.134 | **2.7x** |
| `stream_values` | 0.971 | 0.037 | **26.2x** |
| `checkpoint` | 3.573 | 0.163 | **21.9x** |

**关键发现**: Rust 端 invoke 耗时基本恒定（73-165 us），不随节点数线性增长；Python 端随节点数近似线性增长。stream 和 checkpoint 场景中 Rust 的内存分配与 I/O 优势进一步放大。

### 限制

- 递归限制 25（硬编码），linear_chain 最大测试到 20 节点
- stream benchmark 中 compiled.stream() 必须在 tokio runtime 上下文内调用

## Example

**examples/plan_execute_review.rs** — Plan,Execute,Review 多步骤 agent：

- **5 个状态字段**: task, plan, current_step, results, review_count
- **3 个节点**: plan（拆分任务）、execute（执行子任务）、review（审核判定）
- **条件边重试**: review 节点的 path_fn 读取 pre-tick 状态做路由
- **12 supersteps**: 3 子任务 x 3 轮审核（2 重试 + 1 批准），累积 9 条结果

关键技术点：review 节点的 NodeOutput（重置 step）与 path_fn 的路由决策共享同一 pre-tick 状态快照，避免了单 tick 内读写顺序问题。

## readme.md

新增性能和 Example 两个章节，包含方法论、实际对比数据和架构说明。

## 验证

- cargo test: 202 测试通过
- cargo fmt: 格式化干净
- cargo clippy --all-targets: 仅既有代码 warning，零新 error
- cargo bench: 5 组 benchmark 全部通过，输出对比基线
- cargo run --example plan_execute_review: agent 正常工作
