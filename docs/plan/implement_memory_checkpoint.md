# 实现内存级 Checkpoint 持久化机制

## 涉及文件

- docs/plan/implement_memory_checkpoint.md
- src/checkpoint/mod.rs（新增）
- src/checkpoint/saver.rs（新增）
- src/checkpoint/memory.rs（新增）
- src/pregel/loops.rs
- src/pregel/mod.rs
- src/lib.rs
- docs/doc/implementation_scope_persistence.md（更新）
- docs/doc/rust_improvements_over_original.md（更新）

## 背景分析

源项目 checkpoint 体系包含以下核心类型和接口：

| Python 类型 | 作用 | Rust 对应 |
|---|---|---|
| Checkpoint (TypedDict) | step 边界上的图状态快照 | Checkpoint struct |
| CheckpointMetadata (TypedDict) | checkpoint 来源/步数/父级 | CheckpointMetadata struct |
| CheckpointTuple (NamedTuple) | get_tuple 返回的完整结果包 | CheckpointTuple struct |
| PendingWrite (tuple) | checkpoint 后执行的写入日志 | PendingWrite struct |
| BaseCheckpointSaver | checkpointer 抽象 | CheckpointSaver trait |
| InMemorySaver | 内存实现 | MemorySaver struct |
| RunnableConfig[configurable] | 定位 checkpoint 的配置键 | CheckpointConfig struct |

本计划按 6 个阶段逐步实现，每个阶段可独立测试验证。

---

## P1: PregelLoop 加入版本表字段

### 目标

在 PregelLoop 中新增 channel_versions 和 versions_seen 字段，为后续调度精确性和 checkpoint 做准备。

### 变更

1. src/pregel/loops.rs:
   - PregelLoop 新增字段：channel_versions 和 versions_seen
   - new() 初始化为空 map
   - first() 输入写入后为每个写入的 channel 设置 channel_versions[channel] = 1
   - apply_writes() 中对每个实际更新了 channel 值的 channel 递增 channel_versions[channel]
   - 每轮 after_tick() 结束时更新 versions_seen

2. 测试：新增单元测试验证版本递增和 versions_seen 更新

### 验证

cargo fmt && cargo test && cargo clippy --all-targets --all-features

---

## P2: 实现检查点数据结构

### 目标

定义 Checkpoint、CheckpointMetadata、PendingWrite、CheckpointTuple、CheckpointConfig 等核心数据结构。

### 变更

1. 新增 src/checkpoint/mod.rs：定义所有数据结构
2. src/lib.rs 新增 pub mod checkpoint

### 验证

cargo fmt && cargo check

---

## P3: 实现 CheckpointSaver trait 和 MemorySaver

### 目标

定义 checkpointer trait 接口，并实现纯内存存储版本。

### 变更

1. src/checkpoint/saver.rs：CheckpointSaver trait，包含 get_tuple/put/put_writes/delete_thread/get_next_version
2. src/checkpoint/memory.rs：MemorySaver，使用 HashMap + BTreeMap 存储，channel_values 直接存在 Checkpoint 结构体中（不需要 Python 版的 blobs 拆分），put_writes 采用同一 task_id 覆盖语义

### 验证

cargo fmt && cargo test && cargo clippy --all-targets --all-features

---

## P4: 实现辅助函数

### 目标

提供 empty_checkpoint、create_checkpoint、copy_checkpoint 三个辅助函数。

### 变更

1. src/checkpoint/mod.rs 新增：
   - empty_checkpoint()：创建空 checkpoint
   - create_checkpoint()：从当前 channels 快照构建新 checkpoint
   - copy_checkpoint()：深拷贝
2. 检查是否需要添加 uuid 或 chrono 依赖到 Cargo.toml

### 验证

cargo fmt && cargo test

---

## P5: PregelLoop 接入 Checkpoint

### 目标

让 PregelLoop 在 step 边界时通过 MemorySaver 保存和加载 checkpoint。

### 变更

1. src/pregel/loops.rs：
   - PregelLoop 新增字段：checkpointer/checkpoint/checkpoint_pending_writes/checkpoint_config
   - enter() 从 saver 加载最近 checkpoint 或使用 empty_checkpoint
   - first() 结束时调用 put_checkpoint(Input)
   - after_tick() 中清空 pending_writes 并调用 put_checkpoint(Loop)
   - 新增 put_checkpoint 辅助方法

2. Pregel::invoke()/stream() 传入 Option<MemorySaver> 参数

### 验证

cargo fmt && cargo test && cargo clippy --all-targets --all-features

---

## P6: 单元测试

### 目标

验证内存检查点的核心语义。

### 测试用例

1. empty_checkpoint 创建空检查点：所有 map 为空，id 非空
2. MemorySaver put + get_tuple 往返
3. get_tuple 无 checkpoint_id 时取最新
4. put_writes 覆盖同一 task_id 的旧写入
5. create_checkpoint 从 channels 快照
6. create_checkpoint 递增版本号
7. PregelLoop 带 checkpointer 完成一轮 tick
8. delete_thread 清空存储

### 验证

cargo fmt && cargo test && cargo clippy --all-targets --all-features

---

## 文档同步

1. 更新 docs/doc/implementation_scope_persistence.md：添加第二阶段完成段落
2. 更新 docs/doc/rust_improvements_over_original.md：添加第 22 条，记录 u64 版本号替代 str 版本、StateValue 直存替代 blobs 拆分、CheckpointSaver trait 不含 async 变体、PendingWrite 用命名结构体替代 tuple 等取舍

---

## 与 interrupt/resume 的关系

本计划只实现 checkpoint 保存/加载机制。interrupt() 和 Command(resume=...) 的恢复语义依赖 checkpoint + pending writes，但属于第二阶段的 interrupt/resume 路径，不在本计划范围内。本计划完成后，interrupt/resume 可以复用 CheckpointSaver 和 PendingWrite 基础设施。
