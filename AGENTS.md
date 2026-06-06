# 仓库指南

## 项目结构与模块组织

本仓库是一个名为 `mini-langgraph-rs` 的 Rust 二进制 crate。

- `Cargo.toml` 定义包元数据、Rust edition 和依赖。
- `Cargo.lock` 固定依赖版本，用于可复现构建。
- `src/main.rs` 是当前二进制入口。
- `target/` 是 Cargo 生成目录，不应提交。

随着项目扩展，应用代码应放在 `src/` 下。优先拆分为职责明确的模块，例如 `src/graph.rs`、`src/node.rs` 或 `src/runtime.rs`，避免把所有逻辑堆在 `main.rs`。跨模块的集成测试放在 `tests/` 下。

## 构建、测试与开发命令

- `cargo build`：以 debug 模式编译项目。
- `cargo run`：构建并在本地运行二进制程序。
- `cargo test`：运行单元测试和集成测试。
- `cargo fmt`：使用 `rustfmt` 格式化 Rust 代码。
- `cargo clippy --all-targets --all-features`：检查所有目标和 feature 组合下的常见 Rust 问题。

提交 PR 前应运行 `cargo fmt`、`cargo test` 和 `cargo clippy --all-targets --all-features`。

## 编码风格与命名约定

使用 `rustfmt` 规定的标准 Rust 风格：四空格缩进、由格式化工具插入尾随逗号，并保持惯用模块布局。函数、变量、模块和测试使用 `snake_case`；结构体、枚举、trait 和类型别名使用 `PascalCase`；常量使用 `SCREAMING_SNAKE_CASE`。

保持 `main.rs` 精简。可复用逻辑应移动到模块中，只暴露调用方真正需要的 API。

## 业务背景

本项目是 `langgraph` 的 mini Rust 实现。修改图构建、节点执行、边语义、状态更新或运行时行为前，必须阅读 `docs/doc/project_background.md`。原始参考项目位于本地路径：`E:\codes\Python\langgraph`。

## 测试规范

项目使用 Rust 内置测试框架。单元测试应放在被测代码旁的 `#[cfg(test)] mod tests` 中；端到端测试或公共 API 行为测试放在 `tests/` 下。

测试名称应描述被验证的行为，例如 `rejects_missing_start_node` 或 `executes_nodes_in_order`。修复 bug 时，条件允许应先添加回归测试再修改实现。

## 提交与 Pull Request 规范

当前仓库还没有提交历史，因此尚无项目专属提交规范。提交信息应简洁、使用祈使句，例如 `add graph execution state` 或 `fix node lookup error`。

Pull Request 应包含变更摘要、变更原因和已执行的验证。有关联 issue 时应链接。只有未来涉及 UI 变更时才需要截图。

## 安全与配置建议

不要提交密钥、本地凭据或机器专属 IDE 文件。生成的构建产物保留在 `target/` 中。新增依赖到 `Cargo.toml` 前应先审查其维护状态、功能范围和许可证，优先选择小而稳定的 crate。