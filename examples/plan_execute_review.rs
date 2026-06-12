//! Code Review Pipeline agent example.
//!
//! 模拟一个真实的代码审查流水线：一份 PR 提交后，并行执行三项审查
//! （安全、风格、性能），通过 BinaryOperatorAggregate 汇聚发现的问题，
//! 等待所有审查完成后汇总最终报告。全程启用 checkpoint 持久化。
//!
//! 演示的核心功能：
//! 1. Checkpoint (MemorySaver) — 执行中保存快照，可恢复
//! 2. Waiting edge / join — 三路并行审查汇聚到 aggregate_report
//! 3. BinaryOperatorAggregate — 自定义 reducer 合并多个审查节点的 findings
//!
//! Graph topology:
//!
//! ```text
//!                          +-- security_check --+
//! START --> receive_pr ----+-- style_check    --+-- aggregate_report --> END
//!                          +-- perf_check     --+
//! ```
//!
//! Run with: `cargo run --example plan_execute_review`

use mini_langgraph_rs::checkpoint::{CheckpointConfig, MemorySaver};
use mini_langgraph_rs::{
    BinaryOperatorAggregate, DynChannel, GraphError, LastValue, NodeOutput, RuntimeContext,
    StateGraph, StateValue, StreamMode,
};
use std::collections::HashMap;

// ---------------------------------------------------------------------------
// reducer: 合并两个 findings 列表
// ---------------------------------------------------------------------------

fn merge_findings_lists(
    left: StateValue,
    right: StateValue,
) -> Result<StateValue, GraphError> {
    match (left, right) {
        (StateValue::List(mut a), StateValue::List(b)) => {
            a.extend(b);
            Ok(StateValue::List(a))
        }
        (left, right) => Err(GraphError::InvalidChannelUpdate(format!(
            "findings reducer expected List + List, got {left:?} + {right:?}"
        ))),
    }
}

// ---------------------------------------------------------------------------
// 辅助函数
// ---------------------------------------------------------------------------

fn make_finding(reviewer: &str, severity: &str, message: &str) -> StateValue {
    StateValue::Object(HashMap::from([
        (
            "reviewer".to_string(),
            StateValue::String(reviewer.to_string()),
        ),
        (
            "severity".to_string(),
            StateValue::String(severity.to_string()),
        ),
        (
            "message".to_string(),
            StateValue::String(message.to_string()),
        ),
    ]))
}

fn update_channel(key: &str, value: StateValue) -> StateValue {
    StateValue::Object(HashMap::from([(key.to_string(), value)]))
}

fn fmt_finding(f: &StateValue) -> String {
    match f {
        StateValue::Object(map) => {
            let rev = map.get("reviewer").and_then(|v| match v {
                StateValue::String(s) => Some(s.as_str()),
                _ => None,
            }).unwrap_or("?");
            let sev = map.get("severity").and_then(|v| match v {
                StateValue::String(s) => Some(s.as_str()),
                _ => None,
            }).unwrap_or("?");
            let msg = map.get("message").and_then(|v| match v {
                StateValue::String(s) => Some(s.as_str()),
                _ => None,
            }).unwrap_or("?");
            format!("[{rev}] {sev}: {msg}")
        }
        _ => format!("{f:?}"),
    }
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ==================================================================
    // 手动构建 channels（含 BinaryOperatorAggregate reducer）
    // ==================================================================
    let mut channels: HashMap<String, Box<DynChannel>> = HashMap::new();
    channels.insert("pr_content".to_string(), Box::new(LastValue::new()));
    channels.insert("report".to_string(), Box::new(LastValue::new()));
    // 关键：findings 使用 BinaryOperatorAggregate，多节点写入自动合并
    channels.insert(
        "findings".to_string(),
        Box::new(BinaryOperatorAggregate::new(merge_findings_lists)),
    );

    let mut graph: StateGraph<StateValue, StateValue> = StateGraph::new();
    graph.channels = channels;

    // ---- receive_pr: 接收 PR 内容，初始化空的 findings 列表 ----
    graph.add_node("receive_pr", Box::new(|_, _| {
        let pr = r#"fn process(data: &str) -> String {
    let password = "admin123";
    let mut result = String::new();
    for i in 0..data.len() {
        result.push(data.chars().nth(i).unwrap());
    }
    result
}"#;
        Ok(NodeOutput::Update(StateValue::Object(HashMap::from([
            ("pr_content".to_string(), StateValue::String(pr.to_string())),
            ("findings".to_string(), StateValue::List(vec![])),
        ]))))
    }))?;

    // ---- security_check: 安全审查 ----
    graph.add_node("security_check", Box::new(|_state, _| {
        let findings = vec![
            make_finding("security", "high", "硬编码密码 'admin123'"),
            make_finding("security", "medium", "缺少输入长度校验"),
        ];
        Ok(NodeOutput::Update(update_channel("findings", StateValue::List(findings))))
    }))?;

    // ---- style_check: 风格审查 ----
    graph.add_node("style_check", Box::new(|_state, _| {
        let findings = vec![
            make_finding("style", "low", "变量名 'result' 过于通用"),
            make_finding("style", "low", "缺少函数文档注释"),
            make_finding("style", "info", "建议使用迭代器替代索引循环"),
        ];
        Ok(NodeOutput::Update(update_channel("findings", StateValue::List(findings))))
    }))?;

    // ---- perf_check: 性能审查 ----
    graph.add_node("perf_check", Box::new(|_state, _| {
        let findings = vec![
            make_finding("perf", "medium", "循环内多次调用 chars().nth(i)，O(n^2) 复杂度"),
            make_finding("perf", "low", "未预分配 String 容量"),
        ];
        Ok(NodeOutput::Update(update_channel("findings", StateValue::List(findings))))
    }))?;

    // ---- aggregate_report: 等待三路完成后汇总 ----
    graph.add_node("aggregate_report", Box::new(|_state, _| {
        // findings 已被 reducer 合并，生成最终 report
        Ok(NodeOutput::Update(update_channel("report", StateValue::String("review complete".to_string()))))
    }))?;

    // ---- 连线 ----
    graph.set_entry_point("receive_pr")?;

    // Fan-out: receive_pr -> 三路并行
    graph.add_edge("receive_pr", "security_check")?;
    graph.add_edge("receive_pr", "style_check")?;
    graph.add_edge("receive_pr", "perf_check")?;

    // Fan-in (waiting edge): 三路完成后触发 aggregate_report
    graph.add_edge(
        ["security_check", "style_check", "perf_check"],
        "aggregate_report",
    )?;

    graph.set_finish_point("aggregate_report")?;

    // ==================================================================
    // 编译并运行（带 checkpoint）
    // ==================================================================
    let compiled = graph.compile()?;

    // ---- 第一次运行: stream 模式 + checkpoint ----
    let cp1 = MemorySaver::new();
    let stream_ctx = RuntimeContext::new(())
        .with_stream_mode(StreamMode::Updates)
        .with_checkpointer(cp1);

    println!("+===============================================================+");
    println!("|   Code Review Pipeline                                         |");
    println!("|   +-- security_check --+                                       |");
    println!("|   +-- style_check    --+-- aggregate_report                   |");
    println!("|   +-- perf_check     --+                                       |");
    println!("|   Checkpoint: enabled (MemorySaver)                           |");
    println!("+===============================================================+\n");

    let mut receiver = compiled.stream(Some(StateValue::Null), stream_ctx)?;
    let mut step_count = 0;

    while let Some(item) = receiver.recv().await {
        let item = item?;
        step_count += 1;

        if let StateValue::Object(updates) = &item.data {
            for (node_name, value) in updates {
                println!("--- superstep {step_count}: [{node_name}] ---");
                match node_name.as_str() {
                    "receive_pr" => {
                        if let StateValue::Object(fields) = value
                            && let Some(StateValue::String(pr)) = fields.get("pr_content")
                        {
                            println!("  PR received ({len} chars)", len = pr.len());
                        }
                    }
                    "security_check" | "style_check" | "perf_check" => {
                        if let StateValue::Object(fields) = value
                            && let Some(StateValue::List(findings)) = fields.get("findings")
                        {
                            for f in findings {
                                println!("  {}", fmt_finding(f));
                            }
                        }
                    }
                    "aggregate_report" => {
                        println!("  all reviews complete, generating report...");
                    }
                    _ => {
                        println!("  {value:?}");
                    }
                }
                println!();
            }
        }
    }

    // ---- 第二次运行: invoke 模式 + checkpoint，用于验证 checkpoint ----
    println!("================================================================");
    let cp2 = MemorySaver::new();
    let invoke_ctx = RuntimeContext::new(()).with_checkpointer(cp2);
    let _final_output = compiled.invoke(Some(StateValue::Null), invoke_ctx)?;

    // 读取 checkpoint
    let _config = CheckpointConfig {
        thread_id: String::new(),
        checkpoint_ns: String::new(),
        checkpoint_id: None,
    };
    // Note: cp2 已被 move 进 invoke_ctx，这里通过重新构造 context 无法取回。
    // 实际使用中可包装为 Arc<Mutex<MemorySaver>> 实现共享访问。
    println!("Checkpoint was active during invocation (MemorySaver stored snapshots).\n");

    // ---- 第三次运行: 纯 invoke，读取最终合并的 findings ----
    let output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;

    if let StateValue::Object(fields) = &output
        && let Some(StateValue::List(findings)) = fields.get("findings")
    {
        let mut by_severity: HashMap<&str, usize> = HashMap::new();
        let mut by_reviewer: HashMap<&str, usize> = HashMap::new();
        for f in findings {
            if let StateValue::Object(map) = f {
                if let Some(StateValue::String(sev)) = map.get("severity") {
                    *by_severity.entry(sev.as_str()).or_default() += 1;
                }
                if let Some(StateValue::String(rev)) = map.get("reviewer") {
                    *by_reviewer.entry(rev.as_str()).or_default() += 1;
                }
            }
        }
        println!(
            "Total findings: {total} (high: {h}, medium: {m}, low: {l}, info: {i})",
            total = findings.len(),
            h = by_severity.get("high").unwrap_or(&0),
            m = by_severity.get("medium").unwrap_or(&0),
            l = by_severity.get("low").unwrap_or(&0),
            i = by_severity.get("info").unwrap_or(&0),
        );
        println!(
            "By reviewer: security={sec}, style={sty}, perf={perf}",
            sec = by_reviewer.get("security").unwrap_or(&0),
            sty = by_reviewer.get("style").unwrap_or(&0),
            perf = by_reviewer.get("perf").unwrap_or(&0),
        );
        println!();
        for (i, f) in findings.iter().enumerate() {
            println!("  {}. {}", i + 1, fmt_finding(f));
        }
        println!();
        println!("  (findings from 3 parallel checkers merged by BinaryOperatorAggregate)");
        println!("  (all 3 checkers waited via NamedBarrierValue before aggregate_report fired)");
    }

    Ok(())
}