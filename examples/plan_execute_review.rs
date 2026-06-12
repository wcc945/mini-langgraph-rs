//! Plan → Execute → Review agent example.
//!
//! Demonstrates a multi-step agent with conditional edges for retry loops
//! and state accumulation across iterations.
//!
//! Graph topology:
//!
//! ```text
//! START → plan → execute → review
//!                     ↑        ↓ (retry → 回到 execute)
//!                     └────────┘
//!                                  ↓ (approved → END)
//! ```
//!
//! Run with: `cargo run --example plan_execute_review`

use mini_langgraph_rs::{NodeOutput, RuntimeContext, StateGraph, StateValue};
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ------------------------------------------------------------------
    // Build graph
    // ------------------------------------------------------------------
    let mut graph: StateGraph<StateValue, StateValue> =
        StateGraph::with_channels(["task", "plan", "current_step", "results", "review_count"]);

    // ---- plan node: 将 task 拆分为 3 个子任务 ----
    graph.add_node(
        "plan",
        Box::new(|_, _| {
            Ok(NodeOutput::Update(StateValue::Object(HashMap::from([
                (
                    "task".to_string(),
                    StateValue::String("write a blog post".to_string()),
                ),
                (
                    "plan".to_string(),
                    StateValue::List(vec![
                        StateValue::String("research topic".to_string()),
                        StateValue::String("draft content".to_string()),
                        StateValue::String("polish final".to_string()),
                    ]),
                ),
                ("current_step".to_string(), StateValue::Number(0.0)),
                ("results".to_string(), StateValue::List(vec![])),
                ("review_count".to_string(), StateValue::Number(0.0)),
            ]))))
        }),
    )?;

    // ---- execute node: 取 plan[current_step] 生成模拟结果 ----
    graph.add_node(
        "execute",
        Box::new(|state, _| {
            let fields = match state {
                StateValue::Object(fields) => fields,
                _ => return Ok(NodeOutput::None),
            };

            let plan = match fields.get("plan") {
                Some(StateValue::List(items)) => items.clone(),
                _ => return Ok(NodeOutput::None),
            };

            let step = match fields.get("current_step") {
                Some(StateValue::Number(n)) => *n as usize,
                _ => return Ok(NodeOutput::None),
            };

            let mut results = match fields.get("results") {
                Some(StateValue::List(items)) => items.clone(),
                _ => vec![],
            };

            if step < plan.len() {
                let subtask = plan[step].clone();
                results.push(StateValue::String(format!(
                    "completed: {subtask}",
                    subtask = match &subtask {
                        StateValue::String(s) => s.as_str(),
                        _ => "?",
                    }
                )));

                Ok(NodeOutput::Update(StateValue::Object(HashMap::from([
                    (
                        "current_step".to_string(),
                        StateValue::Number((step + 1) as f64),
                    ),
                    ("results".to_string(), StateValue::List(results)),
                ]))))
            } else {
                Ok(NodeOutput::None)
            }
        }),
    )?;

    // ---- review node: 全部步骤完成 + 判定 → approve 或 retry ----
    // 判定逻辑 (基于 review_count):
    //   review_count >= 2 → 批准, 路由到 END
    //   review_count < 2  → 拒绝, 重置 current_step 并路由回 execute
    graph.add_node(
        "review",
        Box::new(|state, _| {
            let fields = match state {
                StateValue::Object(fields) => fields,
                _ => return Ok(NodeOutput::None),
            };

            let plan_len = match fields.get("plan") {
                Some(StateValue::List(items)) => items.len(),
                _ => return Ok(NodeOutput::None),
            };

            let current_step = match fields.get("current_step") {
                Some(StateValue::Number(n)) => *n as usize,
                _ => return Ok(NodeOutput::None),
            };

            let review_count = match fields.get("review_count") {
                Some(StateValue::Number(n)) => *n as usize,
                _ => return Ok(NodeOutput::None),
            };

            if current_step < plan_len {
                // 还未完成所有步骤, 继续执行
                return Ok(NodeOutput::None);
            }

            // 全部步骤完成, 基于 review_count 判定
            if review_count >= 2 {
                // 批准: 不再修改状态, conditional edge 将路由到 END
                Ok(NodeOutput::None)
            } else {
                // 拒绝: 重置 current_step, 递增 review_count
                Ok(NodeOutput::Update(StateValue::Object(HashMap::from([
                    ("current_step".to_string(), StateValue::Number(0.0)),
                    (
                        "review_count".to_string(),
                        StateValue::Number((review_count + 1) as f64),
                    ),
                ]))))
            }
        }),
    )?;

    // ---- 连线 ----
    graph.set_entry_point("plan")?;
    graph.add_edge("plan", "execute")?;
    graph.add_edge("execute", "review")?;

    // 条件边: review → execute (retry) 或 END (approved)
    graph.add_conditional_edges(
        "review",
        "decide",
        Box::new(|state, _| {
            let fields = match state {
                StateValue::Object(fields) => fields,
                _ => return None,
            };

            let plan_len = match fields.get("plan") {
                Some(StateValue::List(items)) => items.len(),
                _ => return None,
            };

            let current_step = match fields.get("current_step") {
                Some(StateValue::Number(n)) => *n as usize,
                _ => return None,
            };

            let review_count = match fields.get("review_count") {
                Some(StateValue::Number(n)) => *n as usize,
                _ => return None,
            };

            if current_step >= plan_len && review_count >= 2 {
                Some("approved".to_string())
            } else {
                Some("retry".to_string())
            }
        }),
        HashMap::from([
            ("approved".to_string(), mini_langgraph_rs::END.to_string()),
            ("retry".to_string(), "execute".to_string()),
        ]),
    )?;

    // ------------------------------------------------------------------
    // Compile and run
    // ------------------------------------------------------------------
    let compiled = graph.compile()?;

    println!("╔══════════════════════════════════════════════════════╗");
    println!("║   Plan → Execute → Review Agent                      ║");
    println!("║   task: write a blog post                            ║");
    println!("║   plan: [research topic, draft content, polish final]║");
    println!("║   max retries: 2 (review_count < 2 → retry)          ║");
    println!("╚══════════════════════════════════════════════════════╝\n");

    let context = RuntimeContext::new(()).with_stream_mode(mini_langgraph_rs::StreamMode::Updates);

    let mut receiver = compiled.stream(Some(StateValue::Null), context)?;
    let mut step_count = 0;

    while let Some(item) = receiver.recv().await {
        let item = item?;
        step_count += 1;

        if let StateValue::Object(updates) = &item.data {
            for (node_name, value) in updates {
                println!("--- superstep {}: [{}] ---", step_count, node_name);
                print_node_update(node_name, value);
                println!();
            }
        }
    }

    // Final state
    println!("══════════════════════════════════════════════════════════");
    println!("Workflow complete in {} supersteps.\n", step_count);

    // Read final state via a single invoke
    let final_output = compiled.invoke(Some(StateValue::Null), RuntimeContext::default())?;
    if let StateValue::Object(fields) = &final_output {
        if let Some(StateValue::List(results)) = fields.get("results") {
            println!("Final results:");
            for (i, r) in results.iter().enumerate() {
                println!("  {}: {}", i + 1, format_state_value(r));
            }
        }
        if let Some(StateValue::Number(rc)) = fields.get("review_count") {
            println!("Total review attempts: {}", *rc as usize + 1);
        }
    }

    Ok(())
}

fn print_node_update(node_name: &str, value: &StateValue) {
    match node_name {
        "plan" => {
            if let StateValue::Object(fields) = value {
                if let Some(StateValue::String(task)) = fields.get("task") {
                    println!("  task: {task}");
                }
                if let Some(StateValue::List(plan)) = fields.get("plan") {
                    println!("  plan:");
                    for (i, item) in plan.iter().enumerate() {
                        println!("    {}. {}", i + 1, format_state_value(item));
                    }
                }
            }
        }
        "execute" => {
            if let StateValue::Object(fields) = value {
                if let Some(StateValue::List(results)) = fields.get("results") {
                    if let Some(last) = results.last() {
                        println!("  {format}", format = format_state_value(last));
                    }
                }
            }
        }
        "review" => {
            if let StateValue::Object(fields) = value {
                if let Some(StateValue::Number(step)) = fields.get("current_step") {
                    if *step == 0.0 {
                        println!("  decision: NEEDS REVISION → retry from beginning");
                    }
                }
                if let Some(StateValue::Number(rc)) = fields.get("review_count") {
                    println!("  review_count: {rc}");
                }
            }
        }
        _ => {
            println!("  {v:?}", v = value);
        }
    }
}

fn format_state_value(value: &StateValue) -> String {
    match value {
        StateValue::Null => "null".to_string(),
        StateValue::Bool(b) => b.to_string(),
        StateValue::Number(n) => n.to_string(),
        StateValue::String(s) => s.clone(),
        StateValue::List(items) => {
            let items: Vec<_> = items.iter().map(format_state_value).collect();
            format!("[{}]", items.join(", "))
        }
        StateValue::Object(map) => {
            let items: Vec<_> = map
                .iter()
                .map(|(k, v)| format!("{k}: {}", format_state_value(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}
