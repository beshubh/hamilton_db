//! `EXPLAIN`: the plan tree as text.
//!
//! Each node is a line with its name and what it reads (a table, an index, a
//! count), followed by `Label: value` lines for its expressions, and then
//! its children, indented under `->` as in PostgreSQL:
//!
//! ```text
//! Projection
//!   Output: name, total
//!   ->  HashJoin (inner)
//!         Keys: left.id = right.user_id
//!         ->  SeqScan on users
//!         ->  SeqScan on orders
//!               Filter: total > 100
//! ```
//!
//! Columns are shown by their name in the input plan's schema. In a join's
//! expressions they are prefixed by the input they read: `left` and `right`,
//! or `outer` and `inner` for an index nested loop join.

use std::fmt;
use std::sync::Arc;

use common::{Column, DataType, Schema};

use super::{
    AbstractPlanNode, Aggregate, IndexScanRange, JoinType, KeyBound, OrderBy, PlanNode,
    WindowColumn, WindowFunction, WindowFunctionKind, only_child,
};
use crate::expressions::{AbstractExpr, ExprInput};

/// Shows the plan of its child, one row per line of text, without running
/// it.
#[derive(Clone, Debug)]
pub struct ExplainPlan {
    /// One VARCHAR column, `QUERY PLAN`.
    output_schema: Schema,
    /// Always `[child]`.
    children: Vec<Arc<AbstractPlanNode>>,
}

impl ExplainPlan {
    pub fn new(child: Arc<AbstractPlanNode>) -> Self {
        Self {
            output_schema: Schema::new(vec![Column::new(
                "QUERY PLAN",
                DataType::Varchar(u32::MAX),
                false,
            )]),
            children: vec![child],
        }
    }

    /// The plan explained.
    pub fn child(&self) -> &Arc<AbstractPlanNode> {
        &self.children[0]
    }

    /// The lines of the child's plan.
    pub fn lines(&self) -> Vec<String> {
        let mut lines = Vec::new();
        explain_node(self.child(), 0, &mut lines);
        lines
    }
}

impl PlanNode for ExplainPlan {
    fn get_output_schema(&self) -> &Schema {
        &self.output_schema
    }

    fn get_children(&self) -> &[Arc<AbstractPlanNode>] {
        &self.children
    }

    fn clone_with_children(&self, children: Vec<Arc<AbstractPlanNode>>) -> AbstractPlanNode {
        Self::new(only_child("Explain", children)).into()
    }
}

/// The plan tree as EXPLAIN shows it, one line per row.
impl fmt::Display for AbstractPlanNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut lines = Vec::new();
        explain_node(self, 0, &mut lines);
        f.write_str(&lines.join("\n"))
    }
}

/// Appends the lines of `plan` and its children, `depth` levels down.
fn explain_node(plan: &AbstractPlanNode, depth: usize, lines: &mut Vec<String>) {
    let (title, properties) = describe(plan);
    // A child's `->` sits under the text of its parent's title, which starts
    // at column `6 * depth`; its properties are indented under its title.
    match depth {
        0 => lines.push(title),
        _ => lines.push(format!("{}->  {title}", " ".repeat(6 * depth - 4))),
    }
    let indent = " ".repeat(6 * depth + 2);
    for (label, value) in properties {
        lines.push(format!("{indent}{label}: {value}"));
    }
    for child in plan.get_children() {
        explain_node(child, depth + 1, lines);
    }
}

/// A node's title line and its `(label, value)` properties.
fn describe(plan: &AbstractPlanNode) -> (String, Vec<(&'static str, String)>) {
    // The expressions of a plan with one child read the child's rows.
    let child: Vec<_> = plan
        .get_children()
        .iter()
        .take(1)
        .map(|child| input(None, child.get_output_schema()))
        .collect();
    let child = child.as_slice();
    let mut properties = Vec::new();
    let title = match plan {
        AbstractPlanNode::CreateTablePlan(plan) => format!("CreateTable {}", plan.table_name()),
        AbstractPlanNode::CreateIndexPlan(plan) => format!(
            "CreateIndex {} on {} ({})",
            plan.index_name(),
            plan.table_name(),
            plan.key_columns().join(", ")
        ),
        AbstractPlanNode::SeqScanPlan(plan) => {
            if let Some(filter) = plan.filter() {
                let table = [input(None, plan.get_output_schema())];
                properties.push(("Filter", filter.display_with(&table).to_string()));
            }
            format!("SeqScan on {}", plan.table_name())
        }
        AbstractPlanNode::IndexScanPlan(plan) => {
            match plan.range() {
                IndexScanRange::Full => {}
                IndexScanRange::Points(keys) => {
                    properties.push(("Keys", list(keys.iter().map(|key| key_text(key)))));
                }
                IndexScanRange::Range { lower, upper } => {
                    properties.push(("Range", range_text(lower.as_ref(), upper.as_ref())));
                }
            }
            if let Some(filter) = plan.filter() {
                let table = [input(None, plan.get_output_schema())];
                properties.push(("Filter", filter.display_with(&table).to_string()));
            }
            format!("IndexScan using {}", plan.index_name())
        }
        AbstractPlanNode::ValuesPlan(plan) => match plan.rows().len() {
            1 => "Values (1 row)".to_string(),
            rows => format!("Values ({rows} rows)"),
        },
        AbstractPlanNode::InsertPlan(plan) => format!("Insert on {}", plan.table_name()),
        AbstractPlanNode::UpdatePlan(plan) => {
            // A column the UPDATE does not set reads itself.
            let table = plan.child().get_output_schema();
            let assignments = plan
                .target_exprs()
                .iter()
                .enumerate()
                .filter(|(idx, expr)| !reads_column(expr, *idx))
                .map(|(idx, expr)| {
                    format!(
                        "{} = {}",
                        table.columns()[idx].name,
                        expr.display_with(child)
                    )
                });
            properties.push(("Set", list(assignments)));
            format!("Update on {}", plan.table_name())
        }
        AbstractPlanNode::DeletePlan(plan) => format!("Delete on {}", plan.table_name()),
        AbstractPlanNode::FilterPlan(plan) => {
            properties.push((
                "Condition",
                plan.predicate().display_with(child).to_string(),
            ));
            "Filter".to_string()
        }
        AbstractPlanNode::ProjectionPlan(plan) => {
            properties.push(("Output", exprs_text(plan.exprs(), child)));
            "Projection".to_string()
        }
        AbstractPlanNode::NestedLoopJoinPlan(plan) => {
            if let Some(predicate) = plan.predicate() {
                let inputs = join_inputs(plan.left(), plan.right());
                properties.push(("Condition", predicate.display_with(&inputs).to_string()));
            }
            format!("NestedLoopJoin ({})", join_type(plan.join_type()))
        }
        AbstractPlanNode::HashJoinPlan(plan) => {
            let inputs = join_inputs(plan.left(), plan.right());
            // The right keys read the right row on its own, as tuple 0.
            let right = &inputs[1..];
            let keys = plan
                .left_keys()
                .iter()
                .zip(plan.right_keys())
                .map(|(l, r)| format!("{} = {}", l.display_with(&inputs), r.display_with(right)));
            if !plan.left_keys().is_empty() {
                properties.push(("Keys", list(keys)));
            }
            if let Some(predicate) = plan.predicate() {
                properties.push(("Condition", predicate.display_with(&inputs).to_string()));
            }
            format!("HashJoin ({})", join_type(plan.join_type()))
        }
        AbstractPlanNode::NestedIndexJoinPlan(plan) => {
            let inputs = [
                input(Some("outer"), plan.outer().get_output_schema()),
                input(Some("inner"), plan.inner_schema()),
            ];
            properties.push(("Key", exprs_text(plan.key_exprs(), &inputs)));
            if let Some(predicate) = plan.predicate() {
                properties.push(("Condition", predicate.display_with(&inputs).to_string()));
            }
            format!(
                "NestedIndexJoin ({}) using {}",
                join_type(plan.join_type()),
                plan.index_name()
            )
        }
        AbstractPlanNode::AggregationPlan(plan) => {
            if !plan.group_bys().is_empty() {
                properties.push(("Group By", exprs_text(plan.group_bys(), child)));
            }
            let aggregates = plan.aggregates().iter();
            properties.push((
                "Aggregates",
                list(aggregates.map(|aggregate| aggregate_text(aggregate, child))),
            ));
            "Aggregation".to_string()
        }
        AbstractPlanNode::WindowPlan(plan) => {
            let columns = plan.columns().iter().map(|column| match column {
                WindowColumn::Expr(expr) => expr.display_with(child).to_string(),
                WindowColumn::Window(function) => window_text(function, child),
            });
            properties.push(("Output", list(columns)));
            "Window".to_string()
        }
        AbstractPlanNode::SortPlan(plan) => {
            properties.push(("Key", order_by_text(plan.order_bys(), child)));
            "Sort".to_string()
        }
        AbstractPlanNode::LimitPlan(plan) => format!("Limit {}", plan.limit()),
        AbstractPlanNode::TopNPlan(plan) => {
            properties.push(("Key", order_by_text(plan.order_bys(), child)));
            format!("TopN {}", plan.n())
        }
        AbstractPlanNode::TopNPerGroupPlan(plan) => {
            properties.push(("Group By", exprs_text(plan.group_bys(), child)));
            properties.push(("Key", order_by_text(plan.order_bys(), child)));
            format!("TopNPerGroup {}", plan.n())
        }
        AbstractPlanNode::ExplainPlan(_) => "Explain".to_string(),
    };
    (title, properties)
}

fn input<'a>(prefix: Option<&'a str>, schema: &'a Schema) -> ExprInput<'a> {
    ExprInput { schema, prefix }
}

/// The inputs of a join's expressions: the left row, then the right.
fn join_inputs<'a>(left: &'a AbstractPlanNode, right: &'a AbstractPlanNode) -> [ExprInput<'a>; 2] {
    [
        input(Some("left"), left.get_output_schema()),
        input(Some("right"), right.get_output_schema()),
    ]
}

fn list<T: fmt::Display>(items: impl IntoIterator<Item = T>) -> String {
    items
        .into_iter()
        .map(|item| item.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

fn exprs_text(exprs: &[AbstractExpr], inputs: &[ExprInput]) -> String {
    list(exprs.iter().map(|expr| expr.display_with(inputs)))
}

/// Whether `expr` is column `idx` of the input.
fn reads_column(expr: &AbstractExpr, idx: usize) -> bool {
    matches!(
        expr,
        AbstractExpr::ColumnValueExpr(column) if column.tuple_idx() == 0 && column.col_idx() == idx
    )
}

fn join_type(join_type: JoinType) -> &'static str {
    match join_type {
        JoinType::Inner => "inner",
        JoinType::Left => "left",
    }
}

/// An index key, which reads no input: the value itself for one column,
/// else `(v1, v2, ...)`.
fn key_text(key: &[AbstractExpr]) -> String {
    match key {
        [value] => value.to_string(),
        _ => format!("({})", list(key)),
    }
}

/// `[lower, upper)`, with `inf` for an open end.
fn range_text(lower: Option<&KeyBound>, upper: Option<&KeyBound>) -> String {
    let (open, lower) = match lower {
        Some(bound) => (
            if bound.inclusive { "[" } else { "(" },
            key_text(&bound.key),
        ),
        None => ("(", "-inf".to_string()),
    };
    let (close, upper) = match upper {
        Some(bound) => (
            if bound.inclusive { "]" } else { ")" },
            key_text(&bound.key),
        ),
        None => (")", "inf".to_string()),
    };
    format!("{open}{lower}, {upper}{close}")
}

fn aggregate_text(aggregate: &Aggregate, inputs: &[ExprInput]) -> String {
    let name = match aggregate {
        Aggregate::CountStar => return "count(*)".to_string(),
        Aggregate::Count(_) => "count",
        Aggregate::Sum(_) => "sum",
        Aggregate::Min(_) => "min",
        Aggregate::Max(_) => "max",
        Aggregate::Avg(_) => "avg",
    };
    let arg = aggregate.arg().expect("only count(*) has no argument");
    format!("{name}({})", arg.display_with(inputs))
}

fn order_by_text(order_bys: &[OrderBy], inputs: &[ExprInput]) -> String {
    list(order_bys.iter().map(|order_by| {
        let direction = if order_by.desc { " DESC" } else { "" };
        format!("{}{direction}", order_by.expr.display_with(inputs))
    }))
}

/// `function OVER (PARTITION BY ... ORDER BY ...)`.
fn window_text(window: &WindowFunction, inputs: &[ExprInput]) -> String {
    let function = match &window.function {
        WindowFunctionKind::Aggregate(aggregate) => aggregate_text(aggregate, inputs),
        WindowFunctionKind::Rank => "rank()".to_string(),
        WindowFunctionKind::RowNumber => "row_number()".to_string(),
    };
    let mut over = Vec::new();
    if !window.partition_by.is_empty() {
        over.push(format!(
            "PARTITION BY {}",
            exprs_text(&window.partition_by, inputs)
        ));
    }
    if !window.order_by.is_empty() {
        over.push(format!(
            "ORDER BY {}",
            order_by_text(&window.order_by, inputs)
        ));
    }
    format!("{function} OVER ({})", over.join(" "))
}

#[cfg(test)]
mod tests {
    use common::Value;

    use super::*;
    use crate::expressions::{
        ArithmeticExpr, ArithmeticOp, ColumnValueExpr, ComparisonExpr, ComparisonOp, ConstantExpr,
    };
    use crate::plans::{
        AggregationPlan, HashJoinPlan, IndexScanPlan, NestedIndexJoinPlan, SeqScanPlan,
        TopNPerGroupPlan, UpdatePlan,
    };

    fn users() -> Schema {
        Schema::new(vec![
            Column::new("id", DataType::Int, false),
            Column::new("name", DataType::Varchar(16), true),
        ])
    }

    fn orders() -> Schema {
        Schema::new(vec![
            Column::new("user_id", DataType::Int, false),
            Column::new("total", DataType::Int, false),
        ])
    }

    fn scan(name: &str, schema: Schema, filter: Option<AbstractExpr>) -> Arc<AbstractPlanNode> {
        Arc::new(SeqScanPlan::new(1, name, schema, filter).into())
    }

    fn column(tuple_idx: usize, schema: &Schema, idx: usize) -> AbstractExpr {
        ColumnValueExpr::new(tuple_idx, idx, schema.column(idx).unwrap().clone()).into()
    }

    fn int(v: i32) -> AbstractExpr {
        ConstantExpr::new(Value::Int(v), Column::new("?column?", DataType::Int, false)).into()
    }

    fn cmp(op: ComparisonOp, left: AbstractExpr, right: AbstractExpr) -> AbstractExpr {
        ComparisonExpr::new(op, left, right).unwrap().into()
    }

    fn lines(plan: impl Into<AbstractPlanNode>) -> String {
        plan.into().to_string()
    }

    #[test]
    fn hash_join_names_the_side_of_each_column() {
        let total_over_100 = cmp(ComparisonOp::Gt, column(0, &orders(), 1), int(100));
        let id_below_total = cmp(
            ComparisonOp::Lt,
            column(0, &users(), 0),
            column(1, &orders(), 1),
        );
        let join = HashJoinPlan::new(
            JoinType::Left,
            scan("users", users(), None),
            scan("orders", orders(), Some(total_over_100)),
            vec![column(0, &users(), 0)],
            vec![column(0, &orders(), 0)],
            Some(id_below_total),
        );
        assert_eq!(
            lines(join),
            "HashJoin (left)
  Keys: left.id = right.user_id
  Condition: left.id < right.total
  ->  SeqScan on users
  ->  SeqScan on orders
        Filter: total > 100"
        );
    }

    #[test]
    fn index_scans_show_their_keys() {
        let scan = |range| lines(IndexScanPlan::new(1, 2, "users_id", users(), range, None));
        assert_eq!(scan(IndexScanRange::Full), "IndexScan using users_id");
        let points = IndexScanRange::Points(vec![vec![int(1)], vec![int(2), int(3)]]);
        assert_eq!(scan(points), "IndexScan using users_id\n  Keys: 1, (2, 3)");
        let range = IndexScanRange::Range {
            lower: Some(KeyBound {
                key: vec![int(10)],
                inclusive: false,
            }),
            upper: None,
        };
        assert_eq!(scan(range), "IndexScan using users_id\n  Range: (10, inf)");
        let range = IndexScanRange::Range {
            lower: None,
            upper: Some(KeyBound {
                key: vec![int(20)],
                inclusive: true,
            }),
        };
        assert_eq!(scan(range), "IndexScan using users_id\n  Range: (-inf, 20]");
    }

    #[test]
    fn index_join_reads_an_outer_and_an_inner_row() {
        let join = NestedIndexJoinPlan::new(
            JoinType::Inner,
            scan("users", users(), None),
            2,
            orders(),
            3,
            "orders_user_id",
            vec![column(0, &users(), 0)],
            Some(cmp(ComparisonOp::Gt, column(1, &orders(), 1), int(5))),
        );
        assert_eq!(
            lines(join),
            "NestedIndexJoin (inner) using orders_user_id
  Key: outer.id
  Condition: inner.total > 5
  ->  SeqScan on users"
        );
    }

    #[test]
    fn update_shows_only_the_columns_it_sets() {
        let plus_one = ArithmeticExpr::new(ArithmeticOp::Add, column(0, &users(), 0), int(1));
        let targets = vec![plus_one.unwrap().into(), column(0, &users(), 1)];
        let update = UpdatePlan::new(1, "users", targets, scan("users", users(), None));
        assert_eq!(
            lines(update),
            "Update on users\n  Set: id = id + 1\n  ->  SeqScan on users"
        );
    }

    #[test]
    fn aggregation_and_top_n_per_group() {
        let aggregation = AggregationPlan::new(
            vec![column(0, &orders(), 0)],
            vec![
                Aggregate::CountStar,
                Aggregate::Sum(column(0, &orders(), 1)),
            ],
            scan("orders", orders(), None),
        );
        let grouped = aggregation.get_output_schema().clone();
        let top = TopNPerGroupPlan::new(
            vec![column(0, &grouped, 0)],
            vec![OrderBy {
                expr: column(0, &grouped, 2),
                desc: true,
            }],
            2,
            Arc::new(aggregation.into()),
        );
        assert_eq!(
            lines(top),
            "TopNPerGroup 2
  Group By: user_id
  Key: sum DESC
  ->  Aggregation
        Group By: user_id
        Aggregates: count(*), sum(total)
        ->  SeqScan on orders"
        );
    }
}
