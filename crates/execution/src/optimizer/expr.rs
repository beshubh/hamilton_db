//! Expression rewrites the rules share.

use crate::expressions::{AbstractExpr, ColumnValueExpr, ComparisonOp, Expr, LogicExpr, LogicOp};

/// The operands of the ANDs at the top of `expr`, or `expr` itself.
pub(super) fn conjuncts(expr: &AbstractExpr) -> Vec<&AbstractExpr> {
    match expr {
        AbstractExpr::LogicExpr(and) if and.op() == LogicOp::And => {
            and.children().iter().flat_map(conjuncts).collect()
        }
        _ => vec![expr],
    }
}

/// `conjuncts` ANDed together, or `None` if there are none.
pub(super) fn conjunction(
    conjuncts: impl IntoIterator<Item = AbstractExpr>,
) -> Option<AbstractExpr> {
    conjuncts.into_iter().reduce(|left, right| {
        LogicExpr::new(LogicOp::And, left, right)
            .expect("conjuncts are BOOL")
            .into()
    })
}

/// `expr` with each column it reads replaced by `f` of the column.
pub(super) fn map_columns(
    expr: &AbstractExpr,
    f: &impl Fn(&ColumnValueExpr) -> AbstractExpr,
) -> AbstractExpr {
    if let AbstractExpr::ColumnValueExpr(column) = expr {
        return f(column);
    }
    let children = expr
        .children()
        .iter()
        .map(|child| map_columns(child, f))
        .collect();
    expr.clone_with_children(children)
}

/// Calls `f` with each column `expr` reads.
pub(super) fn for_each_column(expr: &AbstractExpr, f: &mut impl FnMut(&ColumnValueExpr)) {
    if let AbstractExpr::ColumnValueExpr(column) = expr {
        f(column);
    }
    for child in expr.children() {
        for_each_column(child, f);
    }
}

/// Which join sides a pair condition reads.
#[derive(Clone, Copy)]
pub(super) enum Sides {
    Neither,
    Left,
    Right,
    Both,
}

pub(super) fn sides_read(on_pair: &AbstractExpr) -> Sides {
    let (mut left, mut right) = (false, false);
    for_each_column(on_pair, &mut |column| match column.tuple_idx() {
        0 => left = true,
        _ => right = true,
    });
    match (left, right) {
        (false, false) => Sides::Neither,
        (true, false) => Sides::Left,
        (false, true) => Sides::Right,
        (true, true) => Sides::Both,
    }
}

/// `on_pair`, which reads only the right row, as a condition on that row.
pub(super) fn to_right_row(on_pair: &AbstractExpr) -> AbstractExpr {
    map_columns(on_pair, &|column| {
        ColumnValueExpr::new(0, column.col_idx(), column.return_type().clone()).into()
    })
}

/// `on_pair` as hash join `(left key, right key)`, if it is `left = right`.
pub(super) fn hash_keys(on_pair: &AbstractExpr) -> Option<(AbstractExpr, AbstractExpr)> {
    let AbstractExpr::ComparisonExpr(comparison) = on_pair else {
        return None;
    };
    if comparison.op() != ComparisonOp::Eq {
        return None;
    }
    let [a, b] = comparison.children() else {
        unreachable!("a comparison has two operands");
    };
    match (sides_read(a), sides_read(b)) {
        (Sides::Left, Sides::Right) => Some((a.clone(), to_right_row(b))),
        (Sides::Right, Sides::Left) => Some((b.clone(), to_right_row(a))),
        _ => None,
    }
}
