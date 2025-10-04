use polars::prelude::*;

pub trait ExprExtension {
    /// Round to given decimal numbers with RoundMode::HalfAwayFromZero.
    fn round_expr(self, decimals: u32) -> Self;
}

impl ExprExtension for Expr {
    fn round_expr(self, decimals: u32) -> Self {
        self.round(decimals, RoundMode::HalfAwayFromZero)
    }
}
