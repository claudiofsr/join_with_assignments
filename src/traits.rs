use std::collections::HashSet;

use polars::prelude::*;

use crate::{
    MyColumn,
    Side::{Left, Middle, Right},
    coluna, get_cnpj_base_expr,
};

pub trait ExprExtension {
    /// Round to given decimal numbers with RoundMode::HalfAwayFromZero.
    fn round_expr(self, decimals: u32) -> Self;
}

impl ExprExtension for Expr {
    fn round_expr(self, decimals: u32) -> Self {
        self.round(decimals, RoundMode::HalfAwayFromZero)
    }
}

/// Trait extension for `LazyFrame` to provide additional functionalities.
pub trait LazyFrameExtension {
    /// Formats string values in specific columns of a LazyFrame.
    ///
    /// - Replaces multiple whitespaces with a single space.
    /// - Removes leading/trailing whitespaces and '&' characters.
    fn format_values(self) -> Self;

    /// Rounds float columns (Float32 and Float64) in a LazyFrame to a specified
    /// number of decimal places using optimized Polars expressions.
    ///
    /// Columns of other data types remain unchanged.
    fn round_float_columns(self, decimals: u32) -> Self;

    /// Adicionar colunas auxiliares das situações de glosa.
    ///
    /// Adicionar 3 colunas contendo CNPJ Base
    fn adicionar_colunas_auxiliares(self) -> Self;

    /// Removes specified columns from a `LazyFrame`.
    ///
    /// This function takes a list of column names and attempts to drop them.
    /// It first checks which columns exist in the `LazyFrame`'s schema.
    /// Existing columns are dropped, while non-existent ones are noted with a warning.
    ///
    /// ### Arguments
    ///
    /// * `self` - The `LazyFrame` instance.
    /// * `columns_to_drop` - A slice of string slices representing the names of the
    ///   columns to be dropped.
    ///
    /// ### Returns
    ///
    /// A `PolarsResult` containing the modified `LazyFrame` with the specified columns
    /// removed, or an error if the schema collection fails.
    ///
    /// ### Examples
    ///
    /// ```rust
    /// use polars::prelude::*;
    /// use crate::join_with_assignments::LazyFrameExtension;
    ///
    /// fn main() -> PolarsResult<()> {
    ///     let df = df! {
    ///         "col1" => &[1, 2, 3],
    ///         "col2" => &["a", "b", "c"],
    ///         "col3" => &[true, false, true],
    ///         "col4" => &[10.0, 20.0, 30.0],
    ///     }?;
    ///
    ///     let lf = df.lazy();
    ///     // Attempt to drop existing and non-existing columns
    ///     let dropped_lf = lf.drop_columns(&["col2", "col_nonexistent", "col4"])?;
    ///
    ///     let collected_df = dropped_lf.collect()?;
    ///     println!("{:?}", collected_df);
    ///
    ///     // Expected output will not contain "col2" and "col4",
    ///     // and "col_nonexistent" will be ignored with a warning printed to stderr.
    ///     // Output should only have "col1" and "col3".
    ///
    ///     let col_names: Vec<&str> = collected_df
    ///         .get_column_names()
    ///         .into_iter()
    ///         .map(|c| c.as_str())
    ///         .collect();
    ///     
    ///     assert!(col_names.contains(&"col1"));
    ///     assert!(col_names.contains(&"col3"));
    ///
    ///     assert!(!col_names.contains(&"col2"));
    ///     assert!(!col_names.contains(&"col4"));
    ///
    ///     // Assert the final number of columns
    ///     assert_eq!(collected_df.width(), 2); // 4 initial - 2 removed = 2
    ///     Ok(())
    /// }
    /// ```
    fn drop_columns(self, columns_to_drop: &[&str]) -> PolarsResult<Self>
    where
        Self: std::marker::Sized;
}

impl LazyFrameExtension for LazyFrame {
    fn format_values(self) -> Self {
        // Column names:
        let glosar: &str = coluna(Middle, "glosar");
        let valor_bc: &str = coluna(Left, "valor_bc");

        self.with_columns([
            col(valor_bc).round_expr(2),
            col(glosar)
                // Substituir multiple_whitespaces " " por apenas um " "
                .str()
                .replace_all(lit(r"\s{2,}"), lit(" "), false)
                // Remover multiple_whitespaces " " e/or "&" das extremidades da linha
                .str()
                .replace_all(lit(r"^[\s&]+|[\s&]+$"), lit(""), false),
        ])
    }

    fn round_float_columns(self, decimals: u32) -> Self {
        // Select columns with Float32 or Float64 data types
        let float_cols_selector = dtype_cols(&[DataType::Float32, DataType::Float64])
            .as_selector()
            .as_expr();

        self.with_columns([
            // Apply the round expression directly to the selected float columns
            float_cols_selector
                //.round(decimals, RoundMode::HalfAwayFromZero)
                .round_expr(decimals)
                .name()
                .keep(), // Keep the original column name
        ])
    }

    fn adicionar_colunas_auxiliares(self) -> Self {
        let columns: Vec<&str> = vec![
            coluna(Left, "contribuinte_cnpj"), // "CNPJ dos Estabelecimentos do Contribuinte"
            "CNPJ Base do Contribuinte",       // Coluna auxiliar
            coluna(Right, "remetente_cnpj2"), // "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe"
            "CNPJ Base do Remetente",         // Coluna auxiliar
            coluna(Right, "destinatario_cnpj"), // "CTe - Informações do Destinatário do CT-e: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes"
            "CNPJ Base do Destinatário",        // Coluna auxiliar
            coluna(Right, "chave_de_acesso"), // "Inf. NFe - Chave de acesso da NF-e : ConhecimentoInformacaoNFe"
            "Valor Total de Documentos Vinculados", // Coluna auxiliar
        ];

        // CTe: 123, 2 NFes: [123, 123] de valor total = 345.85
        // NFe: 123, 2 CTes: [123, 123] de valor total = 217.01
        // https://docs.pola.rs/user-guide/expressions/strings/#extract-a-pattern
        let pattern: Expr = lit(r"(?i)valor total = (.*)"); // regex

        self.with_columns([
            // Add 3 columns containing CNPJ Base
            get_cnpj_base_expr(columns[0]).alias(columns[1]),
            get_cnpj_base_expr(columns[2]).alias(columns[3]),
            get_cnpj_base_expr(columns[4]).alias(columns[5]),
            col(columns[6])
                .str()
                .extract(pattern, 1)
                .cast(DataType::Float64)
                .alias(columns[7]), // Coluna auxiliar
        ])
    }

    fn drop_columns(self, columns_to_drop: &[&str]) -> PolarsResult<Self>
    where
        Self: std::marker::Sized,
    {
        // Attempt to collect the schema of the LazyFrame.
        // If this fails, return the error.
        let schema = self.clone().collect_schema()?;

        // Partition the `columns_to_drop` into two groups:
        // 1. `existing_columns`: Columns that are present in the LazyFrame's schema.
        // 2. `non_existent_columns`: Columns that are NOT present in the schema.
        let (existing_columns_to_drop, non_existent_columns): (Vec<&str>, Vec<&str>) =
            columns_to_drop
                .iter()
                .partition(|&col_name| schema.contains(col_name));

        // Warn about columns that do not exist and therefore cannot be dropped.
        if !non_existent_columns.is_empty() {
            eprintln!(
                "Warning: The following columns were not found and could not be dropped: {:#?}",
                non_existent_columns
            );
        }

        if existing_columns_to_drop.is_empty() {
            // If no valid columns are found to drop, return the original LazyFrame as is.
            // This avoids an unnecessary `drop` operation on an empty list.
            Ok(self)
        } else {
            // Drop only the columns that actually exist in the LazyFrame.
            // `by_name` with `true` indicates that we are providing a list of column names to drop.
            Ok(self.drop(by_name(existing_columns_to_drop, true)))
        }
    }
}

/// A trait to convert a Polars Series into a literal List Expression.
///
/// This is suitable for use cases like `Expr::is_in` when checking column values
/// against a fixed collection of values. The resulting expression represents a
/// Literal Series containing a single row with a List value containing all elements
/// from the original Series.
///
/// Internally, this implodes the Series into a single-row List Series, and then
/// converts that into a Literal Expr. This pattern is the idiomatic way in recent
/// Polars versions (0.47.1) to correctly represent a fixed list of values for
/// membership checks within the expression API.
///
/// See feature: Add 'nulls_equal' parameter to is_in
/// #[21426](https://github.com/pola-rs/polars/pull/21426)
///
pub trait ToLiteralListExpr {
    /// Converts the Series into a Polars literal Expression
    /// representing a List value.
    fn to_list_expr(&self) -> PolarsResult<Expr>;
}

// --- Trait Implementation for Series ---

impl ToLiteralListExpr for Series {
    fn to_list_expr(&self) -> PolarsResult<Expr> {
        // 1. Implode the Series into a single ChunkedArray<ListType> (height 1).
        let imploded_chunked_array = self.implode()?; // implode returns PolarsResult, propagate error with `?`

        // 2. Convert the ChunkedArray<ListType> back into a Series (height 1).
        let imploded_series: Series = imploded_chunked_array.into_series(); // into_series does not return Result

        // 3. Create a Literal expression from this single-value List Series using its `.lit()` method.
        let literal_expr: Expr = imploded_series.lit(); // .lit() does not return Result

        // Return the Expression wrapped in Ok, as the method signature requires PolarsResult<Expr>
        Ok(literal_expr)
    }
}

/**
`FloatIterExt` provides extension methods for iterators over `f64` values,
specifically for finding the minimum and maximum values.

This implementation uses `f64::max` and `f64::min` for comparisons.

**Important `NaN` Handling:**
- If one argument is `NaN`, `f64::max` and `f64::min` return the *other* (non-NaN) argument.
  (e.g., `f64::max(1.0, f64::NAN)` returns `1.0`).
- If both arguments are `NaN`, `NaN` is returned.
- If the iterator is empty, `f64::NAN` is returned (as the initial accumulator for `fold`).

Example:
```
use join_with_assignments::FloatIterExtension;

let vector = vec![4.2, -3.7, 8.1, 0.9, -2.8, 0.0, -0.0];
let max = vector
    .iter()
    .cloned() // Required because `vector.iter()` yields `&f64`
    .float_max();
assert_eq!(max, 8.1);

let min = vector
    .iter()
    .cloned()
    .float_min();
assert_eq!(min, -3.7);

let empty_vector: Vec<f64> = vec![];
let max_empty = empty_vector.iter().cloned().float_max();
assert!(max_empty.is_nan()); // Empty iterator yields NaN

let nan_vector: Vec<f64> = vec![1.0, f64::NAN, 3.0];
let max_with_nan = nan_vector.iter().cloned().float_max();
assert_eq!(max_with_nan, 3.0); // NaN is ignored, 3.0 is max

let min_with_nan = nan_vector.iter().cloned().float_min();
assert_eq!(min_with_nan, 1.0); // NaN is ignored, 1.0 is min

let all_nan_vector: Vec<f64> = vec![f64::NAN, f64::NAN];
let max_all_nan = all_nan_vector.iter().cloned().float_max();
assert!(max_all_nan.is_nan()); // If all are NaN, result is NaN
```
 */
pub trait FloatIterExtension {
    /// Finds the maximum `f64` value in the iterator.
    ///
    /// If the iterator is empty, `f64::NAN` is returned.
    /// If any element is `f64::NAN`, it is ignored in favor of non-`NaN` values.
    /// If all elements are `f64::NAN`, then `f64::NAN` is returned.
    ///
    /// ### Returns
    /// The maximum `f64` value, or `f64::NAN` if empty or all elements are `NaN`.
    fn float_max(&mut self) -> f64;

    /// Finds the minimum `f64` value in the iterator.
    ///
    /// If the iterator is empty, `f64::NAN` is returned.
    /// If any element is `f64::NAN`, it is ignored in favor of non-`NaN` values.
    /// If all elements are `f64::NAN`, then `f64::NAN` is returned.
    ///
    /// ### Returns
    /// The minimum `f64` value, or `f64::NAN` if empty or all elements are `NaN`.
    fn float_min(&mut self) -> f64;
}

impl<I> FloatIterExtension for I
where
    I: Iterator<Item = f64>,
{
    fn float_max(&mut self) -> f64 {
        // `f64::max` behavior: if one argument is NaN, the other is returned.
        // If both are NaN, NaN is returned.
        // Initial f64::NAN ensures empty iterators return NaN.
        self.fold(f64::NAN, f64::max)
    }

    fn float_min(&mut self) -> f64 {
        // `f64::min` behavior: if one argument is NaN, the other is returned.
        // If both are NaN, NaN is returned.
        // Initial f64::NAN ensures empty iterators return NaN.
        self.fold(f64::NAN, f64::min)
    }
}

/// Trait providing DataFrame extension methods.
pub trait DataFrameExtension {
    /// Reorders the DataFrame columns according to a predefined canonical order.
    ///
    /// Columns present in the DataFrame but not in the canonical order are omitted.
    /// Columns present in the canonical order but not in the DataFrame are ignored.
    /// The canonical order is typically defined externally (e.g., `MyColumn::get_columns()`).
    ///
    /// This method uses `DataFrame::select` for efficient column reordering.
    fn sort_by_columns(&self, opt_msg: Option<&str>) -> PolarsResult<Self>
    where
        Self: std::marker::Sized;
}

impl DataFrameExtension for DataFrame {
    /// Reorders the DataFrame columns according to the order defined by `MyColumn::get_columns()`.
    fn sort_by_columns(&self, opt_msg: Option<&str>) -> PolarsResult<Self> {
        // Get the names of columns currently present in the DataFrame for quick lookup.
        let current_columns: HashSet<PlSmallStr> =
            self.get_column_names_owned().into_iter().collect();

        if let Some(msg) = opt_msg {
            println!("{msg}")
        }

        // Filter the canonical column list to include only those present in the DataFrame.
        // Then extract just the names in the desired order.
        let columns_to_select: Vec<&str> = MyColumn::get_columns()
            .iter()
            // Keep only columns from the canonical list that actually exist in the DataFrame
            .filter(|col| current_columns.contains(col.name))
            //.filter(|col| self.column(col.name).is_ok())
            .enumerate()
            .map(|(index, col)| {
                // Print column names and their respective types
                if opt_msg.is_some() {
                    println!(
                        "column {:02}: (\"{}\", DataType::{}),",
                        index + 1,
                        col.name,
                        col.dtype
                    );
                }
                col.name
            })
            .collect();

        // Perform the select operation with the ordered list of existing columns.
        // Using df.select ensures only specified columns are kept and they are in the specified order.
        self.select(columns_to_select)
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_drop_columns
#[cfg(test)]
mod tests_drop_columns {
    use super::*;

    // cargo test -- --help
    // cargo test -- --show-output
    // cargo test -- --show-output multiple_values

    #[test]
    /// `cargo test -- --show-output test_drop_columns`
    fn test_drop_columns() -> PolarsResult<()> {
        let lf = df![
            "col_a" => &[1, 2, 3],
            "col_b" => &["a", "b", "c"],
            "col_c_temp" => &[true, false, true],
            "col_d" => &[10.0, 20.0, 30.0],
            "col_e_temp" => &[4, 5, 6],
        ]?
        .lazy();

        let columns_to_remove = &["col_c_temp", "non_existent", "col_e_temp"];

        let lf = lf.drop_columns(columns_to_remove)?;
        let df = lf.collect()?;

        let col_names: Vec<&str> = df.get_column_names().iter().map(|c| c.as_str()).collect();

        // Assert that only existing temporary columns are removed
        assert!(!col_names.contains(&"col_c_temp"));
        assert!(!col_names.contains(&"col_e_temp"));

        // Assert that other original columns are still present
        assert!(col_names.contains(&"col_a"));
        assert!(col_names.contains(&"col_b"));
        assert!(col_names.contains(&"col_d"));

        // Assert the final number of columns
        assert_eq!(df.width(), 3); // 5 initial - 2 removed = 3

        Ok(())
    }
}

/// Run tests with:
/// cargo test -- --show-output tests_to_list_expr
#[cfg(test)]
mod tests_to_list_expr {
    use super::*;

    #[test]
    fn test_is_in_default_nulls_equal_false() -> PolarsResult<()> {
        // 1. Create the DataFrame (or just the Series to operate on in tests)
        //    DataFrame with a column named "numbers" mirroring pl.Series([1, 2, None])
        let df_original = df!("numbers" => &[Some(1i32), Some(2), None])?;
        println!("DataFrame Original:\n{df_original}");

        // 2. Create the 'other' list expression [1, 3]
        let series: Series = Series::new("null_vals".into(), &[1i32, 3]); // Series of i32
        println!("series: {series}");

        let new_col = series.implode()?.into_series();
        let df = df_original
            .lazy()
            .with_columns([new_col.lit().first().alias("filter")])
            .collect()?;
        println!("df: {df}");

        let list_expr: Expr = series.to_list_expr()?;

        // 3. Apply the is_in expression
        //    Select the column "numbers" and check if values are in the literal list expr
        let expr: Expr = col("numbers").is_in(list_expr, false); // nulls_equal=False (default)

        // 4. Execute the expression on the DataFrame
        let modified_df = df
            .lazy()
            .with_columns([expr.alias("is_in_result")])
            .collect()?;

        // 5. Define the expected result
        let expected_result = Series::new("is_in_result".into(), &[Some(true), Some(false), None]);
        // Explanation: 1 is in [1, 3], 2 is not, None is not considered equal to 1 or 3 when nulls_equal=False

        // 6. Assert
        println!("Test default nulls_equal=false:\n{modified_df}");
        assert_eq!(modified_df["is_in_result"], expected_result.into());

        Ok(())
    }

    #[test]
    fn test_is_in_nulls_equal_true() -> PolarsResult<()> {
        // 1. Create the DataFrame (or just the Series to operate on in tests)
        //    DataFrame with a column named "numbers" mirroring pl.Series([1, 2, None])
        let df_original = df!("numbers" => &[Some(1i32), Some(2), None])?;
        println!("DataFrame Original:\n{df_original}");

        // 2. Create the 'other' list expression [1, 3]
        let series: Series = Series::new("null_vals".into(), &[1i32, 3]); // Series of i32
        println!("series: {series}");

        let new_col = series.implode()?.into_series();
        let df = df_original
            .lazy()
            .with_columns([new_col.lit().first().alias("filter")])
            .collect()?;
        println!("df: {df}");

        let list_expr: Expr = series.to_list_expr()?;

        // 3. Apply the is_in expression
        //    Select the column "numbers" and check if values are in the literal list expr
        let expr: Expr = col("numbers").is_in(list_expr, true); // nulls_equal=True

        // 4. Execute the expression on the DataFrame
        let modified_df = df
            .lazy()
            .with_columns([expr.alias("is_in_result")])
            .collect()?;

        // 5. Define the expected result
        let expected_result = Series::new(
            "is_in_result".into(),
            &[Some(true), Some(false), Some(false)],
        );
        // Explanation: 1 is in [1, 3], 2 is not, None is not considered equal to 1 or 3 when nulls_equal=False

        // 6. Assert
        println!("Test default nulls_equal=false:\n{modified_df}");
        assert_eq!(modified_df["is_in_result"], expected_result.into());

        Ok(())
    }

    #[test]
    fn test_is_in_nulls_equal_true_with_null_in_list() -> PolarsResult<()> {
        // 1. Create the DataFrame (or just the Series to operate on in tests)
        //    DataFrame with a column named "numbers" mirroring pl.Series([1, 2, None])
        let df_original = df!("numbers" => &[Some(1i32), Some(2), None])?;
        println!("DataFrame Original:\n{df_original}");

        // 2. Create the 'other' list expression [1, None]
        //    We need a Series that can hold Option<i32> values
        let series: Series = Series::new("null_vals".into(), &[Some(1i32), None]); // Series of Option<i32> (underlying dtype is i32 with nulls)
        println!("series: {series}");

        let new_col = series.implode()?.into_series();
        let df = df_original
            .lazy()
            .with_columns([new_col.lit().first().alias("filter")])
            .collect()?;
        println!("df: {df}");

        let list_expr: Expr = series.to_list_expr()?;

        // 3. Apply the is_in expression
        let expr: Expr = col("numbers").is_in(list_expr, true); // nulls_equal=True

        // 4. Execute the expression on the DataFrame
        let modified_df = df
            .lazy()
            .with_columns([expr.alias("is_in_result")])
            .collect()?;

        // 5. Define the expected result
        // Explanation: 1 is in [1, None]. 2 is not. None on left matches None on right because nulls_equal=True.
        let expected_result = Series::new(
            "is_in_result".into(),
            &[Some(true), Some(false), Some(true)],
        );

        // 6. Assert
        println!("Test nulls_equal=true with list [1, None]:\n{modified_df}");
        assert_eq!(modified_df["is_in_result"], expected_result.into());

        Ok(())
    }
}

#[cfg(test)]
/// `cargo test -- --show-output min_max_f64_tests`
mod min_max_f64_tests {
    use super::*; // Import your trait and its impl

    #[test]
    fn test_max_f64_standard() {
        let vector: Vec<f64> = vec![4.2, -3.7, 8.1, 0.9, -2.8, 0.0, -0.0];
        let max = vector.iter().cloned().float_max();
        println!("Max Value for vector: {:?}", max);
        assert!(
            max == 8.1,
            "Expected 8.1 Max Value for vector, got {:?}",
            max
        );
    }

    #[test]
    fn test_min_f64_standard() {
        let vector: Vec<f64> = vec![4.2, -3.7, 8.1, 0.9, -2.8, 0.0, -0.0];
        let min = vector.iter().cloned().float_min();
        println!("Min Value for vector: {:?}", min);
        assert!(
            min == -3.7,
            "Expected -3.7 Min Value for vector, got {:?}",
            min
        );
    }

    #[test]
    fn test_max_f64_with_nan_ignored() {
        let nan_vector: Vec<f64> = vec![1.0, f64::NAN, 3.0];
        let max_val = nan_vector.iter().cloned().float_max();
        println!("Value for nan_vector (max, NaN ignored): {:?}", max_val);
        // Expect 3.0 because f64::max ignores NaN if a non-NaN value is present
        assert_eq!(
            max_val, 3.0,
            "Expected 3.0 when NaN is present, got {:?}",
            max_val
        );
    }

    #[test]
    fn test_min_f64_with_nan_ignored() {
        let nan_vector: Vec<f64> = vec![1.0, f64::NAN, 3.0];
        let min_val = nan_vector.iter().cloned().float_min();
        println!("Value for nan_vector (min, NaN ignored): {:?}", min_val);
        // Expect 1.0 because f64::min ignores NaN if a non-NaN value is present
        assert_eq!(
            min_val, 1.0,
            "Expected 1.0 when NaN is present, got {:?}",
            min_val
        );
    }

    #[test]
    fn test_empty_vector_returns_nan() {
        let empty_vector: Vec<f64> = vec![];

        let max_val = empty_vector.iter().cloned().float_max();
        println!("Value for empty_vector (max): {:?}", max_val);
        assert!(
            max_val.is_nan(),
            "Expected NaN for empty vector, got {:?}",
            max_val
        );

        let min_val = empty_vector.iter().cloned().float_min();
        println!("Value for empty_vector (min): {:?}", min_val);
        assert!(
            min_val.is_nan(),
            "Expected NaN for empty vector, got {:?}",
            min_val
        );
    }

    #[test]
    fn test_all_nan_vector_returns_nan() {
        let all_nan_vector: Vec<f64> = vec![f64::NAN, f64::NAN];

        let max_val = all_nan_vector.iter().cloned().float_max();
        println!("Value for all_nan_vector (max): {:?}", max_val);
        // If all values are NaN, and initial accumulator is NaN, the result is NaN
        assert!(
            max_val.is_nan(),
            "Expected NaN for all-NaN vector, got {:?}",
            max_val
        );

        let min_val = all_nan_vector.iter().cloned().float_min();
        println!("Value for all_nan_vector (min): {:?}", min_val);
        assert!(
            min_val.is_nan(),
            "Expected NaN for all-NaN vector, got {:?}",
            min_val
        );
    }
}
