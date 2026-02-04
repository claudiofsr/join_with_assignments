use colored::*;
use pathfinding::prelude::{Matrix, MatrixFormatError, kuhn_munkres_min};
use rayon::prelude::*;
use std::{
    cmp::{self, Ordering},
    fmt::{Debug, Display},
};
use tabled::{
    builder::Builder,
    settings::{Alignment, Style},
};

use crate::{FloatIterExtension, JoinError, JoinResult};

/**
Hungarian algorithm to optimally solving the assignment (matching) problem.

Get the minimal Munkres Assignments from two sets of numbers.

(See too k-nearest neighbors algorithm)

The two sets can be expressed by two arrays: array1 and array2.

The two sets are correlated by the weight matrix.

The weight matrix chosen was:

`matrix[i][j] = abs(array1[i] - array2[j])`

To remove the constraint from the original [PathFinding](https://docs.rs/pathfinding),
always work with a square matrix.

The square matrix is ​​obtained by filling in zeros if necessary.

So, the number of rows can be greater than the number of columns and vice versa:

`array1 >= array2` or `array1 <= array2`.

To see examples, run:

`cargo test -- --show-output munkres_assignments_example`
*/
pub fn munkres_assignments<T, U>(
    slice_a: &[T],
    slice_b: &[U],
    verbose: bool,
) -> JoinResult<Vec<u64>>
where
    T: Debug + Copy,
    U: Debug + Copy,
    f64: From<T> + From<U>,
{
    // Try to convert slices &[T] and &[U] to Vec<f64>.
    let floats_a: Vec<f64> = try_convert(slice_a)?;
    let floats_b: Vec<f64> = try_convert(slice_b)?;

    // Get correlation matrix between vector items.
    let mut matrix: Vec<Vec<i64>> = get_matrix(&floats_a, &floats_b)?;

    // The number of rows can be greater than the number of columns and vice versa.
    convert_to_square_matrix(&mut matrix);

    // Assign weights to everybody choices.
    let weights: Matrix<i64> =
        Matrix::from_rows(matrix.clone()).map_err(|error: MatrixFormatError| {
            JoinError::MatrixCreationError {
                source: error, // Passa o erro original diretamente
                slice_a_len: slice_a.len(),
                slice_b_len: slice_b.len(),
            }
        })?;

    // Get assignments: "Compute a minimum weight maximum matching between
    // two disjoints sets of vertices using the Kuhn-Munkres algorithm".
    let (sum, assignments): (i64, Vec<usize>) = kuhn_munkres_min(&weights);

    if verbose {
        let sum_obtained = show_assignments(&floats_a, &floats_b, &matrix, &assignments);
        assert_eq!(sum, sum_obtained);
    }

    // Try to convert Vec<usize> to Vec<u64>.
    try_convert(&assignments)
}

/**
Generic numeric conversion.

Try to convert `&[T]` to `Vec<U>`.

Example:
```
use join_with_assignments::{try_convert, JoinResult};

fn main() -> JoinResult<()> {
    let array: [i16; 5] = [20, 35, 456, -15, 7];
    let result: Vec<f64> = try_convert(&array)?;
    let valid = vec![20.0, 35.0, 456.0, -15.0, 7.0];
    assert_eq!(valid, result);
    Ok(())
}
```
<https://users.rust-lang.org/t/generic-numeric-conversion/37052>

<https://www.justanotherdot.com/posts/how-do-you-cast-generic-values-youre-sure-are-numbers>
*/
pub fn try_convert<T, U>(slice: &[T]) -> JoinResult<Vec<U>>
where
    T: Copy,
    U: TryFrom<T>,
    <U as TryFrom<T>>::Error: Display,
{
    slice
        .iter()
        .map(|&type_t| {
            U::try_from(type_t).map_err(|error| {
                let from_type = std::any::type_name::<T>().to_string();
                let to_type = std::any::type_name::<U>().to_string();
                let reason = error.to_string();

                JoinError::ConversionError {
                    from_type,
                    to_type,
                    reason,
                }
            })
        })
        .collect()
}

/// Get the matrix with the chosen weight.
///
/// Try to force approximately equal values ​​to match.
///
/// <https://boydjohnson.dev/blog/concurrency-matrix-multiplication>
///
/// <https://dev.to/eblocha/parallel-matrix-multiplication-in-rust-39f6>
fn get_matrix(slice_a: &[f64], slice_b: &[f64]) -> JoinResult<Vec<Vec<i64>>> {
    let col_number: usize = slice_a.len();
    let row_number: usize = slice_b.len();

    // Add a gap to force equal values ​​to match.
    // The gap is the maximum value of the slices.
    let gap = [slice_a, slice_b]
        .concat()
        .iter()
        .cloned()
        .float_max()
        .abs();

    let matrix: Vec<Vec<i64>> = (0..col_number)
        .into_par_iter() // rayon parallel iterator
        .map(|i| {
            (0..row_number)
                //.into_par_iter() // rayon parallel iterator
                .map(|j| {
                    let mut delta: f64 = (slice_a[i] - slice_b[j]).abs();

                    // force matching of approximately equal values
                    if delta < 1.0 {
                        delta += gap * delta;
                    } else {
                        delta += gap;
                    }

                    // Precision: 2 decimal places, then multiply by 100.0
                    delta *= 100.0;

                    // as: silently lossy conversions
                    // Check for overflow before casting to i64.
                    // https://doc.rust-lang.org/book/ch03-02-data-types.html
                    // i64::MAX; // 9_223_372_036_854_775_807i64
                    // An i64 can store numbers from:
                    // -2^(n - 1) to 2^(n - 1) - 1, where n = 64
                    // -2^(63) to 2^(63) - 1
                    // [-9_223_372_036_854_775_808, 9_223_372_036_854_775_807]
                    if delta > i64::MAX as f64 || delta < i64::MIN as f64 {
                        // Check both upper and lower bounds
                        return Err(JoinError::I64OutOfBounds { value: delta });
                    }

                    Ok(delta as i64)
                })
                .collect::<JoinResult<Vec<i64>>>() // Collect inner results
        })
        .collect::<JoinResult<Vec<Vec<i64>>>>()?; // Collect outer results and propagate error if any

    Ok(matrix)
}

/// Get the matrix with the chosen weight: square difference.
///
/// But without rayon parallel iterator.
#[allow(dead_code)]
fn get_matrix_v2(slice_a: &[f64], slice_b: &[f64]) -> Vec<Vec<i64>> {
    let mut matrix = Vec::new();

    for a1 in slice_a {
        let mut array = Vec::new();
        for a2 in slice_b {
            let delta: f64 = (a1 - a2).abs();
            let square_value: f64 = delta.powi(2).ceil();
            array.push(square_value as i64);
        }
        matrix.push(array);
    }

    matrix
}

// https://stackoverflow.com/questions/59314686/how-to-efficiently-create-a-large-vector-of-items-initialized-to-the-same-value
// https://stackoverflow.com/questions/29530011/creating-a-vector-of-zeros-for-a-specific-size

/// Check if the `matrix` is a square matrix,
/// if not convert it to square matrix by padding zeroes.
fn convert_to_square_matrix(matrix: &mut Vec<Vec<i64>>) {
    if matrix.is_empty() {
        return; // Nothing to do if matrix is empty
    }
    if matrix[0].is_empty() {
        // If rows exist but are empty, no columns to pad.
        // Depending on desired behavior, might insert one column of 0s,
        // but for now, it's safer to just return.
        return;
    }

    let row_number: usize = matrix.len();
    let col_number: usize = matrix[0].len();
    let delta: usize = row_number.abs_diff(col_number);

    match row_number.cmp(&col_number) {
        Ordering::Less => {
            // Add rows
            let row: Vec<i64> = vec![0; col_number];
            let rows_to_add = vec![row; delta];
            matrix.extend(rows_to_add);
        }

        Ordering::Greater => {
            // Add columns
            for vector in &mut matrix[..] {
                let zeroes: Vec<i64> = vec![0; delta];
                vector.extend(zeroes);
            }
        }

        Ordering::Equal => (), // Already a square matrix
    }
}

fn show_assignments(
    vec_a: &[f64],
    vec_b: &[f64],
    matrix: &[Vec<i64>],
    assignments: &[usize],
) -> i64 {
    let width: usize = get_max_width(vec_a, vec_b);
    println!("\nFind the minimum bipartite matching:");
    println!("array1: {vec_a:width$?}");
    println!("array2: {vec_b:width$?}\n");

    println!("Square Matrix (zero padding):\n");

    print_matrix(matrix, vec_a, vec_b, &[], false);

    let sum = display_bipartite_matching(width, matrix, vec_a, vec_b, assignments, false);

    println!("Solution to the Assignment Problem (Nearest Neighbors):\n");

    print_matrix(matrix, vec_a, vec_b, assignments, true);

    sum
}

fn get_max_width<T>(slice_a: &[T], slice_b: &[T]) -> usize
where
    T: Clone + ToString,
{
    [slice_a, slice_b]
        .concat()
        .iter()
        .map(|a| a.to_string().chars().count())
        .fold(0, usize::max)
    //.max_by(|a, b| a.partial_cmp(b).unwrap())
    //.expect("Failed to get the maximum width!")
}

fn print_matrix(
    matrix: &[Vec<i64>],
    array1: &[f64],
    array2: &[f64],
    assignments: &[usize],
    filter: bool,
) {
    let row_number: usize = array1.len();
    let col_number: usize = array2.len();
    let min_dim: usize = cmp::min(row_number, col_number);

    let mut rows = Vec::new();

    // Add header
    let mut header = vec!["".to_string()];
    let b_headers = array2.iter().map(|x| x.to_string().green().to_string());
    header.extend(b_headers);
    rows.push(header);

    for (i, line) in matrix.iter().enumerate() {
        // Filter condition for rows when filter is true and row_number < col_number
        if filter && row_number < col_number && i >= min_dim {
            break;
        }

        let mut row = vec![
            array1
                .get(i)
                .map(|x| x.to_string().green().to_string())
                .unwrap_or_default(), // Fallback if index out of bounds (e.g., padded rows)
        ];

        for (j, integer) in line.iter().enumerate() {
            if filter && j >= col_number {
                // Filter condition for columns
                break;
            }

            let mut string = integer.to_string();

            // Add color to assignment
            if Some(&j) == assignments.get(i) {
                string = string.green().bold().to_string();
            };

            row.push(string);
        }

        rows.push(row);
    }

    print_table(&rows);

    println!();
}

/// Pretty print tables
///
/// Examples:
///
/// <https://github.com/zhiburt/tabled>
fn print_table(rows: &[Vec<String>]) {
    let table = Builder::from_iter(rows)
        .build()
        .with(Alignment::right())
        .with(Style::rounded())
        .to_string();
    println!("{table}");
}

fn display_bipartite_matching(
    width: usize,
    matrix: &[Vec<i64>],
    array1: &[f64],
    array2: &[f64],
    assignments: &[usize],
    filter: bool,
) -> i64 {
    let row_number: usize = array1.len();
    let col_number: usize = array2.len();
    let min: usize = cmp::min(row_number, col_number);
    let max: usize = cmp::max(row_number, col_number);
    let widx = max.to_string().len();

    let mut bipartite: Vec<(i64, i64, u64)> = Vec::new();
    let mut assign: Vec<usize> = Vec::new(); // assignments after filter
    let mut values: Vec<i64> = Vec::new();
    let mut sum = 0;

    // https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain
    // assignments.to_vec().retain(|&col| col < min);

    for (row, &col) in assignments.iter().enumerate() {
        if filter
            && ((row_number > col_number && col >= min) || (row_number < col_number && row >= min))
        {
            continue;
        }

        let value = matrix[row][col];
        values.push(value);
        assign.push(col);
        sum += value;
    }

    let width_index: usize = get_max_width(&assign, &[]);
    let width_value: usize = get_max_width(&[], &values);
    let width_b: usize = width_index.max(width_value);

    println!("matrix indexes: {assign:>width_b$?}"); // assignments
    println!("matrix values:  {values:>width_b$?}");
    println!("sum of values: {sum}\n");

    for (row, &col) in assignments.iter().enumerate() {
        if (row_number > col_number && col >= min) || (row_number < col_number && row >= min) {
            continue;
        }

        let delta: u64 = (array1[row] - array2[col]).abs().round() as u64;
        let vec_1: i64 = array1[row].round() as i64;
        let vec_2: i64 = array2[col].round() as i64;

        println!(
            "(array1[{row:widx$}], array2[{col:widx$}], abs_diff): ({:>width$}, {:>width$}, {delta:>width$})",
            array1[row], array2[col]
        );
        bipartite.push((vec_1, vec_2, delta));
    }
    println!();

    sum
}

#[cfg(test)]
mod tests {
    use super::*;

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// `cargo test -- --show-output convert_types`
    fn convert_types() -> JoinResult<()> {
        let array_a: [i32; 7] = [20, 2, 5, 35, 456, -15, 47];
        let array_b: [i16; 7] = [20, 2, 5, 35, 456, -15, 47];
        let array_03: [u32; 7] = [20, 2, 5, 35, 456, 15, 47];

        let mut result = Vec::new();

        let floats64: Vec<f64> = try_convert(&array_a)?;
        result.push(floats64);

        let floats64: Vec<f64> = try_convert(&array_b)?;
        result.push(floats64);

        let floats64: Vec<f64> = try_convert(&array_03)?;
        result.push(floats64);

        for floats in &result {
            println!("result: {floats:5?}");
        }

        assert_eq!(
            result,
            vec![
                [20.0, 2.0, 5.0, 35.0, 456.0, -15.0, 47.0],
                [20.0, 2.0, 5.0, 35.0, 456.0, -15.0, 47.0],
                [20.0, 2.0, 5.0, 35.0, 456.0, 15.0, 47.0],
            ]
        );

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output max_width`
    fn max_width() -> JoinResult<()> {
        let array01 = [2.34, 0.1]; // rows
        let array02 = [2.34, 5.0]; // columns

        println!("array01: {array01:?}");
        println!("array02: {array02:?}");

        let max_width = get_max_width(&array01, &array02);

        println!("max_width: {max_width}");

        assert_eq!(max_width, 4);

        Ok(())
    }

    #[test]
    /// rows > columns
    ///
    /// `cargo test -- --show-output munkres_assignments_example01`
    fn munkres_assignments_example01() -> JoinResult<()> {
        let array01 = [20.01, 2.34, 5.0, 35.2, 456.04, -15.2, 47.65]; // rows
        let array02 = [35.2, 2.34, 0.1, 22.6, 99.03]; // columns

        let result: Vec<u64> = munkres_assignments(&array01, &array02, true)?;

        println!("result: {result:?}");

        assert_eq!(result, [3, 1, 2, 0, 5, 6, 4]);

        Ok(())
    }

    #[test]
    /// rows < columns
    ///
    /// `cargo test -- --show-output munkres_assignments_example02`
    fn munkres_assignments_example02() -> JoinResult<()> {
        let array01 = [35.2, 2.34, 0.1, 22.6, 99.03]; // rows
        let array02 = [20.01, 2.34, 5.0, 35.2, 456.04, -15.2, 47.65]; // columns

        let result: Vec<u64> = munkres_assignments(&array01, &array02, true)?;

        println!("result: {result:?}");

        assert_eq!(result, [3, 1, 2, 0, 6, 4, 5]);

        Ok(())
    }

    #[test]
    /// rows = columns
    ///
    /// `cargo test -- --show-output munkres_assignments_example03`
    fn munkres_assignments_example03() -> JoinResult<()> {
        let array01 = [35.2, 2.34, 0.1, 22.6, 99.03]; // rows
        let array02 = [20.01, 2.34, 5.0, 35.2, 456.04]; // columns

        let result: Vec<u64> = munkres_assignments(&array01, &array02, true)?;

        println!("result: {result:?}");

        assert_eq!(result, [3, 1, 2, 0, 4]);

        Ok(())
    }

    #[test]
    /// rows > columns
    ///
    /// slice_a: &[T], where T: i32
    ///
    /// slice_b: &[T], where T: f64
    ///
    /// `cargo test -- --show-output munkres_assignments_example04`
    fn munkres_assignments_example04() -> JoinResult<()> {
        let array01: [i32; 7] = [20, 2, 5, 35, 456, -15, 47]; // rows
        let array02: [f64; 5] = [35.2, 2.34, 0.1, 22.6, 99.03]; // columns

        let result: Vec<u64> = munkres_assignments(&array01, &array02, true)?;

        println!("result: {result:?}");

        assert_eq!(result, [3, 1, 2, 0, 5, 6, 4]);

        Ok(())
    }

    #[test]
    /// rows = columns
    ///
    /// `cargo test -- --show-output munkres_assignments_example05`
    fn munkres_assignments_example05() -> JoinResult<()> {
        let array01 = [2.34, 0.1]; // rows
        let array02 = [2.34, 5.0]; // columns

        let result: Vec<u64> = munkres_assignments(&array01, &array02, true)?;

        println!("result: {result:?}");

        assert_eq!(result, [0, 1]);

        Ok(())
    }

    #[test]
    /// rows > columns
    ///
    /// `cargo test -- --show-output munkres_assignments_example06`
    fn munkres_assignments_example06() -> JoinResult<()> {
        let array01 = [
            20.01, 35.2, 2.34, 5.0, 35.2, 2.34, -15.2, 35.2, 35.2, 47.65, 2.36,
        ]; // rows
        let array02 = [35.2, 2.34, 0.1, 22.6, 99.03, 35.2, 2.35]; // columns

        let result: Vec<u64> = munkres_assignments(&array01, &array02, true)?;

        println!("result: {result:?}");

        assert_eq!(result, [3, 0, 1, 8, 5, 2, 10, 9, 7, 4, 6]);

        Ok(())
    }
}
