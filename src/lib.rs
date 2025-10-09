mod analise_do_periodo_de_apuracao;
mod args;
mod columns;
mod consolidacao_da_natureza;
mod descricoes;
mod excel;
mod filtros;
mod glosar_base_de_calculo;
mod legislacao_aliquota_zero;
mod legislacao_credito_presumido;
mod legislacao_incidencia_monofasica;
mod munkres;
mod polars_assignments;
mod traits;

/// A module that exports the `ExcelWriter` struct which implements the Polars
/// `SerWriter` trait to serialize a dataframe to an Excel Xlsx file.
///
/// authors = ["John McNamara <jmcnamara@cpan.org>"]
///
/// repository = "https://github.com/jmcnamara/polars_excel_writer"
mod write;

/// A module that exports the `PolarsXlsxWriter` struct which provides an Excel
/// Xlsx serializer that works with Polars dataframes and which can also
/// interact with the [`rust_xlsxwriter`] writing engine that it wraps.
mod xlsx_writer;

pub use self::{
    analise_do_periodo_de_apuracao::adicionar_coluna_periodo_de_apuracao_inicial_e_final,
    args::*,
    columns::{
        MyColumn,
        Side::{self, Left, Middle, Right},
        coluna,
    },
    consolidacao_da_natureza::obter_consolidacao_nat,
    descricoes::{
        descricao_da_natureza_da_bc_dos_creditos, descricao_da_origem, descricao_do_mes,
        descricao_do_tipo_de_credito, descricao_do_tipo_de_operacao,
    },
    excel::*,
    filtros::*,
    glosar_base_de_calculo::glosar_bc,
    legislacao_aliquota_zero::adicionar_coluna_de_aliquota_zero,
    legislacao_credito_presumido::adicionar_coluna_de_credito_presumido,
    legislacao_incidencia_monofasica::adicionar_coluna_de_incidencia_monofasica,
    munkres::{munkres_assignments, try_convert},
    polars_assignments::get_dataframe_after_assignments,
    traits::{
        DataFrameExtension, ExprExtension, FloatIterExtension, LazyFrameExtension,
        ToLiteralListExpr,
    },
    write::ExcelWriter,
    xlsx_writer::PolarsXlsxWriter,
};

use chrono::NaiveDate;
use polars::prelude::*;
use regex::Regex;
use std::{
    any,
    collections::{HashMap, HashSet},
    env,
    fmt::Write,
    fs::File,
    path::PathBuf,
    sync::LazyLock as Lazy,
};
use sysinfo::System;

pub type MyError = Box<dyn std::error::Error + Send + Sync>;
pub type MyResult<T> = Result<T, MyError>;

/// Struct to represent a correlation between lines from two different sources (e.g., EFD and NFE).
#[derive(Debug, Clone)]
pub struct CorrelatedLines {
    pub chave: String, // The common key used for correlation.
    pub line_efd: u64, // Line number from the 'Left' table (e.g., EFD).
    pub line_nfe: u64, // Line number from the 'Right' table (e.g., NFE).
}

/// Type alias for the collection of all correlations.
pub type AllCorrelations = Vec<Option<Vec<CorrelatedLines>>>;

// https://pola-rs.github.io/polars/sysinfo/index.html
pub fn show_sysinfo() {
    // Please note that we use "new_all" to ensure that all list of
    // components, network interfaces, disks and users are already
    // filled!
    let mut sys = System::new_all();

    // First we update all information of our `System` struct.
    sys.refresh_all();

    let opt_sys_name: Option<String> = System::name();
    let opt_sys_kerv: Option<String> = System::kernel_version();
    let opt_sys_osve: Option<String> = System::os_version();

    match (opt_sys_name, opt_sys_kerv, opt_sys_osve) {
        (Some(sys_name), Some(sys_kerv), Some(sys_osve)) => {
            // Display system information:
            println!("System name:           {sys_name}");
            println!("System kernel version: {sys_kerv}");
            println!("System OS version:     {sys_osve}");
        }
        _ => return,
    }

    // RAM and swap information
    // 1 Byte = 8 bits
    let sys_used_memory: u64 = sys.used_memory() / (1024 * 1024);
    let sys_total_memory: u64 = sys.total_memory() / (1024 * 1024);

    println!("Memory used/total: {sys_used_memory:>8}/{sys_total_memory} Mbytes");

    // Number of CPUs:
    println!("Number of CPUs: {:>9}\n", sys.cpus().len());
}

/// Polar arguments with ENV vars
pub fn configure_the_environment() {
    // https://stackoverflow.com/questions/70830241/rust-polars-how-to-show-all-columns/75675569#75675569
    // https://pola-rs.github.io/polars/polars/index.html#config-with-env-vars
    unsafe {
        env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
        env::set_var("POLARS_FMT_MAX_COLS", "10"); // maximum number of columns shown when formatting DataFrames.
        env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
        env::set_var("POLARS_FMT_STR_LEN", "52"); // maximum number of characters printed per string value.
    }
}

/**
Returns a Field closure that indicates the output Series will have
the same type as the input field.

This mimics `GetOutput::same_type()`.
geany polars-plan-0.50.0/src/dsl/expr/expr_dyn_fn.rs&

GetOutput::same_type()
|_, f| Ok(f.clone())

GetOutput::from_type(DataType::UInt32)]
|_, f| Ok(Field::new(f.name().clone(), DataType::UInt32))
*/
pub fn get_output_same_type(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(field.clone())
}

pub fn get_output_as_int32_fields(_: &Schema, field: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new(field[0].name().clone(), DataType::Int32))
}

/**
Macro para definir funções get_output_as_X_datatype

pub fn get_output_as_uint64(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(Field::new(field.name().clone(), DataType::UInt64))
}

pub fn get_output_as_float64(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(Field::new(field.name().clone(), DataType::Float64))
}

pub fn get_output_as_string(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(Field::new(field.name().clone(), DataType::String))
}

pub fn get_output_as_boolean(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(Field::new(field.name().clone(), DataType::Boolean))
}

pub fn get_output_as_date(_: &Schema, field: &Field) -> PolarsResult<Field> {
    Ok(Field::new(field.name().clone(), DataType::Date))
}
*/
macro_rules! define_output_field_fn {
    ($fn_name:ident, $data_type:expr) => {
        pub fn $fn_name(_: &Schema, field: &Field) -> PolarsResult<Field> {
            Ok(Field::new(field.name().clone(), $data_type))
        }
    };
}

define_output_field_fn!(get_output_as_uint64, DataType::UInt64);
define_output_field_fn!(get_output_as_float64, DataType::Float64);
define_output_field_fn!(get_output_as_string, DataType::String);
define_output_field_fn!(get_output_as_boolean, DataType::Boolean);
define_output_field_fn!(get_output_as_date, DataType::Date);

pub enum Frame {
    Lazy(Box<LazyFrame>),
    Data(DataFrame),
}

/// Integrates a specific column from a source DataFrame into a result DataFrame,
/// renames an existing column in the result DataFrame, and sorts the final columns.
pub fn integrate_and_sort_column(
    df_source: DataFrame,
    mut df_result: DataFrame,
) -> PolarsResult<DataFrame> {
    // Column names:
    let valor_bc: &str = coluna(Left, "valor_bc");
    let valor_bc_auditado: &str = coluna(Left, "valor_bc_auditado");

    // 1. Select the column from the source DataFrame first.
    //    Cloning the Series is generally efficient due to Arc pointers.
    let column_to_add = df_source.column(valor_bc)?.clone();

    // 2. Chain operations on df_result: Rename, add column, sort
    let df_final: DataFrame = df_result
        .rename(
            valor_bc,                 // original_name
            valor_bc_auditado.into(), // new_name
        )?
        .with_column(column_to_add)?
        .sort_by_columns(Some("sort_by_columns:"))?;

    Ok(df_final)
}

/// Removes columns that consist entirely of null values from a DataFrame or LazyFrame.
pub fn remove_null_columns(frame: Frame) -> PolarsResult<DataFrame> {
    // Collect if lazy, or use the DataFrame directly.
    let df: DataFrame = match frame {
        Frame::Lazy(lz) => lz.collect()?,
        Frame::Data(df) => df,
    };

    // Keep the names of these columns:
    let pa_mes: &str = coluna(Left, "pa_mes"); // "Mês do Período de Apuração"
    let glosar: &str = coluna(Middle, "glosar"); // "Glosar Base de Cálculo de PIS/PASEP e COFINS"

    let mandatory_columns = [pa_mes, glosar];

    // Use a HashSet for efficient lookup of mandatory columns.
    let mandatory_set: HashSet<&str> = mandatory_columns.into_iter().collect();

    // Determine which columns to keep.
    let columns_to_keep: Vec<&str> = df
        .get_columns()
        .iter()
        .filter_map(|col| {
            let name = col.name().as_str();
            // Condition: Keep if it's mandatory OR if it contains any non-null value.
            if mandatory_set.contains(name) || col.is_not_null().any() {
                Some(name) // Keep this column name
            } else {
                None // Filter out this column (it's fully null and not mandatory)
            }
        })
        .collect(); // Collect the names of columns to keep

    // Select only the desired columns. This is efficient.
    df.select(columns_to_keep)
}

/// Conditionally removes fully null columns based on program arguments.
pub fn conditionally_remove_null_columns(
    data_frame: DataFrame,
    args: &Arguments,
) -> PolarsResult<DataFrame> {
    if args.remove_null_columns == Some(true) {
        remove_null_columns(Frame::Data(data_frame))
    } else {
        Ok(data_frame)
    }
}

/// Calculates Munkres assignments between two Series of f64 values.
///
/// This function expects two Series, attempts to downcast them to `Float64Type` (f64),
/// extracts the numerical values, and then applies the `munkres_assignments` algorithm.
///
/// # Arguments
/// * `series_efd` - A Series containing f64 values for the EFD items.
/// * `series_nfe` - A Series containing f64 values for the NFE items.
///
/// # Returns
/// An `Option<Series>` containing a Series of u64 assignments if successful, otherwise `None`.
pub fn get_option_assignments(series_efd: &Series, series_nfe: &Series) -> Option<Series> {
    // Attempt to downcast the Series to a ChunkedArray of Float64Type.
    let result_chunkedarray_f64_efd: Result<&ChunkedArray<Float64Type>, PolarsError> =
        series_efd.f64();
    let result_chunkedarray_f64_nfe: Result<&ChunkedArray<Float64Type>, PolarsError> =
        series_nfe.f64();

    match (result_chunkedarray_f64_efd, result_chunkedarray_f64_nfe) {
        (Ok(chunkedarray_f64_efd), Ok(chunkedarray_f64_nfe)) => {
            let vec_float64_efd: Vec<f64> = chunkedarray_f64_efd
                .iter()
                .filter_map(verbose_option) //.map_while(verbose_option)
                .collect();

            let vec_float64_nfe: Vec<f64> = chunkedarray_f64_nfe
                .iter()
                .filter_map(verbose_option)
                .collect();

            // if vec_float64_efd.len() * vec_float64_nfe.len() > 0 {

            // Perform Munkres assignment only if both vectors are not empty.
            if !vec_float64_efd.is_empty() && !vec_float64_nfe.is_empty() {
                let assignments: Vec<u64> =
                    munkres_assignments(&vec_float64_efd, &vec_float64_nfe, false);
                // Return the assignments as a new Series.
                Some(Series::new("new".into(), assignments))
            } else {
                // If either vector is empty, no assignments can be made.
                None
            }
        }
        _ => {
            eprintln!("Error: Expected Float64Type, but received different types.");
            eprintln!(
                "series_efd.dtype(): {} ; series_efd: {series_efd}",
                series_efd.dtype()
            );
            eprintln!(
                "series_nfe.dtype(): {} ; series_nfe: {series_nfe}",
                series_nfe.dtype()
            );
            None
        }
    }
}

fn verbose_option<T>(opt: Option<T>) -> Option<T> {
    match opt {
        Some(value) => Some(value),
        None => {
            let generic_type_name: &str = any::type_name::<T>();
            eprintln!("\n\tAll values are expected to be Some({generic_type_name}).");
            eprintln!("\tBut at least one value was None!");
            // panic!("Error: Option with None value!");
            None
        }
    }
}

/**
This function processes the optional Series for a single row into Option<CorrelatedLineTuples>.

It encapsulates the logic for extracting `u64` vectors and performing line assignments.
*/
pub fn get_opt_vectuples(
    opt_key: Option<&str>,
    opt_efd_series: Option<Series>,
    opt_nfe_series: Option<Series>,
    opt_asg_series: Option<Series>,
) -> Option<Vec<CorrelatedLines>> {
    // If the document key is missing, we cannot proceed for this row.
    let chave_doc = opt_key?; // Propagate None if aggregation key is missing for this row

    // Helper closure to safely extract a Vec<u64> from an Option<Series>.
    // It captures `doc_key` for informative error messages.
    let get_u64_vec_from_opt_series =
        |opt_series: Option<Series>, series_name: &str| {
            let series = opt_series
                .ok_or_else(|| {
                    let msg = format!("Missing {series_name} Series for key {chave_doc}.");
                    eprintln!("Error: {msg} Skipping row.");
                    PolarsError::ComputeError(msg.into())
                })
                .ok()?; // Propagate None if the Series itself is missing.        

            series
            .u64() // Attempt to cast Series to ChunkedArray<UInt64Type>
            .map(|ca| ca.iter().filter_map(verbose_option).collect::<Vec<u64>>()) // Collect non-None u64s
            .inspect_err(|error| {
                eprintln!("Error getting u64 ChunkedArray: {error}");
                eprintln!("Error converting {series_name} Series to u64 for key '{chave_doc}'",);
                eprintln!("Series dtype: {:?} ; Series data: {:?}", series.dtype(), series);
            })
            .ok() // Propagate None if conversion to u64 ChunkedArray fails.
        };

    // Extract all required Vec<u64> using the helper closure.
    // The '?' operator will propagate None if any extraction fails.
    let vec_u64_efd: Vec<u64> = get_u64_vec_from_opt_series(opt_efd_series, "EFD lines")?;
    let vec_u64_nfe: Vec<u64> = get_u64_vec_from_opt_series(opt_nfe_series, "NFe lines")?;
    let vec_u64_asg: Vec<u64> = get_u64_vec_from_opt_series(opt_asg_series, "Assignments")?;

    // If any of the extracted vectors are empty, it means there's no data to correlate.
    if vec_u64_efd.is_empty() || vec_u64_nfe.is_empty() || vec_u64_asg.is_empty() {
        eprintln!(
            "Warning: One or more input vectors (EFD, NFe or Assignments) are empty for key '{chave_doc}'."
        );
        eprintln!("Skipping correlation for this row.");
        return None;
    }

    line_assignments(chave_doc, &vec_u64_efd, &vec_u64_nfe, &vec_u64_asg)
}

/// Perform the line assignment correlation.
fn line_assignments(
    chave_doc: &str,
    slice_lines_efd: &[u64],
    slice_lines_nfe: &[u64],
    assignments: &[u64],
) -> Option<Vec<CorrelatedLines>> {
    let mut chaves_com_linhas_correlacionadas: Vec<CorrelatedLines> = Vec::new();

    // Iterate through the assignments.
    // `row_idx` is the index into `assignments` and conceptually into `slice_lines_efd`.
    // `col_idx` is the value from `assignments`, used as an index into `slice_lines_nfe`.
    for (row_idx, &col_idx) in assignments.iter().enumerate() {
        let opt_line_efd: Option<&u64> = slice_lines_efd.get(row_idx);
        let opt_line_nfe: Option<&u64> = slice_lines_nfe.get(col_idx as usize);

        // If both lines exist at their respective indices, form a tuple and add it.
        if let (Some(&line_efd), Some(&line_nfe)) = (opt_line_efd, opt_line_nfe) {
            let correlated_lines = CorrelatedLines {
                chave: chave_doc.to_string(),
                line_efd,
                line_nfe,
            };
            //println!("rrow_idx: {row_idx} ; correlated_lines: {correlated_lines:?}");
            chaves_com_linhas_correlacionadas.push(correlated_lines);
        } else {
            // If either index is out of bounds or the value at that index is None,
            // print a warning and skip this particular assignment.
            eprintln!(
                "Warning: Invalid index encountered for key '{chave_doc}'. 
                EFD row_idx: {row_idx}, NFe col_idx: {col_idx}. 
                Skipping this assignment.",
            );
        }
    }

    // If no correlations were found after checking all assignments, return None.
    // Otherwise, return Some with the collected correlated tuples.

    if chaves_com_linhas_correlacionadas.is_empty() {
        None
    } else {
        Some(chaves_com_linhas_correlacionadas)
    }
}

/*
#[allow(dead_code)]
async fn download(url: &str) -> Result<Vec<u8>, reqwest::Error> {
    let response = reqwest::get(url).await?;
    println!("Response: {:?} {}", response.version(), response.status());
    println!("Headers: {:#?}\n", response.headers());
    let body = response.text().await?;
    let data: Vec<u8> = body.bytes().collect();
    Ok(data)
}

// https://georgik.rocks/how-to-download-binary-file-in-rust-by-reqwest/
#[allow(dead_code)]
async fn fetch_url(url: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    let response = reqwest::get(url).await?;
    let mut file = std::fs::File::create(output_file)?;
    let mut content =  Cursor::new(response.bytes().await?);
    std::io::copy(&mut content, &mut file)?;
    println!("download '{output_file}' from '{url}'.");
    Ok(())
}

#[allow(dead_code)]
fn download_file_from_the_internet(url: &str, output_file: &str) {
    let resp = reqwest::blocking::get(url).expect("request failed");
    let body = resp.text().expect("body invalid");
    let mut out = File::create(output_file).expect("failed to create file");
    std::io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");
    println!("download '{output_file}' from '{url}'.");
}
*/

// https://docs.rs/polars
// https://pola-rs.github.io/polars/polars/prelude/struct.StrptimeOptions.html
// We recommend to build your queries directly with polars-lazy.
// This allows you to combine expression into powerful aggregations and column selections.
// All expressions are evaluated in parallel and your queries are optimized just in time.

pub fn get_lazyframe_from_csv(
    file_path: Option<PathBuf>,
    delimiter: Option<char>,
    side: Side,
) -> PolarsResult<LazyFrame> {
    validate_entries(file_path.clone(), delimiter, side)?;

    let mut options = StrptimeOptions {
        format: None,
        strict: false, // If set then polars will return an error if any date parsing fails
        exact: true, // If polars may parse matches that not contain the whole string e.g. “foo-2021-01-01-bar” could match “2021-01-01”
        cache: true, // use a cache of unique, converted dates to apply the datetime conversion.
    };

    match side {
        Side::Left => options.format = Some("%Y-%-m-%-d".into()),
        Side::Right => options.format = Some("%-d/%-m/%Y".into()),
        Side::Middle => {
            return Err(PolarsError::InvalidOperation(
                "The middle side is not valid!".into(),
            ));
        }
    }

    let replacement_expr: Expr = build_null_expression(true)?;

    // Format date
    let mut lazyframe: LazyFrame = read_csv_lazy(file_path, delimiter, side)?
        .with_columns([replacement_expr])
        .with_column(
            col("^(Período|Data|Dia).*$") // regex
                .str()
                .to_date(options),
        );

    println!("{}\n", lazyframe.clone().collect()?);

    // Print column names and their respective types
    // Iterates over the `(&name, &dtype)` pairs in this schema
    // Schema: a map from column names to data types
    lazyframe
        .collect_schema()?
        .iter()
        .enumerate()
        .for_each(|(index, (column_name, data_type))| {
            println!(
                "column {:02}: (\"{column_name}\", DataType::{data_type}),",
                index + 1
            );
        });

    println!();

    // println!("teste dataframe: {:#?}", lazyframe.clone().collect()?);

    Ok(lazyframe)
}

/// Define values to be interpreted as null across all columns.
pub static NULL_VALUES: [&str; 3] = [
    "",           // Represents empty strings --> null
    "<N/D>",      // Specific placeholder string 1
    "*DIVERSOS*", // Specific placeholder string 2
];

/// Builds a Polars Expression to replace specified string values (after trimming)
/// with NULL within selected columns of a DataFrame.
///
/// Values are replaced if they match any string in the hardcoded list
/// `null_value_list: Vec<&str>` after trimming leading/trailing whitespace.
///
pub fn build_null_expression(apply_to_all_columns: bool) -> PolarsResult<Expr> {
    // Create a Polars Series containing the *strings* to be treated as null markers.
    let series = Series::new("null_vals".into(), NULL_VALUES);
    let literal_series: Expr = series.to_list_expr()?;

    // --- Define Replacement Logic based on the flag ---
    let replacement_expr: Expr = if apply_to_all_columns {
        // Universal Mode: Apply to ALL columns via casting and trimming string representation
        let condition = all() // Select current column value
            .as_expr()
            .cast(DataType::String) // Cast to String
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from string representation
            .is_in(literal_series, true); // Check if trimmed string is in the list

        when(condition) // WHEN the trimmed string representation matches...
            .then(lit(NULL)) // THEN replace original value with NULL
            .otherwise(all()) // OTHERWISE keep the original value
            .name()
            .keep() // Keep original column name
    } else {
        // String-Only Mode: Apply only to String columns, trim original string
        let string_cols_selector = dtype_col(&DataType::String).as_selector().as_expr();

        let condition = string_cols_selector // Select only string columns
            .clone() // Clone needed for use in `otherwise`
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from the original string value
            .is_in(literal_series, true); // Check if trimmed string is in the list

        when(condition) // WHEN the trimmed string matches...
            // THEN replace with NULL (cast needed for type consistency within String col expr)
            .then(lit(NULL).cast(DataType::String))
            // OTHERWISE keep the original string value
            .otherwise(string_cols_selector)
            .name()
            .keep() // Keep original column name
    };

    Ok(replacement_expr)
}

/// If valid, print the variables (file_path, delimiter, side).
fn validate_entries(
    file_path: Option<PathBuf>,
    delimiter: Option<char>,
    side: Side,
) -> PolarsResult<()> {
    match file_path {
        Some(p) if p.is_file() => println!("file path: {p:#?}"),
        _ => {
            eprintln!("fn validate_entries()");
            eprintln!("file_path: {file_path:?}");
            return Err(PolarsError::InvalidOperation("file_path error!".into()));
        }
    };

    match delimiter {
        Some(d) => println!("delimiter: {d:?}"),
        None => {
            eprintln!("fn validate_entries()");
            eprintln!("delimiter: {delimiter:?}");
            return Err(PolarsError::InvalidOperation("delimiter error!".into()));
        }
    };

    match side {
        Side::Left | Side::Right => println!("side: {side:?}"),
        Side::Middle => {
            eprintln!("fn validate_entries()");
            eprintln!("side: {side:?}");
            return Err(PolarsError::InvalidOperation(
                "The middle side is not valid!".into(),
            ));
        }
    };

    Ok(())
}

/// Helper function to modify the schema obtained from CSV headers.
/// It applies data types from a predefined map and defaults to String for others.
/// Receives schema by value, applies modifications, and returns a new schema result.
fn apply_custom_schema_rules(
    schema: Schema, // Input schema (passed by value, inferred from CSV headers)
    cols_dtype_map: &HashMap<&'static str, DataType>, // Map of custom types
    side: Side,     // Contextual side information
) -> PolarsResult<Schema> {
    // Build a new list of fields based on our desired types, preserving original order.
    let mut modified_fields: Vec<Field> = Vec::with_capacity(schema.len());

    // Iterate through the fields found in the CSV header (order is preserved)
    for field in schema.iter_fields() {
        let col_name = field.name().as_str();
        match cols_dtype_map.get(col_name) {
            Some(desired_dtype) => {
                // If the column is in our map, use the specified type.
                modified_fields.push(Field::new(col_name.into(), desired_dtype.clone()));
            }
            None => {
                // If not in map, keep it as String and warn the user.
                eprintln!("Insert DataType for column '{col_name}' in Column {side:?}!");
                modified_fields.push(Field::new(col_name.into(), DataType::String));
                // Ensure String
            }
        }
    }

    // Create and return the new schema.
    // Using `Schema::from_iter` is clean as we built a new Vec<Field>.
    let new_schema = Schema::from_iter(modified_fields);

    Ok(new_schema) // Return the new/modified schema wrapped in Result
}

/// Function to read a CSV file into a lazy frame using Polars.
/// Configures reading using schema modification based on a user-provided map.
fn read_csv_lazy(
    file_path: Option<PathBuf>, // Optional path to the CSV file
    delimiter: Option<char>,    // Optional delimiter character
    side: Side,                 // Custom parameter (e.g., determines schema)
) -> PolarsResult<LazyFrame> {
    match (&file_path, delimiter) {
        (Some(path), Some(separator)) => {
            // Get the expected column names and their data types BEFORE the closure.
            // Use Arc to efficiently share the map ownership with the 'move' closure.
            let cols_dtype: HashMap<&str, DataType> = MyColumn::get_cols_dtype(side);

            let plpath = PlPath::Local(path.clone().into());

            // Create a LazyCsvReader to process the file lazily.
            let result_lazyframe: PolarsResult<LazyFrame> =
                LazyCsvReader::new(plpath) // Start lazy reader for the given path
                    .with_encoding(CsvEncoding::LossyUtf8) // Specify UTF-8 encoding with lossy conversion
                    .with_try_parse_dates(false) // Disable automatic date parsing during initial read
                    .with_separator(separator as u8) // Set the column delimiter
                    .with_quote_char(Some(b'"')) // Set the quote character (default)
                    .with_has_header(true) // Indicate the CSV file has a header row
                    .with_ignore_errors(true) // Continue reading even if parsing errors occur
                    //.with_null_values(Some(NullValues::AllColumns(null_values))) // Apply the predefined null values list
                    .with_null_values(None) // Apply fn build_null_expression()
                    .with_missing_is_null(true) // Treat missing fields as null
                    // Infer schema length 0 reads only headers. Polars gets column names.
                    .with_infer_schema_length(Some(0))
                    // Modify the schema using the separate helper function.
                    // The closure's role is now just to bridge from the Polars API signature
                    // to the helper function signature, passing the captured data.
                    .with_schema_modify(|schema: Schema| {
                        apply_custom_schema_rules(schema, &cols_dtype, side)
                    })? // Add the '?' here to unwrap the result of with_schema_modify
                    .with_rechunk(true) // Optional rechunking step
                    .finish(); // Finalize configuration and get the LazyFrame

            // Print error if creating the LazyFrame failed during finish().
            if result_lazyframe.is_err() {
                eprintln!("\nError: Failed to finish lazy reader setup for file {path:#?}");
            }

            result_lazyframe // Return the LazyFrame result
        }
        // Handle cases where file path or delimiter is missing.
        _ => {
            eprintln!("File path: {file_path:#?}"); // Debug output
            eprintln!("Delimiter: {delimiter:#?}"); // Debug output
            panic!("File path or delimiter error!"); // Panic as essential configuration is missing.
        }
    }
}

/// Write Dataframe to CSV file
pub fn write_csv(df: &DataFrame, basename: &str, delimiter: char) -> PolarsResult<()> {
    let mut filepath = PathBuf::from(basename);
    filepath.set_extension("csv");
    println!("Write DataFrame to {filepath:?}\n");

    let mut df_formated: DataFrame = format_dataframe(df)?;
    println!("{df_formated}\n");

    let mut output_csv: File = File::create(filepath)?;

    CsvWriter::new(&mut output_csv)
        .with_separator(delimiter as u8)
        .include_header(true)
        .with_quote_style(QuoteStyle::Necessary)
        .finish(&mut df_formated)?;

    Ok(())
}

/// Format CSV file
///
/// Substituir código por sua descrição nas colunas selecionadas.
fn format_dataframe(data_frame: &DataFrame) -> PolarsResult<DataFrame> {
    // Column names:
    let natureza: &str = coluna(Left, "natureza");
    let pa_mes: &str = coluna(Left, "pa_mes");
    let tipo_operacao: &str = coluna(Left, "tipo_operacao");
    let tipo_cred: &str = coluna(Left, "tipo_cred");
    let origem: &str = coluna(Left, "origem");

    // 1. Get the names of columns currently present in the DataFrame for quick lookup.
    let current_columns: HashSet<PlSmallStr> =
        data_frame.get_column_names_owned().into_iter().collect();

    let columns_origem: Vec<&str> = vec![origem];

    // 2. Filter the target list to include only columns that *actually exist*
    //    in the current DataFrame.
    // Verificar a existência da coluna "Indicador de Origem" antes aplicar alterações.
    let columns_to_transform: Vec<&str> = columns_origem
        .into_iter()
        .filter(|&col| current_columns.contains(col))
        .collect();

    data_frame
        .clone()
        .lazy()
        .with_column(col(pa_mes).apply(
            descricao_do_mes,
            // GetOutput::from_type(DataType::String)
            |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
        ))
        .with_column(col(tipo_operacao).apply(
            descricao_do_tipo_de_operacao,
            // GetOutput::from_type(DataType::String),
            |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
        ))
        .with_column(col(tipo_cred).apply(
            descricao_do_tipo_de_credito,
            // GetOutput::from_type(DataType::String),
            |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
        ))
        .with_column(col(natureza).apply(
            descricao_da_natureza_da_bc_dos_creditos,
            // GetOutput::from_type(DataType::String),
            |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
        ))
        .with_columns([
            // Apply cast only to the intersection of target and existing columns
            cols(columns_to_transform).as_expr().apply(
                descricao_da_origem,
                // GetOutput::from_type(DataType::String)
                |_, f| Ok(Field::new(f.name().clone(), DataType::String)),
            ),
        ])
        .collect()
}

/// Write Dataframe to Parquet file
pub fn write_pqt(df: &DataFrame, basename: &str) -> PolarsResult<()> {
    let mut filepath = PathBuf::from(basename);
    filepath.set_extension("parquet");
    println!("Write DataFrame to {filepath:?}\n");

    let mut df_formated = format_dataframe(df)?;

    let mut output_parquet: File = File::create(filepath)?;

    ParquetWriter::new(&mut output_parquet)
        .with_statistics(StatisticsOptions::default())
        .set_parallel(true)
        //.with_compression(ParquetCompression::Lz4Raw)
        .finish(&mut df_formated)?;

    Ok(())
}

/// Obter o CNPJ Base a partir do CNPJ.
///
/// #### Exemplo fictício:
///
/// Se CNPJ: `12.345.678/0009-23`, então CNPJ Base: `12.345.678`.
pub fn get_cnpj_base(col: Column) -> PolarsResult<Column> {
    match col.dtype() {
        DataType::String => cnpj_base(col),
        _ => {
            eprintln!("fn get_cnpj_base()");
            eprintln!("Polars Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!("Not supported for Series with DataType {:?}", col.dtype()).into(),
            ))
        }
    }
}

/// Obter CNPJ Base
///
/// Exemplos com CNPJs fictícios:
///
/// `12.345.678/0009-23` -> `12.345.678`
///
/// `<N/D> [Info do CT-e: 12.345.678/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12.345.678/0009-23]` -> `12.345.678`
fn cnpj_base(col: Column) -> PolarsResult<Column> {
    let new_col: Column = col
        .str()?
        .iter()
        .map(|option_str: Option<&str>| {
            option_str.and_then(|text| {
                let mut cnpjs: Vec<String> = extract_cnpjs(text);

                cnpjs.sort_unstable();
                cnpjs.dedup(); // Removes consecutive repeated elements

                // Capturar apenas CNPJ base iguais
                // Capturar apenas o primeiro CNPJ

                if cnpjs.len() == 1 {
                    cnpjs.first().cloned()
                } else {
                    None
                }
            })
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(new_col)
}

pub fn add_leading_zeros(series: Series, fill: usize) -> PolarsResult<Option<Series>> {
    match series.dtype() {
        DataType::Int64 => leading_zeros(series, fill),
        _ => {
            eprintln!("fn add_leading_zeros()");
            eprintln!("Series: {series:?}");
            eprintln!("Leading Zeroes: {fill}");
            Err(PolarsError::InvalidOperation(
                format!(
                    "Not supported for Series with DataType {:?}",
                    series.dtype()
                )
                .into(),
            ))
        }
    }
}

fn leading_zeros(series: Series, fill: usize) -> PolarsResult<Option<Series>> {
    let new_series: Series = series
        .i64()?
        .iter()
        .map(|option_i64: Option<i64>| option_i64.map(|int64| format!("{int64:0fill$}")))
        .collect::<StringChunked>()
        .into_series();

    Ok(Some(new_series))
}

/// NCM format: "12345678" --> "1234.56.78"
pub fn formatar_ncm(col: Column) -> PolarsResult<Column> {
    let new_col: Column = col
        .str()?
        .iter()
        .map(|option_str| option_str.map(extract_ncm))
        .collect::<StringChunked>()
        .into_column();

    Ok(new_col)
}

pub fn formatar_chave_eletronica(col: Column) -> PolarsResult<Column> {
    match col.dtype() {
        DataType::String => format_digits(col),
        _ => {
            eprintln!("fn formatar_chave_eletronica()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!("Not supported for Series with DataType {:?}", col.dtype()).into(),
            ))
        }
    }
}

// https://docs.rs/polars/latest/polars/prelude/string/struct.StringNameSpace.html#
fn format_digits(col: Column) -> PolarsResult<Column> {
    let new_col: Column = col
        .str()?
        .iter()
        .map(retain_only_digits)
        .collect::<StringChunked>()
        .into_column();

    Ok(new_col)
}

/// We use the as_ref method to get a reference to the optional string (opt_str).
///
/// Then, we use the map method to transform the optional string into an optional string
/// containing only the ASCII digit characters.
fn retain_only_digits(opt_str: Option<&str>) -> Option<String> {
    opt_str.as_ref().and_then(|string| {
        let digits: String = string
            .chars()
            .filter(|c| c.is_ascii_digit())
            .collect::<String>();

        if !digits.is_empty() {
            Some(digits)
        } else {
            None
        }
    })
}

/**
Regex:

^     the beginning of text (or start-of-line with multi-line mode)

$     the end of text (or end-of-line with multi-line mode)

\A    only the beginning of text (even with multi-line mode enabled)

\z    only the end of text (even with multi-line mode enabled)

\b    a Unicode word boundary (\w on one side and \W, \A, or \z on other)

\B    not a Unicode word boundary

\d, \D: ANY ONE digit/non-digit character. Digits are [0-9]

\w, \W: ANY ONE word/non-word character. For ASCII, word characters are [a-zA-Z0-9_]

(?:exp) non-capturing group

*/
pub fn extract_cnpjs(input: &str) -> Vec<String> {
    // Define a static Regex to avoid recompiling the regex on every call.
    static FIND_CNPJS: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?x)
            (?:\A|\W) # beginning of text or not word ; or (?:^|\W)
            (\w{2})   # capture 2 alphanumeric
            \.?
            (\w{3})   # capture 3 alphanumeric
            \.?
            (\w{3})   # capture 3 alphanumeric
            \/?
            \w{4}     # check 4 alphanumeric
            -?
            \d{2}     # check 2 digits
            (?:\z|\W) # end of text or not word ; or (?:$|\W)
        ",
        )
        .expect("fn extract_cnpjs()\nFailed to compile regex") // Panic if regex compilation fails.
    });

    /*
    FIND_CNPJS
        .captures_iter(input)
        .map(|caps| caps.extract())
        .map(|(_full, [a, b, c])| [a, ".", b, ".", c].concat())
        .collect()
    */

    FIND_CNPJS
        .captures_iter(input)
        .filter_map(|caps| {
            // Extract the captured groups.
            // Using ? for early return if any capture fails.
            let part1 = caps.get(1)?.as_str();
            let part2 = caps.get(2)?.as_str();
            let part3 = caps.get(3)?.as_str();

            // Construct the CNPJ string.
            let mut cnpj = String::new();
            write!(&mut cnpj, "{part1}.{part2}.{part3}").ok()?;
            Some(cnpj) // Return the constructed CNPJ.
        })
        .collect()
}

/// Extracts the first NCM (Nomenclatura Comum do Mercosul) code found in a given string.
/// If no valid NCM is found, returns the original input string.
///
/// The function searches for an NCM code using a regular expression and returns it
/// as a string. If no NCM code is found, it returns the original input string.
///
/// ### Arguments
///
/// * `input` - The string to search for the NCM code.
///
/// ### Returns
///
/// A string containing the extracted NCM code, or the original input string if no NCM is found.
pub fn extract_ncm(input: &str) -> String {
    // Define a static Regex to avoid recompiling the regex on every call.
    static FIND_NCM: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?x)
            (?:\A|\D) # beginning of text or not digit; Ensures the match is not preceded by a digit.
            (\d{3,4}) # capture 3 or 4 digits (first part of NCM)
            \.?       # optional dot
            (\d{2})   # capture 2 digits (second part of NCM)
            \.?       # optional dot
            (\d{2})   # capture 2 digits (third part of NCM)
            (?:\z|\D) # end of text or not digit; Ensures the match is not followed by a digit.
        ",
        )
        .expect("fn extract_ncm()\nFailed to compile regex") // Panic if regex compilation fails.
    });

    FIND_NCM
        .captures_iter(input)
        .filter_map(|caps| {
            // Extract the captured groups.
            // Using ? for early return if any capture fails.
            let part1 = caps.get(1)?.as_str();
            let part2 = caps.get(2)?.as_str();
            let part3 = caps.get(3)?.as_str();

            // Construct the NCM string.
            let mut ncm = String::new();
            if part1.len() == 3 {
                // Add a leading zero if the first part has 3 digits.
                write!(&mut ncm, "0{part1}.{part2}.{part3}").ok()?;
            } else {
                // Otherwise, use the first part as is.
                write!(&mut ncm, "{part1}.{part2}.{part3}").ok()?;
            }
            Some(ncm) // Return the constructed NCM.
        })
        .next() // Take only the first match.
        .unwrap_or_else(|| input.to_string()) // Return the original input if no match is found.
}

pub fn formatar_lista_de_datas(coluna: Column) -> PolarsResult<Column> {
    // println!("coluna: {coluna:?}");

    // Crie um iterador e aplique a formatação
    let str_date: PolarsResult<Vec<Option<String>>> = coluna
        .list()?
        .into_iter()
        .map(|opt_series| {
            match opt_series {
                Some(list_series) => {
                    // Se a lista não é nula, formate cada data e junte com um espaço
                    let dates_as_strings: Vec<String> = list_series
                        .date()? // Retorna um PolarsResult<DateChunked>
                        .as_date_iter() // Retorna um iterator de Option<NaiveDate>
                        .flatten() // Remove os Nones do iterador, deixando apenas NaiveDate
                        .map(|date: NaiveDate| date.format("%d/%m/%Y").to_string())
                        .collect();
                    let string = format!("[{}]", dates_as_strings.join(", "));
                    Ok(Some(string)) // Retorna Some(String) para esta linha
                }
                None => Ok(None), // Se a Series original era null, a nova Series também será null
            }
        })
        .collect();

    let new_col = Column::new("new".into(), str_date?);
    // println!("new_col: {new_col:?}");

    Ok(new_col)
}

pub fn quit() {
    std::process::exit(0);
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_functions
#[cfg(test)]
mod tests_functions {
    use super::*;
    use std::error::Error;

    // cargo test -- --help
    // cargo test -- --show-output
    // cargo test -- --show-output multiple_values

    #[test]
    /// `cargo test -- --show-output find_cnpj_base`
    fn find_cnpj_base() -> PolarsResult<()> {
        let mut result = Vec::new();

        // Exemplo com CNPJ fictício

        let text_0 = "12.345.678/0009-23";

        let text_1 = "<N/D> [Info do CT-e: 12.ABC.678/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12.ABC.679/0009-66] [Info do CT-e: 12.ABC.678/0009-23]";

        let text_2 = "<N/D> [Info do CT-e: 12.345.CDE/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12345CDE/1234-88] [Info do CT-e: 12345CDE901234] 12345CDE9012345";

        let text_3 = "02.345.678/12345-12 123456781234123 foo 012.345.678/1234-23";

        let option_strs = [Some(text_0), Some(text_1), Some(text_2), Some(text_3), None];

        for (index, option_str) in option_strs.iter().enumerate() {
            println!("text_{index}: {option_str:?}");

            let cnpj_base: Option<String> = option_str.and_then(|text| {
                let mut cnpjs: Vec<String> = extract_cnpjs(text);

                println!("cnpjs: {cnpjs:?}");

                cnpjs.sort_unstable();
                cnpjs.dedup(); // Removes consecutive repeated elements

                println!("cnpjs uniques: {cnpjs:?}");

                // Capturar apenas CNPJ base iguais
                // Capturar apenas o primeiro CNPJ

                if cnpjs.len() == 1 {
                    cnpjs.first().cloned()
                } else {
                    None
                }
            });

            println!("cnpj_base: {cnpj_base:?}\n");

            result.push(cnpj_base);
        }

        let valid: Vec<Option<String>> = vec![
            Some("12.345.678".to_string()),
            None,
            Some("12.345.CDE".to_string()),
            None,
            None,
        ];

        assert_eq!(valid, result);

        let series: Series = Series::new("CNPJ do Remetente".into(), &option_strs);

        println!("series: {series:?}\n");

        let col = get_cnpj_base(series.into())?;

        println!("column: {col:?}\n");

        assert_eq!(Column::new("".into(), &valid), col);

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output formatar_datas`
    fn formatar_datas() -> PolarsResult<()> {
        let datas1 = vec![
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 5, 10).unwrap(),
        ];
        let datas2 = vec![NaiveDate::from_ymd_opt(2024, 9, 15).unwrap()];

        let series_datas1 = Series::new("row1".into(), datas1);
        let series_datas2 = Series::new("row2".into(), datas2);

        // 1. Crie o conteúdo da Series como uma estrutura serializável.
        let datas = vec![Some(series_datas1), Some(series_datas2), None];

        // 2. Crie a Series explicitamente, com um nome e o conteúdo.
        let series_list_dates = Series::new("Períodos de Apuração".into(), datas);

        // 3. Crie o DataFrame usando a Series criada.
        // let df = DataFrame::new(vec![series_list_dates.into()])?;
        let df = df! { "Períodos de Apuração" => series_list_dates }?;

        println!("DataFrame Original:\n{}", df);

        // use .eval()
        let df_formatado_eval = df
            .clone()
            .lazy()
            /*
            .with_column(
                col("Períodos de Apuração")
                    .list()
                    .eval(col("").dt().strftime("%d/%m/%Y"))
                    .list()
                    .join(lit(", "), true)
                    .map(
                        |col| {
                            let strings: Vec<Option<String>> = col
                                .str()?
                                .iter()
                                .map(|opt_str| opt_str.map(|s| format!("[{s}]")))
                                .collect();
                            Ok(Column::new("".into(), strings))
                        },
                        get_output_as_string,
                    )
                    .alias("Períodos Formatados"),
            )
            */
            .with_column(
                when(col("Períodos de Apuração").is_null()) // Condição: Se a coluna original é null
                    .then(lit(NULL)) // Então, a saída é NULL
                    .otherwise(
                        // Caso contrário (se não é null), execute toda a lógica de formatação
                        concat_str(
                            [
                                lit("["),
                                col("Períodos de Apuração")
                                    .list()
                                    .eval(col("").dt().strftime("%d/%m/%Y"))
                                    .list()
                                    .join(lit(", "), true)
                                    .alias("Períodos Formatados"),
                                lit("]"),
                            ],
                            "",
                            true,
                        ),
                    )
                    .alias("Períodos Formatados"),
            )
            .collect()?;

        println!("\nDataFrame Formatado com eval:\n{}", df_formatado_eval);

        // use .map()
        let df_formatado_map = df
            .lazy()
            .with_column(
                col("Períodos de Apuração")
                    .map(formatar_lista_de_datas, get_output_as_date)
                    .alias("Períodos Formatados"),
            )
            .collect()?;

        println!("\nDataFrame Formatado com map:\n{}", df_formatado_map);

        let datas = vec![Some("[01/01/2023, 10/05/2023]"), Some("[15/09/2024]"), None];
        let coluna_formatada = Column::new("fmt".into(), datas);

        assert_eq!(df_formatado_eval["Períodos Formatados"], coluna_formatada);
        assert_eq!(df_formatado_map["Períodos Formatados"], coluna_formatada);

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output test_extract_ncm`
    fn test_extract_ncm() {
        let text1 = "1234.56.78";
        let text2 = "1234567";
        let text3 = "123456"; // return the original input
        let text4 = "NCM 0912.3456";
        let text5 = "Invalid: 23.45.67"; // return the original input
        let text6 = "Multiple: 1234.5678 and 9012.34.56";
        let text7 = "<N/D>"; // return the original input

        assert_eq!(extract_ncm(text1), "1234.56.78");
        assert_eq!(extract_ncm(text2), "0123.45.67");
        assert_eq!(extract_ncm(text3), text3);
        assert_eq!(extract_ncm(text4), "0912.34.56");
        assert_eq!(extract_ncm(text5), text5);
        assert_eq!(extract_ncm(text6), "1234.56.78"); // Only the *first* NCM is extracted.
        assert_eq!(extract_ncm(text7), text7);
    }

    #[test]
    /// `cargo test -- --show-output split_string_by_index`
    fn split_string_by_index() {
        let cnpj = "ඞ12.グ345.678/0009-2/3";

        let result = match cnpj.find('/') {
            Some(index) => &cnpj[..index],
            None => cnpj,
        };

        let valid = "ඞ12.グ345.678";

        println!("cnpj: {cnpj}");
        println!("result: {result}");

        assert_eq!(valid, result);
    }

    #[test]
    /// `cargo test -- --show-output collect_values_into_vec`
    fn collect_values_into_vec() -> Result<(), Box<dyn Error>> {
        // https://stackoverflow.com/questions/71376935/how-to-get-a-vec-from-polars-series-or-chunkedarray

        let series = Series::new("a".into(), 0..10i32);
        println!("series: {series}");

        let vec_opt_i32: Vec<Option<i32>> = series.i32()?.iter().collect();
        println!("vec_opt_i32: {vec_opt_i32:?}");

        // if we are certain we don't have missing values
        //let vec_i32: Vec<i32> = s.i32()?.into_no_null_iter().collect();

        assert_eq!(vec_opt_i32[9], Some(9));

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output test_get_option_assignments`
    fn test_get_option_assignments() -> Result<(), Box<dyn Error>> {
        let mut results = Vec::new();

        for (index, (vec_efd, vec_nfe)) in [
            (
                vec![Some(1.4), Some(0.0), Some(23.1), Some(3.5), Some(5.7)],
                vec![Some(1.3), Some(0.2), Some(22.1), Some(4.2), Some(5.8)],
            ),
            (
                vec![Some(1.4), Some(0.0), Some(23.1), None, Some(5.7)],
                vec![Some(1.3), Some(0.2), Some(22.1), Some(4.2), Some(5.8)],
            ),
        ]
        .iter()
        .enumerate()
        {
            println!("Example {}:", index + 1);

            println!("vec_efd: {vec_efd:?}");
            let series_efd = Series::new("efd".into(), vec_efd);
            println!("series_efd: {series_efd}");

            println!("vec_nfe: {vec_nfe:?}");
            let series_nfe = Series::new("efd".into(), vec_nfe);
            println!("series_nfe: {series_nfe}");

            // fn get_option_assignments(series_efd: Series, series_nfe: Series) -> Option<Series>

            if let Some(assignments) = get_option_assignments(&series_efd, &series_nfe) {
                // println!("assignments: {assignments}");
                let result: Vec<u64> = assignments.u64()?.iter().flatten().collect();
                results.push(result);
            }

            println!();
        }

        let valid = vec![vec![0, 1, 2, 3, 4], vec![0, 1, 2, 4, 3]];

        assert_eq!(valid, results);

        Ok(())
    }
}

#[cfg(test)]
mod tests_remove_null_columns {
    use super::*;
    use std::error::Error;

    #[test]
    /// `cargo test -- --show-output teste_remove_null_columns`
    fn teste_remove_null_columns() -> Result<(), Box<dyn Error>> {
        let dataframe: DataFrame = df!(
            "integers"  => &[1, 2, 3, 4],
            "options A" => [None::<u32>, None, None, None],
            "float64"   => [23.654, 0.319, 10.0049, -3.41501],
            "options B" => [None::<u32>, None, None, None],
            "options C" => [Some(28), Some(300), None, Some(2)],
            "options D" => [None::<u32>, None, None, None],
        )?;

        println!("dataframe: {dataframe}\n");

        let df_clean: DataFrame = remove_null_columns(Frame::Data(dataframe))?;

        println!("df_clean: {df_clean}\n");

        assert_eq!(
            df_clean,
            df!(
                "integers"  => &[1, 2, 3, 4],
                "float64"   => [23.654, 0.319, 10.0049, -3.41501],
                "options C" => [Some(28), Some(300), None, Some(2)],
            )?
        );

        Ok(())
    }
}

/// Run tests with:
/// cargo test -- --show-output tests_read_csv
#[cfg(test)]
mod tests_read_csv {
    use super::*;
    use std::fs::File;
    use std::io::Write;
    use tempfile::tempdir;

    fn create_csv(dir: &std::path::Path, filename: &str, contents: &str) -> MyResult<PathBuf> {
        let file_path = dir.join(filename);
        let mut file = File::create(&file_path)?;
        file.write_all(contents.as_bytes())?;
        Ok(file_path)
    }

    #[test]
    fn test_read_csv_lazy_schema_modify() -> MyResult<()> {
        configure_the_environment();
        let dir = tempdir()?;
        let csv_content = "Linhas,Registro,Valor Total do Item,unknown_csv_col,extra_col_defined_in_map\n\
             10,hello,1.1,u1,true\n\
             <N/D>, world ,2.2,u2,false\n\
             30,test,*DIVERSOS*,u3,"; // Note: empty field for extra_col
        let file_path = create_csv(dir.path(), "test_modify.csv", csv_content)?;
        let lazy_frame = read_csv_lazy(Some(file_path), Some(','), Side::Left)?;
        let df_output = lazy_frame.collect()?;

        println!("df_output:\n{df_output}");

        let df_expected = df! {
            "Linhas" => &[Some(10u64), None, Some(30u64)], // UInt64 with null
            "Registro" => &[Some("hello"), Some(" world "), Some("test")], // String, not trimmed
            "Valor Total do Item" => &[Some(1.1f64), Some(2.2f64), None], // Float64 with null
            "unknown_csv_col" => &[Some("u1"), Some("u2"), Some("u3")] as &[Option<&str>], // String (default type), not null
            "extra_col_defined_in_map" => &[Some("true"), Some("false"), None], // String, with null for empty string ""
        }?;

        println!("df_expected:\n{df_expected}");

        // --- Compare the output DataFrame with the expected DataFrame ---
        // assert_eq! uses the PartialEq implementation for DataFrame,
        // which compares schema, shape, and cell values (including nulls).
        assert_eq!(
            df_output, df_expected,
            "DataFrame mismatch after schema modify and null handling"
        );

        assert_eq!(
            df_output.schema(),
            df_expected.schema(),
            "DataFrame mismatch schema"
        );

        Ok(())
    }

    #[test]
    fn test_read_csv_lazy_empty_fields() -> MyResult<()> {
        configure_the_environment();
        let dir = tempdir()?;
        let csv_content = "Linhas,Registro,Valor Total do Item\n10,,1.1\n,hello,2.2\n30, world";
        let file_path = create_csv(dir.path(), "empty_fields.csv", csv_content)?;
        let lazy_frame = read_csv_lazy(Some(file_path), Some(','), Side::Left)?;
        let df_output = lazy_frame.collect()?;

        println!("df_output:\n{df_output}");

        let df_expected = df! {
            "Linhas" => &[Some(10u64), None, Some(30u64)], // Int64 with null
            "Registro" => &[None, Some("hello"), Some(" world")], // String, not trimmed
            "Valor Total do Item" => &[Some(1.1f64), Some(2.2f64), None], // Float64 with null
        }?;

        println!("df_expected:\n{df_expected}");

        assert_eq!(
            df_output, df_expected,
            "DataFrame mismatch after schema modify and null handling"
        );

        assert_eq!(
            df_output.schema(),
            df_expected.schema(),
            "DataFrame mismatch schema"
        );

        Ok(())
    }

    // Include the panic tests as well
    #[test]
    #[should_panic(expected = "File path or delimiter error!")]
    fn test_read_csv_lazy_panic_on_missing_args() {
        let _ = read_csv_lazy(None, Some(','), Side::Left).unwrap();
    }

    #[test]
    #[should_panic(expected = "File path or delimiter error!")]
    fn test_read_csv_lazy_panic_on_missing_delimiter() {
        let dir = tempdir().unwrap();
        let file_path = create_csv(dir.path(), "dummy.csv", "a,b\n1,2").unwrap();
        let _ = read_csv_lazy(Some(file_path), None, Side::Left).unwrap();
    }
}

/// Run tests with:
/// cargo test -- --show-output tests_replace_values_with_null
#[cfg(test)]
mod tests_replace_values_with_null {
    use super::*;
    use polars::functions::concat_df_horizontal;

    #[test]
    fn test_remove_leading_and_trailing_chars() -> MyResult<()> {
        configure_the_environment();

        let df_input = df! {
            "foo" => &["", " ", "hello ", " <N/D> ", " *DIVERSOS* \n ", " world", " \n\r *DIVERSOS* \n ", "<N/D>"],
        }?;

        println!("df_input: {df_input}");

        // Create a Polars Series containing the *strings* to be treated as null markers.
        let series = Series::new("null_vals".into(), NULL_VALUES);
        let literal_series: Expr = series.to_list_expr()?;

        let condition = all() // Select current column value
            .as_expr()
            .cast(DataType::String) // Cast to String
            .str()
            .strip_chars(lit(NULL)) // Trim whitespace from string representation
            .is_in(literal_series, true); // Check if trimmed string is in the list
        println!("condition: {condition}");

        let replacement_expr: Expr = build_null_expression(true)?;
        println!("replacement_expr: {replacement_expr}");

        let mut df_temp = df_input
            .clone()
            .lazy()
            .with_columns([condition.alias("other name"), replacement_expr]) // Apply the selected expression
            .collect()?;
        df_temp.set_column_names(["foo_stripped", "is_in condition"])?;

        // Concat DataFrames horizontally.
        // let df_output = df_input.hstack(df_temp.get_columns())?;
        let df_output = concat_df_horizontal(&[df_input, df_temp], true)?;

        println!("df_output: {df_output}");

        let vec_from_series: Vec<&str> = df_output["foo_stripped"]
            .str()?
            .iter() // Iterator over Option<&str>
            .map(|opt_str| opt_str.unwrap_or("null"))
            .collect();

        println!("vec_from_series: {vec_from_series:?}");

        let vec_from_series: Vec<Option<&str>> = df_output
            .column("foo_stripped")?
            .str()?
            .iter() // Iterator over Option<&str>
            .collect();

        println!("vec_from_series: {vec_from_series:?}");

        let df_expected = df! {
            "foo" => &["", " ", "hello ", " <N/D> ", " *DIVERSOS* \n ", " world", " \n\r *DIVERSOS* \n ", "<N/D>"],
            "foo_stripped" => &[None, None, Some("hello "), None, None, Some(" world"), None, None],
            "is_in condition" => &[true, true, false, true, true, false, true, true],
        }?;

        assert_eq!(
            df_output, df_expected,
            "DataFrame mismatch after schema modify and null handling"
        );

        assert_eq!(
            df_output.schema(),
            df_expected.schema(),
            "DataFrame mismatch schema"
        );

        Ok(())
    }
}
