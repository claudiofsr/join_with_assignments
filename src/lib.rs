mod analise_do_periodo_de_apuracao;
mod args;
mod columns;
mod consolidacao_da_natureza;
mod descricoes;
mod error;
mod excel;
mod filtros;
mod glosar_base_de_calculo;
mod legislacao_aliquota_zero;
mod legislacao_credito_presumido;
mod legislacao_incidencia_monofasica;
mod munkres;
mod polars_assignments;
mod regimes_fiscais;
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
    error::{JoinError, JoinResult},
    excel::*,
    filtros::*,
    glosar_base_de_calculo::glosar_bc,
    munkres::{munkres_assignments, try_convert},
    polars_assignments::get_dataframe_after_assignments,
    regimes_fiscais::{
        adicionar_coluna_de_aliquota_zero, adicionar_coluna_de_credito_presumido,
        adicionar_coluna_de_incidencia_monofasica,
    },
    traits::{
        DataFrameExtension, ExprExtension, FloatIterExtension, LazyFrameExtension,
        ToLiteralListExpr,
    },
    write::ExcelWriter,
    xlsx_writer::PolarsXlsxWriter,
};

use polars::prelude::*;
use std::{
    any,
    collections::{HashMap, HashSet},
    env,
    fs::File,
    path::PathBuf,
};
use sysinfo::System;

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
                    munkres_assignments(&vec_float64_efd, &vec_float64_nfe, false).ok()?;
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
) -> JoinResult<LazyFrame> {
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
            return Err(JoinError::InvalidSide(side.to_string()));
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
) -> JoinResult<LazyFrame> {
    match (&file_path, delimiter) {
        (Some(path), Some(separator)) => {
            // Get the expected column names and their data types BEFORE the closure.
            // Use Arc to efficiently share the map ownership with the 'move' closure.
            let cols_dtype: HashMap<&str, DataType> = MyColumn::get_cols_dtype(side);

            let plpath = PlPath::Local(path.clone().into());

            // Create a LazyCsvReader to process the file lazily.
            let result_lazyframe: JoinResult<LazyFrame> =
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
                    .finish() // Finalize configuration and get the LazyFrame
                    .map_err(|e| JoinError::CSVReadError(e, path.clone()));

            result_lazyframe // Return the LazyFrame result
        }
        // Handle cases where file path or delimiter is missing.
        _ => {
            // Criar uma mensagem de erro mais detalhada
            let mut message = String::from("Missing essential CSV read configuration.");
            if file_path.is_none() {
                message.push_str(" File path is missing.");
            }
            if delimiter.is_none() {
                message.push_str(" Delimiter is missing.");
            }

            Err(JoinError::IncompleteCsvConfig {
                message,
                file_path,
                delimiter,
            })
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

/**
Extracts and formats a unique 8-character CNPJ (Cadastro Nacional da Pessoa Jurídica) base
from a string column.

The visual pattern will follow the mask: AA.AAA.AAA/AAAA-DV, where "A" can be a letter or a number.
This function specifically extracts the first 8 alphanumeric characters (AA.AAA.AAA)
and formats them with dots.

### Example of usage:
```
use polars::prelude::*;
use join_with_assignments::get_cnpj_base_expr;

fn main() -> PolarsResult<()> {

    let df: DataFrame = df!(
        "text_col"  => &[
            Some("Empresa ABC 129.333.678/0001-90 LTDA com filial 12.345.678/0001-90 LTDA"),
            Some("CNPJ: 12456ABC000123"),
            Some("Multiple bases: 11.222.333/0001-00 and 44.555.666/0001-00"),
            Some("Invalid CNPJ base 12345"),
            None,
        ],
    )?;

    let result_df = df
        .lazy()
        .with_column(get_cnpj_base_expr("text_col"))
        .collect()?;

    let expected_df = df!(
        "cnpj_col" => &[
            Some("12.345.678"),
            Some("12.456.ABC"),
            Some("11.222.333"),
            None,
            None,
        ]
    )?;

    assert_eq!(result_df, expected_df);
    Ok(())
}
```
*/
pub fn get_cnpj_base_expr(column_name: &str) -> Expr {
    // Regex to identify a full CNPJ pattern and capture its 8-character base.
    let cnpj_extract_pattern: Expr = lit(r"(?x)
        (?:\A|\b)         # Non-capturing: the start of the string or a word boundary
        (                 # Start Capture Group (the CNPJ base) (e.g., 12.CDE.678)
            [\dA-Z]{2}    # Matches exactly 2 alphanumeric characters (first part)
            \.?           # Optional dot
            [\dA-Z]{3}    # Matches exactly 3 alphanumeric characters (second part)
            \.?           # Optional dot
            [\dA-Z]{3}    # Matches exactly 3 alphanumeric characters (third part)
        )
        (?:               # Non-capturing group for the suffix
            \/?           # Optional slash
            [\dA-Z]{4}    # Matches exactly 4 alphanumeric characters (branch/filial code)
            -?            # Optional hyphen
            \d{2}         # Matches exactly 2 digits (checksum).
        )
        (?:\z|\b)         # Non-capturing: the end of the string or a word boundary
    ");

    // Regex to format the extracted 8-character CNPJ base.
    // This will take a base like "12CDE678" or "12.CDE.678" and format it as "12.CDE.678".
    let cnpj_format_pattern: Expr = lit(r"(?x)
        ^
        ([\dA-Z]{2})  # Capture Group 1: Matches and captures the first 2 alphanumeric characters.
        \.?           # Matches an optional literal dot.
        ([\dA-Z]{3})  # Capture Group 2: Matches and captures the next 3 alphanumeric characters.
        \.?           # Matches an optional literal dot.
        ([\dA-Z]{3})  # Capture Group 3: Matches and captures the last 3 alphanumeric characters.
        $
    ");

    col(column_name)
        .cast(DataType::String) // Cast the input column to String type.
        .str()
        .extract(cnpj_extract_pattern, 1) // Capture the first group (the base CNPJ)
        .str()
        // This regex specifically captures the three parts of the 8-char base
        // and replaces the entire matched string with the formatted version.
        .replace(cnpj_format_pattern, lit("$1.$2.$3"), false) // `false` indicates `cnpj_format_pattern` is a regex
        .alias(column_name) // Keep original column name for the output
}

/**
Formats an NCM (Nomenclatura Comum do Mercosul) code from a string column.

This function is designed to extract the *first* potential NCM code from a string,
clean it, pad it with leading zeros if it's a 7-digit NCM, and then format it
into the "XXXX.XX.XX" standard pattern. If no valid NCM-like sequence (7 or 8 digits)
is found, the original string value is preserved.

Examples:
- "12345678"     -> "1234.56.78"
- "1234-56.78"   -> "1234.56.78"
- "1234567"      -> "0123.45.67" (padded with a leading zero)
- "NCM: 12345678" -> "1234.56.78"
- "Invalid NCM 12345" -> "Invalid NCM 12345" (original string if not NCM-like)
- "Multiple: 1234.5678 and 90.12.3456" -> "1234.56.78" (only the first is extracted)

### Arguments:
- `column_name`: The name of the string column to process.

### Returns:
An `Expr` that, when applied to a DataFrame, will attempt to extract and format NCM codes.

### Example of usage:
```
use polars::prelude::*;
use join_with_assignments::formatar_ncm_expr;

fn main() -> PolarsResult<()> {

    let df: DataFrame = df!(
        "text_col"  => &[
            Some("12345678"),
            Some("1234-56.78"),
            Some("NCM 1234567"), // 7-digit NCM
            Some("Invalid NCM 12345"),
            Some("Multiple: 1234.5678 and 90.12.3456"),
            None,
            Some("abc"),
        ],
    )?;

    let result_df = df
        .lazy()
        .with_column(formatar_ncm_expr("text_col"))
        .collect()?;

    let expected_df = df!(
        "cnpj_col" => &[
            Some("1234.56.78"),
            Some("1234.56.78"),
            Some("0123.45.67"), // 7-digit NCM padded
            Some("Invalid NCM 12345"),
            Some("1234.56.78"),
            None,
            Some("abc"),
        ]
    )?;

    assert_eq!(result_df, expected_df);
    Ok(())
}
```
*/
pub fn formatar_ncm_expr(column_name: &str) -> Expr {
    // Cast the input column to String type. This handles various input types like i64.
    let string_col = col(column_name).cast(DataType::String);

    // This regex pattern is designed to capture the *first* potential NCM sequence.
    // It looks for:
    // - A non-digit boundary `(?:\A|\D)` to ensure we're starting a new NCM.
    let ncm_extraction_regex = lit(r"(?x)
        (?:\A|\D) # Non-capturing: beginning of text or not digit; Ensures the match is not preceded by a digit.
        (              # Start Capture Group
            [\d]{3,4}  # Matches exactly 3 or 4 digits (first part)
            [\.\-]?    # Optional dot or hyphen
            [\d]{2}    # Matches exactly 2 digits (second part)
            [\.\-]?    # Optional dot or hyphen
            [\d]{2}    # Matches exactly 2 digits (third part)
        )
        (?:\z|\D) # Non-capturing: end of text or not digit; Ensures the match is not followed by a digit.
    ");

    // Regex pattern to remove all non-digit characters.
    // This will clean inputs like "1234.56.78" or "abc12345678def" into "12345678".
    let non_digit_pattern = lit(r"\D");

    // Step 1: Extract the first match and clean the extracted NCM candidate by removing non-digit characters.
    let cleaned_digits = string_col
        .clone()
        //.str()
        //.replace(lit(r"^\s*0+\s*$"), lit("0000.00.00"), false) // `false` indicates `pat` is a regex
        .str()
        .extract(ncm_extraction_regex, 1)
        .str()
        .replace_all(non_digit_pattern, lit(""), false) // `false` indicates `pat` is a regex
        .alias("cleaned_digits_temp");

    // Regex to check if the cleaned string is an NCM-like digit sequence (7 or 8 digits).
    let is_ncm_like_pattern = lit(r"^\d{7,8}$");

    // Check if the string contains only digits and has a length that makes sense for NCM (e.g., 7 or 8).
    let is_ncm_like = cleaned_digits
        .clone()
        .str()
        .contains(is_ncm_like_pattern, false);

    // Step 2: Conditionally apply zfill(8).
    // Conditionally apply zfill(8) only if the cleaned string is 7 or 8 digits long.
    // Otherwise, keep the original string value (before cleaning).
    // Fill with leading zeros to ensure a length of 8, but only if it's purely numeric.
    // We need to use `when().then().otherwise()` to apply zfill conditionally.
    let zfilled_ncm = when(is_ncm_like)
        .then(cleaned_digits.clone().str().zfill(lit(8)))
        .otherwise(string_col) // If not NCM-like, revert to the original string_col
        .alias("zfilled_ncm_temp");

    // Define the regex pattern for the final NCM format "XXXX.XX.XX".
    // This expects an exactly 8-digit string after zfill.
    let ncm_format_pattern = lit(r"^(\d{4})(\d{2})(\d{2})$");

    // Step 3: Apply the final formatting.
    // `replace` will transform "12345678" into "1234.56.78".
    // If `zfilled_ncm` (which is either z-filled NCM or the original string_col)
    // does NOT match `ncm_format_pattern`, `replace` will simply return `zfilled_ncm` unchanged.
    // This implicitly handles the cases where we reverted to `string_col` in the previous step,
    // as well as cases like "abc" which would not match `ncm_format_pattern`.
    zfilled_ncm
        .str()
        .replace(ncm_format_pattern, lit("$1.$2.$3"), false) // `false` indicates `ncm_format_pattern` is a regex
        .alias(column_name) // Alias back to the original column name.
}

/// Formats a `List<Date>` column into a string representation like `[DD/MM/YYYY, DD/MM/YYYY]`.
///
/// This function processes a column containing lists of dates. For each row, it formats
/// the individual dates within the list to "DD/MM/YYYY" strings, joins them with ", ",
/// and then encloses the entire string in square brackets.
///
/// If the input column contains nulls for a list, the output for that row will also be null
/// due to Polars' automatic null propagation in string operations.
///
/// # Arguments
///
/// * `column_name` - The name of the `List<Date>` column to format.
///
/// # Returns
///
/// A Polars `Expr` that, when applied, will transform the specified column.
pub fn format_list_dates(column_name: &str) -> Expr {
    lit("[")
    + col(column_name)
        .list()
        .eval(col("").dt().strftime("%d/%m/%Y")) // Format inner dates
        .list()
        .join(lit(", "), true) // Join formatted dates
    + lit("]")
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

/// Creates an expression to retain only digits from a specified column.
///
/// This function generates a Polars expression that replaces all non-digit characters
/// in the target column with an empty string, effectively retaining only numeric characters.
/// The resulting column will keep its original name.
///
/// Arguments:
/// * `column_name`: The name of the column to process.
///
/// Returns:
/// A Polars expression that, when applied, will clean the specified column.
pub fn retain_only_digits(column_name: &str) -> Expr {
    // Regex expression to remove non-numeric characters
    // [^0-9] will match any character that is not a digit (0-9)
    // \D means "match any character that is NOT a digit".
    let pattern: Expr = lit(r"\D"); // Regex to match non-numeric characters

    col(column_name)
        .str()
        .replace_all(pattern, lit(""), false)
        //.name().keep() // Keep the original column name
        .alias(column_name) // Use alias to explicitly keep the original column name
}

pub fn quit() {
    std::process::exit(0);
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
///
/// `cargo test -- --show-output tests_functions`
#[cfg(test)]
mod tests_functions {
    use super::*;
    use chrono::NaiveDate;
    use std::error::Error;

    // cargo test -- --help
    // cargo test -- --show-output
    // cargo test -- --show-output multiple_values

    #[test]
    /// `cargo test -- --show-output format_list_dates`
    fn test_format_list_dates() -> PolarsResult<()> {
        let datas1 = vec![
            NaiveDate::from_ymd_opt(2023, 1, 1).unwrap(),
            NaiveDate::from_ymd_opt(2023, 5, 10).unwrap(),
        ];
        let datas2 = vec![NaiveDate::from_ymd_opt(2024, 9, 15).unwrap()];

        // Create the inner Series first
        let series_datas1 = Series::new("row1".into(), datas1);
        let series_datas2 = Series::new("row2".into(), datas2);

        let df = df! { "Períodos de Apuração" => &[
            Some(series_datas1),
            Some(series_datas2),
            None
        ]}?;

        println!("DataFrame Original:\n{}", df);

        // Using the expression directly for the first case.
        // Polars automatically handles null propagation for string operations.
        let df_formatado = df
            .clone() // Clone for the first case if you need the original df for subsequent operations
            .lazy()
            .with_column(format_list_dates("Períodos de Apuração").alias("Períodos Formatados"))
            .collect()?;

        println!("\nDataFrame Formatado com concat_str:\n{}", df_formatado);

        let datas = vec![Some("[01/01/2023, 10/05/2023]"), Some("[15/09/2024]"), None];
        let coluna_formatada = Column::new("fmt".into(), datas);

        assert_eq!(df_formatado["Períodos Formatados"], coluna_formatada);

        Ok(())
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

    fn create_csv(dir: &std::path::Path, filename: &str, contents: &str) -> JoinResult<PathBuf> {
        let file_path = dir.join(filename);
        let mut file = File::create(&file_path)?;
        file.write_all(contents.as_bytes())?;
        Ok(file_path)
    }

    // O teste de sucesso não deveria ter esse problema, pois ele espera Ok(LazyFrame)
    #[test]
    fn test_read_csv_lazy_success() -> JoinResult<()> {
        let dir = tempdir()?;
        let file_path = create_csv(dir.path(), "data.csv", "col1,col2\n1,a\n2,b")?;

        let lazy_frame = read_csv_lazy(Some(file_path), Some(','), Side::Left)?;

        let df = lazy_frame.collect()?; // Coletar para um DataFrame

        println!("df:\n{df}");

        assert_eq!(df.height(), 2);
        assert_eq!(df.get_column_names(), &["col1", "col2"]);

        Ok(())
    }

    #[test]
    fn test_read_csv_lazy_schema_modify() -> JoinResult<()> {
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
    fn test_read_csv_lazy_empty_fields() -> JoinResult<()> {
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

    #[test]
    fn test_read_csv_lazy_error_on_missing_file_path() {
        // A função retorna JoinResult<LazyFrame>, então o Ok type é LazyFrame
        let result = read_csv_lazy(None, Some(','), Side::Left);

        assert!(result.is_err());

        match result.err() {
            Some(JoinError::IncompleteCsvConfig {
                message,
                file_path,
                delimiter,
            }) => {
                assert!(message.contains("File path is missing."));
                assert!(file_path.is_none());
                assert_eq!(delimiter, Some(','));
            }
            // para cobrir o caso None (se result fosse Ok) e o caso de um erro de tipo diferente.
            e => panic!(
                "Esperava JoinError::IncompleteCsvConfig, mas obteve: {:?}",
                e
            ),
        }
    }

    #[test]
    fn test_read_csv_lazy_error_on_missing_delimiter() {
        let dir = tempdir().unwrap();
        let file_path = create_csv(dir.path(), "dummy.csv", "a,b\n1,2").unwrap();
        let file_path_clone = file_path.clone();

        let result = read_csv_lazy(Some(file_path), None, Side::Left);

        assert!(result.is_err());

        match result.err() {
            Some(JoinError::IncompleteCsvConfig {
                message,
                file_path,
                delimiter,
            }) => {
                assert!(message.contains("Delimiter is missing."));
                assert_eq!(file_path, Some(file_path_clone));
                assert!(delimiter.is_none());
            }
            e => panic!(
                "Esperava JoinError::IncompleteCsvConfig, mas obteve: {:?}",
                e
            ),
        }
    }
}

/// Run tests with:
/// cargo test -- --show-output tests_replace_values_with_null
#[cfg(test)]
mod tests_replace_values_with_null {
    use super::*;
    use polars::functions::concat_df_horizontal;

    #[test]
    fn test_remove_leading_and_trailing_chars() -> JoinResult<()> {
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

    #[test]
    /// Run tests with:
    /// `cargo test -- --show-output test_retain_only_digits`
    fn test_retain_only_digits() -> PolarsResult<()> {
        let df: DataFrame = df!(
            "text_col"  => &[
                Some("abc123def456"),
                Some("789xyz0"),
                Some(""),   // empty_string ""
                None,       // with null
                Some("0"),
                Some("  "), // empty_string "  "
                None,       // with null
                Some("!@#"),
                Some("123"),
                Some("abc")
            ],
        )?;

        println!("df: {df}");

        let cleaned_df = df
            .lazy()
            .with_column(retain_only_digits("text_col"))
            .collect()?;

        let expected_df: DataFrame = df!(
            "text_col"  => &[
                Some("123456"), // abc123def456
                Some("7890"),   // 789xyz0
                Some(""),       // empty_string ""
                None,           // with null
                Some("0"),      // 0
                Some(""),       // empty_string "  "
                None,           // with null
                Some(""),       // !@#
                Some("123"),    // 123
                Some("")        // abc
            ],
        )?;

        println!("expected_df: {expected_df}");

        assert_eq!(cleaned_df, expected_df);

        Ok(())
    }
}

#[cfg(test)]
/// Run tests with:
/// `cargo test -- --show-output ncm_tests`
mod ncm_tests {
    use super::*;

    #[test]
    fn test_formatar_ncm_expr() -> PolarsResult<()> {
        let df = df!(
            "ncm_col" => &[
                Some("12345678"),
                Some("1234.56.78"),
                Some("1234567"),
                Some("abc"),
                Some("def.12345678.ghi"), // NCM embedded
                Some("1234"),             // Too short
                Some("123456789"),        // Too long after cleaning
                Some(""), // Empty string
                None,     // Null value
                Some("12.34.56.78"), // Already malformed but contains digits
                Some("123.45.67"),   // 7 digits, needs zero-padding
                Some("12345678 and some text"), // NCM at start
                Some("text and 12345678"), // NCM at end
                Some("1 2 3 4 5 6 7 8"),   // Spaces
                Some("1-2-3-4-5-6-7-8"),   // Dashes
                Some("NCM 0912.3456"),
                Some("Invalid: 23.45.67"), // return the original input
                Some("Multiple: 1234.5678 and 90.12.3456"),
                Some("<N/D>"), // return the original input
            ]
        )?;

        println!("df: {df}");

        let expected_df = df!(
            "ncm_col" => &[
                Some("1234.56.78"),
                Some("1234.56.78"),
                Some("0123.45.67"),
                Some("abc"),
                Some("1234.56.78"), // NCM embedded
                Some("1234"),       // Remains too short
                Some("123456789"),  // Remains too long after cleaning
                Some(""),
                None,
                Some("12.34.56.78"), // return the original input
                Some("0123.45.67"),  // Correctly z-fills and formats (123.45.67 -> 1234567 -> 01234567)
                Some("1234.56.78"),  // Correctly extracts and formats
                Some("1234.56.78"),  // Correctly extracts and formats
                Some("1 2 3 4 5 6 7 8"),   // Spaces: return the original input
                Some("1-2-3-4-5-6-7-8"),   // Dashes: return the original input
                Some("0912.34.56"),
                Some("Invalid: 23.45.67"), // return the original input
                Some("1234.56.78"),        // Only the *first* NCM is extracted
                Some("<N/D>"),             // return the original input
            ]
        )?;

        println!("expected_df: {expected_df}");

        let result_df = df
            .lazy()
            .select(&[formatar_ncm_expr("ncm_col")])
            .collect()?;

        println!("result_df: {result_df}");

        assert_eq!(result_df, expected_df);
        Ok(())
    }

    #[test]
    fn test_formatar_ncm_expr_integer_input() -> PolarsResult<()> {
        let df = df!(
            "ncm_col_i64" => &[
                Some(12345678i64),
                Some(1234567i64),
                Some(98765432i64),
                None,
                Some(123i64), // Too short
            ]
        )?;

        println!("df: {df}");

        let expected_df = df!(
            "ncm_col_str" => &[
                Some("1234.56.78"),
                Some("0123.45.67"),
                Some("9876.54.32"),
                None,
                Some("123"), // Remains as is after cast to string and cleaning
            ]
        )?;

        println!("expected_df: {expected_df}");

        let result_df = df
            .lazy()
            .select(&[formatar_ncm_expr("ncm_col_i64")])
            .collect()?;

        println!("result_df: {result_df}");

        assert_eq!(result_df, expected_df);
        Ok(())
    }

    #[test]
    fn test_formatar_ncm_expr_empty_dataframe() -> PolarsResult<()> {
        let df = df!("ncm_col" => &Vec::<String>::new())?;
        let expected_df = df!("ncm_col" => &Vec::<String>::new())?;

        let result_df = df
            .lazy()
            .select(&[formatar_ncm_expr("ncm_col")])
            .collect()?;

        assert_eq!(result_df, expected_df);
        Ok(())
    }

    #[test]
    fn test_formatar_ncm_expr_mixed_valid_invalid_formats() -> PolarsResult<()> {
        let df = df!(
            "ncm_col" => &[
                Some("abc12345678def"),  // Embedded NCM
                Some("987.65.43"),       // 7 digits, already dotted
                Some("invalid_ncm"),     // Completely invalid
                Some("00000000"),        // All zeros
                Some("0"),               // one zero
                Some("1.2.3.4.5.6.7.8"), // Too many dots
            ]
        )?;

        let expected_df = df!(
            "ncm_col" => &[
                Some("1234.56.78"),  // Extracts and formats the 8-digit part
                Some("0987.65.43"),  // Cleans (987.65.43 -> 9876543), z-fills to 09876543, then formats to 0987.65.43
                Some("invalid_ncm"), // Remains unchanged
                Some("0000.00.00"),  // Formats correctly
                Some("0"),                // Remains unchanged
                Some("1.2.3.4.5.6.7.8"),  // Remains unchanged
            ]
        )?;

        let result_df = df
            .lazy()
            .select(&[formatar_ncm_expr("ncm_col")])
            .collect()?;

        assert_eq!(result_df, expected_df);
        Ok(())
    }
}

#[cfg(test)]
/// Run tests with:
/// `cargo test -- --show-output cnpj_tests`
mod cnpj_tests {
    use super::*;
    use polars::df;

    #[test]
    /// `cargo test -- --show-output test_get_cnpj_base_expr_v1`
    fn test_get_cnpj_base_expr_v1() -> PolarsResult<()> {
        // Exemplo com CNPJ fictício

        let text_1 = "12345678000923";

        let text_2 = "<N/D> [Info do CT-e: 12.ABC.678/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12.ABC.679/0009-66] [Info do CT-e: 12.ABC.678/0009-23]";

        let text_3 = "<N/D> [Info do CT-e: 12.345CDE/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12345.CDE/1234-88] [Info do CT-e: 12345CDE901234] 12345CDE9012345";

        let text_4 = "12.345.678/12345-123 02345678123412 foo 012.345.678/1234-23";

        let text_5 = "123456781234123 foo 012.345.678/1234-23 bar 12.FGH.678/1234-23 zz";

        let df: DataFrame = df!(
            "text_col"  => &[
                Some(text_1),
                Some(text_2),
                Some(text_3),
                None,
                Some(text_4),
                Some(text_5),
            ],
        )?;

        println!("df: {df}");

        let cleaned_df = df
            .lazy()
            .with_column(get_cnpj_base_expr("text_col"))
            .collect()?;

        let expected_df: DataFrame = df!(
            "text_col"  => &[
            Some("12.345.678"),
            Some("12.ABC.678"),
            Some("12.345.CDE"),
            None,
            Some("02.345.678"),
            Some("12.FGH.678"),
            ],
        )?;

        println!("expected_df: {expected_df}");

        assert_eq!(cleaned_df, expected_df);

        Ok(())
    }

    #[test]
    fn test_get_cnpj_base_expr_v2() -> PolarsResult<()> {
        let df = df!(
            "cnpj_col" => &[
                Some("12.345.FGH/0009-23"), // Standard dotted CNPJ
                Some("12345678000199"),     // Undotted CNPJ
                Some("abc 12.345.678/0001-00 def"), // Embedded CNPJ
                Some("foo 12.345.678/0001-00 bar 12345678/0002-00 baz"), // Same base, different branch
                Some("12.345.678/0001-00 and 98.765.432/0001-00"), // Multiple unique bases
                Some("<N/D> [Info do CT-e: 12.345.678/0009-23] [Info do CT-e: 12345678] [Info do CT-e: 12345678/000923]"), // Embedded multiple times, same base
                Some("no cnpj here"),   // No CNPJ
                Some("1234567"),        // Too short
                Some("123456789"),      // Too long (not matching base pattern)
                None, // Null input
                Some("123.456.789-01"), // Malformed base (9 digits for the last part) - should not match
                Some("12.abc.678"),     // Base only
                Some("12345678"),       // Base only, undotted
                Some("12.345.678/0009-23"),
                Some("<N/D> [Info do CT-e: 12.ABC.678/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12.ABC.679/0009-66] [Info do CT-e: 12.ABC.678/0009-23]"),
                Some("<N/D> [Info do CT-e: 12.345.CDE/0009-23] [Info do CT-e: <N/D>] [Info do CT-e: 12345CDE/1234-88] [Info do CT-e: 12345CDE901234] 12345CDE9012345"),
                Some("02345678/12345-12 123456781234123 0234567A/4444-12 foo 012.345.678/1234-23"),
                Some("02345678/12345-12 123456781234123 foo 012.345.678/1234-23"),
                Some("12345678123412 foo 012.345.678/1234-23"),
        ])?;

        println!("df: {df}");

        let expected_df = df!(
            "cnpj_col" => &[
                Some("12.345.FGH"), // Standard dotted CNPJ
                Some("12.345.678"), // Undotted CNPJ
                Some("12.345.678"), // Embedded CNPJ
                Some("12.345.678"), // Same base, different branch
                Some("12.345.678"), // Multiple unique bases
                Some("12.345.678"), // Embedded multiple times, same base
                None, // No CNPJ
                None, // Too short
                None, // Too long
                None, // Null input
                None, // Malformed base
                None, // Base only
                None, // Base only, undotted
                Some("12.345.678"),
                Some("12.ABC.678"),
                Some("12.345.CDE"),
                Some("02.345.67A"),
                None,
                Some("12.345.678"),
            ]
        )?;

        println!("expected_df: {expected_df}");

        let result_df = df
            .lazy()
            .select(&[get_cnpj_base_expr("cnpj_col")])
            .collect()?;

        println!("result_df: {result_df}");

        assert_eq!(result_df, expected_df);
        Ok(())
    }

    #[test]
    fn test_get_cnpj_base_expr_empty_dataframe() -> PolarsResult<()> {
        let df = df!("cnpj_col" => &Vec::<String>::new())?;
        let expected_df = df!("cnpj_col" => &Vec::<String>::new())?;

        let result_df = df
            .lazy()
            .select(&[get_cnpj_base_expr("cnpj_col")])
            .collect()?;

        assert_eq!(result_df, expected_df);
        Ok(())
    }

    #[test]
    fn test_get_cnpj_base_expr_integer_input() -> PolarsResult<()> {
        let df = df!(
            "cnpj_col_i64" => &[
                Some(12345678000123i64),
                Some(99887766000210i64),
                None,
                Some(123i64),
            ]
        )?;

        let expected_df = df!(
            "cnpj_col_i64" => &[
                Some("12.345.678"),
                Some("99.887.766"),
                None,
                None, // 123 is too short to be a CNPJ base
            ]
        )?;

        let result_df = df
            .lazy()
            .select(&[get_cnpj_base_expr("cnpj_col_i64")])
            .collect()?;

        assert_eq!(result_df, expected_df);
        Ok(())
    }
}
