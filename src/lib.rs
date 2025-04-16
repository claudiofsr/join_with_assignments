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
        coluna, MyColumn,
        Side::{self, Left, Middle, Right},
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
    munkres::{munkres_assignments, try_convert, FloatIterExt},
    polars_assignments::get_dataframe_after_assignments,
    write::ExcelWriter,
    xlsx_writer::PolarsXlsxWriter,
};

use claudiofsr_lib::RoundFloat;
use polars::prelude::*;
use regex::Regex;
use std::{
    any,
    collections::{HashMap, HashSet},
    env,
    error::Error,
    fmt::Write,
    fs::File,
    num::ParseFloatError,
    path::PathBuf,
    process,
    sync::LazyLock as Lazy,
};
use sysinfo::System;

pub type VecTuples = Vec<(String, u64, u64)>;

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
            .into_iter()
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

pub enum Frame {
    Lazy(LazyFrame),
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

/// Polar arguments with ENV vars
pub fn configure_the_environment() {
    // https://stackoverflow.com/questions/70830241/rust-polars-how-to-show-all-columns/75675569#75675569
    // https://pola-rs.github.io/polars/polars/index.html#config-with-env-vars
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    env::set_var("POLARS_FMT_MAX_COLS", "10"); // maximum number of columns shown when formatting DataFrames.
    env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
    env::set_var("POLARS_FMT_STR_LEN", "52"); // maximum number of characters printed per string value.
}

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

    println!(
        "Memory used/total: {:>8}/{} Mbytes",
        sys_used_memory, sys_total_memory
    );

    // Number of CPUs:
    println!("Number of CPUs: {:>9}\n", sys.cpus().len());
}

/// See polars-core-0.27.2/src/utils/mod.rs and macro_rules! split_array {...}
pub fn split_series(series: &Series) -> PolarsResult<Vec<Series>> {
    let vec_series: Vec<Series> = (0..series.len())
        .map(|i| series.slice(i as i64, 1).explode().unwrap())
        .collect();

    Ok(vec_series)
}

pub fn get_option_assignments(series_efd: Series, series_nfe: Series) -> Option<Series> {
    let result_chunkedarray_f64_efd: Result<&ChunkedArray<Float64Type>, PolarsError> =
        series_efd.f64();
    let result_chunkedarray_f64_nfe: Result<&ChunkedArray<Float64Type>, PolarsError> =
        series_nfe.f64();

    match (result_chunkedarray_f64_efd, result_chunkedarray_f64_nfe) {
        (Ok(chunkedarray_f64_efd), Ok(chunkedarray_f64_nfe)) => {
            let vec_float64_efd: Vec<f64> = chunkedarray_f64_efd
                .into_iter()
                .filter_map(verbose_option) //.map_while(verbose_option)
                .collect();

            let vec_float64_nfe: Vec<f64> = chunkedarray_f64_nfe
                .into_iter()
                .filter_map(verbose_option)
                .collect();

            // if vec_float64_efd.len() * vec_float64_nfe.len() > 0 {

            if !vec_float64_efd.is_empty() && !vec_float64_nfe.is_empty() {
                let assignments: Vec<u64> =
                    munkres_assignments(&vec_float64_efd, &vec_float64_nfe, false);
                Some(Series::new("new".into(), assignments))
            } else {
                None
            }
        }
        _ => {
            eprintln!("Float64Type PolarsError!");
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

pub fn get_opt_vectuples(
    chave_doc: &str,
    series_efd: Series,
    series_nfe: Series,
    series_asg: Series,
) -> Option<VecTuples> {
    let result_chunkedarray_u64_efd: Result<&ChunkedArray<UInt64Type>, PolarsError> =
        series_efd.u64();
    let result_chunkedarray_u64_nfe: Result<&ChunkedArray<UInt64Type>, PolarsError> =
        series_nfe.u64();
    let result_chunkedarray_u64_asg: Result<&ChunkedArray<UInt64Type>, PolarsError> =
        series_asg.u64();

    match (
        result_chunkedarray_u64_efd,
        result_chunkedarray_u64_nfe,
        result_chunkedarray_u64_asg,
    ) {
        (Ok(chunkedarray_u64_efd), Ok(chunkedarray_u64_nfe), Ok(chunkedarray_u64_asg)) => {
            let vec_u64_efd: Vec<u64> = chunkedarray_u64_efd
                .into_iter()
                .filter_map(verbose_option)
                .collect();

            let vec_u64_nfe: Vec<u64> = chunkedarray_u64_nfe
                .into_iter()
                .filter_map(verbose_option)
                .collect();

            let vec_u64_asg: Vec<u64> = chunkedarray_u64_asg
                .into_iter()
                .filter_map(verbose_option)
                .collect();

            // if vec_float64_efd.len() * vec_float64_nfe.len() * vec_float64_asg.len() > 0 {

            if !vec_u64_efd.is_empty() && !vec_u64_nfe.is_empty() && !vec_u64_asg.is_empty() {
                line_assignments(chave_doc, &vec_u64_efd, &vec_u64_nfe, &vec_u64_asg)
            } else {
                None
            }
        }
        _ => {
            eprintln!("UInt64Type PolarsError!");
            eprintln!("chave_doc: {chave_doc}");
            eprintln!(
                "series_efd.dtype(): {} ; series_efd: {series_efd}",
                series_efd.dtype()
            );
            eprintln!(
                "series_nfe.dtype(): {} ; series_nfe: {series_nfe}",
                series_nfe.dtype()
            );
            eprintln!(
                "series_asg.dtype(): {} ; series_asg: {series_asg}",
                series_asg.dtype()
            );
            None
        }
    }
}

fn line_assignments(
    chave_doc: &str,
    slice_lines_efd: &[u64],
    slice_lines_nfe: &[u64],
    assignments: &[u64],
) -> Option<VecTuples> {
    let mut chaves_com_linhas_correlacionadas: VecTuples = Vec::new();

    for (row, &col) in assignments.iter().enumerate() {
        let opt_line_efd: Option<&u64> = slice_lines_efd.get(row);
        let opt_line_nfe: Option<&u64> = slice_lines_nfe.get(col as usize);

        if let (Some(&line_efd), Some(&line_nfe)) = (opt_line_efd, opt_line_nfe) {
            let tuple: (String, u64, u64) = (chave_doc.to_string(), line_efd, line_nfe);
            //println!("row: {row} ; tuple: {tuple:?}");
            chaves_com_linhas_correlacionadas.push(tuple);
        }
    }

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
            ))
        }
    }

    // Format date
    let mut lazyframe: LazyFrame = read_csv_lazy(file_path, delimiter, side)?.with_column(
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

/**
Get headers from CSV file (valid UTF-8 or not valid UTF-8).
```
    use csv::{ReaderBuilder, StringRecord};
    use join_with_assignments::get_csv_headers;

    fn main() -> Result<(), Box<dyn std::error::Error>> {
        let delimiter = ';';
        let file_path = "src/tests/csv_file01";
        let headers = get_csv_headers(file_path, delimiter as u8)?;
        let col_names: Vec<&str> = headers.into_iter().collect();

        assert_eq!(col_names, [
            "Número",
            "Dia da Emissão",
            "Alíquota",
            "Descrição",
            "Value T",
            "Value P",
        ]);
        Ok(())
    }
```
*/
pub fn get_csv_headers(
    path: impl AsRef<std::path::Path>,
    delimiter: u8,
) -> Result<csv::StringRecord, Box<dyn Error>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
        .trim(csv::Trim::All)
        .from_path(path)?;

    let bytes = reader.byte_headers()?.to_owned();
    let headers = csv::StringRecord::from_byte_record_lossy(bytes);

    Ok(headers)
}

// https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html
fn read_csv_lazy(
    file_path: Option<PathBuf>,
    delimiter: Option<char>,
    side: Side,
) -> PolarsResult<LazyFrame> {
    // Set values that will be interpreted as missing/null.
    let null_values: Vec<PlSmallStr> = vec![
        "", // foo;"";bar --> foo;;bar
        " ",
        "<N/D>",
        "*DIVERSOS*",
    ]
    .into_iter()
    .map(|s| s.into())
    .collect();

    match (&file_path, delimiter) {
        (Some(path), Some(separator)) => {
            // Schema: a map from column names to data types
            let mut schema: Schema = Schema::default();

            // HashMap<name, dtype> used to make Schema
            let cols_dtype: HashMap<&str, DataType> = MyColumn::get_cols_dtype(side);

            // headers, nomes das colunas, primeira linha do arquivo CSV.
            if let Ok(headers) = get_csv_headers(path, separator as u8) {
                // Colunas adicionadas a Schema de acordo com a ordem das colunas no arquivo CSV.
                headers
                    .into_iter()
                    .for_each(|name| match cols_dtype.get(name) {
                        Some(dtype) => {
                            schema.with_column(name.into(), dtype.clone());
                        }
                        None => {
                            eprintln!("Inserir DataType da coluna '{name}' em Column {side:?}!");
                            schema.with_column(name.into(), DataType::String);
                        }
                    });
            }

            let result_lazyframe: PolarsResult<LazyFrame> = LazyCsvReader::new(path)
                .with_encoding(CsvEncoding::LossyUtf8)
                .with_try_parse_dates(false) // use regex
                .with_separator(separator as u8)
                .with_quote_char(Some(b'"')) // default
                .with_has_header(true)
                .with_ignore_errors(true)
                .with_null_values(Some(NullValues::AllColumns(null_values)))
                //.with_null_values(None)
                .with_missing_is_null(true)
                //.with_infer_schema_length(Some(200))
                .with_rechunk(true)
                .with_schema(Some(Arc::new(schema)))
                .finish();

            // Add error description
            if result_lazyframe.is_err() {
                eprintln!("\nError: Failed to read the file {:#?}", path);
            }

            result_lazyframe
        }
        _ => {
            eprintln!("File path: {:#?}", file_path);
            eprintln!("Delimiter: {:#?}", delimiter);
            panic!("File path or delimiter error!")
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
fn format_dataframe(df: &DataFrame) -> PolarsResult<DataFrame> {
    // Column names:
    let natureza: &str = coluna(Left, "natureza");

    let df_formated: DataFrame = df
        .clone()
        .lazy()
        .with_column(
            col("Mês do Período de Apuração")
                .apply(descricao_do_mes, GetOutput::from_type(DataType::String)),
        )
        .with_column(col("Tipo de Operação").apply(
            descricao_do_tipo_de_operacao,
            GetOutput::from_type(DataType::String),
        ))
        .with_column(col("Tipo de Crédito").apply(
            descricao_do_tipo_de_credito,
            GetOutput::from_type(DataType::String),
        ))
        .with_column(col(natureza).apply(
            descricao_da_natureza_da_bc_dos_creditos,
            GetOutput::from_type(DataType::String),
        ))
        .collect()?;

    // Verificar a existência da coluna "Indicador de Origem" antes aplicar alterações.
    let col_names: Vec<&PlSmallStr> = df_formated.get_column_names();

    if find_name(&col_names, "Indicador de Origem") {
        let df = df_formated
            .clone()
            .lazy()
            .with_column(
                col("Indicador de Origem")
                    .apply(descricao_da_origem, GetOutput::from_type(DataType::String)),
            )
            .collect()?;
        return Ok(df);
    }

    Ok(df_formated)
}

/// Searching a &str into Vec<&str>
///
/// <https://stackoverflow.com/questions/57633089/searching-a-string-into-vecstring-in-rust>
pub fn find_name(names: &[&PlSmallStr], name: &str) -> bool {
    names.iter().any(|&x| x == name)
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
pub fn get_cnpj_base(col: Column) -> PolarsResult<Option<Column>> {
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
fn cnpj_base(col: Column) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .str()?
        .into_iter()
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

    Ok(Some(new_col))
}

pub fn desprezar_pequenos_valores(col: Column, delta: f64) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .f64()?
        .into_iter()
        .map(|opt_f64: Option<f64>| match opt_f64 {
            Some(value) if value.abs() > delta => Some(value),
            _ => None,
        })
        .collect::<Float64Chunked>()
        .into_column();

    Ok(Some(new_col))
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
        .into_iter()
        .map(|option_i64: Option<i64>| option_i64.map(|int64| format!("{int64:0fill$}")))
        .collect::<StringChunked>()
        .into_series();

    Ok(Some(new_series))
}

/// Filtra colunas do tipo float64.
///
/// Posteriormente, arredonda os valores da coluna
pub fn round_float64_columns(col: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let series = match col.as_series() {
        Some(s) => s,
        None => return Ok(Some(col)),
    };

    match series.dtype() {
        DataType::Float64 => Ok(Some(series.round(decimals)?.into())),
        _ => Ok(Some(col)),
    }
}

pub fn round_column(col: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    match col.dtype() {
        // DataType::Float64 => Ok(Some(series.round(decimals)?)), <-- Bug panicking::panic_fmt
        DataType::Float64 => round_column_f64(col, decimals),
        DataType::String => round_column_str(col, decimals),
        _ => {
            eprintln!("fn round_series()");
            eprintln!("Column: {col:?}");
            eprintln!("Decimals: {decimals}");
            Err(PolarsError::InvalidOperation(
                format!("Not supported for Series with DataType {:?}", col.dtype()).into(),
            ))
        }
    }
}

fn round_column_f64(col: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .f64()?
        .into_iter()
        .map(|opt_f64: Option<f64>| opt_f64.map(|float64| float64.round_float(decimals)))
        .collect::<Float64Chunked>()
        .into_column();

    Ok(Some(new_col))
}

fn round_column_str(col: Column, decimals: u32) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .str()?
        .into_iter()
        .map(|opt_str: Option<&str>| get_opt_from_str(opt_str, &col, decimals))
        .collect::<Float64Chunked>()
        .into_column();

    Ok(Some(new_col))
}

fn get_opt_from_str(opt_str: Option<&str>, col: &Column, decimals: u32) -> Option<f64> {
    let opt_float64: Option<f64> = match opt_str {
        Some(str) => {
            let result: Result<f64, ParseFloatError> =
                str.trim().replace('.', "").replace(',', ".").parse::<f64>();

            match result {
                Ok(float) => Some(float.round_float(decimals)),
                Err(why) => {
                    eprintln!("fn get_opt_from_str()");
                    eprintln!("Error parse f64: {why}");
                    process::exit(1)
                }
            }
        }
        None => {
            eprintln!("fn get_opt_from_str()");
            eprintln!("Found None value in column:");
            eprintln!("col: {col:?}\n");
            None
        }
    };

    opt_float64
}

/// NCM format: "12345678" --> "1234.56.78"
pub fn formatar_ncm(col: Column) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .str()?
        .into_iter()
        .map(|option_str| option_str.map(extract_ncm))
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn formatar_chave_eletronica(col: Column) -> PolarsResult<Option<Column>> {
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
fn format_digits(col: Column) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .str()?
        .into_iter()
        .map(retain_only_digits)
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
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
            write!(&mut cnpj, "{}.{}.{}", part1, part2, part3).ok()?;
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
                write!(&mut ncm, "0{}.{}.{}", part1, part2, part3).ok()?;
            } else {
                // Otherwise, use the first part as is.
                write!(&mut ncm, "{}.{}.{}", part1, part2, part3).ok()?;
            }
            Some(ncm) // Return the constructed NCM.
        })
        .next() // Take only the first match.
        .unwrap_or_else(|| input.to_string()) // Return the original input if no match is found.
}

pub fn quit() {
    std::process::exit(0);
}

#[cfg(test)]
mod test_functions {
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

        assert_eq!(Some(Column::new("".into(), &valid)), col);

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
    /// `cargo test -- --show-output test_round_f64`
    fn test_round_f64() {
        let decimals: u32 = 2;

        let numbers: Vec<f64> = vec![0.025, 4.354999, 4.365, 0.01499999999999];

        let result: Vec<f64> = vec![0.03, 4.35, 4.37, 0.01];

        let mut rounded_number: Vec<f64> = Vec::new();

        for number in &numbers {
            let decimals_usize = decimals as usize;
            let num = number.round_float(decimals);
            println!("round_f64: {num} ; println: {number:.decimals_usize$}");
            rounded_number.push(num);
        }

        assert_eq!(rounded_number, result);
    }

    #[test]
    /// `cargo test -- --show-output function_returning_multiple_values`
    fn function_returning_multiple_values() -> Result<(), Box<dyn Error>> {
        // https://stackoverflow.com/questions/70959170/is-there-a-way-to-apply-a-udf-function-returning-multiple-values-in-rust-polars

        let df = df![
            "a" => [1.0, 2.0, 3.0],
            "b" => [1.0, 2.0, 3.0]
        ]?;

        let df: DataFrame = df
            .lazy()
            .select([map_multiple(
                |columns| {
                    Ok(Some(
                        columns[0]
                            .f64()?
                            .into_no_null_iter()
                            .zip(columns[1].f64()?.into_no_null_iter())
                            .map(|(a, b)| {
                                let out = black_box(a, b);
                                Series::new("".into(), [out.0, out.1, out.2])
                            })
                            .collect::<ChunkedArray<ListType>>()
                            .into_column(),
                    ))
                },
                [col("a"), col("b")],
                GetOutput::from_type(DataType::Float64),
            )
            .alias("Multiple Values")])
            .collect()?;

        //dbg!(df);
        println!("{df}");

        /*
        shape: (3, 1)
        ┌─────────────────┐
        │ Multiple Values │
        │ ---             │
        │ list[f64]       │
        ╞═════════════════╡
        │ [2.0, 3.3, 1.0] │
        │ [4.0, 6.6, 4.0] │
        │ [6.0, 9.9, 9.0] │
        └─────────────────┘
        */

        let column_multiple_values = df.column("Multiple Values")?;
        let vec_opt_lines_efd: Vec<Option<Series>> =
            column_multiple_values.list()?.into_iter().collect();

        // É necessário formatar o número de casas decimais
        let col_formatted: Vec<Option<Column>> = vec_opt_lines_efd
            .into_iter()
            .flat_map(|opt_series| {
                opt_series.and_then(|series| round_column(series.into(), 1).ok())
            })
            .collect();

        let vec_col: Vec<Column> = col_formatted.into_iter().flatten().collect();

        let vec_lines: Result<Vec<Vec<f64>>, Box<dyn Error>> = vec_col
            .iter()
            .map(|col| {
                let chunkedarray_f64: &ChunkedArray<Float64Type> = col.f64()?;

                let vec_float64: Vec<f64> = chunkedarray_f64
                    .into_iter()
                    .filter_map(verbose_option)
                    .collect();

                Ok(vec_float64)
            })
            .collect();

        let first_list = vec![2.0, 3.3, 1.0];

        assert!(first_list
            .into_iter()
            .zip(vec_lines?[0].clone())
            .all(|(a, b)| {
                println!("a: {a:>3} ; b: {b:>3}");
                a == b
            }));

        Ok(())
    }

    /// Your function that takes 2 argument and returns 3
    fn black_box(a: f64, b: f64) -> (f64, f64, f64) {
        (a + b, 5.4 * a - 2.1 * b, a * b)
    }

    #[test]
    /// `cargo test -- --show-output collect_values_into_vec`
    fn collect_values_into_vec() -> Result<(), Box<dyn Error>> {
        // https://stackoverflow.com/questions/71376935/how-to-get-a-vec-from-polars-series-or-chunkedarray

        let series = Series::new("a".into(), 0..10i32);
        println!("series: {series}");

        let vec_opt_i32: Vec<Option<i32>> = series.i32()?.into_iter().collect();
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

            if let Some(assignments) = get_option_assignments(series_efd, series_nfe) {
                // println!("assignments: {assignments}");
                let result: Vec<u64> = assignments.u64()?.into_iter().flatten().collect();
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
