//! This module handles exporting structured Polars DataFrames to formatted Excel files.
//!
//! It organizes worksheet creation by executing operations concurrently via `rayon`,
//! mapping column values to standardized styles, and implementing custom row styling.

use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use rust_xlsxwriter::{Color, Format, FormatAlign, Workbook, Worksheet};
use std::{collections::HashMap, sync::LazyLock};

use crate::{JoinError, JoinResult, PolarsExcelWriter, format_dataframe};

// --- Aesthetic Constants ---
const FONT_SIZE: f64 = 14.0;
const HEADER_FONT_SIZE: f64 = 12.0;
const MAX_NUMBER_OF_ROWS: usize = 1_000_000;
const WIDTH_MIN: usize = 10;
const WIDTH_MAX: usize = 140;
const ADJUSTMENT: f64 = 1.45;

const COLOR_SOMA: Color = Color::RGB(0xBFBFBF);
const COLOR_DESCONTO: Color = Color::RGB(0xCCC0DA);
const COLOR_SALDO_RED: Color = Color::RGB(0xE6B8B7);
const COLOR_SALDO_GREEN: Color = Color::RGB(0xC4D79B);

// Thread-safe regular expression to identify CNPJ/CPF columns with minimized allocation overhead.
static REGEX_CNPJ_CPF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?ix)^(:?CNPJ|CPF)").unwrap());

// --- Enums and Context Management ---

/// Defines the semantic category of a worksheet, determining its layout behavior and visual presentation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SheetContext {
    /// Associated with item-level fiscal documents, applying layout shifts for CST identifiers.
    Itens,

    /// Represents the baseline, unaltered EFD data.
    EfdOriginal,

    /// Corresponds to post-audit analysis sheets, changing balance row colors to green.
    EfdAuditoria,
}

impl SheetContext {
    /// Determines whether this context corresponds to the "Itens de Docs Fiscais" scope.
    #[inline]
    pub fn is_itens(self) -> bool {
        matches!(self, Self::Itens)
    }

    /// Resolves the specific color used for background balance highlights.
    #[inline]
    pub fn balance_color(self) -> Color {
        match self {
            Self::EfdAuditoria => COLOR_SALDO_GREEN,
            _ => COLOR_SALDO_RED,
        }
    }

    /// Returns the static, unquoted string representation of this context.
    ///
    /// This is a zero-cost compiler-optimized mapping that requires no serialization libraries.
    #[inline]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Itens => "Itens de Docs Fiscais",
            Self::EfdOriginal => "EFD (original)",
            Self::EfdAuditoria => "EFD (após auditoria)",
        }
    }

    /// Resolves the context from a raw worksheet name.
    ///
    /// This searches for mapped strings as substrings within the input,
    /// making it robust against split-sheet indexes (e.g., "Itens de Docs Fiscais 2").
    pub fn from_name(name: &str) -> JoinResult<Self> {
        if name.contains(Self::Itens.as_str()) {
            Ok(Self::Itens)
        } else if name.contains(Self::EfdAuditoria.as_str()) {
            Ok(Self::EfdAuditoria)
        } else if name.contains(Self::EfdOriginal.as_str()) {
            Ok(Self::EfdOriginal)
        } else {
            Err(JoinError::Other(format!(
                "Failed to resolve SheetContext from name: {name}"
            )))
        }
    }
}

/// Identifiers for column formatting styles.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FormatKey {
    Default,
    Center,
    Value,
    Aliquota,
    Date,
}

impl FormatKey {
    /// Returns default definitions mapping layout variants to formatting rules.
    pub fn new() -> [(FormatKey, FormatAlign, Option<&'static str>); 5] {
        [
            (FormatKey::Default, FormatAlign::Left, None),
            (FormatKey::Center, FormatAlign::Center, None),
            (FormatKey::Value, FormatAlign::Right, Some("#,##0.00")),
            (FormatKey::Aliquota, FormatAlign::Center, Some("0.0000")),
            (FormatKey::Date, FormatAlign::Center, Some("dd/mm/yyyy")),
        ]
    }
}

/// Logical row styles representing standard entries or special computed summaries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RowStyle {
    Normal,
    Soma,
    Desconto,
    Saldo,
}

impl RowStyle {
    /// Maps each logical state to its respective background highlight color.
    pub fn styles_with_colors(color_saldo: Color) -> [(RowStyle, Option<Color>); 4] {
        [
            (RowStyle::Normal, None),
            (RowStyle::Soma, Some(COLOR_SOMA)),
            (RowStyle::Desconto, Some(COLOR_DESCONTO)),
            (RowStyle::Saldo, Some(color_saldo)),
        ]
    }
}

/// A cache registry managing structural permutations of cell styles.
///
/// Pre-computes cell configurations to avoid expensive reallocation during row formatting iterations.
#[derive(Debug, Default)]
pub struct FormatRegistry {
    matrix: HashMap<(FormatKey, RowStyle), Format>,
}

impl FormatRegistry {
    /// Instantiates a new registry mapping all permutations of column key types and row highlights.
    pub fn new(color_saldo: Color) -> Self {
        let mut matrix = HashMap::new();
        let keys = FormatKey::new();
        let styles = RowStyle::styles_with_colors(color_saldo);

        for (f_key, align, num_fmt) in keys {
            for (r_style, color) in styles {
                let mut f = Format::new()
                    .set_align(align)
                    .set_align(FormatAlign::VerticalCenter)
                    .set_font_size(FONT_SIZE);

                if let Some(fmt) = num_fmt {
                    f = f.set_num_format(fmt);
                }
                if let Some(c) = color {
                    f = f.set_background_color(c);
                }

                matrix.insert((f_key, r_style), f);
            }
        }
        Self { matrix }
    }

    /// Retrieves the reference to a registered cell format match.
    #[inline]
    fn get_format(&self, f_key: FormatKey, r_style: RowStyle) -> Option<&Format> {
        self.matrix.get(&(f_key, r_style))
    }

    /// Provides standard header row formatting styles.
    pub fn header() -> Format {
        Format::new()
            .set_text_wrap()
            .set_align(FormatAlign::Center)
            .set_align(FormatAlign::VerticalCenter)
            .set_font_size(HEADER_FONT_SIZE)
    }
}

/// Container struct designed to hold properties for concurrent sheet construction.
struct WorksheetTask {
    slice: DataFrame,
    sheet_name: String,
}

impl WorksheetTask {
    /// Partitions a source `DataFrame` into multiple `WorksheetTask` segments based on the
    /// maximum structural rows allowed per Excel sheet.
    pub fn partition(df: &DataFrame, context: SheetContext) -> Vec<Self> {
        let number_of_rows = df.height();
        let number_of_sheets = number_of_rows.div_ceil(MAX_NUMBER_OF_ROWS);
        let base_name = context.as_str();

        eprintln!(
            "Info: Dataset '{}' contains {} rows. Partitioning into {} worksheet chunk(s)...",
            base_name, number_of_rows, number_of_sheets
        );

        let mut tasks = Vec::with_capacity(number_of_sheets);

        for i in 0..number_of_sheets {
            let offset = (i * MAX_NUMBER_OF_ROWS) as i64;
            let slice = df.slice(offset, MAX_NUMBER_OF_ROWS);
            let sheet_name = determine_sheet_name(base_name, i);

            tasks.push(Self { slice, sheet_name });
        }

        tasks
    }
}

// --- Main Structural Functions ---

/// Orchestrates formatting DataFrames and generating target Excel worksheets.
///
/// This coordinator validates that exactly three DataFrames are provided, mapping each to its
/// designated structural context. It uses Rayon to prepare, split, and format data chunks
/// across multiple CPU threads before sequentially writing them to a single workbook.
///
/// # Errors
///
/// Returns a [`JoinError::InvalidDataFrameCount`] if the slice does not contain exactly
/// 3 DataFrames. Also returns standard inner formatting and IO errors.
pub fn write_xlsx(dfs: &[DataFrame]) -> JoinResult<()> {
    let output = "EFD Contribuicoes x Documentos Fiscais.xlsx";
    println!("Generating Excel file: {output}\n");

    // Perform an explicit check to avoid out-of-bounds panics on the configs array layout.
    if dfs.len() != 3 {
        return Err(JoinError::InvalidDataFrameCount {
            expected: 3,
            found: dfs.len(),
        });
    }

    let mut workbook = Workbook::new();

    // Map each dataframe to its corresponding sheet context based on its static position.
    let configs = [
        (&dfs[0], SheetContext::Itens),
        (&dfs[1], SheetContext::EfdOriginal),
        (&dfs[2], SheetContext::EfdAuditoria),
    ];

    // Partition all dataframes into a unified list of tasks.
    eprintln!("Info: Preparing worksheet tasks...");
    let tasks: Vec<WorksheetTask> = configs
        .iter()
        .flat_map(|(df, context)| WorksheetTask::partition(df, *context))
        .collect();

    // Process worksheet configurations in parallel using Rayon.
    eprintln!("Info: Starting concurrent worksheet generation across thread pool...");
    let worksheets_result: Result<Vec<Worksheet>, JoinError> = tasks
        .into_par_iter()
        .map(|task| {
            eprintln!("Info: Thread working on worksheet '{}'...", task.sheet_name);
            make_worksheet(&task.slice, &task.sheet_name)
        })
        .collect();

    let worksheets = worksheets_result.map_err(|err| {
        eprintln!("Error occurred during concurrent worksheet formatting: {err}");
        err
    })?;

    eprintln!("Info: Consolidating generated sheets into the final workbook registry...");
    for worksheet in worksheets {
        workbook.push_worksheet(worksheet);
    }

    eprintln!("Info: Writing workbook data to disk...");
    workbook.save(output).map_err(|err| {
        eprintln!("Error: Failed to write Excel file to target path '{output}': {err}");
        JoinError::from(err)
    })?;

    eprintln!("Success: Excel document successfully generated and saved to '{output}'\n");
    Ok(())
}

/// Generates and styles a worksheet from a target DataFrame chunk.
///
/// The function parses the worksheet name to establish context, formats cell transitions,
/// executes background auto-fits, and styles rows safely inside worker threads.
pub fn make_worksheet(df: &DataFrame, sheet_name: &str) -> JoinResult<Worksheet> {
    // 1. Resolve context and format the data-frame.
    let context = SheetContext::from_name(sheet_name)?;
    let df_formatted: DataFrame = format_dataframe(df, context.is_itens())?;
    let df_to_excel: DataFrame = format_to_excel(&df_formatted)?;

    let mut worksheet = Worksheet::new();
    let headers = df_to_excel.get_column_names();

    // Pre-calculate column style configurations to avoid nested loops.
    let col_configs: Vec<FormatKey> = headers
        .iter()
        .map(|&name| get_format_key(name, context))
        .collect();

    // Configure structural baseline row formats and names.
    worksheet.set_name(sheet_name)?;
    worksheet.set_row_height(0, 64)?;
    worksheet.set_row_format(0, &FormatRegistry::header())?;

    let registry = FormatRegistry::new(context.balance_color());

    // Apply primary style overrides down respective column blocks.
    for (i, &f_key) in col_configs.iter().enumerate() {
        if let Some(fmt) = registry.get_format(f_key, RowStyle::Normal) {
            worksheet.set_column_format(i as u16, fmt)?;
        }
    }

    // 2. Write structural contents via specialized Polars writer wrapper.
    let mut writer = PolarsExcelWriter::new();

    if let Some(date_format) = registry.get_format(FormatKey::Date, RowStyle::Normal) {
        writer.set_date_format(date_format);
    }

    writer.set_freeze_panes(1, 0);
    writer.write_dataframe_to_worksheet(&df_to_excel, &mut worksheet, 0, 0)?;

    // 3. Evaluate row contents and apply custom summary styles.
    apply_conditional_styles(&df_to_excel, &mut worksheet, &registry, &col_configs)?;

    // 4. Calculate dynamic column widths inside worker threads.
    auto_fit(&df_to_excel, &mut worksheet)?;

    Ok(worksheet)
}

// --- Pure Helper Functions ---

/// Computes a unique name for a worksheet chunk based on its slice index.
///
/// Indexes are 0-based. The first chunk (index 0) retains the base name.
/// Subsequent chunks append the 1-based index suffix.
#[inline]
fn determine_sheet_name(base_name: &str, chunk_index: usize) -> String {
    if chunk_index == 0 {
        base_name.to_string()
    } else {
        format!("{} {}", base_name, chunk_index + 1)
    }
}

/// Decides column alignment and styling keys based on header name matches and context.
fn get_format_key(name: &str, context: SheetContext) -> FormatKey {
    if context.is_itens() && (name.contains("CST") || name.contains("Situação Tributária")) {
        return FormatKey::Default;
    }

    // Center alignment rules
    if REGEX_CNPJ_CPF.is_match(name)
        || name.contains("Código")
        || name.contains("Registro")
        || name.contains("Chave do Documento")
        || name.contains("Chave da Nota Fiscal Eletrônica")
        || name.contains("Ano do Período de Apuração")
        || name.contains("Trimestre do Período de Apuração")
    {
        return FormatKey::Center;
    }

    // Numeric formatting rules for monetary values
    if name.contains("Valor")
        || name.contains("ICMS")
        || name.contains("ISS")
        || name.contains("Crédito vinculado à Receita Bruta Não Cumulativa")
        || name.contains("Crédito vinculado à Receita Bruta Cumulativa")
        || name.contains("Crédito vinculado à Receita Bruta Total")
    {
        return FormatKey::Value;
    }

    // Rate representation formatting rules
    if name.contains("PIS: Alíquota ad valorem")
        || name.contains("COFINS: Alíquota ad valorem")
        || name.contains("Alíquota de PIS/PASEP")
        || name.contains("Alíquota de COFINS")
    {
        return FormatKey::Aliquota;
    }

    // Standard date parsing patterns
    if name.contains("Data da Emissão")
        || name.contains("Data da Entrada")
        || name.contains("Período de Apuração")
        || name.contains("Dia da Emissão")
    {
        return FormatKey::Date;
    }

    FormatKey::Default
}

/// Applies contextual row backgrounds matching metadata keywords like totals or balances.
fn apply_conditional_styles(
    df: &DataFrame,
    worksheet: &mut Worksheet,
    registry: &FormatRegistry,
    col_keys: &[FormatKey],
) -> JoinResult<()> {
    let nature_idx = df.get_column_names().iter().position(|n| {
        n.as_str()
            .contains("Natureza da Base de Cálculo dos Créditos")
    });

    let nature_idx = match nature_idx {
        Some(idx) => idx,
        None => return Ok(()),
    };

    let ca = df.columns()[nature_idx].as_materialized_series().str()?;

    ca.iter()
        .enumerate()
        .try_for_each(|(i, opt_val)| -> JoinResult<()> {
            let style = match opt_val {
                Some(s) if s.contains("(Soma)") => RowStyle::Soma,
                Some(s) if s.contains("Crédito Disponível após Descontos") => RowStyle::Desconto,
                Some(s) if s.contains("Saldo de Crédito Passível") => RowStyle::Saldo,
                _ => RowStyle::Normal,
            };

            if style != RowStyle::Normal {
                let row_idx = (i + 1) as u32;
                for (col_idx, &f_key) in col_keys.iter().enumerate() {
                    if let Some(fmt) = registry.get_format(f_key, style) {
                        // Apply custom row background based on row classification
                        // while maintaining custom numeric formats and column alignments.
                        worksheet.set_cell_format(row_idx, col_idx as u16, fmt)?;
                    }
                }
            }
            Ok(())
        })?;

    Ok(())
}

/// Estimates appropriate character column widths to prevent text clipping.
///
/// This calculates character lengths across columns in parallel. Since column tasks
/// are mapped directly, no thread synchronization (`AtomicUsize`) is needed.
fn auto_fit(df: &DataFrame, worksheet: &mut Worksheet) -> JoinResult<()> {
    let widths: Vec<usize> = df
        .columns()
        .par_iter()
        .map(|column| {
            let series = column.as_materialized_series();
            let col_name = series.name().as_str();

            // Allow long header titles to wrap comfortably.
            let header_len = col_name.chars().count().div_ceil(4);
            let mut max_w = WIDTH_MIN.max(header_len);

            for val in series.iter() {
                let text = val.to_string();
                let mut w = text.chars().count();

                // Apply proportional adjustment for long descriptions.
                if [
                    "Natureza da Base de Cálculo dos Créditos",
                    "Tipo de Crédito",
                    "Código de Situação Tributária (CST)",
                ]
                .contains(&col_name)
                {
                    w = (w * 80) / 100;
                }

                if w > max_w {
                    max_w = w;
                }
                if max_w > WIDTH_MAX {
                    max_w = WIDTH_MAX;
                    break;
                }
            }
            max_w
        })
        .collect();

    for (i, width) in widths.into_iter().enumerate() {
        let final_width = (width as f64) * ADJUSTMENT;
        worksheet.set_column_width(i as u16, final_width)?;
    }
    Ok(())
}

/// Adjusts column datatypes and truncates text values to fit Excel cell character limits.
fn format_to_excel(df: &DataFrame) -> PolarsResult<DataFrame> {
    let exprs: Vec<Expr> = df
        .get_column_names()
        .iter()
        .map(|name| {
            let name_str = name.as_str();
            let dtype = df
                .column(name_str)
                .expect("Target column should exist")
                .dtype();

            match dtype {
                DataType::Int64 => col(name_str).cast(DataType::Int32),
                DataType::UInt64 => col(name_str).cast(DataType::UInt32),
                // Safe truncation mapping for Excel character limitations
                DataType::String => col(name_str).str().slice(lit(0), lit(32767)),
                _ => col(name_str),
            }
        })
        .collect();

    df.clone().lazy().with_columns(exprs).collect()
}
