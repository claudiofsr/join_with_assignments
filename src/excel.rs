//! # Excel Worksheet Generation Module
//!
//! Este módulo lida com a exportação de DataFrames estruturados para arquivos Excel formatados,
//! oferecendo diferentes estratégias de processamento de acordo com o limite de memória do sistema.

use clap::ValueEnum;
use claudiofsr_lib::Colors;
use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use rust_xlsxwriter::{Color, Workbook, Worksheet};
use serde::{Deserialize, Serialize};
use std::sync::LazyLock;

use crate::{
    JoinError, JoinResult, PolarsExcelWriter,
    all_data::AllData,
    format::{COLOR_SALDO_GREEN, COLOR_SALDO_RED, FormatKey, FormatRegistry, RowStyle},
    format_dataframe,
};

const MAX_NUMBER_OF_ROWS: usize = 1_000_000;
const WIDTH_MIN: usize = 10;
const WIDTH_MAX: usize = 140;
const ADJUSTMENT: f64 = 1.45;

static REGEX_CNPJ_CPF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?ix)^(:?CNPJ|CPF)").unwrap());

/// Estratégias de consumo de memória para a geração do documento Excel.
#[derive(Default, ValueEnum, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExcelMemoryMode {
    /// Perfil de memória constante que grava progressivamente os dados em disco.
    ConstantMemory,
    /// Perfil de baixa memória com uso de strings compartilhadas.
    LowMemory,
    /// Processa todo o Workbook em memória através de execução paralela do Rayon.
    #[default]
    InMemory,
}

/// Define a categoria e o comportamento visual de cada aba de dados.
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

/// Orquestra a geração final do arquivo Excel, distribuindo conforme a estratégia de memória.
pub fn write_xlsx(dfs: &[DataFrame], memory_mode: Option<ExcelMemoryMode>) -> JoinResult<()> {
    let output = "EFD Contribuicoes x Documentos Fiscais.xlsx";
    println!("Generating Excel file: {output}\n");

    // Validação estrita para evitar pânico de índice fora dos limites
    if dfs.len() != 3 {
        return Err(JoinError::InvalidDataFrameCount {
            expected: 3,
            found: dfs.len(),
        });
    }

    let mut workbook = Workbook::new();
    let all_data = AllData::new(&dfs[0], &dfs[1], &dfs[2]);

    match memory_mode.unwrap_or_default() {
        ExcelMemoryMode::InMemory => {
            eprintln!("Info: Starting concurrent worksheet generation across thread pool...");

            all_data
                .generate_worksheets_in_parallel()?
                .into_iter()
                .for_each(|worksheet| {
                    workbook.push_worksheet(worksheet);
                });
        }
        mode => {
            eprintln!(
                "Info: Starting sequential worksheet generation (memory mode: {:?})...",
                mode
            );

            all_data.write_sequentially_to_workbook(&mut workbook, mode)?;
        }
    }

    eprintln!("Info: Writing workbook data to disk...\n");

    workbook
        .save(output)
        .map_err(|err| JoinError::ExcelWriteError {
            path: output.to_string(),
            source: err,
        })?;

    eprintln!(
        "{}: Excel document saved to '{}'\n",
        "Success".green(),
        output.blue()
    );
    Ok(())
}

/// Processa um DataFrame de origem para criar coleções de Worksheets em paralelo.
pub fn process_sheet_type(df: &DataFrame, context: SheetContext) -> JoinResult<Vec<Worksheet>> {
    // df.is_empty()
    if df.height() == 0 || df.width() == 0 {
        return Ok(Vec::new());
    }

    let number_of_rows = df.height();
    let number_of_sheets = number_of_rows.div_ceil(MAX_NUMBER_OF_ROWS);
    let base_name = context.as_str();

    eprintln!(
        "Info: Dataset '{}' contains {} rows. Partitioning into {} worksheet chunk(s)...",
        base_name, number_of_rows, number_of_sheets
    );

    let worksheets = (0..number_of_sheets)
        .into_par_iter()
        .map(|k| {
            let offset = (k * MAX_NUMBER_OF_ROWS) as i64;
            let slice = df.slice(offset, MAX_NUMBER_OF_ROWS);
            let name = determine_sheet_name(context.as_str(), k);
            // Log informativo de execução paralela
            eprintln!("Info: Thread working on worksheet '{}'...", name);
            generate_worksheet(&slice, &name)
        })
        .collect::<JoinResult<Vec<_>>>()?;

    Ok(worksheets)
}

/// Processa um DataFrame sequencialmente, escrevendo blocos de dados em disco sob demanda.
pub fn process_sheet_type_sequential(
    workbook: &mut Workbook,
    df: &DataFrame,
    context: SheetContext,
    memory_mode: ExcelMemoryMode,
) -> JoinResult<()> {
    // df.is_empty()
    if df.height() == 0 || df.width() == 0 {
        return Ok(());
    }

    let number_of_rows = df.height();
    let number_of_sheets = number_of_rows.div_ceil(MAX_NUMBER_OF_ROWS);
    let base_name = context.as_str();

    eprintln!(
        "Info: Dataset '{}' contains {} rows. Partitioning into {} worksheet chunk(s)...",
        base_name, number_of_rows, number_of_sheets
    );

    for k in 0..number_of_sheets {
        let offset = (k * MAX_NUMBER_OF_ROWS) as i64;
        let slice = df.slice(offset, MAX_NUMBER_OF_ROWS);
        let name = determine_sheet_name(context.as_str(), k);

        // Log informativo de execução sequencial
        eprintln!("Info: Writing worksheet '{}' sequentially...", name);

        let worksheet = match memory_mode {
            ExcelMemoryMode::ConstantMemory => workbook.add_worksheet_with_constant_memory(),
            ExcelMemoryMode::LowMemory => workbook.add_worksheet_with_low_memory(),
            ExcelMemoryMode::InMemory => workbook.add_worksheet(),
        };

        worksheet.set_name(&name)?;
        populate_worksheet_data(worksheet, &slice, &name)?;
    }

    Ok(())
}

/// Inicialização de worksheet isolada.
pub fn generate_worksheet(df: &DataFrame, sheet_name: &str) -> JoinResult<Worksheet> {
    let mut worksheet = Worksheet::new();
    worksheet.set_name(sheet_name)?;
    populate_worksheet_data(&mut worksheet, df, sheet_name)?;
    Ok(worksheet)
}

/// Preenche a planilha de destino aplicando regras de formatação de colunas e estilos de realce.
pub fn populate_worksheet_data(
    worksheet: &mut Worksheet,
    df: &DataFrame,
    sheet_name: &str,
) -> JoinResult<()> {
    let context = SheetContext::from_name(sheet_name)?;
    let df_formatted: DataFrame = format_dataframe(df, context.is_itens())?;
    let df_to_excel: DataFrame = format_to_excel(&df_formatted)?;

    let headers = df_to_excel.get_column_names();
    let col_configs: Vec<FormatKey> = headers
        .iter()
        .map(|&name| get_format_key(name, context))
        .collect();

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
    writer.write_dataframe_to_worksheet(&df_to_excel, worksheet, 0, 0)?;

    apply_conditional_styles(&df_to_excel, worksheet, &registry, &col_configs)?;
    auto_fit(&df_to_excel, worksheet)?;

    Ok(())
}

/// Define o nome de cada bloco/aba baseado em um índice de divisão.
#[inline]
pub fn determine_sheet_name(base_name: &str, chunk_index: usize) -> String {
    if chunk_index == 0 {
        base_name.to_string()
    } else {
        format!("{} {}", base_name, chunk_index + 1)
    }
}

/// Resolve a chave de formatação correspondente ao cabeçalho.
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

/// Aplica cores de fundo condicionalmente (Totais, Saldos e Descontos).
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

/// Executa o cálculo de largura aproximada de coluna em paralelo sem concorrência de travas.
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

/// Trunca dados extensos de texto e redefine os tipos de inteiro mapeados para o Excel.
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
