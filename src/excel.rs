use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use rust_xlsxwriter::{Color, Format, FormatAlign, Workbook, Worksheet};
use std::{
    collections::HashMap,
    sync::{
        LazyLock,
        atomic::{AtomicUsize, Ordering},
    },
};

use crate::{JoinResult, PolarsXlsxWriter, format_dataframe};

// --- Constantes Estéticas ---
const FONT_SIZE: f64 = 11.0;
const HEADER_FONT_SIZE: f64 = 10.0;
const MAX_NUMBER_OF_ROWS: usize = 1_000_000;
const WIDTH_MIN: usize = 8;
const WIDTH_MAX: usize = 140;
const ADJUSTMENT: f64 = 1.12;

const COLOR_SOMA: Color = Color::RGB(0xBFBFBF);
const COLOR_DESCONTO: Color = Color::RGB(0xCCC0DA);
const COLOR_SALDO_RED: Color = Color::RGB(0xE6B8B7);
const COLOR_SALDO_GREEN: Color = Color::RGB(0xC4D79B);

// Regex para identificação de colunas (estático para performance)
static REGEX_CNPJ_CPF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?ix)^(:?CNPJ|CPF)").unwrap());

// --- Enums e Gerenciamento de Estilos ---

/// Identificadores para tipos de formatação de coluna.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum FormatKey {
    Default,
    Center,
    Value,
    Aliquota,
    Date,
}

impl FormatKey {
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

/// Estados de estilo para uma linha inteira.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RowStyle {
    Normal,
    Soma,
    Desconto,
    Saldo,
}

impl RowStyle {
    pub fn new(color_saldo: Color) -> [(RowStyle, Option<Color>); 4] {
        [
            (RowStyle::Normal, None),
            (RowStyle::Soma, Some(COLOR_SOMA)),
            (RowStyle::Desconto, Some(COLOR_DESCONTO)),
            (RowStyle::Saldo, Some(color_saldo)),
        ]
    }
}

/// Gerenciador central de formatos que mapeia (Tipo de Coluna x Estilo de Linha).
#[derive(Debug, Default)]
pub struct FormatRegistry {
    matrix: HashMap<(FormatKey, RowStyle), Format>,
}

impl FormatRegistry {
    /// Cria um novo registro com todos os formatos pré-calculados.
    pub fn new(color_saldo: Color) -> Self {
        let mut matrix = HashMap::new();
        let keys = FormatKey::new();
        let styles = RowStyle::new(color_saldo);

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

    /// Obtém um formato específico da matriz.
    #[inline]
    fn get_format(&self, f_key: FormatKey, r_style: RowStyle) -> Option<&Format> {
        self.matrix.get(&(f_key, r_style))
    }

    /// Atalho para formato de cabeçalho.
    pub fn header() -> Format {
        Format::new()
            .set_text_wrap()
            .set_align(FormatAlign::Center)
            .set_align(FormatAlign::VerticalCenter)
            .set_font_size(HEADER_FONT_SIZE)
    }
}

// --- Funções Principais ---

/// 1. Write Dataframes to Worksheets;
///
/// 2. Push Worksheets to Workbook;
///
/// 3. Write Workbook to xlsx Excel file.
///
/// <https://crates.io/crates/polars_excel_writer>
///
/// <https://github.com/jmcnamara/polars_excel_writer/issues/4>
pub fn write_xlsx(dfs: &[DataFrame]) -> JoinResult<()> {
    let output = "EFD Contribuicoes x Documentos Fiscais.xlsx";
    println!("Gerando arquivo Excel: {output}\n");

    let mut workbook = Workbook::new();

    let configs = [
        (&dfs[0], "Itens de Docs Fiscais"),
        (&dfs[1], "EFD (original)"),
        (&dfs[2], "EFD (após auditoria)"),
    ];

    for (df, name) in configs {
        let number_of_rows = df.height();
        let number_of_sheet = number_of_rows.div_ceil(MAX_NUMBER_OF_ROWS);

        for i in 0..number_of_sheet {
            let offset = (i * MAX_NUMBER_OF_ROWS) as i64;
            let slice = df.slice(offset, MAX_NUMBER_OF_ROWS);
            let sheet_name = if i == 0 {
                name.to_string()
            } else {
                format!("{} {}", name, i + 1)
            };

            let worksheet = make_worksheet(&slice, &sheet_name)?;
            workbook.push_worksheet(worksheet);
        }
    }

    workbook.save(output)?;
    Ok(())
}

/// Write Dataframe to xlsx Excel file
///
/// <https://crates.io/crates/polars_excel_writer>
///
/// <https://github.com/jmcnamara/polars_excel_writer/issues/4>
pub fn make_worksheet(df: &DataFrame, sheet_name: &str) -> JoinResult<Worksheet> {
    // Adicioanr Descrição de  CST apenas na aba de "Itens de Docs Fiscais"
    let is_itens = sheet_name.contains("Itens de Docs Fiscais");
    let is_auditoria = sheet_name.contains("auditoria");

    // Transformações iniciais do DataFrame
    let df_formated: DataFrame = format_dataframe(df, is_itens)?;
    let df_to_excel: DataFrame = format_to_excel(&df_formated)?;

    dbg!(&df_to_excel);

    let mut worksheet = Worksheet::new();

    // 1. Determinação de Cores e Registro
    let color_saldo = if is_auditoria {
        COLOR_SALDO_GREEN
    } else {
        COLOR_SALDO_RED
    };

    let headers = df_to_excel.get_column_names();

    // Mapeamento de tipos de coluna (cacheado para evitar re-match em cada linha)
    let col_configs: Vec<FormatKey> = headers
        .iter()
        .map(|&name| get_format_key(name, is_itens))
        .collect();

    // 1. Setup básico da Worksheet
    worksheet
        .set_name(sheet_name)?
        .set_row_height(0, 64)?
        .set_row_format(0, &FormatRegistry::header())?;

    let registry = FormatRegistry::new(color_saldo);

    // 2. Aplicar Formatação Base nas Colunas
    for (i, &f_key) in col_configs.iter().enumerate() {
        if let Some(fmt) = registry.get_format(f_key, RowStyle::Normal) {
            worksheet.set_column_format(i as u16, fmt)?;
        }
    }

    // 3. Escrita dos Dados via PolarsXlsxWriter
    let mut writer = PolarsXlsxWriter::new();

    if let Some(date_format) = registry.get_format(FormatKey::Date, RowStyle::Normal) {
        writer
            .set_date_format(date_format)
            .set_freeze_panes(1, 0)
            .write_dataframe_to_worksheet(&df_to_excel, &mut worksheet, 0, 0)?;
    }

    // 4. Estilos Condicionais (Linhas de Soma/Saldo)
    apply_conditional_styles(&df_to_excel, &mut worksheet, &registry, &col_configs)?;

    // 5. Ajuste de Largura (Funcional e Paralelo)
    auto_fit(&df_to_excel, &mut worksheet)?;

    Ok(worksheet)
}

// --- Funções Auxiliares de Lógica ---

/// Identifica a chave de formatação baseada no nome da coluna.
fn get_format_key(name: &str, is_itens_context: bool) -> FormatKey {
    // Aplica alinhamento à esquerda (Default) para CST apenas no contexto de Itens.
    // Note o uso de 'is_itens_context' como guarda (guard clause).
    if is_itens_context && (name.contains("CST") || name.contains("Situação Tributária")) {
        return FormatKey::Default;
    }

    // Regras de Centralização (Baseadas na configuração antiga)
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

    // Regras de Valores Financeiros
    if name.contains("Valor")
        || name.contains("ICMS")
        || name.contains("Crédito vinculado à Receita Bruta Não Cumulativa")
        || name.contains("Crédito vinculado à Receita Bruta Cumulativa")
        || name.contains("Crédito vinculado à Receita Bruta Total")
    {
        return FormatKey::Value;
    }

    // Regras de Alíquotas
    if name.contains("PIS: Alíquota ad valorem")
        || name.contains("COFINS: Alíquota ad valorem")
        || name.contains("Alíquota de PIS/PASEP")
        || name.contains("Alíquota de COFINS")
    {
        return FormatKey::Aliquota;
    }

    // Regras de Datas
    if name.contains("Data da Emissão")
        || name.contains("Data da Entrada")
        || name.contains("Período de Apuração")
        || name.contains("Dia da Emissão")
    {
        return FormatKey::Date;
    }

    FormatKey::Default
}

/// Aplica cores de fundo em linhas inteiras baseado no conteúdo de colunas específicas.
fn apply_conditional_styles(
    df: &DataFrame,
    worksheet: &mut Worksheet,
    registry: &FormatRegistry,
    col_keys: &[FormatKey],
) -> JoinResult<()> {
    // Localiza a coluna que define o comportamento da linha (Natureza)
    let nature_idx = df
        .get_column_names()
        .iter()
        .position(|n| {
            n.as_str()
                .contains("Natureza da Base de Cálculo dos Créditos")
        })
        .unwrap_or(0);

    let ca = df.columns()[nature_idx].as_materialized_series().str()?;

    ca.into_iter()
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
                        // Sobrescreve o formato da célula para aplicar a cor de fundo,
                        // mantendo o alinhamento/formato numérico da coluna.
                        worksheet.set_cell_format(row_idx, col_idx as u16, fmt)?;
                    }
                }
            }

            Ok(())
        })?;

    Ok(())
}

fn auto_fit(df: &DataFrame, worksheet: &mut Worksheet) -> PolarsResult<()> {
    let headers = df.get_column_names();
    let widths: Vec<_> = headers
        .iter()
        .map(|h| AtomicUsize::new(WIDTH_MIN.max(h.as_str().chars().count().div_ceil(4))))
        .collect();

    df.columns()
        .par_iter()
        .enumerate()
        .for_each(|(col_idx, column)| {
            let series = column.as_materialized_series();
            let mut max_w = widths[col_idx].load(Ordering::Relaxed);
            let col_name = series.name().as_str();

            for val in series.iter() {
                let text = val.to_string();
                let mut w = text.chars().count();

                // Lógica de ajuste proporcional (natureza e tipo_cred costumam ser longos)
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
            widths[col_idx].fetch_max(max_w, Ordering::Relaxed);
        });

    for (i, atomic) in widths.into_iter().enumerate() {
        let final_width = (atomic.load(Ordering::Relaxed) as f64) * ADJUSTMENT;
        worksheet.set_column_width(i as u16, final_width)?;
    }
    Ok(())
}

fn format_to_excel(df: &DataFrame) -> PolarsResult<DataFrame> {
    let exprs: Vec<Expr> = df
        .get_column_names()
        .iter()
        .map(|name| {
            let name_str = name.as_str();
            let dtype = df.column(name_str).expect("Coluna deve existir").dtype();

            match dtype {
                DataType::Int64 => col(name_str).cast(DataType::Int32),
                DataType::UInt64 => col(name_str).cast(DataType::UInt32),
                // Truncamento seguro para o limite de caracteres do Excel
                DataType::String => col(name_str).str().slice(lit(0), lit(32767)),
                _ => col(name_str),
            }
        })
        .collect();

    df.clone().lazy().with_columns(exprs).collect()
}
