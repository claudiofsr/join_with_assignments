use polars::prelude::*;
use rayon::prelude::*;
use regex::Regex;
use rust_xlsxwriter::{Color, Format, FormatAlign, Workbook, Worksheet};
use std::sync::{
    LazyLock,
    atomic::{AtomicUsize, Ordering},
};

use crate::{PolarsXlsxWriter, format_dataframe};

// --- Constantes Estéticas ---
const FONT_SIZE: f64 = 11.0;
const HEADER_FONT_SIZE: f64 = 10.0;
const MAX_NUMBER_OF_ROWS: usize = 1_000_000;
const WIDTH_MIN: usize = 8;
const WIDTH_MAX: usize = 140;
const ADJUSTMENT: f64 = 1.1;

const COLOR_SOMA: Color = Color::RGB(0xBFBFBF);
const COLOR_DESCONTO: Color = Color::RGB(0xCCC0DA);
const COLOR_SALDO_RED: Color = Color::RGB(0xE6B8B7);
const COLOR_SALDO_GREEN: Color = Color::RGB(0xC4D79B);

// Regex para identificação de colunas (estático para performance)
static REGEX_CNPJ_CPF: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?ix)^(:?CNPJ|CPF)").unwrap());

// --- Enums e Gerenciamento de Estilos ---

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(usize)]
enum RowStyle {
    Normal = 0,
    Soma = 1,
    Desconto = 2,
    Saldo = 3,
}

#[derive(Debug, Clone, Copy)]
#[repr(usize)]
enum FormatKey {
    Default = 0,
    Center = 1,
    Value = 2,
    Aliq = 3,
    Date = 4,
}

struct FormatGroup {
    formats: [Format; 4],
}

impl FormatGroup {
    fn new(base: Format, saldo_color: Color) -> Self {
        Self {
            formats: [
                base.clone(),
                base.clone().set_background_color(COLOR_SOMA),
                base.clone().set_background_color(COLOR_DESCONTO),
                base.clone().set_background_color(saldo_color),
            ],
        }
    }
    #[inline]
    fn get(&self, style: RowStyle) -> &Format {
        &self.formats[style as usize]
    }
}

struct FormatRegistry {
    groups: [FormatGroup; 5],
}

impl FormatRegistry {
    fn new(saldo_color: Color) -> Self {
        let base_c = Format::new()
            .set_align(FormatAlign::Center)
            .set_align(FormatAlign::VerticalCenter)
            .set_font_size(FONT_SIZE);
        let base_l = Format::new()
            .set_align(FormatAlign::Left)
            .set_align(FormatAlign::VerticalCenter)
            .set_font_size(FONT_SIZE);

        let keys = [
            base_l.clone(),                              // Default
            base_c.clone(),                              // Center
            base_l.clone().set_num_format("#,##0.00"),   // Value
            base_c.clone().set_num_format("0.0000"),     // Aliq
            base_c.clone().set_num_format("dd/mm/yyyy"), // Date
        ];

        Self {
            groups: keys.map(|f| FormatGroup::new(f, saldo_color)),
        }
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
pub fn write_xlsx(dfs: &[DataFrame]) -> PolarsResult<()> {
    let output = "EFD Contribuicoes x Documentos Fiscais.xlsx";
    println!("Write DataFrames to {output:?}\n");

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
                name.into()
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
pub fn make_worksheet(df: &DataFrame, sheet_name: &str) -> PolarsResult<Worksheet> {
    // Adicioanr Descrição de  CST apenas na aba de "Itens de Docs Fiscais"
    let is_itens = sheet_name.contains("Itens de Docs Fiscais");
    let df_formated: DataFrame = format_dataframe(df, is_itens)?;
    let df_to_excel: DataFrame = format_to_excel(&df_formated)?;

    dbg!(&df_to_excel);

    let mut worksheet = Worksheet::new();

    // 1. Determinação de Cores e Registro
    let saldo_color = if sheet_name.contains("auditoria") {
        COLOR_SALDO_GREEN
    } else {
        COLOR_SALDO_RED
    };
    let registry = FormatRegistry::new(saldo_color);

    // --- AJUSTE: Formato de Data para o Writer ---
    let date_format = registry.groups[FormatKey::Date as usize]
        .get(RowStyle::Normal)
        .clone();

    let headers = df_to_excel.get_column_names();

    // 2. Estética do Cabeçalho
    let header_fmt = Format::new()
        .set_align(FormatAlign::Center)
        .set_align(FormatAlign::VerticalCenter)
        .set_text_wrap()
        .set_font_size(HEADER_FONT_SIZE);

    worksheet
        .set_name(sheet_name)?
        .set_row_height(0, 64)?
        .set_row_format(0, &header_fmt)?;

    // 3. Formatação Base das Colunas (Alignment e NumFormat)
    // Aplicamos o formato "Normal" em nível de coluna antes de escrever os dados
    for (i, &name) in headers.iter().enumerate() {
        let key = get_format_key(name.as_str(), is_itens);
        worksheet.set_column_format(
            i as u16,
            registry.groups[key as usize].get(RowStyle::Normal),
        )?;
    }

    // 4. Escrita dos Dados
    let mut writer = PolarsXlsxWriter::new();
    writer
        .set_date_format(date_format)
        .set_freeze_panes(1, 0)
        .write_dataframe_to_worksheet(&df_to_excel, &mut worksheet, 0, 0)?;

    // 5. Aplicar Cores Condicionais (Linhas de Soma/Saldo)
    let col_groups: Vec<&FormatGroup> = headers
        .iter()
        .map(|h| &registry.groups[get_format_key(h.as_str(), is_itens) as usize])
        .collect();

    apply_conditional_styles(&df_to_excel, &mut worksheet, &col_groups)?;

    // 6. Auto-ajuste de Colunas
    auto_fit(&df_to_excel, &mut worksheet)?;

    Ok(worksheet)
}

// --- Funções Auxiliares de Lógica ---

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
        return FormatKey::Aliq;
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

fn apply_conditional_styles(
    df: &DataFrame,
    worksheet: &mut Worksheet,
    groups: &[&FormatGroup],
) -> PolarsResult<()> {
    // Localiza a coluna que define o comportamento da linha (Natureza)
    let nature_idx = df
        .get_column_names()
        .iter()
        .position(|n| {
            n.as_str()
                .contains("Natureza da Base de Cálculo dos Créditos")
        })
        .unwrap_or(0);

    let ca = df.get_columns()[nature_idx]
        .as_materialized_series()
        .str()?;

    ca.into_iter().enumerate().for_each(|(i, opt_val)| {
        let style = match opt_val {
            Some(s) if s.contains("(Soma)") => RowStyle::Soma,
            Some(s) if s.contains("Crédito Disponível após Descontos") => RowStyle::Desconto,
            Some(s) if s.contains("Saldo de Crédito Passível") => RowStyle::Saldo,
            _ => RowStyle::Normal,
        };

        if style != RowStyle::Normal {
            let row_idx = (i + 1) as u32;
            for (col_idx, group) in groups.iter().enumerate() {
                // Sobrescreve o formato da célula para aplicar a cor de fundo,
                // mantendo o alinhamento/formato numérico da coluna.
                let _ = worksheet.set_cell_format(row_idx, col_idx as u16, group.get(style));
            }
        }
    });
    Ok(())
}

fn auto_fit(df: &DataFrame, worksheet: &mut Worksheet) -> PolarsResult<()> {
    let headers = df.get_column_names();
    let widths: Vec<_> = headers
        .iter()
        .map(|h| AtomicUsize::new(WIDTH_MIN.max(h.as_str().chars().count().div_ceil(4))))
        .collect();

    df.get_columns()
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
        let _ = worksheet.set_column_width(i as u16, final_width);
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
