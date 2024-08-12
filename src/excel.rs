use std::collections::HashMap;

use crate::{
    coluna, format_dataframe, Arguments, DataFrameExtension, PolarsXlsxWriter,
    Side::{
        Left,
        Middle,
        //Right,
    },
};
use polars::prelude::*;
use regex::Regex;
use rust_xlsxwriter::{Format, FormatAlign, Workbook, Worksheet, XlsxColor};

const FONT_SIZE: f64 = 10.0;
const MAX_NUMBER_OF_ROWS: usize = 1_000_000;
const WIDTH_MIN: usize = 8;
const WIDTH_MAX: usize = 140;
const ADJUSTMENT: f64 = 1.1;

/// 1. Write Dataframes to Worksheets;
///
/// 2. Push Worksheets to Workbook;
///
/// 3. Write Workbook to xlsx Excel file.
///
/// <https://crates.io/crates/polars_excel_writer>
///
/// <https://github.com/jmcnamara/polars_excel_writer/issues/4>
pub fn write_xlsx(args: &Arguments, dfs: &[DataFrame]) -> PolarsResult<()> {
    let output = "EFD Contribuicoes x Documentos Fiscais.xlsx";
    println!("Write DataFrames to {output:?}\n");

    let dataframes: Vec<DataFrame> = dfs
        .iter()
        .filter_map(|df| {
            if let Some(true) = args.remove_null_columns {
                remove_null_columns(Frame::Data(df.clone())).ok()
            } else {
                Some(df.clone())
            }
        })
        .collect();

    let df_itens_de_docs_fiscais = &dataframes[0];
    let df_itens_de_docs_fiscais_result = &dataframes[1];
    let df_consolidacao_natureza_da_bcalc = &dataframes[2];
    let df_consolidacao_natureza_da_bcalc_result = &dataframes[3];

    // Add column from one dataframe to another.
    let joined: DataFrame =
        add_column_from_df_to_another(df_itens_de_docs_fiscais, df_itens_de_docs_fiscais_result)?;

    // Workbook with worksheets
    let mut workbook = Workbook::new();

    for (df, sheet_name) in [
        (&joined, "Itens de Docs Fiscais"),
        (df_consolidacao_natureza_da_bcalc, "EFD (original)"),
        (
            df_consolidacao_natureza_da_bcalc_result,
            "EFD (após auditoria)",
        ),
    ] {
        let number_of_rows = df.height();
        let number_of_sheet = number_of_rows.div_ceil(MAX_NUMBER_OF_ROWS);

        for index in 0..number_of_sheet {
            let offset = (MAX_NUMBER_OF_ROWS * index) as i64;
            let data = df.slice_par(offset, MAX_NUMBER_OF_ROWS);

            let mut new_name = sheet_name.to_string();
            if index >= 1 {
                new_name = format!("{} {}", sheet_name, index + 1);
            }

            let worksheet = make_worksheet(&data, &new_name)?;
            workbook.push_worksheet(worksheet);
        }
    }

    // Save the workbook to disk.
    workbook.save(output)?;

    Ok(())
}

/// Select a column from df_source and copy it to df_result
fn add_column_from_df_to_another(
    df_source: &DataFrame,
    df_result: &DataFrame,
) -> Result<DataFrame, PolarsError> {
    // Column names:
    let valor_bc: &str = coluna(Left, "valor_bc");
    let valor_bc_auditado: &str = coluna(Left, "valor_bc_auditado");

    let joined: DataFrame = df_result
        .clone()
        .rename(
            valor_bc,          // original_name
            valor_bc_auditado, // new_name
        )?
        .with_column(
            // Select a column and copy it to another df
            df_source.column(valor_bc)?.clone(),
        )?
        .sort_by_columns(Some("write_xlsx sort_by_columns:"))?;

    Ok(joined)
}

/// Write Dataframe to xlsx Excel file
///
/// <https://crates.io/crates/polars_excel_writer>
///
/// <https://github.com/jmcnamara/polars_excel_writer/issues/4>
pub fn make_worksheet(df: &DataFrame, sheet_name: &str) -> PolarsResult<Worksheet> {
    let df_formated: DataFrame = format_dataframe(df)?;
    let df_to_excel: DataFrame = format_to_excel(&df_formated)?;

    let mut worksheet = Worksheet::new();

    format_worksheet(df, &mut worksheet, sheet_name)?;

    // Date format must be applied to PolarsXlsxWriter.
    let fmt_date = Format::new()
        .set_align(FormatAlign::Center)
        .set_num_format("dd/mm/yyyy");

    // Write the dataframe to the worksheet using `PolarsXlsxWriter`.
    PolarsXlsxWriter::new()
        .set_date_format(fmt_date)
        // .set_autofit(true)
        // .set_float_format("#,##0.00")
        .set_freeze_panes(1, 0)
        .write_dataframe_to_worksheet(&df_to_excel, &mut worksheet, 0, 0)?;

    //worksheet.autofit();
    auto_fit(&df_to_excel, &mut worksheet)?;
    auto_color(&df_to_excel, &mut worksheet, sheet_name)?;

    Ok(worksheet)
}

/// Format worksheet
fn format_worksheet(
    df: &DataFrame,
    worksheet: &mut Worksheet,
    sheet_name: &str,
) -> PolarsResult<()> {
    let fmt_header: Format = Format::new()
        .set_align(FormatAlign::Center) // horizontally
        .set_align(FormatAlign::VerticalCenter)
        .set_text_wrap()
        .set_font_size(FONT_SIZE);

    let fmt_center = Format::new().set_align(FormatAlign::Center);

    let fmt_values = Format::new().set_num_format("#,##0.00");

    let fmt_aliquotas = Format::new()
        .set_num_format("0.0000")
        .set_align(FormatAlign::Center);

    worksheet
        .set_name(sheet_name)?
        .set_row_format(0, &fmt_header)?
        //.set_freeze_panes(1, 0)?
        .set_row_height(0, 64)?;

    let regex_cnpj_cpf = Regex::new(
        r"(?ix)
        ^(:?CNPJ|CPF)
    ",
    )
    .unwrap();

    let col_center = [
        // "CNPJ", "CPF",
        "Código",
        "Registro",
        "Chave do Documento",
        "Chave da Nota Fiscal Eletrônica",
        "Ano do Período de Apuração",
        "Trimestre do Período de Apuração",
    ];

    let col_values = [
        "Valor",
        "ICMS",
        "Crédito vinculado à Receita Bruta Não Cumulativa",
        "Crédito vinculado à Receita Bruta Cumulativa",
        "Crédito vinculado à Receita Bruta Total",
    ];

    let col_aliquotas = [
        "PIS: Alíquota ad valorem",
        "COFINS: Alíquota ad valorem",
        "Alíquota de PIS/PASEP",
        "Alíquota de COFINS",
    ];

    for (column_number, col_name) in df.get_column_names().iter().enumerate() {
        if regex_cnpj_cpf.is_match(col_name) {
            worksheet.set_column_format(column_number as u16, &fmt_center)?;
            continue;
        }

        for pattern in col_center {
            if col_name.contains(pattern) {
                worksheet.set_column_format(column_number as u16, &fmt_center)?;
                break;
            }
        }

        for value in col_values {
            if col_name.contains(value) {
                worksheet.set_column_format(column_number as u16, &fmt_values)?;
                break;
            }
        }

        for aliquota in col_aliquotas {
            if col_name.contains(aliquota) {
                worksheet.set_column_format(column_number as u16, &fmt_aliquotas)?;
                break;
            }
        }
    }

    Ok(())
}

/// Iterate over all DataFrame and find the max data width for each column.
///
/// See:
///
/// <https://crates.io/crates/unicode-width>
///
/// <https://tomdebruijn.com/posts/rust-string-length-width-calculations>
#[allow(dead_code)]
fn auto_fit(df: &DataFrame, worksheet: &mut Worksheet) -> PolarsResult<()> {
    // Nome de Colunas para ajustes
    let natureza: &str = coluna(Left, "natureza");
    let tipo_credito: &str = coluna(Left, "tipo_cred");

    for (col_num, series) in df.iter().enumerate() {
        let col_name = series.name();
        let col_width = col_name.chars().count().div_ceil(4);
        let mut width = WIDTH_MIN.max(col_width);

        // analyze all column fields
        for row in series.iter() {
            let text = match row.dtype() {
                DataType::Float64 => {
                    let num: f64 = row.try_extract::<f64>()?;
                    //num.to_string()
                    format!("{num:0.2}") // two digits after the decimal point
                }
                DataType::Float32 => {
                    let num: f32 = row.try_extract::<f32>()?;
                    //num.to_string()
                    format!("{num:0.2}") // two digits after the decimal point
                }
                _ => row.to_string(),
            };

            let mut text_width = text.chars().count(); // chars number

            // Aplicar ajustes
            if [natureza, tipo_credito].contains(&col_name) {
                text_width = text_width * 82 / 100
            }

            if text_width > width {
                width = text_width;
            }

            if width > WIDTH_MAX {
                width = WIDTH_MAX;
                break;
            }
        }
        // println!("col_num: {col_num}, col_name: {col_name}, width: {width}");
        worksheet.set_column_width(col_num as u16, (width as f64) * ADJUSTMENT)?;
    }

    Ok(())
}

/// Iterate over all DataFrame and color some columns.
fn auto_color(df: &DataFrame, worksheet: &mut Worksheet, sheet_name: &str) -> PolarsResult<()> {
    let radix = 16; // hexadecimal

    let color = if sheet_name.contains("EFD (original)") {
        "e6b8b7" // vermelho ; "f8cbad"; // rosa
    } else if sheet_name.contains("EFD (após auditoria)") {
        "c4d79b" // verde
    } else {
        return Ok(());
    };

    let color_saldoc: u32 = u32::from_str_radix(color, radix).unwrap();

    // BG Color: Saldo de Crédito Passível de Desconto ou Ressarcimento
    let format_saldoc: Format = Format::new().set_background_color(XlsxColor::RGB(color_saldoc));

    let color_bcsoma: u32 = u32::from_str_radix("bfbfbf", radix).unwrap();

    // BG Color: Base de Cálculo dos Créditos - Alíquota Básica (Soma)
    let format_bcsoma: Format = Format::new().set_background_color(XlsxColor::RGB(color_bcsoma));

    let color_debito: u32 = u32::from_str_radix("ccc0da", radix).unwrap();
    let format_debito: Format = Format::new().set_background_color(XlsxColor::RGB(color_debito));

    // BG Color: "Crédito vinculado à Receita Bruta Não Cumulativa"
    let format_credito_nao_cumulativo: Format = Format::new()
        .set_background_color(XlsxColor::RGB(color_saldoc))
        .set_num_format("#,##0.00");

    let mut selected_rows = HashMap::new();

    for (col_num, series) in df.iter().enumerate() {
        let col_name: &str = series.name();

        if col_name.contains("Natureza da Base de Cálculo") {
            for (row_num, data) in series.iter().enumerate() {
                if let Some(text) = data.get_str() {
                    if text.contains("(Soma)") {
                        worksheet.write_with_format(
                            row_num as u32 + 1,
                            col_num as u16,
                            text,
                            &format_bcsoma,
                        )?;
                    } else if text.contains("Débitos:") {
                        worksheet.write_with_format(
                            row_num as u32 + 1,
                            col_num as u16,
                            text,
                            &format_debito,
                        )?;
                    } else if text.contains("Saldo de Crédito") {
                        worksheet.write_with_format(
                            row_num as u32 + 1,
                            col_num as u16,
                            text,
                            &format_saldoc,
                        )?;
                        selected_rows.insert(row_num, 1);
                    }
                }
            }
        }

        if col_name == "Crédito vinculado à Receita Bruta Não Cumulativa" {
            for (row_num, data) in series.iter().enumerate() {
                match data {
                    AnyValue::Float64(value) if selected_rows.contains_key(&row_num) => {
                        worksheet.write_with_format(
                            row_num as u32 + 1,
                            col_num as u16,
                            value,
                            &format_credito_nao_cumulativo,
                        )?;
                    }
                    _ => continue,
                }
            }
        }
    }

    Ok(())
}

/// Format data supported by Excel
fn format_to_excel(data_frame: &DataFrame) -> PolarsResult<DataFrame> {
    let df_formated: DataFrame = data_frame
        .clone()
        .lazy()
        .with_columns([all().apply(format_data, GetOutput::same_type())])
        .collect()?;

    Ok(df_formated)
}

/// Format DataType
fn format_data(series: Series) -> PolarsResult<Option<Series>> {
    match series.dtype() {
        DataType::Int64 => Ok(Some(series.cast(&DataType::Int32)?)),
        DataType::UInt64 => Ok(Some(series.cast(&DataType::UInt32)?)),
        DataType::String => truncate_series(series), // to_n_chars(series)
        _ => Ok(Some(series)),
    }
}

fn truncate_series(series: Series) -> PolarsResult<Option<Series>> {
    let new_series: Series = series
        .str()?
        .into_iter()
        .map(
            |option_str: Option<&str>| option_str.map(|s| truncate_string(s, 32767)), // 2 ^ 15 - 1
        )
        .collect::<StringChunked>()
        .into_series();

    Ok(Some(new_series))
}

// https://stackoverflow.com/questions/38461429/how-can-i-truncate-a-string-to-have-at-most-n-characters
fn truncate_string(s: &str, max_chars: usize) -> &str {
    match s.char_indices().nth(max_chars) {
        Some((idx, _)) => &s[..idx],
        None => s,
    }
}

pub enum Frame {
    Lazy(LazyFrame),
    Data(DataFrame),
}

/// Eliminate columns that contain only null values.
///
/// frame can be LazyFrame or DataFrame
///
/// <https://github.com/pola-rs/polars/issues/1613>
///
/// <https://stackoverflow.com/questions/76338261/polars-and-the-lazy-api-how-to-drop-columns-that-contain-only-null-values>
pub fn remove_null_columns(frame: Frame) -> PolarsResult<DataFrame> {
    let df: DataFrame = match frame {
        Frame::Lazy(lz) => lz.collect()?,
        Frame::Data(df) => df,
    };

    // Keep the names of these columns:
    let pa_mes: &str = coluna(Left, "pa_mes"); // "Mês do Período de Apuração"
    let glosar: &str = coluna(Middle, "glosar"); // "Glosar Base de Cálculo de PIS/PASEP e COFINS"

    let df_clean: DataFrame = df
        .iter()
        .filter(|series| {
            series.is_not_null().any()
                || series.name().contains(pa_mes)
                || series.name().contains(glosar)
        })
        .cloned()
        .collect();

    Ok(df_clean)
}

#[cfg(test)]
mod test_functions {
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
