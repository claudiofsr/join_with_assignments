//! # Excel Formatting Module
//!
//! Este módulo gerencia os formatos visuais, cores e o registro de estilo
//! das células geradas nas planilhas Excel.

use rust_xlsxwriter::{Color, Format, FormatAlign};
use std::collections::HashMap;

pub const FONT_SIZE: f64 = 14.0;
pub const HEADER_FONT_SIZE: f64 = 12.0;

pub const COLOR_SOMA: Color = Color::RGB(0xBFBFBF);
pub const COLOR_DESCONTO: Color = Color::RGB(0xCCC0DA);
pub const COLOR_SALDO_RED: Color = Color::RGB(0xE6B8B7);
pub const COLOR_SALDO_GREEN: Color = Color::RGB(0xC4D79B);

/// Identificadores para os tipos de alinhamento e formatação de colunas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FormatKey {
    Default,
    Center,
    Value,
    Aliquota,
    Date,
}

impl FormatKey {
    /// Mapeia cada chave de formato à sua respectiva regra de exibição.
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

/// Estilos lógicos aplicados às linhas de dados.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RowStyle {
    Normal,
    Soma,
    Desconto,
    Saldo,
}

impl RowStyle {
    /// Associa cada estado lógico de linha à sua respectiva cor de fundo.
    pub fn styles_with_colors(color_saldo: Color) -> [(RowStyle, Option<Color>); 4] {
        [
            (RowStyle::Normal, None),
            (RowStyle::Soma, Some(COLOR_SOMA)),
            (RowStyle::Desconto, Some(COLOR_DESCONTO)),
            (RowStyle::Saldo, Some(color_saldo)),
        ]
    }
}

/// Registro que gerencia e preestabelece as combinações de formatos de célula.
#[derive(Debug, Default, Clone)]
pub struct FormatRegistry {
    matrix: HashMap<(FormatKey, RowStyle), Format>,
}

impl FormatRegistry {
    /// Instancia um novo registro mapeando todas as permutações de chaves e realces de linha.
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

    /// Retorna a referência do formato de célula correspondente.
    #[inline]
    pub fn get_format(&self, f_key: FormatKey, r_style: RowStyle) -> Option<&Format> {
        self.matrix.get(&(f_key, r_style))
    }

    /// Retorna a formatação padrão para a linha de cabeçalho.
    pub fn header() -> Format {
        Format::new()
            .set_text_wrap()
            .set_align(FormatAlign::Center)
            .set_align(FormatAlign::VerticalCenter)
            .set_font_size(HEADER_FONT_SIZE)
    }
}
