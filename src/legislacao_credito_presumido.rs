use polars::{prelude::*, series::Series};
use rayon::prelude::*;
use regex::Regex;
use std::sync::LazyLock as Lazy;

use crate::{MyResult, operacoes_de_entrada_ou_saida};

/// Analisar legislação vigente das Contribuições conforme código NCM e descrição dos itens.
///
/// Ou seja, a legislação é resultado da função: fn(NCM, Descrição dos Itens).
///
/// O resultado é uma nova coluna com informações sobre a incidência das Contribuições.
///
/// Nome da nova coluna: `Crédito Presumido`
pub fn adicionar_coluna_de_credito_presumido(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
    let columns: Vec<&'static str> = vec!["Tipo de Operação", "Crédito Presumido"];

    let side_a: Vec<&'static str> = vec!["Código NCM", "Descrição do Item", "Coluna Temporária A"];

    let side_b: Vec<&'static str> = vec![
        "Código NCM : NF Item (Todos)",
        "Descrição da Mercadoria/Serviço : NF Item (Todos)",
        "Coluna Temporária B",
    ];

    let boolean_a: Expr = col(side_a[2]).is_not_null(); // .and(operacoes_de_entrada_ou_de_saida());
    let boolean_b: Expr = col(side_b[2]).is_not_null(); // .and(operacoes_de_entrada_ou_de_saida());

    // Exemplo: 'NCM 2207.10.90 : Crédito Presumido - Lei xxx.'
    let exp_a: Expr = concat_str(
        [lit("NCM"), col(side_a[0]), lit(":"), col(side_a[2])],
        " ",
        true,
    );
    let exp_b: Expr = concat_str(
        [lit("NCM"), col(side_b[0]), lit(":"), col(side_b[2])],
        " ",
        true,
    );

    let lazyframe: LazyFrame = lazyframe
        .with_column(
            // Adicionar 2 colunas temporárias
            as_struct([col(side_a[0]).cast(DataType::String), col(side_a[1])].to_vec())
                .apply(
                    |col: Column| analisar_colunas_selecionadas(&col),
                    GetOutput::same_type(),
                ) // GetOutput::from_type(DataType::String)
                .alias(side_a[2]),
        )
        .with_column(
            as_struct([col(side_b[0]).cast(DataType::String), col(side_b[1])].to_vec())
                .apply(
                    |col: Column| analisar_colunas_selecionadas(&col),
                    GetOutput::same_type(),
                ) // GetOutput::from_type(DataType::String)
                .alias(side_b[2]),
        )
        .with_column(
            // Adicionar 1 coluna que concentra as informações das 2 colunas temporárias
            when(boolean_a)
                .then(exp_a)
                .when(boolean_b)
                .then(exp_b)
                .otherwise(lit(NULL))
                .alias(columns[1]),
        )
        .with_column(
            // Filtrar Operações de Entrada ou de Saída
            when(operacoes_de_entrada_ou_saida()?)
                .then(columns[1]) // keep original value
                .otherwise(lit(NULL)) // replace by null
                .alias(columns[1]), // .keep_name()
        )
        .drop([
            // Remover 2 colunas temporárias
            side_a[2], side_b[2],
        ]);

    Ok(lazyframe)
}

fn analisar_colunas_selecionadas(col: &Column) -> Result<Option<Column>, PolarsError> {
    // add feature "dtype-struct"
    let struct_chunked: &StructChunked = col.struct_()?;

    // Get the fields as Series
    let ser_codigoncm: &Series = &struct_chunked.fields_as_series()[0];
    let ser_descricao: &Series = &struct_chunked.fields_as_series()[1];

    // Get columns with into_iter()
    let vec_opt_str_ncm: Vec<Option<&str>> = ser_codigoncm.str()?.into_iter().collect();
    let vec_opt_str_dsc: Vec<Option<&str>> = ser_descricao.str()?.into_iter().collect();

    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
    let col: Column = (vec_opt_str_ncm, vec_opt_str_dsc)
        .into_par_iter() // rayon: parallel iterator
        .map(
            |(opt_str_ncm, opt_str_dsc)| match (opt_str_ncm, opt_str_dsc) {
                (Some(str_ncm), Some(str_dsc)) => {
                    let codigo_ncm = str_ncm.replace('.', "");
                    match codigo_ncm.parse::<u64>() {
                        Ok(ncm) => base_legal(ncm, str_dsc),
                        Err(_) => None,
                    }
                }
                _ => None,
            },
        )
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(col))
}

/// Base Legal conforme código NCM e descrição do item.
fn base_legal(codigo_ncm: u64, descricao: &str) -> Option<&'static str> {
    let especificos: [u64; 1] = [
        3029000, // lei_10925_art01_inciso20a()
    ];

    if especificos.contains(&codigo_ncm) {
        return None;
    }

    match codigo_ncm {
        // Observe que o intervalo de ncm (4010000 ..= 4049999) é analisado
        // em diferentes condições conforme a descrição do item.
        // Decreto 8.533/2015, Art 04 ; LEI Nº 10.925/2004, Art 01 incisos 11 e 13
        ncm @ 4010000..=4049999 => condicoes_ncm_04(descricao, ncm),

        1020000..=1029999 | 1040000..=1049999 => lei_12058_art33(),
        1030000..=1039999 | 1050000..=1059999 => lei_12350_art55(),

        _ => None,
    }
}

fn condicoes_ncm_04(descricao: &str, _ncm: u64) -> Option<&'static str> {
    static LEITE_IN_NATURA: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)Leite (In Natura|Cru)").unwrap());

    if LEITE_IN_NATURA.is_match(descricao) {
        // DECRETO Nº 8.533, DE 30 DE SETEMBRO DE 2015
        // Crédito Presumido: aquisição de leite in natura utilizado como insumo - Programa Mais Leite Saudável.
        // Crédito Presumido: aquisição sem pagamento das Constribuições que gera direito a crédito.
        Some(
            "Crédito Presumido - Decreto 8.533/2015, Art. 4º, Inciso I (Leite In Natura Utilizado como Insumo - Programa Mais Leite Saudável).",
        )
    } else {
        None
    }
}

fn lei_12058_art33() -> Option<&'static str> {
    /*
    Crédito Presumido ; artigos 32 a 34 da Lei nº 12.058/2009 ; PJ que industrializa os produtos classificados nas posições 01.02 (Boi) e 01.04 (Ovino ou Caprino) da NCM.
    Ver também IN RFB 977 de 2009 e Tabela 4.3.9
    Art. 33. As pessoas jurídicas sujeitas ao regime de apuração não cumulativa da Contribuição para o PIS/Pasep e da Cofins, inclusive cooperativas, que produzam mercadorias classificadas nos
    códigos 02.01, 02.02, 02.04, 0206.10.00, 0206.20, 0206.21, 0206.29, 0206.80.00, 0210.20.00, 0506.90.00, 0510.00.10 e 1502.00.1 da NCM, destinadas à exportação, poderão descontar da
    Contribuição para o PIS/Pasep e da Cofins devidas em cada período de apuração crédito presumido, calculado sobre o valor dos bens classificados nas posições 01.02 e 01.04 da NCM,
    adquiridos de pessoa física ou recebidos de cooperado pessoa física. (Redação dada pela Lei nº 12.839, de 2013)
    */
    Some("Crédito Presumido - Lei 12.058/2009, Art. 33 (Animais vivos: bovino, ovino ou caprino).")
}

fn lei_12350_art55() -> Option<&'static str> {
    /*
    Crédito Presumido ; artigos 54 a 56 da Lei nº 12.350/2010 ; PJ que industrializa os produtos classificados nas posições 01.03 (Suíno) e 01.05 (Frango e outras aves) da NCM.
    Ver também IN RFB 1157 de 2011 e Tabela 4.3.9
    Art. 55.  As pessoas jurídicas sujeitas ao regime de apuração não cumulativa da Contribuição para o PIS/Pasep e da Cofins, inclusive cooperativas,
    que produzam mercadorias classificadas nos códigos 02.03, 0206.30.00, 0206.4, 02.07 e 0210.1 da NCM, destinadas a exportação, poderão descontar da
    Contribuição para o PIS/Pasep e da Cofins devidas em cada período de apuração crédito presumido, calculado sobre:
    III – o valor dos bens classificados nas posições 01.03 e 01.05 da NCM, adquiridos de pessoa física ou recebidos de cooperado pessoa física.
    */
    Some("Crédito Presumido - Lei 12.350/2010, Art. 55 (Animais vivos: Suíno ou Frango).")
}
