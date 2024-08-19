use rayon::prelude::*;
use std::error::Error;

use polars::{prelude::*, series::Series};

use crate::filtros::operacoes_de_entrada_ou_saida;

/// Analisar legislação vigente das Contribuições conforme código NCM e descrição dos itens.
///
/// Ou seja, a legislação é resultado da função: fn(NCM, Descrição dos Itens).
///
/// O resultado é uma nova coluna com informações sobre a incidência das Contribuições.
///
/// Nome da nova coluna: `Incidência Monofásica`
pub fn adicionar_coluna_de_incidencia_monofasica(
    lazyframe: LazyFrame,
) -> Result<LazyFrame, Box<dyn Error>> {
    let columns: Vec<&'static str> = vec!["Tipo de Operação", "Incidência Monofásica"];

    let side_a: Vec<&'static str> = vec!["Código NCM", "Descrição do Item", "Coluna Temporária A"];

    let side_b: Vec<&'static str> = vec![
        "Código NCM : NF Item (Todos)",
        "Descrição da Mercadoria/Serviço : NF Item (Todos)",
        "Coluna Temporária B",
    ];

    let boolean_a: Expr = col(side_a[2]).is_not_null(); // .and(operacoes_de_entrada_ou_de_saida());
    let boolean_b: Expr = col(side_b[2]).is_not_null(); // .and(operacoes_de_entrada_ou_de_saida());

    // Exemplo: 'NCM 2207.10.90 : Incidência Monofásica - Lei 9.718/1998, Art. 5º (Álcool, Inclusive para Fins Carburantes).'
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
                    |series: Series| analisar_colunas_selecionadas(&series),
                    GetOutput::same_type(),
                ) // GetOutput::from_type(DataType::String)
                .alias(side_a[2]),
        )
        .with_column(
            as_struct([col(side_b[0]).cast(DataType::String), col(side_b[1])].to_vec())
                .apply(
                    |series: Series| analisar_colunas_selecionadas(&series),
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
            when(operacoes_de_entrada_ou_saida())
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

fn analisar_colunas_selecionadas(series: &Series) -> Result<Option<Series>, PolarsError> {
    // add feature "dtype-struct"
    let struct_chunked: &StructChunked = series.struct_()?;

    // Get the fields as Series
    let ser_codigoncm: &Series = &struct_chunked.fields_as_series()[0];
    let ser_descricao: &Series = &struct_chunked.fields_as_series()[1];

    // Get columns with into_iter()
    let vec_opt_str_ncm: Vec<Option<&str>> = ser_codigoncm.str()?.into_iter().collect();
    let vec_opt_str_dsc: Vec<Option<&str>> = ser_descricao.str()?.into_iter().collect();

    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
    let new_series: Series = (vec_opt_str_ncm, vec_opt_str_dsc)
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
        .into_series();

    Ok(Some(new_series))
}

/// Base Legal conforme código NCM e descrição do item.
fn base_legal(codigo_ncm: u64, _descricao: &str) -> Option<&'static str> {
    let especificos: [u64; 2] = [
        30039056, // exceção em Incidência Monofásica
        30049046, // exceção em Incidência Monofásica
    ];

    if especificos.contains(&codigo_ncm) {
        return None;
    }

    match codigo_ncm {
        27101259 =>  Some("Incidência Monofásica - Lei 9.718/1998, Art. 4º, Inciso I (Gasolinas, exceto Gasolina de Aviação)."),
        27101921 =>  Some("Incidência Monofásica - Lei 9.718/1998, Art. 4º, Inciso II (Óleo Diesel)."),
        27111910 =>  Some("Incidência Monofásica - Lei 9.718/1998, Art. 4º, Inciso III (Gás Liquefeito de Petróleo - GLP)."),
        27101911 =>  Some("Incidência Monofásica - Lei 10.560/2002, Art. 2º (Querosene de Aviação)."),
        38260000 =>  Some("Incidência Monofásica - Lei 11.116/2005, Art. 3º (Biodiesel)."),
        22071000 ..= 22071099 | 22072010 ..= 22072019 | 22089000
                  => Some("Incidência Monofásica - Lei 9.718/1998, Art. 5º (Álcool, Inclusive para Fins Carburantes)."),
        30010000 ..= 30019999 | 30030000 ..= 30039999 | 30040000 ..= 30049999 |
        30021010 ..= 30021039 | 30022010 ..= 30022029 | 30063010 ..= 30063029 |
        30029020 | 30029092 | 30051010 | 30066000 // | 30029099: lei_10925_art01_inciso06()
                  => Some("Incidência Monofásica - Lei 10.147/2000, Art. 1º, Inciso I, alínea A (Produtos Farmacêuticos)."),
        33030000 ..= 33059999 | 33070000 ..= 33079999 | 34012010 | 96032100
                  => Some("Incidência Monofásica - Lei 10.147/2000, Art. 1º, Inciso I, alínea B (Produtos de Perfumaria ou de Higiene Pessoal)."),
        40110000 ..= 40119999 | 40130000 ..= 40139999
                  => Some("Incidência Monofásica - Lei 10.485/2002, Art. 5º (Pneumáticos)."),
        _ => None,
    }
}
