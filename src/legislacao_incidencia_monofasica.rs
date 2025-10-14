use polars::{prelude::*, series::Series};

use crate::{
    LazyFrameExtension, MyResult,
    Side::{Left, Right},
    coluna, get_output_same_type, operacoes_de_entrada_ou_saida,
};

/// Analisar legislação vigente das Contribuições conforme código NCM e descrição dos itens.
///
/// Ou seja, a legislação é resultado da função: fn(NCM, Descrição dos Itens).
///
/// O resultado é uma nova coluna com informações sobre a incidência das Contribuições.
///
/// Nome da nova coluna: `Incidência Monofásica`
pub fn adicionar_coluna_de_incidencia_monofasica(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
    let incidencia_monofasica = "Incidência Monofásica";

    let ncm_col_a: &str = coluna(Left, "ncm"); // "Código NCM";
    let desc_col_a: &str = coluna(Left, "item_desc"); // "Descrição do Item";
    let temp_col_a: &str = "Coluna Temporária A";

    let ncm_col_b: &str = coluna(Right, "ncm"); // "Código NCM : NF Item (Todos)";
    let desc_col_b: &str = coluna(Right, "descricao_mercadoria"); // "Descrição da Mercadoria/Serviço : NF Item (Todos)";
    let temp_col_b: &str = "Coluna Temporária B";

    // Combine null check with the entry/exit operation condition
    let boolean_a: Expr = operacoes_de_entrada_ou_saida()?.and(col(temp_col_a).is_not_null());
    let boolean_b: Expr = operacoes_de_entrada_ou_saida()?.and(col(temp_col_b).is_not_null());

    // Exemplo: 'NCM 2207.10.90 : Incidência Monofásica - Lei 9.718/1998, Art. 5º (Álcool, Inclusive para Fins Carburantes).'
    let exp_a: Expr = concat_str(
        [lit("NCM"), col(ncm_col_a), lit(":"), col(temp_col_a)],
        " ",
        true,
    );
    let exp_b: Expr = concat_str(
        [lit("NCM"), col(ncm_col_b), lit(":"), col(temp_col_b)],
        " ",
        true,
    );

    let lazyframe: LazyFrame = lazyframe
        .with_columns([
            // Adicionar 2 colunas temporárias
            as_struct([col(ncm_col_a).cast(DataType::String), col(desc_col_a)].to_vec())
                .apply(
                    |col: Column| analisar_colunas_selecionadas(&col),
                    get_output_same_type,
                ) // GetOutput::from_type(DataType::String)
                .alias(temp_col_a),
            as_struct([col(ncm_col_b).cast(DataType::String), col(desc_col_b)].to_vec())
                .apply(
                    |col: Column| analisar_colunas_selecionadas(&col),
                    get_output_same_type,
                ) // GetOutput::from_type(DataType::String)
                .alias(temp_col_b),
        ])
        .with_column(
            // Adicionar 1 coluna que concentra as informações das 2 colunas temporárias
            when(boolean_a)
                .then(exp_a)
                .when(boolean_b)
                .then(exp_b)
                .otherwise(lit(NULL))
                .alias(incidencia_monofasica),
        )
        // Remover 2 colunas temporárias
        .drop_columns(&[temp_col_a, temp_col_b])?;

    Ok(lazyframe)
}

/// Analyzes selected columns (NCM code and description) to determine legal basis.
///
/// Expects a StructColumn with two fields: NCM (String) and Description (String).
fn analisar_colunas_selecionadas(col: &Column) -> Result<Column, PolarsError> {
    // Add feature "dtype-struct"
    // Cast the input column to StructChunked
    let struct_chunked: &StructChunked = col.struct_()?;

    // Get the fields as Series
    let ser_codigoncm: &Series = &struct_chunked.fields_as_series()[0];
    let ser_descricao: &Series = &struct_chunked.fields_as_series()[1];

    // Get ChunkedArray<StringType> for NCM and description
    let ca_str_ncm = ser_codigoncm.str()?;
    let ca_str_dsc = ser_descricao.str()?;

    // Iterate over NCM and description, apply base_legal function
    let new_col: Column = ca_str_ncm
        .into_iter()
        .zip(ca_str_dsc)
        .map(
            |(opt_ncm_str, opt_desc_str)| match (opt_ncm_str, opt_desc_str) {
                (Some(ncm_str), Some(desc_str)) => {
                    // Remove dots from NCM string and parse as u64
                    let codigo_ncm = ncm_str.replace('.', "");
                    match codigo_ncm.parse::<u64>() {
                        Ok(ncm) => base_legal(ncm, desc_str),
                        Err(_) => None, // Handle parsing errors by returning None
                    }
                }
                _ => None, // Handle missing NCM or description by returning None
            },
        )
        .collect::<StringChunked>()
        .into_column();

    Ok(new_col)
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
