use polars::{prelude::*, series::Series};

use crate::{
    LazyFrameExtension, MyResult,
    Side::{Left, Right},
    coluna, get_output_same_type, operacoes_de_entrada_ou_saida,
};

/**
Analisar legislação vigente das Contribuições conforme código NCM e descrição dos itens.

Ou seja, a legislação é resultado da função: fn(NCM, Descrição dos Itens).

O resultado é uma nova coluna com informações sobre a incidência das Contribuições.

`lazyframe`: The input LazyFrame.
`base_legal`: A function that determines the legal basis for a given NCM and description.
`output_col_name`: The name of the new column to be added (e.g., "Alíquota Zero").
*/
pub fn adicionar_coluna_de_regime_fiscal<F>(
    lazyframe: LazyFrame,
    base_legal: F,
    output_col_name: &'static str,
) -> MyResult<LazyFrame>
where
    F: Fn(u64, &str) -> Option<&'static str> + Send + Sync + 'static + Copy, // used twice
{
    let ncm_col_a: &str = coluna(Left, "ncm"); // "Código NCM";
    let desc_col_a: &str = coluna(Left, "item_desc"); // "Descrição do Item";
    let temp_col_a: &str = "Coluna Temporária A";

    let ncm_col_b: &str = coluna(Right, "ncm"); // "Código NCM : NF Item (Todos)";
    let desc_col_b: &str = coluna(Right, "descricao_mercadoria"); // "Descrição da Mercadoria/Serviço : NF Item (Todos)";
    let temp_col_b: &str = "Coluna Temporária B";

    // Combine null check with the entry/exit operation condition
    let boolean_a: Expr = operacoes_de_entrada_ou_saida()?.and(col(temp_col_a).is_not_null());
    let boolean_b: Expr = operacoes_de_entrada_ou_saida()?.and(col(temp_col_b).is_not_null());

    // Exemplo: 'NCM 2207.10.90 : Alíquota Zero - Lei xxx.'
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
        // Adicionar 2 colunas temporárias
        .with_columns([
            // Add a temporary column A by applying a custom function on NCM and description
            as_struct([col(ncm_col_a).cast(DataType::String), col(desc_col_a)].to_vec())
                .apply(
                    move |col: Column| analisar_colunas_selecionadas(&col, base_legal),
                    get_output_same_type,
                ) // GetOutput::from_type(DataType::String)
                .alias(temp_col_a),
            // Add a temporary column B by applying a custom function on NCM and description
            as_struct([col(ncm_col_b).cast(DataType::String), col(desc_col_b)].to_vec())
                .apply(
                    move |col: Column| analisar_colunas_selecionadas(&col, base_legal),
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
                .alias(output_col_name),
        )
        // Remover 2 colunas temporárias
        .drop_columns(&[temp_col_a, temp_col_b])?;

    Ok(lazyframe)
}

/// Analyze current legislation for Contributions based on NCM code and item description.
/// Expects a StructColumn with two fields: NCM (String) and Description (String).
///
/// The `base_legal` parameter is a function that takes NCM (u64) and description (&str)
/// and returns an Option<&'static str>.
fn analisar_colunas_selecionadas<F>(col: &Column, base_legal: F) -> Result<Column, PolarsError>
where
    F: Fn(u64, &str) -> Option<&'static str> + Send + Sync + 'static,
{
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
