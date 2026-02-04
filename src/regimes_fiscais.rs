use polars::prelude::*;

use crate::{
    JoinResult, LazyFrameExtension,
    Side::{Left, Right},
    coluna, get_output_same_type,
    legislacao_aliquota_zero::base_legal_de_aliquota_zero,
    legislacao_credito_presumido::base_legal_de_credito_presumido,
    legislacao_incidencia_monofasica::base_legal_de_incidencia_monofasica,
    operacoes_de_entrada_ou_saida,
};

/// Enum representing different fiscal regimes for contributions.
///
/// Each variant corresponds to a specific legal framework.
#[derive(Debug, Clone, Copy)]
pub enum RegimesFiscais {
    AliquotaZero,
    CreditoPresumido,
    IncidenciaMonofasica,
}

impl RegimesFiscais {
    /// Returns the standardized column name for the given fiscal regime.
    /// This name will be used as the output column in the DataFrame.    
    pub fn get_column_name(&self) -> &'static str {
        match self {
            RegimesFiscais::AliquotaZero => "Alíquota Zero",
            RegimesFiscais::CreditoPresumido => "Crédito Presumido",
            RegimesFiscais::IncidenciaMonofasica => "Incidência Monofásica",
        }
    }

    /// Returns the specific legal basis function for the given fiscal regime.
    /// This function takes an NCM code (u64) and an item description (&str)
    /// and returns an `Option<&'static str>` representing the legal justification.    
    pub fn get_base_legal_fn(
        &self,
    ) -> impl Fn(u64, &str) -> Option<&'static str> + Send + Sync + 'static {
        match self {
            RegimesFiscais::AliquotaZero => base_legal_de_aliquota_zero,
            RegimesFiscais::CreditoPresumido => base_legal_de_credito_presumido,
            RegimesFiscais::IncidenciaMonofasica => base_legal_de_incidencia_monofasica,
        }
    }
}

/// Adds a new column for the 'Alíquota Zero' fiscal regime.
///
/// The new column will contain the legal basis for items qualifying for Alíquota Zero,
/// based on NCM code and item description.
pub fn adicionar_coluna_de_aliquota_zero(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    adicionar_coluna_de_regime_fiscal(lazyframe, RegimesFiscais::AliquotaZero)
}

/// Adds a new column for the 'Crédito Presumido' fiscal regime.
///
/// The new column will contain the legal basis for items qualifying for Crédito Presumido,
/// based on NCM code and item description.
pub fn adicionar_coluna_de_credito_presumido(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    adicionar_coluna_de_regime_fiscal(lazyframe, RegimesFiscais::CreditoPresumido)
}

/// Adds a new column for the 'Incidência Monofásica' fiscal regime.
///
/// The new column will contain the legal basis for items qualifying for Incidência Monofásica,
/// based on NCM code and item description.
pub fn adicionar_coluna_de_incidencia_monofasica(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    adicionar_coluna_de_regime_fiscal(lazyframe, RegimesFiscais::IncidenciaMonofasica)
}

/**
Esta função adiciona uma nova coluna ao LazyFrame com informações sobre a
incidência das Contribuições de acordo com o regime fiscal específico.

As informações adicionadas na nova coluna sobre a incidência das Contribuições
são obtidas conforme código NCM e descrição dos itens.

Ou seja, a legislação adicionada é resultado da função: fn(NCM, Descrição dos Itens).

 * `lazyframe`: The input LazyFrame.
 * `regime_fiscal`: The fiscal regime to be analyzed (e.g., AliquotaZero).
*/
fn adicionar_coluna_de_regime_fiscal(
    lazyframe: LazyFrame,
    regime_fiscal: RegimesFiscais,
) -> JoinResult<LazyFrame> {
    let output_col_name = regime_fiscal.get_column_name();

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
                .map(
                    move |col: Column| aplicar_regime_fiscal(&col, regime_fiscal),
                    get_output_same_type,
                ) // GetOutput::from_type(DataType::String)
                .alias(temp_col_a),
            // Add a temporary column B by applying a custom function on NCM and description
            as_struct([col(ncm_col_b).cast(DataType::String), col(desc_col_b)].to_vec())
                .map(
                    move |col: Column| aplicar_regime_fiscal(&col, regime_fiscal),
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

/// Applies the specific fiscal regime logic to a Polars StructColumn containing NCM and description.
///
/// This function is intended to be used within a Polars `apply` expression.
/// It extracts NCM and description, parses NCM, and then calls the appropriate
/// legal basis function determined by the `regime_fiscal`.
///
/// `col`: A reference to a Polars `Column` which is expected to be a `StructColumn`
///        with two fields: NCM (as String) and Description (as String).
/// `regime_fiscal`: The `RegimesFiscais` enum variant indicating which legal
///                  basis function to use.
///
/// Returns a `Result<Column, PolarsError>` where the `Column` contains the
/// legal basis strings or `None`.
fn aplicar_regime_fiscal(
    col: &Column,
    regime_fiscal: RegimesFiscais,
) -> Result<Column, PolarsError> {
    // Cast the input column to StructChunked to access its fields.
    // Ensure the "dtype-struct" feature is enabled in Cargo.toml for StructChunked
    let struct_chunked: &StructChunked = col.struct_()?;

    // Get the fields as Series. Expected order: NCM (index 0), Description (index 1).
    let ser_codigoncm: &Series = &struct_chunked.fields_as_series()[0];
    let ser_descricao: &Series = &struct_chunked.fields_as_series()[1];

    // Get ChunkedArray<StringType> for NCM and description for efficient iteration.
    let ca_str_ncm = ser_codigoncm.str()?;
    let ca_str_dsc = ser_descricao.str()?;

    // Retrieve the specific legal basis function for the given fiscal regime.
    let base_legal = regime_fiscal.get_base_legal_fn(); // Get the specific function here

    // Iterate over NCM and description, apply base_legal function
    let new_col: Column = ca_str_ncm
        .into_iter()
        .zip(ca_str_dsc)
        .map(
            |(opt_ncm_str, opt_desc_str)| match (opt_ncm_str, opt_desc_str) {
                (Some(ncm_str), Some(desc_str)) => {
                    // Remove dots from NCM string (e.g., "2207.10.90" -> "22071090") and parse as u64.
                    let codigo_ncm = ncm_str.replace('.', "");
                    // println!("{ncm_str} -> {codigo_ncm}");
                    match codigo_ncm.parse::<u64>() {
                        Ok(ncm) => base_legal(ncm, desc_str),
                        Err(e) => {
                            eprintln!("Warning: Failed to parse NCM '{}': {}", ncm_str, e);
                            None // Handle parsing errors by returning None
                        }
                    }
                }
                _ => None, // Handle missing NCM or description by returning None
            },
        )
        .collect::<StringChunked>()
        .into_column();

    Ok(new_col)
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
///
/// `cargo test -- --show-output regime_fiscal_tests`
#[cfg(test)]
mod regime_fiscal_tests {
    use super::*; // Import everything from the parent module
    use crate::{
        JoinResult, adicionar_coluna_de_aliquota_zero, adicionar_coluna_de_credito_presumido,
        adicionar_coluna_de_incidencia_monofasica,
    };

    // Helper function to create a basic LazyFrame for testing
    fn create_test_dataframe() -> PolarsResult<DataFrame> {
        df! {
            "Código NCM" => &["3100.00.00", "12011000", "50000000", "00000000", "22071000", "1030000"],
            "Descrição do Item" => &["FERTILIZANTE UREIA", "Semente de Soja", "CADEIRA", "PRODUTO GENERICO", "Álcool", "Produto Crédito"],
            "Código NCM : NF Item (Todos)" => &["31000000", "12011000", "50000000", "00000000", "22071000", "1000000"],
            "Descrição da Mercadoria/Serviço : NF Item (Todos)" => &["UREIA AGRÍCOLA", "Muda de Milho", "MESA", "OUTRO PRODUTO", "Álcool Carburante", "Produto com Crédito"],
            "Tipo de Operação" => &[1, 1, 1, 2, 2, 1], // operacoes_de_entrada_ou_saida
        }
    }

    #[test]
    fn test_adicionar_coluna_de_aliquota_zero() -> JoinResult<()> {
        let df = create_test_dataframe()?;

        // Assert that the new column not exists
        assert!(df.column("Alíquota Zero").is_err());

        let result_lf = adicionar_coluna_de_aliquota_zero(df.lazy());
        let df = result_lf?.collect()?;

        println!("df: {df}");

        // Assert that the new column exists
        assert!(df.column("Alíquota Zero").is_ok());

        // Assert the values in the new column
        let aliquota_zero_col = df.column("Alíquota Zero")?.str()?;

        assert_eq!(
            aliquota_zero_col.get(0),
            Some(
                "NCM 3100.00.00 : Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso I (Adubos ou Fertilizantes)."
            )
        );
        assert_eq!(
            aliquota_zero_col.get(1),
            Some(
                "NCM 12011000 : Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso III (Sementes e Mudas destinadas à semeadura e plantio)."
            )
        );
        assert_eq!(aliquota_zero_col.get(2), None); // No match
        assert_eq!(aliquota_zero_col.get(3), None); // No match
        assert_eq!(aliquota_zero_col.get(4), None); // Match, but not for this regime
        assert_eq!(aliquota_zero_col.get(5), None); // Match, but not for this regime

        // Ensure temporary columns are dropped
        assert!(df.column("Coluna Temporária A").is_err());
        assert!(df.column("Coluna Temporária B").is_err());

        Ok(())
    }

    #[test]
    fn test_adicionar_coluna_de_credito_presumido() -> JoinResult<()> {
        let df = create_test_dataframe()?;
        let result_lf = adicionar_coluna_de_credito_presumido(df.lazy());
        let df = result_lf?.collect()?;

        println!("df: {df}");

        assert!(df.column("Crédito Presumido").is_ok());

        let credito_presumido_col = df.column("Crédito Presumido")?.str()?;

        println!("credito_presumido_col: {credito_presumido_col:?}");

        assert_eq!(
            credito_presumido_col.get(5),
            Some(
                "NCM 1030000 : Crédito Presumido - Lei 12.350/2010, Art. 55 (Animais vivos: Suíno ou Frango)."
            )
        );
        assert_eq!(credito_presumido_col.get(0), None);
        assert_eq!(credito_presumido_col.get(1), None);
        assert_eq!(credito_presumido_col.get(2), None);
        assert_eq!(credito_presumido_col.get(3), None);
        assert_eq!(credito_presumido_col.get(4), None);

        Ok(())
    }

    #[test]
    fn test_adicionar_coluna_de_incidencia_monofasica() -> JoinResult<()> {
        let df = create_test_dataframe()?;
        let result_lf = adicionar_coluna_de_incidencia_monofasica(df.lazy());
        let df = result_lf?.collect()?;

        println!("df: {df}");

        assert!(df.column("Incidência Monofásica").is_ok());

        let incidencia_monofasica_col = df.column("Incidência Monofásica")?.str()?;

        println!("incidencia_monofasica_col: {incidencia_monofasica_col:?}");

        assert_eq!(
            incidencia_monofasica_col.get(4),
            Some(
                "NCM 22071000 : Incidência Monofásica - Lei 9.718/1998, Art. 5º (Álcool, Inclusive para Fins Carburantes)."
            )
        );
        assert_eq!(incidencia_monofasica_col.get(0), None);
        assert_eq!(incidencia_monofasica_col.get(1), None);
        assert_eq!(incidencia_monofasica_col.get(2), None);
        assert_eq!(incidencia_monofasica_col.get(3), None);
        assert_eq!(incidencia_monofasica_col.get(5), None);

        Ok(())
    }

    #[test]
    fn test_adicionar_coluna_no_empty_frame() -> JoinResult<()> {
        let df_empty = df! {
            "Código NCM" => Vec::<&str>::new(),
            "Descrição do Item" => Vec::<&str>::new(),
            "Código NCM : NF Item (Todos)" => Vec::<&str>::new(),
            "Descrição da Mercadoria/Serviço : NF Item (Todos)" => Vec::<&str>::new(),
            "Tipo de Operação" => Vec::<i64>::new(),
        }?;

        println!("df_empty: {df_empty}");

        let result_lf = adicionar_coluna_de_aliquota_zero(df_empty.lazy())?;
        let df_result = result_lf.collect()?;

        println!("df_result: {df_result}");

        // For an empty frame, the column should still be created but be empty
        assert!(df_result.column("Alíquota Zero").is_ok());
        assert_eq!(df_result.height(), 0);

        Ok(())
    }

    #[test]
    fn test_adicionar_coluna_with_all_null_ncm_description() -> JoinResult<()> {
        let df = df! {
            "Código NCM" => &[None::<&str>],
            "Descrição do Item" => &[None::<&str>],
            "Código NCM : NF Item (Todos)" => &[None::<&str>],
            "Descrição da Mercadoria/Serviço : NF Item (Todos)" => &[None::<&str>],
            "Tipo de Operação" => &[1],
        }?;

        println!("df: {df}");

        let result_lf = adicionar_coluna_de_aliquota_zero(df.lazy());
        let df_result = result_lf?.collect()?;

        println!("df_result: {df_result}");

        let aliquota_zero_col = df_result.column("Alíquota Zero")?.str()?;
        assert_eq!(aliquota_zero_col.get(0), None);

        Ok(())
    }

    #[test]
    fn test_operacoes_de_entrada_ou_saida_filter() -> JoinResult<()> {
        let df = df! {
            "Código NCM" => &["31000000", "31000000"],
            "Descrição do Item" => &["FERTILIZANTE UREIA", "FERTILIZANTE UREIA"],
            "Código NCM : NF Item (Todos)" => &["31000000", "31000000"],
            "Descrição da Mercadoria/Serviço : NF Item (Todos)" => &["UREIA AGRÍCOLA", "UREIA AGRÍCOLA"],
            // Assuming this column drives the filter
            "Tipo de Operação" => &[1, 3], // Set to 1:Entrada e 3:Ajustes
        }?;
        println!("df: {df}");

        // Assert that the new column not exists
        assert!(df.column("Alíquota Zero").is_err());

        let result_lf = adicionar_coluna_de_aliquota_zero(df.lazy())?;
        let df_result = result_lf.collect()?;
        println!("df_result: {df_result}");

        // Assert that the new column exists
        assert!(df_result.column("Alíquota Zero").is_ok());

        let aliquota_zero_col = df_result.column("Alíquota Zero")?.str()?;
        println!("aliquota_zero_col: {aliquota_zero_col:?}");

        assert_eq!(
            aliquota_zero_col.get(0),
            Some(
                "NCM 31000000 : Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso I (Adubos ou Fertilizantes)."
            )
        );
        assert_eq!(aliquota_zero_col.get(1), None);

        Ok(())
    }
}
