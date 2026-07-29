//! # Orquestrador de Regimes Fiscais (PIS/COFINS)
//!
//! Este módulo gerencia a aplicação de regras de enquadramento legal com base no código NCM
//! (Nomenclatura Comum do Mercosul) e na descrição dos itens da EFD Contribuições e documentos fiscais (NF-e/CT-e).
//!
//! A análise divide-se em três grandes pilares amparados pela legislação tributária federal:
//! 1. **Alíquota Zero**: Desonerações na venda no mercado interno (ex: Lei nº 10.925/2004 e Lei nº 10.865/2004).
//! 2. **Crédito Presumido**: Benefícios de compensação para a cadeia do agronegócio (ex: Decreto nº 8.533/2015).
//! 3. **Incidência Monofásica**: Concentração do tributo no produtor/importador, com desoneração nas etapas seguintes (ex: Lei nº 10.147/2000).

use polars::prelude::*;

use crate::{
    JoinResult, LazyFrameExtension,
    Side::{Left, Right},
    coluna, get_output_same_type, operacoes_de_entrada_ou_saida,
    regime_fiscal::{
        legislacao_aliquota_zero::base_legal_de_aliquota_zero,
        legislacao_credito_presumido::base_legal_de_credito_presumido,
        legislacao_incidencia_monofasica::base_legal_de_incidencia_monofasica,
    },
};

/// Representa os regimes fiscais gerenciados pelo sistema analítico.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RegimesFiscais {
    /// Regime de Alíquota Zero (vendas desoneradas no mercado interno).
    AliquotaZero,
    /// Regime de Crédito Presumido (compensação de custos de etapas anteriores).
    CreditoPresumido,
    /// Regime de Incidência Monofásica (concentração tributária na produção/importação).
    IncidenciaMonofasica,
}

impl RegimesFiscais {
    /// Retorna o nome da coluna final de saída que será inserida no DataFrame.
    pub const fn get_column_name(&self) -> &'static str {
        match self {
            Self::AliquotaZero => "Alíquota Zero",
            Self::CreditoPresumido => "Crédito Presumido",
            Self::IncidenciaMonofasica => "Incidência Monofásica",
        }
    }

    /// Associa o regime fiscal à sua respectiva função de validação de base legal.
    ///
    /// Cada função avalia o enquadramento com base na tupla `(NCM, Descrição)`.
    pub fn get_base_legal_fn(
        &self,
    ) -> impl Fn(u64, &str) -> Option<&'static str> + Send + Sync + 'static {
        match self {
            Self::AliquotaZero => base_legal_de_aliquota_zero,
            Self::CreditoPresumido => base_legal_de_credito_presumido,
            Self::IncidenciaMonofasica => base_legal_de_incidencia_monofasica,
        }
    }

    /// Cria uma expressão Polars encapsulada para mapear e aplicar as validações do regime fiscal de forma segura.
    ///
    /// Reduz a duplicação de lógica (DRY) ao criar uma estrutura de dados `Struct` contendo
    /// a coluna de NCM convertida em String e a descrição física do item.
    pub fn make_eval_expr(&self, ncm_col: &str, desc_col: &str, alias: &'static str) -> Expr {
        let regime = *self;
        as_struct(vec![col(ncm_col).cast(DataType::String), col(desc_col)])
            .map(
                move |col: Column| aplicar_regime_fiscal(&col, regime),
                get_output_same_type,
            )
            .alias(alias)
    }
}

/// Adiciona a coluna analítica de "Alíquota Zero" ao `LazyFrame`.
///
/// Preenche a base legal correspondente de acordo com as regras estabelecidas na
/// Lei nº 10.925/2004 (Cesta Básica e insumos agrícolas) e Lei nº 10.865/2004.
pub fn adicionar_coluna_de_aliquota_zero(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    adicionar_coluna_de_regime_fiscal(lazyframe, RegimesFiscais::AliquotaZero)
}

/// Adiciona a coluna analítica de "Crédito Presumido" ao `LazyFrame`.
///
/// Preenche os enquadramentos de crédito presumido de insumos do agronegócio,
/// sob as regras da Lei nº 12.058/2009 (bovinos) ou Decreto nº 8.533/2015 (leite).
pub fn adicionar_coluna_de_credito_presumido(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    adicionar_coluna_de_regime_fiscal(lazyframe, RegimesFiscais::CreditoPresumido)
}

/// Adiciona a coluna analítica de "Incidência Monofásica" ao `LazyFrame`.
///
/// Identifica bens sujeitos ao regime monofásico de PIS/COFINS nas cadeias de combustíveis,
/// fármacos, cosméticos, autopeças e bebidas frias (Leis nº 9.718/98, 10.147/00 e 13.097/15).
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
    // Unifica o filtro de validação para as colunas temporárias geradas
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

    let lazyframe = lazyframe
        // Adicionar 2 colunas temporárias
        .with_columns([
            regime_fiscal.make_eval_expr(ncm_col_a, desc_col_a, temp_col_a),
            regime_fiscal.make_eval_expr(ncm_col_b, desc_col_b, temp_col_b),
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

/// Avalia uma coluna estruturada do Polars composta por NCM e descrição física.
///
/// Trata os tipos internos da coluna `StructColumn` de forma segura, prevenindo
/// erros de indexação ou falhas em linhas com esquemas ou valores ausentes.
fn aplicar_regime_fiscal(col: &Column, regime_fiscal: RegimesFiscais) -> PolarsResult<Column> {
    // Cast the input column to StructChunked to access its fields.
    // Ensure the "dtype-struct" feature is enabled in Cargo.toml for StructChunked
    let struct_chunked: &StructChunked = col.struct_()?;
    let fields = struct_chunked.fields_as_series();

    if fields.len() < 2 {
        return Err(PolarsError::ComputeError(
            "Coluna do tipo Struct necessita de ao menos 2 campos (NCM e Descrição)".into(),
        ));
    }

    // Get the fields as Series. Expected order: NCM (index 0), Description (index 1).
    let ser_codigoncm: &Series = &fields[0];
    let ser_descricao: &Series = &fields[1];

    // Get ChunkedArray<StringType> for NCM and description for efficient iteration.
    let ca_str_ncm = ser_codigoncm.str()?;
    let ca_str_dsc = ser_descricao.str()?;

    // Retrieve the specific legal basis function for the given fiscal regime.
    let base_legal = regime_fiscal.get_base_legal_fn(); // Get the specific function here

    // Iterate over NCM and description, apply base_legal function
    let new_col: Column = ca_str_ncm
        .iter()
        .zip(ca_str_dsc.iter())
        .map(|(opt_ncm, opt_desc)| match (opt_ncm, opt_desc) {
            (Some(ncm_str), Some(desc_str)) => match parse_ncm_to_u64(ncm_str) {
                Some(ncm) => base_legal(ncm, desc_str),
                None => {
                    eprintln!("Warning: Failed to extract digits from NCM '{}'", ncm_str);
                    None
                }
            },
            _ => None, // Handle missing NCM or description by returning None
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(new_col)
}

/// Extrai os dígitos de uma string e os converte para `u64`.
///
/// Remove pontuações comuns (ex: "2207.10.90" torna-se `22071090`) de forma otimizada
/// em nível de bytes ASCII.
///
/// ### Funcionamento:
/// 1. **`bytes()`**: Processa a string como bytes brutos (`u8`), evitando a decodificação UTF-8.
/// 2. **`filter()`**: Retém apenas os bytes no intervalo de `'0'` a `'9'` (ASCII 48 a 57).
/// 3. **`map()`**: Converte o byte em seu valor numérico real de 0 a 9 subtraindo `b'0'`.
/// 4. **`fold()`**: Acumula os dígitos deslocando as casas decimais à esquerda (`* 10`).
///    Inicia com `None` para retornar `None` se nenhum dígito for encontrado.
pub fn parse_ncm_to_u64(ncm_str: &str) -> Option<u64> {
    ncm_str
        .bytes()
        .filter(|b| b.is_ascii_digit())
        .map(|b| (b - b'0') as u64)
        .fold(None, |acc, digito| {
            // Inicializa com o primeiro dígito ou
            // desloca o valor existente multiplicando por 10 e soma o novo dígito
            Some(acc.map_or(digito, |valor| valor * 10 + digito))
        })
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
///
/// `cargo test -- --show-output tests_extracao_ncm`
#[cfg(test)]
mod tests_extracao_ncm {
    use super::parse_ncm_to_u64;

    #[test]
    fn test_ncm_formatado_padrao() {
        assert_eq!(parse_ncm_to_u64("2207.10.90"), Some(22071090));
        assert_eq!(parse_ncm_to_u64("1234-56-78"), Some(12345678));
        assert_eq!(parse_ncm_to_u64("12.34.56.78"), Some(12345678));
    }

    #[test]
    fn test_ncm_com_zeros_a_esquerda() {
        assert_eq!(parse_ncm_to_u64("0302.90.00"), Some(3029000));
        assert_eq!(parse_ncm_to_u64("00012345"), Some(12345));
    }

    #[test]
    fn test_ncm_com_prefixos_ou_textos() {
        assert_eq!(parse_ncm_to_u64("NCM: 12345678"), Some(12345678));
        assert_eq!(
            parse_ncm_to_u64("Produto Cod. 84136019 - Tipo A"),
            Some(84136019)
        );
    }

    #[test]
    fn test_entradas_sem_digitos() {
        assert_eq!(parse_ncm_to_u64("abc"), None);
        assert_eq!(parse_ncm_to_u64(""), None);
        assert_eq!(parse_ncm_to_u64(".-./"), None);
    }

    #[test]
    fn test_digito_unico_e_limites() {
        assert_eq!(parse_ncm_to_u64("0"), Some(0));
        assert_eq!(parse_ncm_to_u64(" 7 "), Some(7));
    }
}

// ----------------------------------------------------------------------------
// TESTS
// ----------------------------------------------------------------------------

/// Run tests with:
///
/// `cargo test -- --show-output regime_fiscal_tests`
#[cfg(test)]
mod regime_fiscal_tests {
    use super::*; // Import everything from the parent module
    use crate::JoinResult;

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
                "NCM 3100.00.00 : Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso I (Adubos ou Fertilizantes do Capítulo 31 e suas Matérias-Primas)."
            )
        );
        assert_eq!(
            aliquota_zero_col.get(1),
            Some(
                "NCM 12011000 : Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso III (Sementes destinadas à semeadura em conformidade com a Lei nº 10.711/2003)."
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
                "NCM 1030000 : Crédito Presumido - Lei nº 12.350/2010, Art. 55 (Animais Vivos da Posição 01.03 e 01.05: Suínos ou Aves)."
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
                "NCM 22071000 : Incidência Monofásica - Lei nº 9.718/1998, Art. 5º (Etanol hidratado ou anidro, conforme redação da Lei Complementar nº 214/2025)."
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
                "NCM 31000000 : Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso I (Adubos ou Fertilizantes do Capítulo 31 e suas Matérias-Primas)."
            )
        );
        assert_eq!(aliquota_zero_col.get(1), None);

        Ok(())
    }
}
