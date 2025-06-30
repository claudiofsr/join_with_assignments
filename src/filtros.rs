use crate::{Arguments, Side::Left, coluna};

use claudiofsr_lib::{
    CFOP_DE_EXPORTACAO, CFOP_VENDA_DE_IMOBILIZADO, CODIGO_DA_NATUREZA_BC, CST_CREDITO,
    CST_CREDITO_BASICO, CST_RECEITA_BRUTA, CSTS_NAO_TRIBUTADOS, PATTERN,
};

use polars::prelude::*;

/// Check if the columns are the same
pub fn equal(col_a: &str, col_b: &str) -> Expr {
    // Uma das colunas com campos não nulos!!!
    col(col_a)
        .is_not_null()
        .and(col(col_b).is_not_null()) // it is not necessary?
        .and(col(col_a).eq(col(col_b)))
}

/// Check if the columns are different
pub fn unequal(col_a: &str, col_b: &str) -> Expr {
    col(col_a)
        .is_not_null()
        .and(col(col_b).is_not_null())
        .and(col(col_a).neq(col(col_b)))
}

/// Operaçẽs de Crédito
pub fn operacoes_de_credito() -> PolarsResult<Expr> {
    let expr = operacoes_de_entrada_ou_saida()?
        .and(cst_50_a_66()?)
        .and(codigo_nat_01_a_18()?);
    Ok(expr)
}

/// Filtrar ReceitaBrutaTotal não Nula
///
/// Evitar divisão por Zero
pub fn receita_nao_nula() -> Expr {
    col("ReceitaBrutaTotal")
        .is_not_null()
        .and(col("ReceitaBrutaTotal").neq(lit(0)))
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações de Entrada ou Saída: `[1, 2]`
pub fn operacoes_de_entrada_ou_saida() -> PolarsResult<Expr> {
    operacoes([1, 2])
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações Entrada: `[1]`
pub fn operacoes_de_entrada() -> PolarsResult<Expr> {
    operacoes([1])
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações Saída: `[2]`
pub fn operacoes_de_saida() -> PolarsResult<Expr> {
    operacoes([2])
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações de Ajustes: `[3, 4]`
///
/// Operações de Descontos: `[5, 6]`
pub fn operacoes_de_ajustes_ou_descontos() -> PolarsResult<Expr> {
    operacoes([3, 4, 5, 6])
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações de Entrada ou Saída: `[1, 2]`
///
/// Operações Saída: `[2]`
///
/// Operações de Ajustes: `[3, 4]`
///
/// Operações de Descontos: `[5, 6]`
///
/// E demais opções.
fn operacoes(range: impl IntoIterator<Item = u32>) -> PolarsResult<Expr> {
    let top: &str = coluna(Left, "tipo_operacao");

    // 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento
    let series: Series = range.into_iter().collect();
    let literal_series: Expr = series.to_list_expr()?;

    let expr = col(top)
        .is_not_null()
        .and(col(top).is_in(literal_series, true));

    Ok(expr)
}

/// CST de Receita Bruta (Saídas):
///
/// CST de Receita Bruta Cumulativa e Receita Bruta Não Cumulativa.
///
/// Intervalo de CST: valores entre 1 a 9 e 49.
pub fn cst_de_receita_bruta() -> PolarsResult<Expr> {
    csts(CST_RECEITA_BRUTA)
}

/// CSTs de Operações de Saída Não Tributadas
///
/// CST: [4, 6, 7, 8, 9, 49]
pub fn csts_nao_tributados() -> PolarsResult<Expr> {
    csts(CSTS_NAO_TRIBUTADOS)
}

/// CST com direito ao desconto de crédito das Contribuições:
///
/// Intervalo de CST: valores entre 50 a 56 ou 60 a 66.
pub fn cst_50_a_66() -> PolarsResult<Expr> {
    csts(CST_CREDITO)
}

/// CST com direito ao desconto de crédito das Contribuições,
///
/// com exceção dos Créditos Presumidos (CST entre 60 e 66).
///
/// Intervalo de CST: valores entre 50 a 56.
pub fn cst_50_a_56() -> PolarsResult<Expr> {
    csts(CST_CREDITO_BASICO)
}

/// Códigos de Situação Tributária (CST)
///
/// Intervalo de CST válidos: valores entre 1 a 99.
pub fn csts<T>(range: impl IntoIterator<Item = T>) -> PolarsResult<Expr>
where
    u32: From<T>,
{
    let cst: &str = coluna(Left, "cst");
    let series: Series = get_series(range);
    let literal_series: Expr = series.to_list_expr()?;

    let expr = col(cst)
        .is_not_null()
        .and(col(cst).is_in(literal_series, true));

    Ok(expr)
}

/**
Get series from array
```
    use join_with_assignments::get_series;

    let array: [u32; 4] = [1, 5, 8, 9];
    let series = get_series(array);

    // How do I avoid unwrap when converting a vector of Options or Results to only the successful values?
    // https://stackoverflow.com/questions/36020110/how-do-i-avoid-unwrap-when-converting-a-vector-of-options-or-results-to-only-the
    // vec.into_iter().flatten().collect()

    let values: Vec<u32> = series
        .u32()
        .unwrap()
        .into_iter()
        .flatten()
        .collect();

    assert_eq!(
        values,
        vec![1, 5, 8, 9]
    );
```
*/
pub fn get_series<T>(range: impl IntoIterator<Item = T>) -> Series
where
    u32: From<T>,
{
    range.into_iter().map(u32::from).collect()
}

/// Código da Natureza da Base de Cálculo dos Créditos:
///
/// valores entre 1 a 18.
fn codigo_nat_01_a_18() -> PolarsResult<Expr> {
    let natureza: &str = coluna(Left, "natureza");
    let series: Series = get_series(CODIGO_DA_NATUREZA_BC);
    let literal_series: Expr = series.to_list_expr()?;

    let expr = col(natureza)
        .is_not_null()
        .and(col(natureza).is_in(literal_series, true));

    Ok(expr)
}

/**
CFOP de Exportacao:

. Grupo 7:
    valores entre 7000 e 7999;

. Fim específico de exportação.
*/
pub fn cfop_de_exportacao() -> PolarsResult<Expr> {
    // "Código Fiscal de Operações e Prestações (CFOP)"
    let cfop: &str = coluna(Left, "cfop");
    let series: Series = get_series(CFOP_DE_EXPORTACAO);
    let literal_series: Expr = series.to_list_expr()?;

    let expr = col(cfop)
        .is_not_null()
        .and(col(cfop).is_in(literal_series, true));

    Ok(expr)
}

pub fn venda_de_imobilizado() -> PolarsResult<Expr> {
    // "Código Fiscal de Operações e Prestações (CFOP)"
    let cfop: &str = coluna(Left, "cfop");
    let series: Series = get_series(CFOP_VENDA_DE_IMOBILIZADO);
    let literal_series: Expr = series.to_list_expr()?;

    let expr = col(cfop)
        .is_not_null()
        .and(col(cfop).is_in(literal_series, true));

    Ok(expr)
}

#[allow(dead_code)]
fn doacao_ou_brinde() -> PolarsResult<Expr> {
    // "Código Fiscal de Operações e Prestações (CFOP)"
    let cfop: &str = coluna(Left, "cfop");

    let series = Series::from_iter([5910, 6910]);
    let literal_series: Expr = series.to_list_expr()?;

    let expr = col(cfop)
        .is_not_null()
        .and(col(cfop).is_in(literal_series, true));

    Ok(expr)
}

/**
Receitas Não Operacionais (outras receitas):
são aquelas decorrentes de transações não incluídas nas atividades
principais ou acessórias que constituam objeto da empresa.

Receita Bruta:
receitas das atividades principais ou acessórias oriundas
da venda de produtos e da prestação de serviços.

Esta é uma lista com possíveis Receitas Não Operacionais
a depender das atividades que constituam objeto da empresa.
*/
pub fn descricao_de_outras_receitas() -> Expr {
    let pattern: Expr = lit(PATTERN);

    let item_desc: &str = coluna(Left, "item_desc"); // "Descrição do Item"
    let contabil: &str = coluna(Left, "contabil"); // "Escrituração Contábil: Nome da Conta"
    let informacao: &str = coluna(Left, "informacao"); // "Informação Complementar do Documento Fiscal"

    // Check if this column of strings contains a Regex.
    // fn contains(self, pat: Expr, strict: bool)
    // see polars-plan-0.33.2/src/dsl/string.rs

    let descricao_do_item: Expr = col(item_desc)
        .is_not_null()
        .and(col(item_desc).str().contains(pattern.clone(), false));
    let escritur_contabil: Expr = col(contabil)
        .is_not_null()
        .and(col(contabil).str().contains(pattern.clone(), false));
    let info_complementar: Expr = col(informacao)
        .is_not_null()
        .and(col(informacao).str().contains(pattern, false));

    descricao_do_item
        .or(escritur_contabil)
        .or(info_complementar)
}

/// Alíquota de Receita Financeira
///
/// Indicativo de Outras Receitas
///
/// Alíquota de PIS/PASEP = 0,65%
///
/// Alíquota de COFINS = 4,00%
pub fn aliquota_de_receita_financeira() -> Expr {
    let aliquota_de_pis: &str = coluna(Left, "aliq_pis"); // "Alíquota de PIS/PASEP (em percentual)"
    let aliquota_de_cof: &str = coluna(Left, "aliq_cof"); // "Alíquota de COFINS (em percentual)"

    // Alíquotas de Receitas Financeiras: pis 0,65% e cofins 4,00%
    let aliq_pis: Expr = col(aliquota_de_pis)
        .is_not_null()
        .and(col(aliquota_de_pis).eq(lit(0.65)));
    let aliq_cof: Expr = col(aliquota_de_cof)
        .is_not_null()
        .and(col(aliquota_de_cof).eq(lit(4.0)));

    aliq_pis.and(aliq_cof)
}

/// Alíquota de Receita Bruta Cumulativa
///
/// Alíquota de PIS/PASEP = 0,65%
///
/// Alíquota de COFINS = 3,00%
pub fn aliquota_de_receita_cumulativa() -> Expr {
    let aliquota_de_pis: &str = coluna(Left, "aliq_pis"); // "Alíquota de PIS/PASEP (em percentual)"
    let aliquota_de_cof: &str = coluna(Left, "aliq_cof"); // "Alíquota de COFINS (em percentual)"

    // Alíquotas de Receitas Financeiras: pis 0,65% e cofins 4,00%
    let aliq_pis: Expr = col(aliquota_de_pis)
        .is_not_null()
        .and(col(aliquota_de_pis).eq(lit(0.65)));
    let aliq_cof: Expr = col(aliquota_de_cof)
        .is_not_null()
        .and(col(aliquota_de_cof).eq(lit(3.0)));

    aliq_pis.and(aliq_cof)
}

/// Insumos com direito ao desconto de crédito das Contribuições
pub fn entrada_de_credito() -> PolarsResult<Expr> {
    let tipo_de_credito: &str = coluna(Left, "tipo_cred");
    let aliquota_de_pis: &str = coluna(Left, "aliq_pis"); // "Alíquota de PIS/PASEP (em percentual)"
    let aliquota_de_cof: &str = coluna(Left, "aliq_cof"); // "Alíquota de COFINS (em percentual)"

    let expr = operacoes_de_entrada()? // 1: Entrada
        .and(col(tipo_de_credito).is_not_null())
        .and(col(aliquota_de_pis).is_not_null())
        .and(col(aliquota_de_cof).is_not_null())
        .and(cst_50_a_66()?)
        .and(codigo_nat_01_a_18()?);

    Ok(expr)
}

/**
Receita Bruta

A Receita Bruta compreende:

I - o produto da venda de bens nas operações de conta própria;

II - o preço da prestação de serviços em geral;

III - o resultado auferido nas operações de conta alheia; e

IV - as receitas da atividade ou objeto principal da pessoa jurídica não compreendidas nos incisos I a III.

Legislação:

art. 12 do Decreto-Lei nº 1.598/1977 (legislação do Imposto sobre a Renda (IR))

art. 3º da Lei nº 9.715/1998

Em geral, não integram o cálculo da Receita Bruta:

Venda de Ativo Imobilizado (CFOP 5551, 6551 e 7551),

Receitas Financeiras,

Receitas de Variação Cambial,

Descontos Financeiros,

Juros sobre Recebimento ou Juros sobre Capital Próprio,

Hedge,

Receitas de Aluguéis de bens móveis e imóveis,

entre outras.
*/
pub fn saida_de_receita_bruta() -> PolarsResult<Expr> {
    let expr = operacoes_de_saida()? // 2: Saída
        .and(cst_de_receita_bruta()?)
        .and(venda_de_imobilizado()?.not())
        .and(aliquota_de_receita_financeira().not())
        .and(descricao_de_outras_receitas().not());

    Ok(expr)
}

/**
A Receita Bruta é composta por:

1. Receita Bruta Cumulativa
2. Receita Bruta Não Cumulativa

A Receita Bruta Cumulativa é identificada pelas alíquotas de pis = 0,65% e cofins = 3,0%.
*/
pub fn receita_bruta_nao_cumulativa() -> PolarsResult<Expr> {
    let expr = saida_de_receita_bruta()?.and(aliquota_de_receita_cumulativa().not());
    Ok(expr)
}

/**
A Receita Bruta é composta por:

1. Receita Bruta Cumulativa
2. Receita Bruta Não Cumulativa

A Receita Bruta Cumulativa é identificada pelas alíquotas de pis = 0,65% e cofins = 3,0%.
*/
pub fn receita_bruta_cumulativa() -> PolarsResult<Expr> {
    let expr = saida_de_receita_bruta()?.and(aliquota_de_receita_cumulativa());
    Ok(expr)
}

// Retain only credit entries (50 <= CST <= 66)
pub fn apply_filter(data_frame: DataFrame, args: &Arguments) -> Result<DataFrame, PolarsError> {
    // Opções da coluna "tipo_operacao":
    // 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
    // let tipo_operacao: &str = coluna(Left, "tipo_operacao");

    if let Some(true) = args.operacoes_de_creditos {
        data_frame
            .lazy()
            //.filter(csts([63u32]))
            .filter(operacoes_de_saida()?.not())
            //.filter(entrada_de_credito().or(operacoes_de_entrada_ou_saida().not()))
            .collect()
    } else {
        Ok(data_frame)
    }
}

/// A trait to convert a Polars Series into a literal List Expression.
///
/// This is suitable for use cases like `Expr::is_in` when checking column values
/// against a fixed collection of values. The resulting expression represents a
/// Literal Series containing a single row with a List value containing all elements
/// from the original Series.
///
/// Internally, this implodes the Series into a single-row List Series, and then
/// converts that into a Literal Expr. This pattern is the idiomatic way in recent
/// Polars versions (0.47.1) to correctly represent a fixed list of values for
/// membership checks within the expression API.
///
/// See feature: Add 'nulls_equal' parameter to is_in
/// #[21426](https://github.com/pola-rs/polars/pull/21426)
///
pub trait ToLiteralListExpr {
    /// Converts the Series into a Polars literal Expression
    /// representing a List value.
    fn to_list_expr(&self) -> PolarsResult<Expr>;
}

// --- Trait Implementation for Series ---

impl ToLiteralListExpr for Series {
    fn to_list_expr(&self) -> PolarsResult<Expr> {
        // 1. Implode the Series into a single ChunkedArray<ListType> (height 1).
        let imploded_chunked_array = self.implode()?; // implode returns PolarsResult, propagate error with `?`

        // 2. Convert the ChunkedArray<ListType> back into a Series (height 1).
        let imploded_series: Series = imploded_chunked_array.into_series(); // into_series does not return Result

        // 3. Create a Literal expression from this single-value List Series using its `.lit()` method.
        let literal_expr: Expr = imploded_series.lit(); // .lit() does not return Result

        // Return the Expression wrapped in Ok, as the method signature requires PolarsResult<Expr>
        Ok(literal_expr)
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_is_in
#[cfg(test)]
mod tests_is_in {
    use super::*; // Bring items from the outer scope into tests module
    use polars::df; // For df! macro
    use polars::error::PolarsResult;

    #[test]
    fn test_is_in_default_nulls_equal_false() -> PolarsResult<()> {
        // 1. Create the DataFrame (or just the Series to operate on in tests)
        //    DataFrame with a column named "numbers" mirroring pl.Series([1, 2, None])
        let df_original = df!("numbers" => &[Some(1i32), Some(2), None])?;
        println!("DataFrame Original:\n{df_original}");

        // 2. Create the 'other' list expression [1, 3]
        let series: Series = Series::new("null_vals".into(), &[1i32, 3]); // Series of i32
        println!("series: {series}");

        let new_col = series.implode()?.into_series();
        let df = df_original
            .lazy()
            .with_columns([new_col.lit().first().alias("filter")])
            .collect()?;
        println!("df: {df}");

        let list_expr: Expr = series.to_list_expr()?;

        // 3. Apply the is_in expression
        //    Select the column "numbers" and check if values are in the literal list expr
        let expr: Expr = col("numbers").is_in(list_expr, false); // nulls_equal=False (default)

        // 4. Execute the expression on the DataFrame
        let modified_df = df
            .lazy()
            .with_columns([expr.alias("is_in_result")])
            .collect()?;

        // 5. Define the expected result
        let expected_result = Series::new("is_in_result".into(), &[Some(true), Some(false), None]);
        // Explanation: 1 is in [1, 3], 2 is not, None is not considered equal to 1 or 3 when nulls_equal=False

        // 6. Assert
        println!("Test default nulls_equal=false:\n{modified_df}");
        assert_eq!(modified_df["is_in_result"], expected_result.into());

        Ok(())
    }

    #[test]
    fn test_is_in_nulls_equal_true() -> PolarsResult<()> {
        // 1. Create the DataFrame (or just the Series to operate on in tests)
        //    DataFrame with a column named "numbers" mirroring pl.Series([1, 2, None])
        let df_original = df!("numbers" => &[Some(1i32), Some(2), None])?;
        println!("DataFrame Original:\n{df_original}");

        // 2. Create the 'other' list expression [1, 3]
        let series: Series = Series::new("null_vals".into(), &[1i32, 3]); // Series of i32
        println!("series: {series}");

        let new_col = series.implode()?.into_series();
        let df = df_original
            .lazy()
            .with_columns([new_col.lit().first().alias("filter")])
            .collect()?;
        println!("df: {df}");

        let list_expr: Expr = series.to_list_expr()?;

        // 3. Apply the is_in expression
        //    Select the column "numbers" and check if values are in the literal list expr
        let expr: Expr = col("numbers").is_in(list_expr, true); // nulls_equal=True

        // 4. Execute the expression on the DataFrame
        let modified_df = df
            .lazy()
            .with_columns([expr.alias("is_in_result")])
            .collect()?;

        // 5. Define the expected result
        let expected_result = Series::new(
            "is_in_result".into(),
            &[Some(true), Some(false), Some(false)],
        );
        // Explanation: 1 is in [1, 3], 2 is not, None is not considered equal to 1 or 3 when nulls_equal=False

        // 6. Assert
        println!("Test default nulls_equal=false:\n{modified_df}");
        assert_eq!(modified_df["is_in_result"], expected_result.into());

        Ok(())
    }

    #[test]
    fn test_is_in_nulls_equal_true_with_null_in_list() -> PolarsResult<()> {
        // 1. Create the DataFrame (or just the Series to operate on in tests)
        //    DataFrame with a column named "numbers" mirroring pl.Series([1, 2, None])
        let df_original = df!("numbers" => &[Some(1i32), Some(2), None])?;
        println!("DataFrame Original:\n{df_original}");

        // 2. Create the 'other' list expression [1, None]
        //    We need a Series that can hold Option<i32> values
        let series: Series = Series::new("null_vals".into(), &[Some(1i32), None]); // Series of Option<i32> (underlying dtype is i32 with nulls)
        println!("series: {series}");

        let new_col = series.implode()?.into_series();
        let df = df_original
            .lazy()
            .with_columns([new_col.lit().first().alias("filter")])
            .collect()?;
        println!("df: {df}");

        let list_expr: Expr = series.to_list_expr()?;

        // 3. Apply the is_in expression
        let expr: Expr = col("numbers").is_in(list_expr, true); // nulls_equal=True

        // 4. Execute the expression on the DataFrame
        let modified_df = df
            .lazy()
            .with_columns([expr.alias("is_in_result")])
            .collect()?;

        // 5. Define the expected result
        // Explanation: 1 is in [1, None]. 2 is not. None on left matches None on right because nulls_equal=True.
        let expected_result = Series::new(
            "is_in_result".into(),
            &[Some(true), Some(false), Some(true)],
        );

        // 6. Assert
        println!("Test nulls_equal=true with list [1, None]:\n{modified_df}");
        assert_eq!(modified_df["is_in_result"], expected_result.into());

        Ok(())
    }
}
