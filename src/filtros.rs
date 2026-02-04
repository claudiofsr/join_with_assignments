use crate::{Arguments, Side::Left, ToLiteralListExpr, coluna};

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
use polars::error::PolarsResult;
use join_with_assignments::get_series;

fn main() -> PolarsResult<()> {

    let array: [u32; 4] = [1, 5, 8, 9];
    let series = get_series(array);

    // How do I avoid unwrap when converting a vector of Options or Results to only the successful values?
    // https://stackoverflow.com/questions/36020110/how-do-i-avoid-unwrap-when-converting-a-vector-of-options-or-results-to-only-the
    // vec.into_iter().flatten().collect()

    let values: Vec<u32> = series
        .u32()?
        .into_iter()
        .flatten()
        .collect();

    assert_eq!(
        values,
        vec![1, 5, 8, 9]
    );

    Ok(())
}
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

/*
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
}
*/
