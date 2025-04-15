use crate::{coluna, Arguments, Side::Left};

use claudiofsr_lib::{
    CFOP_DE_EXPORTACAO, CFOP_VENDA_DE_IMOBILIZADO, CODIGO_DA_NATUREZA_BC, CSTS_NAO_TRIBUTADOS,
    CST_CREDITO, CST_CREDITO_BASICO, CST_RECEITA_BRUTA, PATTERN,
};

use polars::prelude::*;

/// Filtrar ReceitaBrutaTotal não Nula
///
/// Evitar divisão por Zero
pub fn receita_nao_nula() -> Expr {
    col("ReceitaBrutaTotal").neq(lit(0))
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações de Entrada ou Saída: `[1, 2]`
pub fn operacoes_de_entrada_ou_saida() -> Expr {
    operacoes([1, 2])
    //operacoes_v2(1..=2)
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações Entrada: `[1]`
pub fn operacoes_de_entrada() -> Expr {
    operacoes([1])
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações Saída: `[2]`
pub fn operacoes_de_saida() -> Expr {
    operacoes([2])
}

/// Entre as opções da coluna "tipo_operacao":
///
/// 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento.
///
/// Operações de Ajustes: `[3, 4]`
///
/// Operações de Descontos: `[5, 6]`
pub fn operacoes_de_ajustes_ou_descontos() -> Expr {
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
fn operacoes(range: impl IntoIterator<Item = u32>) -> Expr {
    let top: &str = coluna(Left, "tipo_operacao");

    // 1: Entrada, 2: Saída, 3 e 4: Ajustes, 5 e 6: Descontos, 7: Detalhamento
    let series: Series = range.into_iter().collect();

    col(top).is_not_null().and(col(top).is_in(lit(series)))
}

/// CST de Receita Bruta (Saídas):
///
/// CST de Receita Bruta Cumulativa e Receita Bruta Não Cumulativa.
///
/// Intervalo de CST: valores entre 1 a 9 e 49.
pub fn cst_de_receita_bruta() -> Expr {
    csts(CST_RECEITA_BRUTA)
}

/// CSTs de Operações de Saída Não Tributadas
///
/// CST: [4, 6, 7, 8, 9, 49]
pub fn csts_nao_tributados() -> Expr {
    csts(CSTS_NAO_TRIBUTADOS)
}

/// CST com direito ao desconto de crédito das Contribuições:
///
/// Intervalo de CST: valores entre 50 a 56 ou 60 a 66.
pub fn cst_50_a_66() -> Expr {
    csts(CST_CREDITO)
}

/// CST com direito ao desconto de crédito das Contribuições,
///
/// com exceção dos Créditos Presumidos (CST entre 60 e 66).
///
/// Intervalo de CST: valores entre 50 a 56.
pub fn cst_50_a_56() -> Expr {
    csts(CST_CREDITO_BASICO)
}

/// Códigos de Situação Tributária (CST)
///
/// Intervalo de CST válidos: valores entre 1 a 99.
pub fn csts<T>(range: impl IntoIterator<Item = T>) -> Expr
where
    u32: From<T>,
{
    let cst: &str = coluna(Left, "cst");
    let series: Series = get_series(range);

    col(cst).is_not_null().and(col(cst).is_in(lit(series)))
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
pub fn codigo_nat_01_a_18() -> Expr {
    let natureza: &str = coluna(Left, "natureza");
    let series: Series = get_series(CODIGO_DA_NATUREZA_BC);

    col(natureza)
        .is_not_null()
        .and(col(natureza).is_in(lit(series)))
}

/**
CFOP de Exportacao:

. Grupo 7:
    valores entre 7000 e 7999;

. Fim específico de exportação.
*/
pub fn cfop_de_exportacao() -> Expr {
    // "Código Fiscal de Operações e Prestações (CFOP)"
    let cfop: &str = coluna(Left, "cfop");
    let series: Series = get_series(CFOP_DE_EXPORTACAO);

    col(cfop)
        .is_null() // <-- IS NULL
        .or(col(cfop).is_in(lit(series)))
}

pub fn venda_de_imobilizado() -> Expr {
    // "Código Fiscal de Operações e Prestações (CFOP)"
    let cfop: &str = coluna(Left, "cfop");
    let series: Series = get_series(CFOP_VENDA_DE_IMOBILIZADO);

    col(cfop).is_not_null().and(col(cfop).is_in(lit(series)))
}

#[allow(dead_code)]
fn doacao_ou_brinde() -> Expr {
    // "Código Fiscal de Operações e Prestações (CFOP)"
    let cfop: &str = coluna(Left, "cfop");

    let series = Series::from_iter([5910, 6910]);

    col(cfop).is_not_null().and(col(cfop).is_in(lit(series)))
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

    // Check if this column of strings contains a Regex.
    // fn contains(self, pat: Expr, strict: bool)
    // see polars-plan-0.33.2/src/dsl/string.rs

    let descricao_do_item: Expr = col(coluna(Left, "item_desc"))
        .str()
        .contains(pattern.clone(), false); // "Descrição do Item"
    let escritur_contabil: Expr = col(coluna(Left, "contabil"))
        .str()
        .contains(pattern.clone(), false); // "Escrituração Contábil: Nome da Conta"
    let info_complementar: Expr = col(coluna(Left, "informacao"))
        .str()
        .contains(pattern, false); // "Informação Complementar do Documento Fiscal"

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
pub fn entrada_de_credito() -> Expr {
    let tipo_de_credito: &str = coluna(Left, "tipo_cred");
    let aliquota_de_pis: &str = coluna(Left, "aliq_pis"); // "Alíquota de PIS/PASEP (em percentual)"
    let aliquota_de_cof: &str = coluna(Left, "aliq_cof"); // "Alíquota de COFINS (em percentual)"

    operacoes_de_entrada() // 1: Entrada
        .and(col(tipo_de_credito).is_not_null())
        .and(col(aliquota_de_pis).is_not_null())
        .and(col(aliquota_de_cof).is_not_null())
        .and(cst_50_a_66())
        .and(codigo_nat_01_a_18())
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
pub fn saida_de_receita_bruta() -> Expr {
    operacoes_de_saida() // 2: Saída
        .and(cst_de_receita_bruta())
        .and(venda_de_imobilizado().not())
        .and(aliquota_de_receita_financeira().not())
        .and(descricao_de_outras_receitas().not())
}

/**
A Receita Bruta é composta por:

1. Receita Bruta Cumulativa
2. Receita Bruta Não Cumulativa

A Receita Bruta Cumulativa é identificada pelas alíquotas de pis = 0,65% e cofins = 3,0%.
*/
pub fn receita_bruta_nao_cumulativa() -> Expr {
    saida_de_receita_bruta().and(aliquota_de_receita_cumulativa().not())
}

/**
A Receita Bruta é composta por:

1. Receita Bruta Cumulativa
2. Receita Bruta Não Cumulativa

A Receita Bruta Cumulativa é identificada pelas alíquotas de pis = 0,65% e cofins = 3,0%.
*/
pub fn receita_bruta_cumulativa() -> Expr {
    saida_de_receita_bruta().and(aliquota_de_receita_cumulativa())
}

// Retain only credit entries (50 <= CST <= 66)
pub fn apply_filter(data_frame: DataFrame, args: &Arguments) -> Result<DataFrame, PolarsError> {
    if args.operacoes_de_creditos == Some(true) {
        data_frame
            .lazy()
            //.filter(entrada_de_credito().or(operacoes_de_entrada_ou_saida().not()))
            .filter(cst_50_a_66())
            .collect()
    } else {
        Ok(data_frame)
    }
}
