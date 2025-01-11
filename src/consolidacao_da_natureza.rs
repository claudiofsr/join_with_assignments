use polars::{datatypes::DataType, prelude::*};
use std::{error::Error, ops::Neg};

use crate::{
    cfop_de_exportacao, coluna, cst_50_a_66, cst_de_receita_bruta, csts, csts_nao_tributados,
    desprezar_pequenos_valores, entrada_de_credito, get_cnpj_base,
    operacoes_de_ajustes_ou_descontos, operacoes_de_saida, receita_bruta_cumulativa,
    receita_bruta_nao_cumulativa, receita_nao_nula, round_float64_columns, saida_de_receita_bruta,
    Side::Left,
};

const SMALL_VALUE: f64 = 0.009; // menor que um centavo

pub fn obter_consolidacao_nat(
    dataframe: &DataFrame,
    auditar: bool,
) -> Result<DataFrame, Box<dyn Error>> {
    let union_args = UnionArgs {
        parallel: true,
        rechunk: true,
        to_supertypes: true,
        diagonal: false,
        from_partitioned_ds: false,
    };

    let lazyframe: LazyFrame = dataframe.clone().lazy();

    let lazyframe: LazyFrame = selecionar_colunas_apos_filtros(lazyframe, auditar)?;

    let lazyframe: LazyFrame = groupby_and_agg_values(lazyframe)?;

    let lazyframe: LazyFrame = replicar_linha_de_soma_da_receita(lazyframe)?;

    let lazyframe: LazyFrame = ratear_bc_dos_creditos_conforme_receita_segregada(lazyframe)?;

    let lazyframe: LazyFrame = analisar_operacoes_de_saida(lazyframe, auditar, union_args)?;

    let lazyframe: LazyFrame = remover_colunas_temporarias(lazyframe)?;

    let lazyframe: LazyFrame = adicionar_valores_trimestrais(lazyframe, union_args)?;

    let lazyframe: LazyFrame = adicionar_linhas_de_soma_da_bc_dos_creditos(lazyframe, union_args)?;

    let lazyframe: LazyFrame = adicionar_linhas_de_apuracao_de_pis(lazyframe, union_args)?;

    let lazyframe: LazyFrame = adicionar_linhas_de_apuracao_de_cofins(lazyframe, union_args)?;

    let lazyframe: LazyFrame = adicionar_bc_dos_creditos_valor_total(lazyframe, union_args)?;

    let lazyframe: LazyFrame =
        adicionar_saldo_de_cred_passivel_de_ressarcimento_pis(lazyframe, union_args)?;

    let lazyframe: LazyFrame =
        adicionar_saldo_de_cred_passivel_de_ressarcimento_cofins(lazyframe, union_args)?;

    let lazyframe: LazyFrame = formatar_valores(lazyframe)?;

    let lazyframe: LazyFrame = ordenar_colunas(lazyframe)?;

    let lazyframe: LazyFrame = rename_columns(lazyframe)?;

    Ok(lazyframe.collect()?)
}

/// Reter apenas as colunas de interesse.
///
/// Em seguida, aplicar filtros.
fn selecionar_colunas_apos_filtros(
    lazyframe: LazyFrame,
    _auditar: bool,
) -> Result<LazyFrame, Box<dyn Error>> {
    //let pa_ano: i32 = 2015;
    //let pa_trimestres = Series::from_iter([1, 2, 3, 4]);

    let cst: &str = coluna(Left, "cst"); // "Código de Situação Tributária (CST)"
    let reg: &str = coluna(Left, "registro"); // "Registro"
    let top: &str = coluna(Left, "tipo_operacao"); // "Tipo de Operação"
    let val: &str = coluna(Left, "valor_item"); // "Valor Total do Item"

    // Tipo de Operação: 1 a 7, tal que:
    // 1: Entrada; 2: Saída; 3: Ajuste de Acréscimo; 4: Ajuste de Redução;
    // 5: Desconto da Contribuição Apurada no Próprio Período;
    // 6: Desconto Efetuado em Período Posterior; 7: Detalhamento.
    let operacoes_desejadas: Expr = col(top).is_not_null().and(col(top).neq(lit(7)));

    //let series = Series::new(reg.into(), ["C170"]);
    //let registros_selecionados = col(reg).is_in(lit(series));
    //let pattern: Expr = lit(r"(i?)C170|C100"); // regex
    //let registros_selecionados = col(reg).str().contains(pattern, false);

    // Selecionar colunas nesta ordem
    let selected: [Expr; 19] = [
        col("CNPJ Base"),
        col("Ano do Período de Apuração"),
        col("Trimestre do Período de Apuração"),
        col("Mês do Período de Apuração"),
        col(top),
        col("Código do Tipo de Crédito"),
        col("Tipo de Crédito"),
        col(cst),
        col(reg),
        col("Código Fiscal de Operações e Prestações (CFOP)"),
        col("Código NCM"),
        col("Alíquota de PIS/PASEP (em percentual)"),
        col("Alíquota de COFINS (em percentual)"),
        col("Natureza da Base de Cálculo dos Créditos"),
        col("Valor da Base de Cálculo das Contribuições"),
        col(val),
        col("RecBrutaNCumulativa"),
        col("RecBrutaCumulativa"),
        col("RecBrutaTotal"),
    ];

    let lazy_filtered: LazyFrame = lazyframe
        //.filter(col("Ano do Período de Apuração").eq(lit(2022)))
        //.filter(col("Mês do Período de Apuração").eq(lit(6)))
        .filter(operacoes_desejadas)
        .filter(
            entrada_de_credito()
                .or(saida_de_receita_bruta())
                //.or(receita_bruta_nao_cumulativa())
                .or(operacoes_de_ajustes_ou_descontos()),
        )
        .with_column(
            when(receita_bruta_nao_cumulativa())
                .then(lit(true))
                .otherwise(lit(false))
                .cast(DataType::Boolean)
                .alias("RecBrutaNCumulativa"),
        )
        .with_column(
            when(receita_bruta_cumulativa())
                .then(lit(true))
                .otherwise(lit(false))
                .cast(DataType::Boolean)
                .alias("RecBrutaCumulativa"),
        )
        .with_column(
            when(saida_de_receita_bruta())
                .then(lit(true))
                .otherwise(lit(false))
                .cast(DataType::Boolean)
                .alias("RecBrutaTotal"),
        )
        /*
        // Correção: CST 9 && Registro C170 --> "valor_item" = 0.0
        .with_column(
            //when(col(cst).eq(9).and(registros_selecionados).and(auditar))
            when(col(cst).eq(9).and(registros_selecionados))
                .then(lit(0.0))
                .otherwise(col(val))
                .cast(DataType::Float64)
                .alias(val),
        )
        */
        /*
        // Correção de CST: 63 -> 60
        .with_column(
            when(col(cst).eq(63).and(auditar))
                .then(lit(60))
                .otherwise(col(cst))
                .cast(DataType::Int64)
                .alias(cst),
        )
        // Correção de 'Código do Tipo de Crédito': 206 -> 106
        .with_column(
            when(col("Código do Tipo de Crédito").eq(206).and(auditar))
                .then(lit(106))
                .otherwise(col("Código do Tipo de Crédito"))
                .cast(DataType::Int64)
                .alias("Código do Tipo de Crédito"),
        )
        */
        .with_column(
            col("CNPJ dos Estabelecimentos do Contribuinte")
                .apply(get_cnpj_base, GetOutput::from_type(DataType::String))
                .alias("CNPJ Base"),
        )
        .select(&selected)
        .collect()?
        .lazy();

    Ok(lazy_filtered)
}

/// Distribuir valores de Ajustes e Descontos nas colunas correspondentes
fn transferir_valores(column_number: i64, receita: &str) -> Expr {
    let codigo_do_credito: &str = coluna(Left, "cod_cred"); // "Código do Tipo de Crédito"

    // De acordo com 4.3.6 – Tabela Código de Tipo de Crédito
    // when(col("Código do Tipo de Crédito").is_in(lit(range)))
    when(
        col(codigo_do_credito)
            .floor_div(lit(100))
            .eq(lit(column_number)),
    )
    .then(col("Valor Total do Item"))
    .otherwise(lit(NULL))
    .cast(DataType::Float64)
    .alias(receita)
}

fn analisar_natureza_da_bc() -> Expr {
    let natureza: &str = coluna(Left, "natureza");

    // Anular código se CST não pertencer ao intervalo [50, 66].
    when(cst_50_a_66())
        .then(col(natureza))
        .otherwise(lit(NULL))
        .cast(DataType::Int64)
        .alias(natureza)
}

fn groupby_and_agg_values(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    let reg: &str = coluna(Left, "registro");
    let top: &str = coluna(Left, "tipo_operacao");
    let valor_item: &str = coluna(Left, "valor_item"); // "Valor Total do Item"
    let valor_bc: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições"

    let range_a: [u32; 4] = [1, 2, 3, 5]; // RBNC_Tributada
    let range_b: [u32; 5] = [4, 6, 7, 9, 49]; // RBNC_NTributada
    let range_c: [u32; 1] = [8]; // RBNC_Exportação

    let filter_a = csts(range_a);
    let filter_b = csts(range_b).or(csts(range_c).and(cfop_de_exportacao().not()));
    let filter_c = csts(range_c).and(cfop_de_exportacao());

    let condition_a = filter_a.or(col("RBNC_Tributada").is_not_null());
    let condition_b = filter_b.or(col("RBNC_NTributada").is_not_null());
    let condition_c = filter_c.or(col("RBNC_Exportação").is_not_null());

    let condition_d = col("RecBrutaNCumulativa").or(operacoes_de_ajustes_ou_descontos());
    let condition_e = col("RecBrutaCumulativa");
    let condition_f = col("RecBrutaTotal").or(operacoes_de_ajustes_ou_descontos());

    let series = Series::new(reg.into(), ["M100", "1100"]);
    //let pattern: Expr = lit(r"(i?)M100|1100"); // regex

    let registros_selecionados = col(reg).is_in(lit(series));
    //let registros_selecionados = col(reg).str().contains(pattern, false);

    let lazy_groupby: LazyFrame = lazyframe
        .with_columns([
            // Adicionar 3 colunas para segregação da Receita Bruta Não Cumulativa
            transferir_valores(1, "RBNC_Tributada"),
            transferir_valores(2, "RBNC_NTributada"),
            transferir_valores(3, "RBNC_Exportação"),
        ])
        .drop([
            // Remover colunas temporárias
            "Código do Tipo de Crédito",
        ])
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação"),
            col("Tipo de Crédito"),
            col("Código de Situação Tributária (CST)"),
            col("Registro"),
            col("Código Fiscal de Operações e Prestações (CFOP)"),
            col("Código NCM"),
            col("Alíquota de PIS/PASEP (em percentual)"),
            col("Alíquota de COFINS (em percentual)"),
            analisar_natureza_da_bc(),
        ])
        .agg([
            col(valor_bc).sum(),
            col(valor_item).sum(),
            // Adicionar 6 colunas de Receita segregadas por CST e CFOP.
            col(valor_item)
                .filter(condition_a.and(condition_d.clone()))
                .sum()
                .alias("RBNC_Tributada"),
            col(valor_item)
                .filter(condition_b.and(condition_d.clone()))
                .sum()
                .alias("RBNC_NTributada"),
            col(valor_item)
                .filter(condition_c.and(condition_d.clone()))
                .sum()
                .alias("RBNC_Exportação"),
            col(valor_item)
                .filter(condition_d)
                .sum()
                .alias("RecBrutaNCumulativa"),
            col(valor_item)
                .filter(condition_e)
                .sum()
                .alias("RecBrutaCumulativa"),
            col(valor_item)
                .filter(condition_f)
                .sum()
                .alias("ReceitaBrutaTotal"),
        ])
        .collect()? // Executar procedimento para reduzir tamanho do dataframe
        .lazy() // Lazy operations don’t execute until we call collect.
        .with_columns([when(operacoes_de_ajustes_ou_descontos())
            .then(
                (
                    lit(10) * col(top) + // 30 ou 40: Ajustes, 50 ou 60: Descontos
                    when(registros_selecionados)
                    .then(lit(1))      // 1: PIS/PASEP
                    .otherwise(lit(5))
                    // 5: COFINS
                )
                .alias("Natureza da Base de Cálculo dos Créditos"),
            )
            .otherwise(col("Natureza da Base de Cálculo dos Créditos"))])
        .with_columns([when(operacoes_de_ajustes_ou_descontos())
            .then(col(valor_item).alias(valor_bc))
            .otherwise(col(valor_bc))]);

    Ok(lazy_groupby)
}

/// Replicar a segregacao da Receita para CST entre 50 e 66.
fn replicar_linha_de_soma_da_receita(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    // https://pola-rs.github.io/polars-book/user-guide/expressions/window/
    // https://stackoverflow.com/questions/74049748/how-to-get-an-item-in-a-polars-dataframe-column-and-put-it-back-into-the-same-co
    let discrimination_window = [
        "CNPJ Base",
        "Ano do Período de Apuração",
        "Trimestre do Período de Apuração",
        "Mês do Período de Apuração",
    ];

    let selected_columns: [&str; 6] = [
        "RBNC_Tributada",
        "RBNC_NTributada",
        "RBNC_Exportação",
        "RecBrutaNCumulativa",
        "RecBrutaCumulativa",
        "ReceitaBrutaTotal",
    ];

    let lazyframe: LazyFrame = lazyframe.with_columns([when(cst_50_a_66())
        .then(
            cols(selected_columns)
                .filter(cst_de_receita_bruta())
                .sum() // soma de valores para cst entre 01 a 09
                .over(discrimination_window),
        )
        .otherwise(cols(selected_columns).over(discrimination_window))]);

    Ok(lazyframe)
}

/// Ratear créditos conforme CST
fn ratear_creditos(receita: &str) -> Expr {
    let cst: &str = coluna(Left, "cst"); // "Código de Situação Tributária (CST)"
    let valor_bc: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições"

    // modulo operation returns the remainder of a division
    // `a % b = a - b * floor(a / b)`

    let cst_50_ou_60: Expr = (col(cst) % lit(10))
        .eq(lit(0))
        .and(lit(receita == "RBNC_Tributada"));
    let cst_51_ou_61: Expr = (col(cst) % lit(10))
        .eq(lit(1))
        .and(lit(receita == "RBNC_NTributada"));
    let cst_52_ou_62: Expr = (col(cst) % lit(10))
        .eq(lit(2))
        .and(lit(receita == "RBNC_Exportação"));
    let cst_53_ou_63: Expr = (col(cst) % lit(10)).eq(lit(3)).and(lit(
        receita == "RBNC_Tributada" || receita == "RBNC_NTributada"
    ));
    let cst_54_ou_64: Expr = (col(cst) % lit(10)).eq(lit(4)).and(lit(
        receita == "RBNC_Tributada" || receita == "RBNC_Exportação"
    ));
    let cst_55_ou_65: Expr = (col(cst) % lit(10)).eq(lit(5)).and(lit(
        receita == "RBNC_NTributada" || receita == "RBNC_Exportação"
    ));
    let cst_56_ou_66: Expr = (col(cst) % lit(10))
        .eq(lit(6))
        .and(lit(receita == "RBNC_Tributada"
            || receita == "RBNC_NTributada"
            || receita == "RBNC_Exportação"));

    let cst_rec_bruta_ncumulativa: Expr = lit(receita == "RecBrutaNCumulativa");
    let cst_rec_bruta_cumulativa: Expr = lit(receita == "RecBrutaCumulativa");
    let cst_rec_bruta_total: Expr = lit(receita == "ReceitaBrutaTotal");

    when(cst_50_a_66().and(receita_nao_nula()))
        .then(
            when(cst_56_ou_66) // ratear valor para as colunas 1 e 2 e 3
                .then(col(valor_bc) * col(receita) / col("RecBrutaNCumulativa"))
                .when(cst_50_ou_60) // transferir valor para a coluna 1
                .then(col(valor_bc))
                .when(cst_51_ou_61) // transferir valor para a coluna 2
                .then(col(valor_bc))
                .when(cst_52_ou_62) // transferir valor para a coluna 3
                .then(col(valor_bc))
                .when(cst_53_ou_63) // ratear valor para as colunas 1 e 2
                .then(
                    col(valor_bc) * col(receita) / (col("RBNC_Tributada") + col("RBNC_NTributada")),
                )
                .when(cst_54_ou_64) // ratear valor para as colunas 1 e 3
                .then(
                    col(valor_bc) * col(receita) / (col("RBNC_Tributada") + col("RBNC_Exportação")),
                )
                .when(cst_55_ou_65) // ratear valor para as colunas 2 e 3
                .then(
                    col(valor_bc) * col(receita)
                        / (col("RBNC_NTributada") + col("RBNC_Exportação")),
                )
                .when(cst_rec_bruta_ncumulativa) // ratear valor para a coluna de Receita Bruta Não Cumulativa
                .then(col(valor_bc) * col(receita) / col("ReceitaBrutaTotal"))
                .when(cst_rec_bruta_cumulativa) // ratear valor para a coluna de Receita Bruta Cumulativa
                .then(col(valor_bc) * col(receita) / col("ReceitaBrutaTotal"))
                .when(cst_rec_bruta_total) // ratear valor para a coluna de Receita Bruta Cumulativa
                .then(col(valor_bc) * col(receita) / col("ReceitaBrutaTotal"))
                .otherwise(lit(NULL)) // .cast(DataType::Float64)
                .alias(receita),
        )
        .otherwise(col(receita)) // .cast(DataType::Float64)
}

fn ratear_bc_dos_creditos_conforme_receita_segregada(
    lazyframe: LazyFrame,
) -> Result<LazyFrame, Box<dyn Error>> {
    let lazyframe: LazyFrame = lazyframe.with_columns([
        ratear_creditos("RBNC_Tributada"),
        ratear_creditos("RBNC_NTributada"),
        ratear_creditos("RBNC_Exportação"),
        ratear_creditos("RecBrutaNCumulativa"),
        ratear_creditos("RecBrutaCumulativa"),
        ratear_creditos("ReceitaBrutaTotal"),
    ]);

    Ok(lazyframe)
}

fn percentual(valor: &str, total: &str) -> Expr {
    (lit(100) * col(valor) / col(total)).alias(valor)
}

/// Adicionar linha com valores da Receita Bruta para rateio
///
/// Adicionar linha com porcentagens do rateio
///
/// Adicionar linhas de débitos omitidos se auditar == true
fn analisar_operacoes_de_saida(
    lazyframe: LazyFrame,
    auditar: bool,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let receita_bruta_valores: LazyFrame = lazyframe
        .clone()
        .filter(operacoes_de_saida())
        .filter(receita_nao_nula())
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação"),
            col("Tipo de Crédito"),
            col("Código de Situação Tributária (CST)")
                .apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Registro").apply(|s| set_some_str_value(s, None), GetOutput::same_type()),
            col("Código Fiscal de Operações e Prestações (CFOP)")
                .apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Código NCM").apply(|s| set_some_str_value(s, None), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Alíquota de COFINS (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Natureza da Base de Cálculo dos Créditos")
                .apply(|s| set_some_i64_value(s, Some(80)), GetOutput::same_type()),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("Valor Total do Item").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ])
        .with_column(
            col("Valor da Base de Cálculo das Contribuições")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
        );

    let receita_bruta_percentuais = receita_bruta_valores
        .clone()
        .with_columns([
            percentual("RBNC_Tributada", "RecBrutaNCumulativa"),
            percentual("RBNC_NTributada", "RecBrutaNCumulativa"),
            percentual("RBNC_Exportação", "RecBrutaNCumulativa"),
            percentual("RecBrutaNCumulativa", "ReceitaBrutaTotal"),
            percentual("RecBrutaCumulativa", "ReceitaBrutaTotal"),
            percentual("ReceitaBrutaTotal", "ReceitaBrutaTotal"),
        ])
        .with_column(
            lit(81)
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        );

    let lazy_restante: LazyFrame = lazyframe.clone().filter(operacoes_de_saida().not());

    // Reunir as partes anteriormente divididas.

    let mut partes = vec![
        receita_bruta_valores,
        receita_bruta_percentuais,
        //debitos_omitidos_ncm_2309,
        lazy_restante,
    ];

    if auditar {
        let debitos_omitidos_ncm_2309 = analisar_debitos_omitidos(lazyframe)?;
        partes.push(debitos_omitidos_ncm_2309);
    }

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(partes, union_args)?
        // Executar procedimento para reduzir tamanho do dataframe
        // Lazy operations don’t execute until we call collect.
        .collect()?
        .lazy();

    Ok(lazy_total)
}

/// Analisar débitos omitidos em Operações de Saída
fn analisar_debitos_omitidos(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    let ncm: &str = coluna(Left, "ncm");

    // NCM 2309.90.xx ou 230990xx
    let pattern: Expr = lit(r"^\D*2309\.?90"); // regex
    let ncm_2309: Expr = col(ncm).str().contains(pattern, false);

    // Instrução Normativa RFB nº 2121/2022 (atualmente em vigor), em seu artigo 569:
    // II - preparações classificadas no código 2309.90 da Tipi
    // É vedada a suspensão quando a aquisição for destinada à revenda.
    // Estas operações devem ser tributadas: revenda de mercadorias de NCM 2309.90

    let debitos_omitidos_ncm_2309: LazyFrame = lazyframe
        .filter(operacoes_de_saida())
        .filter(csts_nao_tributados())
        .filter(cfop_de_exportacao().not())
        .filter(ncm_2309)
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação"),
            col("Tipo de Crédito"),
            col("Código de Situação Tributária (CST)"),
            col("Registro").apply(|s| set_some_str_value(s, None), GetOutput::same_type()),
            col("Código Fiscal de Operações e Prestações (CFOP)")
                .apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Código NCM").apply(|s| set_some_str_value(s, None), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)").apply(
                |s| set_some_f64_value(s, Some(1.65)),
                GetOutput::same_type(),
            ), // Alíquotas Básicas
            col("Alíquota de COFINS (em percentual)").apply(
                |s| set_some_f64_value(s, Some(7.60)),
                GetOutput::same_type(),
            ), // Alíquotas Básicas
            col("Natureza da Base de Cálculo dos Créditos")
                .apply(|s| set_some_i64_value(s, Some(90)), GetOutput::same_type()),
        ])
        .agg([
            // Após soma, negativar valores
            col("Valor da Base de Cálculo das Contribuições")
                .sum()
                .neg(),
            col("Valor Total do Item").sum().neg(),
            col("RBNC_Tributada").sum().neg(),
            col("RBNC_NTributada").sum().neg(),
            col("RBNC_Exportação").sum().neg(),
            col("RecBrutaNCumulativa").sum().neg(),
            col("RecBrutaCumulativa").sum().neg(),
            col("ReceitaBrutaTotal").sum().neg(),
        ])
        .with_column(
            // sobrescrever valor
            col("Valor Total do Item").alias("Valor da Base de Cálculo das Contribuições"),
        )
        .with_column(
            // concentar valores dos Débitos na coluna: RBNC_Tributada.
            col("Valor Total do Item").alias("RBNC_Tributada"),
        )
        .with_columns([cols(["RBNC_NTributada", "RBNC_Exportação"])
            .apply(|s| set_some_f64_value(s, None), GetOutput::same_type())]);

    Ok(debitos_omitidos_ncm_2309.collect()?.lazy())
}

/// Agregar colunas para em seguida remover colunas temporárias
fn remover_colunas_temporarias(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    let lazy: LazyFrame = lazyframe
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação"),
            col("Tipo de Crédito"),
            col("Código de Situação Tributária (CST)"),
            col("Registro").apply(|s| set_some_str_value(s, None), GetOutput::same_type()),
            col("Código Fiscal de Operações e Prestações (CFOP)")
                .apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Código NCM").apply(|s| set_some_str_value(s, None), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)"),
            col("Alíquota de COFINS (em percentual)"),
            col("Natureza da Base de Cálculo dos Créditos"),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("Valor Total do Item").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ])
        .drop([
            // Remover colunas temporárias
            "Registro",
            "Código Fiscal de Operações e Prestações (CFOP)",
            "Código NCM",
            "Valor Total do Item",
            //"ReceitaBrutaTotal",
        ]);

    Ok(lazy)
}

fn adicionar_valores_trimestrais(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let natureza: &str = coluna(Left, "natureza");
    let series: Series = [90, 91, 95].iter().collect();
    let debitos_omitidos: Expr = col(natureza).is_in(lit(series));

    let lazyframe_trimestral: LazyFrame = lazyframe
        .clone()
        .filter(col("Tipo de Crédito").is_not_null().or(debitos_omitidos))
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            // Mês fictício 13, para fins de acumulação de valores trimestrais e ordenação
            col("Mês do Período de Apuração")
                .apply(|s| set_some_i64_value(s, Some(13)), GetOutput::same_type()),
            col("Tipo de Operação"),
            col("Tipo de Crédito"),
            col("Código de Situação Tributária (CST)"),
            col("Alíquota de PIS/PASEP (em percentual)"),
            col("Alíquota de COFINS (em percentual)"),
            col("Natureza da Base de Cálculo dos Créditos"),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ]);

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(&[lazyframe, lazyframe_trimestral], union_args)?
        // Executar procedimento para reduzir tamanho do dataframe
        // Lazy operations don’t execute until we call collect.
        .collect()?
        .lazy();

    Ok(lazy_total)
}

fn adicionar_linhas_de_soma_da_bc_dos_creditos(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let linha_de_soma_da_bc_dos_creditos: LazyFrame = lazyframe
        .clone()
        .filter(cst_50_a_66())
        .with_column(
            (lit(100) + col("Tipo de Crédito"))
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        )
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação"),
            col("Tipo de Crédito"),
            col("Código de Situação Tributária (CST)")
                .apply(|s| set_some_i64_value(s, Some(200)), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)"),
            col("Alíquota de COFINS (em percentual)"),
            col("Natureza da Base de Cálculo dos Créditos"),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ]);

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(&[lazyframe, linha_de_soma_da_bc_dos_creditos], union_args)?
        // Executar procedimento para reduzir tamanho do dataframe
        // Lazy operations don’t execute until we call collect.
        .collect()?
        .lazy();

    Ok(lazy_total)
}

/// Crédito Apurado no Período (PIS/PASEP)
///
/// Crédito Apurado no Período (COFINS)
///
/// As alíquotas têm precisão de 4 casas decimais
fn apuracao(aliquota: &str, valor: &str) -> Expr {
    (col(aliquota) * col(valor) / lit(100)).alias(valor)
}

fn adicionar_linhas_de_apuracao_de_pis(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let cst: &str = coluna(Left, "cst");
    let aliq_pis: &str = coluna(Left, "aliq_pis");
    let aliq_cof: &str = coluna(Left, "aliq_cof");

    // Selecionar apenas a linha de "Base de Cálculo dos Créditos:"
    let cst_200: Expr = col(cst).eq(lit(200));

    let lazy_credito_pis: LazyFrame = lazyframe
        .clone()
        .filter(cst_200)
        .with_column(
            col(cst)
                // CST 210 temporário para fins de ordenação
                .apply(|s| set_some_i64_value(s, Some(210)), GetOutput::same_type()),
        )
        .with_column(col(aliq_cof).apply(|s| set_some_f64_value(s, None), GetOutput::same_type()))
        .with_columns([
            apuracao(aliq_pis, "Valor da Base de Cálculo das Contribuições"),
            apuracao(aliq_pis, "RBNC_Tributada"),
            apuracao(aliq_pis, "RBNC_NTributada"),
            apuracao(aliq_pis, "RBNC_Exportação"),
            apuracao(aliq_pis, "RecBrutaNCumulativa"),
            apuracao(aliq_pis, "RecBrutaCumulativa"),
            apuracao(aliq_pis, "ReceitaBrutaTotal"),
        ])
        .with_column(
            lit(201)
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        );

    let lazy_debitos_pis: LazyFrame = lazyframe
        .clone()
        .filter(col("Natureza da Base de Cálculo dos Créditos").eq(lit(90)))
        .with_column(col(aliq_cof).apply(|s| set_some_f64_value(s, None), GetOutput::same_type()))
        .with_columns([
            apuracao(aliq_pis, "Valor da Base de Cálculo das Contribuições"),
            apuracao(aliq_pis, "RBNC_Tributada"),
            apuracao(aliq_pis, "RBNC_NTributada"),
            apuracao(aliq_pis, "RBNC_Exportação"),
            apuracao(aliq_pis, "RecBrutaNCumulativa"),
            apuracao(aliq_pis, "RecBrutaCumulativa"),
            apuracao(aliq_pis, "ReceitaBrutaTotal"),
        ])
        .with_column(
            lit(91)
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        );

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame =
        concat(&[lazyframe, lazy_credito_pis, lazy_debitos_pis], union_args)?;

    Ok(lazy_total)
}

fn adicionar_linhas_de_apuracao_de_cofins(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let cst: &str = coluna(Left, "cst");
    let aliq_pis: &str = coluna(Left, "aliq_pis");
    let aliq_cof: &str = coluna(Left, "aliq_cof");

    // Selecionar apenas a linha de "Base de Cálculo dos Créditos:"
    let cst_200: Expr = col(cst).eq(lit(200));

    let lazy_credito_cofins: LazyFrame = lazyframe
        .clone()
        .filter(cst_200)
        .with_column(
            col(cst)
                // CST 250 temporário para fins de ordenação
                .apply(|s| set_some_i64_value(s, Some(250)), GetOutput::same_type()),
        )
        .with_column(col(aliq_pis).apply(|s| set_some_f64_value(s, None), GetOutput::same_type()))
        .with_columns([
            apuracao(aliq_cof, "Valor da Base de Cálculo das Contribuições"),
            apuracao(aliq_cof, "RBNC_Tributada"),
            apuracao(aliq_cof, "RBNC_NTributada"),
            apuracao(aliq_cof, "RBNC_Exportação"),
            apuracao(aliq_cof, "RecBrutaNCumulativa"),
            apuracao(aliq_cof, "RecBrutaCumulativa"),
            apuracao(aliq_cof, "ReceitaBrutaTotal"),
        ])
        .with_column(
            lit(205)
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        );

    let lazy_debitos_cofins: LazyFrame = lazyframe
        .clone()
        .filter(col("Natureza da Base de Cálculo dos Créditos").eq(lit(90)))
        .with_column(col(aliq_pis).apply(|s| set_some_f64_value(s, None), GetOutput::same_type()))
        .with_columns([
            apuracao(aliq_cof, "Valor da Base de Cálculo das Contribuições"),
            apuracao(aliq_cof, "RBNC_Tributada"),
            apuracao(aliq_cof, "RBNC_NTributada"),
            apuracao(aliq_cof, "RBNC_Exportação"),
            apuracao(aliq_cof, "RecBrutaNCumulativa"),
            apuracao(aliq_cof, "RecBrutaCumulativa"),
            apuracao(aliq_cof, "ReceitaBrutaTotal"),
        ])
        .with_column(
            lit(95)
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        );

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(
        &[lazyframe, lazy_credito_cofins, lazy_debitos_cofins],
        union_args,
    )?;

    Ok(lazy_total)
}

fn adicionar_bc_dos_creditos_valor_total(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    // Selecionar apenas a linha de "Base de Cálculo dos Créditos:"
    let cst_200: Expr = col("Código de Situação Tributária (CST)").eq(lit(200));

    let bc_dos_creditos_valor_total: LazyFrame = lazyframe
        .clone()
        .filter(cst_200)
        .with_column(
            lit(300)
                .alias("Natureza da Base de Cálculo dos Créditos")
                .cast(DataType::Int64),
        )
        // Soma Mensal
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação").apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Tipo de Crédito")
                .apply(|s| set_some_i64_value(s, Some(100)), GetOutput::same_type()),
            col("Código de Situação Tributária (CST)")
                .apply(|s| set_some_i64_value(s, Some(400)), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Alíquota de COFINS (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Natureza da Base de Cálculo dos Créditos"),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ]);

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(&[lazyframe, bc_dos_creditos_valor_total], union_args)?;

    Ok(lazy_total)
}

fn adicionar_saldo_de_cred_passivel_de_ressarcimento_pis(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let natureza: &str = coluna(Left, "natureza");
    //let aliq_pis: &str = coluna(Left, "aliq_pis");
    //let aliq_cof: &str = coluna(Left, "aliq_cof");

    let series = Series::new(natureza.into(), [31, 41, 51, 61, 91, 201, 211, 221]);
    let nat_pis: Expr = col(natureza).is_in(lit(series));

    let saldo_de_pis: LazyFrame = lazyframe
        .clone()
        .filter(cst_50_a_66().not())
        //.filter(col(aliq_cof).is_null())
        //.filter(col(aliq_pis).is_not_null().or(col(natureza).eq(lit(61))))
        .filter(nat_pis)
        .with_column(lit(301).alias(natureza).cast(DataType::Int64))
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação").apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Tipo de Crédito")
                .apply(|s| set_some_i64_value(s, Some(100)), GetOutput::same_type()),
            col("Código de Situação Tributária (CST)")
                .apply(|s| set_some_i64_value(s, Some(410)), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Alíquota de COFINS (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Natureza da Base de Cálculo dos Créditos"),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ]);

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(&[lazyframe, saldo_de_pis], union_args)?;

    Ok(lazy_total)
}

fn adicionar_saldo_de_cred_passivel_de_ressarcimento_cofins(
    lazyframe: LazyFrame,
    union_args: UnionArgs,
) -> Result<LazyFrame, Box<dyn Error>> {
    let natureza: &str = coluna(Left, "natureza");
    //let aliq_pis: &str = coluna(Left, "aliq_pis");
    //let aliq_cof: &str = coluna(Left, "aliq_cof");

    let series = Series::new(natureza.into(), [35, 45, 55, 65, 95, 205, 215, 225]);
    let nat_cofins: Expr = col(natureza).is_in(lit(series));

    let saldo_de_pis: LazyFrame = lazyframe
        .clone()
        .filter(cst_50_a_66().not())
        //.filter(col(aliq_pis).is_null())
        //.filter(col(aliq_cof).is_not_null().or(col(natureza).eq(lit(65))))
        .filter(nat_cofins)
        .with_column(lit(305).alias(natureza).cast(DataType::Int64))
        .group_by([
            col("CNPJ Base"),
            col("Ano do Período de Apuração"),
            col("Trimestre do Período de Apuração"),
            col("Mês do Período de Apuração"),
            col("Tipo de Operação").apply(|s| set_some_i64_value(s, None), GetOutput::same_type()),
            col("Tipo de Crédito")
                .apply(|s| set_some_i64_value(s, Some(100)), GetOutput::same_type()),
            col("Código de Situação Tributária (CST)")
                .apply(|s| set_some_i64_value(s, Some(450)), GetOutput::same_type()),
            col("Alíquota de PIS/PASEP (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Alíquota de COFINS (em percentual)")
                .apply(|s| set_some_f64_value(s, None), GetOutput::same_type()),
            col("Natureza da Base de Cálculo dos Créditos"),
        ])
        .agg([
            col("Valor da Base de Cálculo das Contribuições").sum(),
            col("RBNC_Tributada").sum(),
            col("RBNC_NTributada").sum(),
            col("RBNC_Exportação").sum(),
            col("RecBrutaNCumulativa").sum(),
            col("RecBrutaCumulativa").sum(),
            col("ReceitaBrutaTotal").sum(),
        ]);

    // https://docs.rs/polars/latest/polars/prelude/fn.concat.html
    let lazy_total: LazyFrame = concat(&[lazyframe, saldo_de_pis], union_args)?;

    Ok(lazy_total)
}

/// Formatar valores das colunas.
///
/// Aliquotas e valores podem ter precisões distintas.
fn formatar_valores(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    let aliq_pis = coluna(Left, "aliq_pis"); // "Alíquota de PIS/PASEP (em percentual)"
    let aliq_cof = coluna(Left, "aliq_cof"); // "Alíquota de COFINS (em percentual)"
    let valor_bc = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições"

    let aliquotas = [aliq_pis, aliq_cof];

    let valores = [
        valor_bc,
        "RBNC_Tributada",
        "RBNC_NTributada",
        "RBNC_Exportação",
        "RecBrutaNCumulativa",
        "RecBrutaCumulativa",
        "ReceitaBrutaTotal",
    ];

    let colunas_float64 = [&aliquotas[..], &valores[..]].concat();

    let lazy_formated: LazyFrame = lazyframe
        .with_columns([when(operacoes_de_saida())
            .then(cols(valores).apply(|col| round_float64_columns(col, 4), GetOutput::same_type()))
            .otherwise(
                cols(valores).apply(|col| round_float64_columns(col, 4), GetOutput::same_type()),
            )])
        .with_columns([
            cols(aliquotas).apply(|col| round_float64_columns(col, 4), GetOutput::same_type())
        ])
        .with_columns([cols(colunas_float64).apply(
            |col| desprezar_pequenos_valores(col, SMALL_VALUE),
            GetOutput::same_type(),
        )]);

    Ok(lazy_formated)
}

/// Ordenar colunas
fn ordenar_colunas(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    let cst: &str = coluna(Left, "cst");

    let lazy_sorted: LazyFrame = lazyframe
        .sort_by_exprs(
            vec![
                col("CNPJ Base"),
                col("Ano do Período de Apuração"),
                col("Trimestre do Período de Apuração"),
                col("Mês do Período de Apuração"),
                col("Tipo de Crédito"),
                col("Tipo de Operação"),
                col("Código de Situação Tributária (CST)"),
                col("Natureza da Base de Cálculo dos Créditos"),
                col("Alíquota de COFINS (em percentual)"),
                col("Alíquota de PIS/PASEP (em percentual)"),
                col("Valor da Base de Cálculo das Contribuições"),
            ],
            // https://github.com/pola-rs/polars/pull/15590
            SortMultipleOptions::default()
                .with_maintain_order(true)
                .with_multithreaded(true)
                .with_order_descending(false)
                .with_nulls_last(false),
        )
        .with_column(
            when(col(cst).gt(lit(100)))
                .then(lit(NULL)) // replace by null
                .otherwise(col(cst)) // keep original value
                .alias(cst),
        );

    Ok(lazy_sorted)
}

fn rename_columns(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {
    // Remover colunas temporárias
    let selected = [
        "RBNC_Tributada",
        "RBNC_NTributada",
        "RBNC_Exportação",
        "RecBrutaNCumulativa",
        "RecBrutaCumulativa",
        "ReceitaBrutaTotal",
    ];

    let lazyframe: LazyFrame = lazyframe.select(&[
        all().exclude(selected),
        col("RBNC_Tributada").alias("Crédito vinculado à Receita Bruta Não Cumulativa: Tributada"),
        col("RBNC_NTributada")
            .alias("Crédito vinculado à Receita Bruta Não Cumulativa: Não Tributada"),
        col("RBNC_Exportação")
            .alias("Crédito vinculado à Receita Bruta Não Cumulativa: de Exportação"),
        col("RecBrutaNCumulativa").alias("Crédito vinculado à Receita Bruta Não Cumulativa"),
        col("RecBrutaCumulativa")
            .alias("Crédito vinculado à Receita Bruta Cumulativa (Valores Excluídos)"),
        col("ReceitaBrutaTotal").alias("Crédito vinculado à Receita Bruta Total"),
    ]);
    //.drop(selected);

    Ok(lazyframe)
}

/*
pub fn set_some_value<T>(series: Series, opt_value: Option<T>) -> PolarsResult<Option<Series>>
where
    T: std::clone::Clone,
    polars::prelude::Series: std::iter::FromIterator<std::option::Option<T>>,
{
    // A vector with all elements equal to the same value.
    let values: Vec<Option<T>> = vec![opt_value; series.len()];
    let series: Series = values.into_iter().collect();
    Ok(Some(series))
}
*/

pub fn set_some_i64_value(col: Column, opt_value: Option<i64>) -> PolarsResult<Option<Column>> {
    /*
    // A vector with all elements equal to the same value.
    let values = vec![opt_value; series.len()];
    let series: Series = values.into_iter().collect();
    Ok(Some(series))
    */

    let new_col: Column = col
        .i64()?
        .into_iter()
        .map(|_option_i64| opt_value)
        .collect::<Int64Chunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn set_some_f64_value(col: Column, opt_value: Option<f64>) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .f64()?
        .into_iter()
        .map(|_option_f64| opt_value)
        .collect::<Float64Chunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn set_some_str_value(col: Column, opt_value: Option<&str>) -> PolarsResult<Option<Column>> {
    let new_col: Column = col
        .str()?
        .into_iter()
        .map(|_option_utf8| opt_value)
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::configure_the_environment;

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// See polars-0.33.2/tests/it/lazy/explodes.rs
    ///
    /// cargo test -- --show-output test_explode_row_numbers
    fn test_explode_row_numbers() -> PolarsResult<()> {
        configure_the_environment();

        let df_init = df![
            "text" => [
                "one two three four",
                "uno dos tres cuatro",
                "um dois três quatro",
            ]
        ]?;

        println!("df_init: {df_init}\n");

        let df = df_init
            .lazy()
            .select([col("text").str().split(lit(" ")).alias("tokens")])
            .with_row_index("row_nr", None)
            .explode([col("tokens")])
            .select([col("row_nr"), col("tokens")])
            .collect()?;

        println!("df: {df}\n");

        assert_eq!(df.shape(), (12, 2));
        Ok(())
    }

    #[test]
    /// See polars-0.33.2/tests/it/lazy/explodes.rs
    ///
    /// cargo test -- --show-output duplicar_linhas_do_dataframe
    fn duplicar_linhas_do_dataframe() -> Result<(), Box<dyn Error>> {
        configure_the_environment();

        let dataframe01: DataFrame = df! [
            "Código" => [108, 201, 308, 201, 101],
            "Registro" => ["CTe", "cte", "xx", "NFe", "NotaFiscal"],
            "Descrição CFOP" => ["Anula", "amostra", "brinde", "Anula", "anulação"],
        ]?;

        println!("dataframe01: {dataframe01}\n");

        let filtro_codigo: Expr = col("Código").floor_div(lit(100)).eq(lit(2));

        let lazyframe: LazyFrame = dataframe01
            .lazy()
            .with_column(
                when(filtro_codigo)
                    .then(lit(9))
                    .otherwise(col("Código"))
                    .alias("Código Div"),
            )
            // repeat each row in a polars dataframe a particular number of times?
            .select(&[all().repeat_by(lit(2)).explode()])
            //.explode([col("contador")]);
            // contador de linhas
            .with_row_index("contador", Some(1u32));

        println!("lazyframe: {}\n", lazyframe.clone().collect()?);

        let lazyframe: LazyFrame = lazyframe.with_columns([
            //when(col("contador") % lit(2) == lit(0))
            when(col("contador").map(
                move |col| {
                    Ok(Some(
                        col.u32()?
                            .into_iter()
                            .map(|opt_u32: Option<u32>| opt_u32.map(|value| value % 2 == 0))
                            .collect::<BooleanChunked>()
                            .into_column(),
                    ))
                },
                GetOutput::from_type(DataType::Boolean),
            ))
            .then(lit("nº par").alias("Registro"))
            .otherwise(col("Registro")),
        ]);

        let dataframe02: DataFrame = lazyframe.collect()?;

        println!("dataframe02: {dataframe02}\n");

        // Get columns from dataframe
        let natureza: &Column = dataframe02.column("Registro")?;

        let col: Column = Column::new(
            "Registro".into(),
            &[
                "CTe",
                "nº par",
                "cte",
                "nº par",
                "xx",
                "nº par",
                "NFe",
                "nº par",
                "NotaFiscal",
                "nº par",
            ],
        );

        assert_eq!(natureza, &col);

        Ok(())
    }

    #[test]
    /// Fonte: polars-core-0.30.0/src/frame/mod.rs
    ///
    /// cargo test -- --show-output test_slice_args
    fn test_slice_args() -> PolarsResult<()> {
        configure_the_environment();

        let groups: StringChunked = std::iter::repeat("a")
            .take(10)
            .chain(std::iter::repeat("b").take(20))
            .collect();

        let dataframe01: DataFrame = df![
            "groups" => groups.into_series(),
            "vals" => 0i32..30
        ]?;

        println!("dataframe01: {dataframe01}\n");

        let dataframe02: DataFrame = dataframe01
            .lazy()
            .group_by_stable([col("groups")])
            // pub fn slice<E, F>(self, offset: E, length: F) -> Expr
            // a_length = 10 * 0.2 = 2
            // b_length = 20 * 0.2 = 4
            .agg([col("vals").slice(lit(2), (len() * lit(2)) / lit(10))])
            .collect()?;

        println!("dataframe02: {dataframe02}\n");

        let out = dataframe02.column("vals")?.explode()?;
        let out = out.i32().unwrap();
        assert_eq!(
            out.into_no_null_iter().collect::<Vec<_>>(),
            &[2, 3, 12, 13, 14, 15]
        );

        Ok(())
    }

    #[test]
    /// See polars-lazy-version/src/tests/streaming.rs
    ///
    /// See polars-lazy-version/src/tests/queries.rs
    ///
    /// cargo test -- --show-output test_sort_by
    fn test_sort_by() -> PolarsResult<()> {
        let df: DataFrame = df![
            "a" => [1, 2, 3, 4, 5],
            "b" => [1, 1, 1, 2, 2],
            "c" => [2, 3, 1, 2, 1],
        ]?;

        println!("dataframe01: {df}\n");

        // evaluate
        let out: DataFrame = df
            .lazy()
            .sort_by_exprs(
                vec![
                    col("b"),
                    col("c"),
                    // col("a"), // "b" e "c" determina a ordem de "a".
                ],
                SortMultipleOptions::default()
                    .with_maintain_order(true)
                    .with_multithreaded(true)
                    .with_order_descending(false)
                    .with_nulls_last(false),
            )
            .collect()?;

        println!("dataframe02: {out}\n");

        assert_eq!(
            out,
            df!(
                "a" => [3, 1, 2, 5, 4],
                "b" => [1, 1, 1, 2, 2],
                "c" => [1, 2, 3, 1, 2],
            )?
        );

        Ok(())
    }

    #[test]
    /// operators: neq, eq, gt, gteq, lt, lteq
    ///
    /// cargo test -- --show-output null_in_conditional
    fn null_in_conditional() -> PolarsResult<()> {
        let df01: DataFrame = df![
            "Category" => ["Food", "Clothes", "Unknown", "Gender", "Unknown", "Gender"],
            "Code"     => [10, 20, 30, 25, 3, 75],
            "Anular"   => [15, 25, 35, 23, 7, 77],
        ]?;

        println!("dataframe01: {df01}\n");

        /*
        dataframe01: shape: (6, 3)
        ┌──────────┬──────┬────────┐
        │ Category ┆ Code ┆ Anular │
        │ ---      ┆ ---  ┆ ---    │
        │ str      ┆ i32  ┆ i32    │
        ╞══════════╪══════╪════════╡
        │ Food     ┆ 10   ┆ 15     │
        │ Clothes  ┆ 20   ┆ 25     │
        │ Unknown  ┆ 30   ┆ 35     │
        │ Gender   ┆ 25   ┆ 23     │
        │ Unknown  ┆ 3    ┆ 7      │
        │ Gender   ┆ 75   ┆ 77     │
        └──────────┴──────┴────────┘
        */

        let condition: Expr = col("Category").eq(lit("Unknown"));

        // if the condition is satisfied, null all values ​​in the row
        let df02: DataFrame = df01
            .lazy()
            .with_columns([
                when(condition)
                    .then(lit(NULL))
                    .otherwise(all())
                    .name()
                    .keep(), // .keep_name()
            ])
            .collect()?;

        println!("dataframe02: {df02}\n");

        /*
        dataframe02: shape: (6, 3)
        ┌──────────┬──────┬────────┐
        │ Category ┆ Code ┆ Anular │
        │ ---      ┆ ---  ┆ ---    │
        │ str      ┆ i32  ┆ i32    │
        ╞══════════╪══════╪════════╡
        │ Food     ┆ 10   ┆ 15     │
        │ Clothes  ┆ 20   ┆ 25     │
        │ null     ┆ null ┆ null   │
        │ Gender   ┆ 25   ┆ 23     │
        │ null     ┆ null ┆ null   │
        │ Gender   ┆ 75   ┆ 77     │
        └──────────┴──────┴────────┘
        */

        let df03: DataFrame = df02
            .clone()
            .lazy()
            .group_by([col("Category")])
            .agg([col("Code").sum(), col("Anular").sum()])
            .collect()?;

        println!("dataframe03: {df03}\n");

        /*
        dataframe03: shape: (4, 3)
        ┌──────────┬──────┬────────┐
        │ Category ┆ Code ┆ Anular │
        │ ---      ┆ ---  ┆ ---    │
        │ str      ┆ i32  ┆ i32    │
        ╞══════════╪══════╪════════╡
        │ Clothes  ┆ 20   ┆ 25     │
        │ Gender   ┆ 100  ┆ 100    │
        │ null     ┆ 0    ┆ 0      │
        │ Food     ┆ 10   ┆ 15     │
        └──────────┴──────┴────────┘
        */

        let df04: DataFrame = df03
            .clone()
            .lazy()
            .filter(col("Category").is_not_null())
            .sort(["Anular"], Default::default())
            .collect()?;

        println!("dataframe04: {df04}\n");

        /*
        dataframe04: shape: (3, 3)
        ┌──────────┬──────┬────────┐
        │ Category ┆ Code ┆ Anular │
        │ ---      ┆ ---  ┆ ---    │
        │ str      ┆ i32  ┆ i32    │
        ╞══════════╪══════╪════════╡
        │ Food     ┆ 10   ┆ 15     │
        │ Clothes  ┆ 20   ┆ 25     │
        │ Gender   ┆ 100  ┆ 100    │
        └──────────┴──────┴────────┘
        */

        // Get columns from dataframe
        let anular: &Column = df04.column("Anular")?;

        let col = Column::new("Anular".into(), [Some(15), Some(25), Some(100)]);

        assert_eq!(anular, &col);

        Ok(())
    }
}
