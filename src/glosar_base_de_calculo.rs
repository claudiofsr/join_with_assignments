use std::{env, fs::File, io::Write};

use crate::{
    Arguments, DataFrameExtension, ExprExtension, JoinResult, LazyFrameExtension,
    Side::{Left, Middle, Right},
    adicionar_coluna_de_aliquota_zero, adicionar_coluna_de_credito_presumido,
    adicionar_coluna_de_incidencia_monofasica,
    adicionar_coluna_periodo_de_apuracao_inicial_e_final, coluna, configure_the_environment,
    cst_50_a_56, equal, format_list_dates, operacoes_de_credito, unequal,
};
use polars::prelude::*;

/// CFOP de Armazenagem de mercadoria
pub const CFOP_DE_ARMAZENAGEM: [i32; 10] =
    [5905, 5907, 5923, 5934, 5949, 6905, 6907, 6923, 6934, 6949];

/// CFOP de Industrialização por encomenda
pub const CFOP_DE_INDUSTRIALIZACAO: [i32; 8] = [1124, 1125, 2124, 2125, 5124, 5125, 6124, 6125];

/*
// ### --- cte_valor.csv --- ###
let myschema = Schema::from_iter([
    Field::new("CTe", DataType::String),
    Field::new("Valor", DataType::Float64),
]);

let csv_file = "Dados - Nd/cte_valor.csv";

let cte_valor_lazyframe: LazyFrame = LazyCsvReader::new(csv_file)
    .with_encoding(CsvEncoding::LossyUtf8)
    .with_separator(b';')
    .has_header(true)
    .with_schema(Some(Arc::new(myschema)))
    .finish()?;

println!("cte_valor_lazyframe: {:?}", cte_valor_lazyframe.collect()?);
// ### --- cte_valor.csv --- ###
*/

pub fn glosar_bc(dataframe: &DataFrame, args: &Arguments) -> JoinResult<DataFrame> {
    let lazyframe: LazyFrame = dataframe.clone().lazy();

    let lazyframe: LazyFrame = adicionar_coluna_de_aliquota_zero(lazyframe)?;
    let lazyframe: LazyFrame = adicionar_coluna_de_credito_presumido(lazyframe)?;
    let lazyframe: LazyFrame = adicionar_coluna_de_incidencia_monofasica(lazyframe)?;

    let lazyframe: LazyFrame = lazyframe.adicionar_colunas_auxiliares();

    let lazyframe: LazyFrame = analisar_situacao01(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao02(lazyframe, args)?;
    let lazyframe: LazyFrame = analisar_situacao03(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao04(lazyframe)?;
    // let lazyframe: LazyFrame = analisar_situacao05(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao06a(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao06b(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao07(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao08(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao09(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao10(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao11(lazyframe)?;
    let lazyframe: LazyFrame = analisar_situacao12(lazyframe)?;
    // let lazyframe: LazyFrame = analisar_situacao13(lazyframe)?;
    // let lazyframe: LazyFrame = analisar_situacao14(lazyframe)?;

    Ok(lazyframe
        //.remover_colunas_auxiliares()
        .format_values()
        .collect()?
        .sort_by_columns(None)?)
}

/// Código de Regime Tributário (CRT) igual a 1
fn optante_do_simples_nacional() -> Expr {
    let regime_tributario: &str = coluna(Right, "regime_tributario"); // "CRT : NF (Todos)"
    col(regime_tributario).eq(lit(1))
}

fn analisar_situacao01(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    // /home/claudio/.cargo/registry/src/index.crates.io-6f17d22bba15001f/polars-plan-0.34.2/src/dsl/string.rs
    // https://pola-rs.github.io/polars/polars/export/regex/index.html

    let glosar: &str = coluna(Middle, "glosar");
    let cancelada: &str = coluna(Right, "cancelada");

    let pattern: Expr = lit(r"(?i)^\s*Sim"); // regex
    let docs_cancelados: Expr = col(cancelada).str().contains(pattern, false);

    let situacao_01: Expr = operacoes_de_credito()?
        .and(col(cancelada).is_not_null())
        .and(docs_cancelados);

    println!("situacao_01: {situacao_01:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 01:"),
            lit("NF-e/CT-e Cancelada (Cláusula 12ª do Ajuste Sinief 07/2005 e Cláusula 14ª"),
            lit(
                "do Ajuste Sinief 09/2007 do CONFAZ e Art. 327 do RIPI - Decreto nº 7.212 de 2010).",
            ),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_01, mensagem, lit(0))?;

    Ok(lf_result)
}

fn analisar_situacao02(lazyframe: LazyFrame, args: &Arguments) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let dia_emissao: &str = coluna(Right, "dia_emissao"); // "Dia da Emissão : NF Item (Todos)",

    // Adicionar 2 colunas temporárias: "Período de Apuração Inicial" e "Período de Apuração Final".
    let pa_ini: &str = "Período de Apuração Inicial";
    let pa_fim: &str = "Período de Apuração Final";

    let lazyframe: LazyFrame =
        adicionar_coluna_periodo_de_apuracao_inicial_e_final(lazyframe, args)?;

    let situacao_02: Expr = operacoes_de_credito()?
        .and(col(dia_emissao).is_not_null())
        .and(
            col(dia_emissao)
                .lt(col(pa_ini))
                .or(col(dia_emissao).gt(col(pa_fim))),
        );

    println!("situacao_02: {situacao_02:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 02:"),
            lit("Crédito extemporâneo."),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_02, mensagem, lit(0))?;

    // Remover 2 colunas temporárias
    let lf_result: LazyFrame = lf_result.drop_columns(&[pa_ini, pa_fim])?;

    Ok(lf_result)
}

fn analisar_situacao03(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let cfop: &str = coluna(Right, "cfop");
    let origem_do_item: &str = coluna(Right, "origem"); // "Registro de Origem do Item : NF Item (Todos)"
    let regime_tributario: &str = coluna(Right, "regime_tributario"); // "CRT : NF (Todos)"

    // let aliq_pis: &str = coluna(Right, "aliq_pis");
    // let aliq_cof: &str = coluna(Right, "aliq_cof");

    let columns: Vec<&str> = vec![
        "Alíquota Zero",
        "Alíquota Zero Temp", // coluna temporária
        "Incidência Monofásica",
        "Incidência Monofásica Temp", // coluna temporária
    ];

    let pattern01: Expr = lit(r"(?i)NFe"); // regex
    let nfe: Expr = col(origem_do_item).str().contains(pattern01, false);

    // CFOPs relacionados a Serviços de Armazanagem ou Industrialização por encomenda.
    let cfop_valido = [
        CFOP_DE_ARMAZENAGEM.as_slice(),
        CFOP_DE_INDUSTRIALIZACAO.as_slice(),
    ]
    .concat();
    let series: Series = cfop_valido.iter().collect();
    let literal_series: Expr = series.implode()?.into_series().lit();

    // Estes serviços são insumos com direito à crédito das Contribuições que devem ser excluídos das glosas.
    let cfop_de_insumos: Expr = col(cfop).is_in(literal_series, true);

    // Alíquotas de PIS/PASEP e de COFINS iguais a Zero
    // let aliquotas_zero: Expr = col(aliq_pis).eq(lit(0)).and(col(aliq_cof).eq(lit(0)));

    let filter: Expr = operacoes_de_credito()?
        .and(cst_50_a_56()?) // Excluir crédito Presumido da Agroindústria
        .and(
            col(regime_tributario)
                .is_null()
                .or(optante_do_simples_nacional().not()),
        )
        .and(col(origem_do_item).is_null().or(nfe))
        .and(col(cfop).is_null().or(cfop_de_insumos.not()));
    //.and(aliquotas_zero);

    // Adicionar coluna temporária
    let lazyframe: LazyFrame = lazyframe
        .with_column(
            when(filter.clone())
                .then(columns[0]) // keep original value: "Alíquota Zero"
                .otherwise(lit(NULL)) // replace by null
                .alias(columns[1]), // Coluna Temporária
        )
        .with_column(
            when(filter)
                .then(columns[2]) // keep original value: "Incidência Monofásica"
                .otherwise(lit(NULL)) // replace by null
                .alias(columns[3]), // Coluna Temporária
        );

    let situacao_03: Expr = col(columns[1])
        .is_not_null()
        .or(col(columns[3]).is_not_null());

    println!("situacao_03: {situacao_03:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 03:"),
            lit("Aquisição de bens ou serviços não sujeitos ao pagamento da contribuição."),
            lit(
                "De acordo com o inciso II do § 2º do art. 3º das Leis 10.637/2002 e 10.833/2003, não dará direito",
            ),
            lit(
                "a crédito o valor da aquisição de bens ou serviços não sujeitos ao pagamento da contribuição.",
            ),
            col(columns[1]),
            col(columns[3]),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_03, mensagem, lit(0))?;

    // Remover coluna temporária
    let lf_result: LazyFrame = lf_result.drop_columns(&[columns[1], columns[3]])?;

    Ok(lf_result)
}

fn analisar_situacao04(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let valor_total_do_item: &str = coluna(Left, "valor_item"); // "Valor Total do Item"
    let valor_bc: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições"
    let valor_da_nota_proporcional_nfe: &str = coluna(Right, "valor_item"); // "Valor da Nota Proporcional : NF Item (Todos) SOMA"

    let tomador1: &str = coluna(Right, "tomador_papel1");
    let tomador2: &str = coluna(Right, "tomador_papel2");

    let cnpj_base_do_contribuinte = "CNPJ Base do Contribuinte";
    let cnpj_base_do_remetente = "CNPJ Base do Remetente";
    let cnpj_base_do_destinatario = "CNPJ Base do Destinatário";
    let valor_cte_vinculado = "Valor Total de Documentos Vinculados";

    // "CNPJ Base do Contribuinte" eq "CNPJ Base do Destinatário"
    // O Contribuinte é o Destinatário das operações.
    let destinatario_das_operacoes: Expr =
        equal(cnpj_base_do_contribuinte, cnpj_base_do_destinatario);

    // "CNPJ Base do Remetente"  eq "CNPJ Base do Destinatário": operação de transferência
    // "CNPJ Base do Remetente" neq "CNPJ Base do Destinatário": operação de compra
    let cnpjs_distintos: Expr = unequal(cnpj_base_do_remetente, cnpj_base_do_contribuinte);

    let operacao_de_compra: Expr = destinatario_das_operacoes.and(cnpjs_distintos);

    let valores_iguais: Expr = equal(valor_bc, valor_da_nota_proporcional_nfe);

    // O Tomador do CTe é o Remetente
    let pattern: Expr = lit(r"(?i)Remetente"); // regex
    let tomador_remetente1: Expr = col(tomador1).str().contains(pattern.clone(), false);
    let tomador_remetente2: Expr = col(tomador2).str().contains(pattern, false);
    let tomador_remetente: Expr = tomador_remetente1.or(tomador_remetente2);

    let cte_valor_minimo = col(valor_cte_vinculado).gt(lit(10));
    let delta: Expr = col(valor_bc) - col(valor_total_do_item) - col(valor_cte_vinculado);
    let base_calculo_superestimada = delta.gt_eq(lit(-0.02));
    let valor_justo: Expr = col(valor_bc) - col(valor_cte_vinculado);

    let situacao_04: Expr = operacoes_de_credito()?
        .and(optante_do_simples_nacional().not())
        .and(valores_iguais)
        .and(operacao_de_compra)
        .and(tomador_remetente)
        .and(cte_valor_minimo)
        .and(base_calculo_superestimada);

    println!("situacao_04: {situacao_04:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 04:"),
            lit(
                "Valor do frete adicionado ao valor do insumo acarretando acréscimo indevido na Base de Cálculo das Contribuições,",
            ),
            lit(
                "tal que o fornecedor do insumo quem efetuou o pagamento do frete, remetente tomador.",
            ),
            lit(
                "Ver colunas: [CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento] e",
            ),
            lit("[Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento] e"),
            lit(
                "[CNPJ Base do Remetente] e [CNPJ Base do Destinatário] e [Valor Total de Documentos Vinculados].",
            ),
            lit("Valor da Base de Cálculo = "),
            col(valor_bc).round_expr(2),
            lit("-"),
            col(valor_cte_vinculado).round_expr(2),
            lit("="),
            valor_justo.clone().round_expr(2),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_04, mensagem, valor_justo)?;

    Ok(lf_result)
}

#[allow(dead_code)]
fn analisar_situacao05(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let valor_bc: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições"
    let valor_da_nota_proporcional_nfe: &str = coluna(Right, "valor_item"); // "Valor da Nota Proporcional : NF Item (Todos) SOMA";
    // let valor_da_base_calculo_icms_nfe: &str = coluna(Right, "valor_bc_icms"); // "ICMS: Base de Cálculo : NF Item (Todos) SOMA"

    let valores_iguais_nota_prop: Expr = equal(valor_bc, valor_da_nota_proporcional_nfe);
    //let valores_iguais_base_icms: Expr = col(valor_da_bcal_da_efd).eq(col(valor_da_base_calculo_icms_nfe));

    let delta: Expr = col(valor_bc) - col("ICMS: Valor do Tributo : NF Item (Todos) SOMA");
    let boolean = col("ICMS: Valor do Tributo : NF Item (Todos) SOMA").gt(lit(0));

    let situacao_05: Expr = operacoes_de_credito()?
        .and(valores_iguais_nota_prop)
        .and(boolean);

    println!("situacao_05: {situacao_05:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 05:"),
            lit(
                "Excluir valor do ICMS destacado em Nota Fiscal da Base de Cálculo das Contribuições.",
            ),
            lit("O valor da Base de Cálculo foi alterado de"),
            col(valor_bc).round_expr(2),
            lit("para"),
            delta.clone().round_expr(2),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_05, mensagem, delta)?;

    Ok(lf_result)
}

fn analisar_situacao06a(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let periodo_de_apuracao: &str = coluna(Left, "pa"); // "Período de Apuração",
    let chave_efd: &str = coluna(Left, "chave");
    let chave_nfe: &str = coluna(Right, "chave");
    let valor_item = coluna(Left, "valor_item");
    let soma_dos_itens: &str = "Soma dos Valores dos Itens";
    let valor_total: &str = coluna(Right, "valor_total"); // "Valor Total : NF (Todos) SOMA"
    let len_min = 10;

    // Define temporary column names
    let chaves_unificadas = "Chaves de Documentos Fiscais";
    let period_count = "Nº de Períodos";
    let periodos = "Períodos de Apuração";
    let periodo_valido = "Período Válido";
    let periodos_invalidos = "Períodos Inválidos";
    let periodos_formatados = "Períodos Formatados";

    // Collect all temporary column names for later removal
    let colunas_temporarias: Vec<&str> = vec![
        chaves_unificadas,
        period_count,
        soma_dos_itens,
        periodos,
        periodo_valido,
        periodos_invalidos,
        periodos_formatados,
    ];

    // --- Step 1: Unify EFD and NFe keys into a single temporary column ---
    // Unificar duas colunas em uma coluna
    let lazyframe = lazyframe.with_column(
        // Criar uma lista com os valores de ambas as chaves para cada linha
        concat_list([col(chave_efd), col(chave_nfe)])?
            .list()
            .drop_nulls() // Remove nulls da lista (se Strings podem ser nulas)
            .list()
            .unique() // Aplica a operação unique dentro de cada lista
            .explode()
            .alias(chaves_unificadas),
    );

    // Selecionar colunas nesta ordem
    let selected: [Expr; 3] = [
        col(periodo_de_apuracao),
        col(chaves_unificadas),
        col(valor_item),
    ];

    // --- Step 2: Group by unified keys to find keys used in multiple accounting periods ---
    let mut df_groupby_chaves = lazyframe
        .clone()
        .select(&selected)
        .filter(col(periodo_de_apuracao).is_not_null())
        .filter(col(chaves_unificadas).is_not_null())
        .filter(col(chaves_unificadas).str().len_bytes().gt(len_min))
        .group_by([col(chaves_unificadas)])
        .agg([
            // Collect all unique accounting periods for each key, sorted
            col(periodo_de_apuracao)
                .unique()
                .sort(SortOptions::default())
                //.dt()
                //.strftime("%d/%m/%Y")
                .alias(periodos),
            // Count how many unique accounting periods each key appears in
            col(periodo_de_apuracao)
                .unique()
                .count()
                .alias(period_count),
            col(valor_item).sum().alias(soma_dos_itens),
        ])
        .filter(col(period_count).gt(1)) // filtar chaves repetidas
        // Add a column for the first (smallest) accounting period for each key
        .with_column(
            col(periodos)
                .list()
                .first() // Get the first period (which is the smallest due to sorting)
                .alias(periodo_valido),
        )
        // Add a column for subsequent (invalid) accounting periods for each key
        .with_column(
            col(periodos)
                .list()
                //.shift(lit(-1))
                .slice(lit(1), col(period_count) - lit(1)) // Exclude the first period
                .alias(periodos_invalidos),
        )
        // Add a column with all unique periods formatted as a comma-separated string
        .with_column(format_list_dates(periodos).alias(periodos_formatados))
        .sort_by_exprs(
            vec![col(periodo_valido), col(chaves_unificadas)],
            // https://github.com/pola-rs/polars/pull/15590
            SortMultipleOptions::default()
                .with_maintain_order(true)
                .with_multithreaded(true)
                .with_order_descending(false)
                .with_nulls_last(false),
        )
        .collect()?;

    // Early exit if no duplicate keys across periods are found
    let number_of_rows = df_groupby_chaves.height();
    if number_of_rows == 0 {
        let lazyframe = lazyframe.drop_columns(&colunas_temporarias)?;
        return Ok(lazyframe);
    }

    // --- Step 3: Join the analysis results back to the original LazyFrame ---
    let lz_unificado: LazyFrame = lazyframe.clone().join(
        df_groupby_chaves.clone().lazy(),
        vec![col(chaves_unificadas)], // Left join key
        vec![col(chaves_unificadas)], // Right join key
        JoinType::Left.into(),
    );

    // --- Step 4: Define 'Situation 06' condition and generate 'glosa' message ---
    let situacao_06a: Expr = operacoes_de_credito()?
        .and(col(period_count).is_not_null())
        .and(col(periodo_de_apuracao).is_not_null())
        .and(col(period_count).gt(1)) // Multipla utilização de Docs Fiscais
        .and(col(periodo_de_apuracao).neq(col(periodo_valido)))
        .and(
            col(soma_dos_itens)
                .round_expr(2)
                .gt(col(valor_total).round_expr(2))
                .or(col(soma_dos_itens).is_null())
                .or(col(valor_total).is_null()),
        );

    println!("situacao_06a: {situacao_06a:?}\n");
    // std::process::exit(1);

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 06:"),
            lit("Documento Fiscal utilizado em Períodos de Apuração distintos."),
            lit("A chave"),
            col(chaves_unificadas),
            lit("pertence a"),
            col(period_count),
            lit("Períodos de Apuração distintos:"),
            col(periodos_formatados),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lz_unificado, situacao_06a, mensagem, lit(0))?;

    // Remove all temporary columns created during this analysis
    let lf_result = lf_result.drop_columns(&colunas_temporarias)?;

    // Remover a coluna periodos_formatados de DataFrame
    df_groupby_chaves.drop_in_place(periodos_formatados)?;

    // Imprimir todas as linhas do DataFrame
    unsafe {
        env::set_var("POLARS_FMT_MAX_ROWS", number_of_rows.to_string()); // maximum number of rows shown when formatting DataFrames.
    }
    println!(
        "Documentos Fiscais utilizados em Períodos de Apuração distintos: {df_groupby_chaves}\n"
    );

    // Criar e escrever DataFrame para um arquivo .txt
    let filename = "dataframe_situacao06a.txt";
    let mut file = File::create(filename)?;
    file.write_all(df_groupby_chaves.to_string().as_bytes())?; // Escreve os bytes da string no arquivo
    println!("DataFrame salvo em '{filename}'\n");

    configure_the_environment(); // Retornar à configuração padrão.    

    Ok(lf_result)
}

fn analisar_situacao06b(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let periodo_de_apuracao: &str = coluna(Left, "pa"); // "Período de Apuração",
    let registro: &str = coluna(Left, "registro");
    let cnpj_particip: &str = coluna(Left, "cnpj_particip");
    let num_doc: &str = coluna(Left, "num_doc");
    let valor_item = coluna(Left, "valor_item");
    let soma_dos_itens: &str = "Soma dos Valores dos Itens";
    let valor_total: &str = coluna(Right, "valor_total"); // "Valor Total : NF (Todos) SOMA"

    // Define temporary column names
    let period_count = "Nº de Períodos";
    let periodos = "Períodos de Apuração";
    let periodo_valido = "Período Válido";
    let periodos_invalidos = "Períodos Inválidos";
    let periodos_formatados = "Períodos Formatados";

    // Collect all temporary column names for later removal
    let colunas_temporarias: Vec<&str> = vec![
        soma_dos_itens,
        period_count,
        periodos,
        periodo_valido,
        periodos_invalidos,
        periodos_formatados,
    ];

    // Selecionar colunas nesta ordem
    let selected: [Expr; 5] = [
        col(periodo_de_apuracao),
        col(registro),
        col(cnpj_particip),
        col(num_doc),
        col(valor_item),
    ];

    // --- Step 1: Group by unified keys to find keys used in multiple accounting periods ---
    let mut df_groupby_cnpj = lazyframe
        .clone()
        .select(&selected)
        .filter(col(periodo_de_apuracao).is_not_null())
        .filter(col(registro).is_not_null())
        .filter(col(cnpj_particip).is_not_null())
        .filter(col(num_doc).is_not_null())
        .group_by([
            // col(registro),
            col(cnpj_particip),
            col(num_doc),
        ])
        .agg([
            // Collect all unique accounting periods for each key, sorted
            col(periodo_de_apuracao)
                .unique()
                .sort(SortOptions::default())
                //.dt()
                //.strftime("%d/%m/%Y")
                .alias(periodos),
            // Count how many unique accounting periods each key appears in
            col(periodo_de_apuracao)
                .unique()
                .count()
                .alias(period_count),
            col(valor_item).sum().alias(soma_dos_itens),
        ])
        .filter(col(period_count).gt(1)) // filtar chaves repetidas
        // Add a column for the first (smallest) accounting period for each key
        .with_column(
            col(periodos)
                .list()
                .first() // Get the first period (which is the smallest due to sorting)
                .alias(periodo_valido),
        )
        // Add a column for subsequent (invalid) accounting periods for each key
        .with_column(
            col(periodos)
                .list()
                //.shift(lit(-1))
                .slice(lit(1), col(period_count) - lit(1)) // Exclude the first period
                .alias(periodos_invalidos),
        )
        // Add a column with all unique periods formatted as a comma-separated string
        .with_column(format_list_dates(periodos).alias(periodos_formatados))
        .sort_by_exprs(
            vec![col(periodo_valido), col(cnpj_particip), col(num_doc)],
            // https://github.com/pola-rs/polars/pull/15590
            SortMultipleOptions::default()
                .with_maintain_order(true)
                .with_multithreaded(true)
                .with_order_descending(false)
                .with_nulls_last(false),
        )
        .collect()?;

    // Early exit if no duplicate keys across periods are found
    let number_of_rows = df_groupby_cnpj.height();
    if number_of_rows == 0 {
        let lazyframe = lazyframe.drop_columns(&colunas_temporarias)?;
        return Ok(lazyframe);
    }

    // --- Step 2: Join the analysis results back to the original LazyFrame ---
    let lz_unificado: LazyFrame = lazyframe.clone().join(
        df_groupby_cnpj.clone().lazy(),
        vec![
            //col(registro),
            col(cnpj_particip),
            col(num_doc),
        ], // Left join key
        vec![
            //col(registro),
            col(cnpj_particip),
            col(num_doc),
        ], // Right join key
        JoinType::Left.into(),
    );

    // --- Step 3: Define 'Situation 06' condition and generate 'glosa' message ---
    let situacao_06b: Expr = operacoes_de_credito()?
        .and(col(period_count).is_not_null())
        .and(col(periodo_de_apuracao).is_not_null())
        .and(col(period_count).gt(1)) // Multipla utilização de Docs Fiscais
        .and(col(periodo_de_apuracao).neq(col(periodo_valido)))
        .and(
            col(soma_dos_itens)
                .round_expr(2)
                .gt(col(valor_total).round_expr(2))
                .or(col(soma_dos_itens).is_null())
                .or(col(valor_total).is_null()),
        );

    println!("situacao_06b: {situacao_06b:?}\n");
    // std::process::exit(1);

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 06:"),
            lit("Documento Fiscal utilizado em Períodos de Apuração distintos."),
            lit("O Documento Fiscal de CNPJ"),
            col(cnpj_particip),
            lit("de número"),
            col(num_doc),
            lit("registrado em"),
            col(registro),
            lit("pertence a"),
            col(period_count),
            lit("Períodos de Apuração distintos:"),
            col(periodos_formatados),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lz_unificado, situacao_06b, mensagem, lit(0))?;

    // Remove all temporary columns created during this analysis
    let lf_result = lf_result.lazy().drop_columns(&colunas_temporarias)?;

    // Remover a coluna periodos_formatados de DataFrame
    df_groupby_cnpj.drop_in_place(periodos_formatados)?;

    // Imprimir todas as linhas do DataFrame
    unsafe {
        env::set_var("POLARS_FMT_MAX_ROWS", number_of_rows.to_string()); // maximum number of rows shown when formatting DataFrames.
    }
    println!(
        "Documentos Fiscais utilizados em Períodos de Apuração distintos: {df_groupby_cnpj}\n"
    );

    // Criar e escrever DataFrame para um arquivo .txt
    let filename = "dataframe_situacao06b.txt";
    let mut file = File::create(filename)?;
    file.write_all(df_groupby_cnpj.to_string().as_bytes())?; // Escreve os bytes da string no arquivo
    println!("DataFrame salvo em '{filename}'\n");

    configure_the_environment(); // Retornar à configuração padrão.    

    Ok(lf_result)
}

fn analisar_situacao07(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let origem_do_item: &str = coluna(Right, "origem"); // "Registro de Origem do Item : NF Item (Todos)"

    let columns: Vec<&str> = vec![
        "Código de Situação Tributária (CST)",
        "Código CFOP : NF Item (Todos)",
        "Alíquota Zero",
        "Alíquota Zero Temp", // coluna temporária
        "Incidência Monofásica",
        "Incidência Monofásica Temp", // coluna temporária
        "Crédito Presumido",          // Coluna auxiliar
        "CNPJ Base do Contribuinte",  // Coluna auxiliar
        "CNPJ Base do Remetente",     // Coluna auxiliar
        "CNPJ Base do Destinatário",  // Coluna auxiliar
    ];

    let pattern: Expr = lit(r"(?i)CTe"); // regex
    let cte: Expr = col(origem_do_item).str().contains(pattern, false);

    // CFOPs relacionados a Serviços de Armazanagem ou Industrialização por encomenda.
    let cfop_valido = [
        CFOP_DE_ARMAZENAGEM.as_slice(),
        CFOP_DE_INDUSTRIALIZACAO.as_slice(),
    ]
    .concat();
    let series: Series = cfop_valido.iter().collect();
    let literal_series: Expr = series.implode()?.into_series().lit();

    // Estes serviços são insumos com direito à crédito das Contribuições que devem ser excluídos das glosas.
    let cfop_de_insumos: Expr = col(columns[1])
        .is_not_null()
        .and(col(columns[1]).is_in(literal_series, true));

    let not_credito_presumido: Expr = col(columns[6]).is_null();

    // "CNPJ Base do Contribuinte" eq "CNPJ Base do Destinatário"
    // O Contribuinte é o Destinatário das operações.
    let destinatario_das_operacoes: Expr = equal(columns[7], columns[9]);

    // "CNPJ Base do Remetente"  eq "CNPJ Base do Destinatário": operação de transferência
    // "CNPJ Base do Remetente" neq "CNPJ Base do Destinatário": operação de compra
    let cnpjs_distintos: Expr = unequal(columns[8], columns[9]);

    let operacao_de_compra: Expr = destinatario_das_operacoes.and(cnpjs_distintos);

    let filter: Expr = operacoes_de_credito()?
        .and(cst_50_a_56()?)
        .and(cte)
        .and(cfop_de_insumos.not())
        .and(not_credito_presumido)
        .and(operacao_de_compra);

    // Adicionar coluna temporária
    let lazyframe: LazyFrame = lazyframe
        .with_column(
            when(filter.clone())
                .then(columns[2]) // keep original value: "Alíquota Zero"
                .otherwise(lit(NULL)) // replace by null
                .alias(columns[3]), // Coluna Temporária
        )
        .with_column(
            when(filter)
                .then(columns[4]) // keep original value: "Incidência Monofásica"
                .otherwise(lit(NULL)) // replace by null
                .alias(columns[5]), // Coluna Temporária
        );

    let situacao_07: Expr = col(columns[3])
        .is_not_null()
        .or(col(columns[5]).is_not_null());

    println!("situacao_07: {situacao_07:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 07:"),
            lit(
                "Fretes sobre Compras cujos insumos NÃO estão sujeitos ao pagamento das Contribuições de PIS/PASEP e COFINS.",
            ),
            col(columns[3]),
            col(columns[5]),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_07, mensagem, lit(0))?;

    // Remover coluna temporária
    let lf_result = lf_result.drop_columns(&[columns[3], columns[5]])?;

    Ok(lf_result)
}

fn analisar_situacao08(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let contabil: &str = coluna(Left, "contabil");

    let columns: Vec<&str> = vec![
        "CNPJ Base do Remetente", // Coluna auxiliar adicionada em fn adicionar_colunas_auxiliares()
        "CNPJ Base do Destinatário", // Coluna auxiliar adicionada em fn adicionar_colunas_auxiliares()
    ];

    let pattern: Expr = lit(r"(?i)Frete.*Venda"); // regex
    let frete_sobre_vendas: Expr = col(contabil).str().contains(pattern, false);

    // "CNPJ Base do Remetente" eq "CNPJ Base do Destinatário"
    let operacao_de_transferencia: Expr = equal(columns[0], columns[1]);

    let situacao_08: Expr = operacoes_de_credito()?
        .and(frete_sobre_vendas)
        .and(operacao_de_transferencia);

    println!("situacao_08: {situacao_08:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 08:"),
            lit("Frete sobre Vendas, operação de Transferência."),
            lit(
                "Conforme Parecer Normativo Cosit nº 5/2018 e § 2º do art. 176 da IN RFB nº 2121 de 2022:",
            ),
            lit("Não são considerados insumos os serviços de transporte de produtos"),
            lit("acabados realizados em ou entre estabelecimentos da pessoa jurídica."),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_08, mensagem, lit(0))?;

    Ok(lf_result)
}

fn analisar_situacao09(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let item_descricao: &str = coluna(Left, "item_desc");
    let escri_contabil: &str = coluna(Left, "contabil");

    let pattern1: Expr = lit(r"(?i)Marketing|MKT|Propaganda|Veiculacao");
    let pattern2: Expr = lit(r"(?i)Marketing|MKT|Propaganda|Veiculacao");

    let condicao1: Expr = col(item_descricao).str().contains(pattern1, false);
    let condicao2: Expr = col(escri_contabil).str().contains(pattern2, false);

    let situacao_09: Expr = operacoes_de_credito()?.and(condicao1.or(condicao2));

    println!("situacao_09: {situacao_09:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 09:"),
            lit("Serviço de Propaganda e Marketing."),
            lit(
                "Os gastos com Serviço de Propaganda e Marketing não são insumos geradores de crédito das Contribuições",
            ),
            lit(
                "segundo os critérios da Essencialidade ou da Relevância (Ver Parecer Normativo nº 5 de 2018).",
            ),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_09, mensagem, lit(0))?;

    Ok(lf_result)
}

fn analisar_situacao10(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let origem_do_item: &str = coluna(Right, "origem"); // "Registro de Origem do Item : NF Item (Todos)"
    let descricao_cfop: &str = coluna(Right, "descricao_cfop");

    let pattern1: Expr = lit(r"(?i)CTe");
    let pattern2: Expr = lit(r"(?i)Anula|Amostra|Brinde|Vasilhame");
    let pattern3: Expr = lit(r"(?i)Simb|Venda|Compra|Export");

    let condicao1: Expr = col(origem_do_item).str().contains(pattern1, false);
    let condicao2: Expr = col(descricao_cfop).str().contains(pattern2, false);
    let condicao3: Expr = col(descricao_cfop).str().contains(pattern3, false);

    let situacao_10: Expr = operacoes_de_credito()?.and(condicao1).and(condicao2).and(
        condicao3.not(), // crédito válido: venda com retorno simbólico de mercadoria do armazém para a empresa.
    );

    println!("situacao_10: {situacao_10:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 10:"),
            lit("Anulação ou Amostras e Brindes ou Retorno de Vasilhame."),
            lit("Ver coluna <Descrição CFOP : NF Item (Todos)>."),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_10, mensagem, lit(0))?;

    Ok(lf_result)
}

fn analisar_situacao11(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let item_descricao: &str = coluna(Left, "item_desc");
    let escri_contabil: &str = coluna(Left, "contabil");

    let pattern1: Expr = lit(r"(?i)Vale (Refeicao|Transporte)|Seguro");
    let pattern2: Expr = lit(r"(?i)Vale (Refeicao|Transporte)|Seguro");

    let condicao1: Expr = col(item_descricao).str().contains(pattern1, false);
    let condicao2: Expr = col(escri_contabil).str().contains(pattern2, false);

    let situacao_11: Expr = operacoes_de_credito()?.and(condicao1.or(condicao2));

    println!("situacao_11: {situacao_11:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 11:"),
            lit("Atividades da Mão de Obra."),
            lit(
                "Conforme Parecer Normativo SRFB n° 5 de 2018, linhas 55 e 168, não são considerados insumos os itens destinados",
            ),
            lit(
                "a viabilizar a atividade da mão de obra empregada pela pessoa jurídica em qualquer de suas áreas, inclusive em",
            ),
            lit(
                "seu processo de produção de bens ou de prestação de serviços, tais como alimentação, vestimenta e transporte.",
            ),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_11, mensagem, lit(0))?;

    Ok(lf_result)
}

fn analisar_situacao12(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let chave: &str = coluna(Left, "chave"); // "Chave do Documento"

    let series: Series = [
        // CTe:
        "12345678901234567890123456789012345678901234",
        "01234567890123456789012345678901234567890123",
        "90123456789012345678901234567890123456789012",
        "89012345678901234567890123456789012345678901",
    ]
    .iter()
    .map(|doc| doc.to_string())
    .collect();

    let literal_series: Expr = series.implode()?.into_series().lit();

    let chave_inexistente: Expr = col(chave).is_in(literal_series, true);

    let situacao_12: Expr = operacoes_de_credito()?.and(chave_inexistente);

    println!("situacao_12: {situacao_12:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 12:"),
            lit(
                "Documento Fiscal inexistente conforme www.nfe.fazenda.gov.br ou www.cte.fazenda.gov.br",
            ),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_12, mensagem, lit(0))?;

    Ok(lf_result)
}

#[allow(dead_code)]
fn analisar_situacao13(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let num_linha: &str = coluna(Left, "num_linha"); // "Linhas"

    let series: Series = [
        95217, 95222, 95223, 95225, 105758, 116292, 116294, 116575, 127290, 127291, 127292, 137505,
        147797, 157909, 157914, 168540, 178655, 188135, 188136, 188140, 188141, 196421, 197989,
        199430, 199913, 201399, 202588, 203362, 203832, 203833, 203836, 204524, 204570, 205051,
        206238, 206249, 206253, 207449, 208792, 210039, 211260, 212538, 213786, 213787, 213788,
        215102,
    ]
    .iter()
    .collect();

    let literal_series: Expr = series.implode()?.into_series().lit();

    let linhas: Expr = col(num_linha).is_in(literal_series, true);

    let situacao_13: Expr = operacoes_de_credito()?.and(linhas);

    println!("situacao_13: {situacao_13:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 13:"),
            lit("Créditos Estornados, conforme respostas do Contribuinte às Intimações Fiscais."),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_13, mensagem, lit(0))?;

    Ok(lf_result)
}

#[allow(dead_code)]
fn analisar_situacao14(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let item_tipo: &str = coluna(Left, "item_tipo");
    let escri_contabil: &str = coluna(Left, "contabil");

    let pattern1: Expr = lit(r"(?i)Embalage(m|ns)");
    let pattern2: Expr = lit(r"(?i)Embalage(m|ns)");

    let condicao1: Expr = col(item_tipo).str().contains(pattern1, false);
    let condicao2: Expr = col(escri_contabil).str().contains(pattern2, false);

    let situacao_14: Expr = operacoes_de_credito()?.and(condicao1.and(condicao2));

    println!("situacao_14: {situacao_14:?}\n");

    /*
    PARECER NORMATIVO Nº 5, DE 17 DE DEZEMBRO DE 2018

    2. INEXISTÊNCIA DE INSUMOS NA ATIVIDADE COMERCIAL

    42.Em razão disso, exemplificativamente, não constituem insumos geradores de créditos para pessoas jurídicas
    dedicadas à atividade de revenda de bens: a) combustíveis e lubrificantes utilizados em veículos próprios de entrega de
    mercadorias2; b) transporte de mercadorias entre centros de distribuição próprios; c) embalagens para transporte das mercadorias;
    etc.

    5. GASTOS POSTERIORES À FINALIZAÇÃO DO PROCESSO DE PRODUÇÃO OU DE PRESTAÇÃO

    56.Destarte, exemplificativamente não podem ser considerados insumos gastos com transporte (frete) de produtos
    acabados (mercadorias) de produção própria entre estabelecimentos da pessoa jurídica, para centros de distribuição ou para entrega
    direta ao adquirente6, como: a) combustíveis utilizados em frota própria de veículos; b) embalagens para transporte de mercadorias
    acabadas; c) contratação de transportadoras.
    */

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 14:"),
            lit("Embalagens."),
            lit("Conforme Parecer Normativo SRFB n° 5 de 2018, linha 42,"),
            lit("não constituem insumos geradores de créditos para pessoas jurídicas"),
            lit("dedicadas à atividade de revenda de bens:"),
            lit("c) embalagens para transporte das mercadorias;"),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_14, mensagem, lit(0))?;

    Ok(lf_result)
}

fn aplicar_situacao(
    lazyframe: LazyFrame,
    situacao: Expr,
    mensagem: Expr,
    new_value: Expr,
) -> JoinResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let valor_bc: &str = coluna(Left, "valor_bc");

    let lf_result: LazyFrame = lazyframe
        .with_column(
            when(situacao.clone())
                .then(mensagem)
                .otherwise(col(glosar))
                .alias(glosar),
        )
        .with_column(
            when(situacao)
                .then(new_value)
                .otherwise(col(valor_bc))
                .alias(valor_bc),
        );

    Ok(lf_result)
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
/// cargo test -- --show-output tests_glosar_base_de_calculo
#[cfg(test)]
mod tests_glosar_base_de_calculo {
    use super::*;
    use crate::configure_the_environment;

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// `cargo test -- --show-output test_analisar_situacao02`
    fn test_analisar_situacao02() -> JoinResult<()> {
        configure_the_environment();

        let glosar: &str = coluna(Middle, "glosar");

        let dataframe: DataFrame = df! [
            "Linhas" => [1, 2, 3, 4, 5, 6],
            "Tipo de Operação" => [1, 2, 4, 2, 1, 1],
            "Período de Apuração" => [
                "2022-01-01", "2022-02-01", "2022-03-01",
                "2022-04-01", "2022-01-01", "2022-02-01"
            ],
            "Código de Situação Tributária (CST)" => [56, 56, 56, 56, 56, 56],
            "Natureza da Base de Cálculo dos Créditos" => [2, 2, 2, 2, 2, 2],
            "Valor da Base de Cálculo das Contribuições" => [23.6, 20.3, 140.0, 859.01, -238.41, 9852.07],
            "Glosar Base de Cálculo de PIS/PASEP e COFINS" => [
                "Situação 01 &", "", "",
                "Situação 01 &", "", ""
            ],
            "Dia da Emissão : NF Item (Todos)" => [
                "2022-01-01", "2022-02-01", "2022-03-01",
                "2011-04-01", "2022-01-01", "2022-05-02"
            ],
            "optional" => [Some(28), Some(300), None, Some(2), Some(-30), None],
        ]?;

        println!("dataframe original:\n{dataframe}\n");

        let options = StrptimeOptions {
            format: Some("%Y-%-m-%-d".into()),
            strict: false, // If set then polars will return an error if any date parsing fails
            exact: true, // If polars may parse matches that not contain the whole string e.g. “foo-2021-01-01-bar” could match “2021-01-01”
            cache: true, // use a cache of unique, converted dates to apply the datetime conversion.
        };

        let lazyframe: LazyFrame = dataframe
            .lazy()
            .with_column(col("^(Período|Data|Dia).*$").str().to_date(options));

        println!(
            "dataframe após formatar 'Dia da Emissão':\n{}\n",
            lazyframe.clone().collect()?
        );

        let args: Arguments = Arguments::default();
        println!("args: {args:#?}\n");

        let lz: LazyFrame = analisar_situacao02(lazyframe, &args)?
            .with_column(col(glosar))
            .format_values();

        let result = lz.collect()?;

        println!("result:\n{result:?}\n");

        let col: &Column = result.column(glosar)?;
        let vec_option: Vec<Option<&str>> = col.str()?.into_iter().collect();

        assert_eq!(
            vec_option,
            vec![
                Some("Situação 01"),
                Some(""),
                Some(""),
                Some("Situação 01 & Situação 02: Crédito extemporâneo."),
                Some(""),
                Some("Situação 02: Crédito extemporâneo.")
            ]
        );

        Ok(())
    }

    #[test]
    /// test columns with nulls
    ///
    /// `cargo test -- --show-output test_analisar_situacao03`
    fn test_analisar_situacao03() -> JoinResult<()> {
        configure_the_environment();

        let cfop: &str = coluna(Right, "cfop");
        let origem_do_item: &str = coluna(Right, "origem"); // "Registro de Origem do Item : NF Item (Todos)"
        let valor_bc: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições";
        let glosar: &str = coluna(Middle, "glosar");

        let columns: Vec<&str> = vec![
            "Alíquota Zero",
            "Alíquota Zero Temp", // coluna temporária
            "Incidência Monofásica",
            "Incidência Monofásica Temp", // coluna temporária
        ];

        let dataframe01: DataFrame = df! [
            cfop           => [Some(1126), Some(6666), None, Some(1125), None, Some(1234)],
            origem_do_item => [Some("nfe"), Some("cte"), None, Some("Nfe"), None, Some("NFE")],
            valor_bc       => [23.6, 0.3, 10.0, 89.01, -3.41, 52.07],
            glosar         => [None, None, Some("Situação 05 &"), None, None, Some("Situação 02 &")],
            "Alíquota Zero"         => [
                Some("NCM 1234 : Alíquota Zero - Lei 4321."),
                Some("NCM 1234 : Alíquota Zero - Lei 7777."),
                Some("NCM 2222 : Alíquota Zero - Lei 8888."),
                None,
                None,
                Some("NCM 3333 : Alíquota Zero - Lei 1234.")
            ],
            "Incidência Monofásica" => [
                None,
                Some("NCM 4567 : Incidência Monofásica - Lei 5555."),
                None,
                Some("NCM 3388 : Incidência Monofásica - Lei 0011."),
                None,
                Some("NCM 8899 : Incidência Monofásica - Lei 3344.")
            ],
        ]?;

        println!("dataframe01: {dataframe01}\n");

        let pattern01: Expr = lit(r"(?i)NFe"); // regex
        let nfe: Expr = col(origem_do_item).str().contains(pattern01, false);

        // CFOPs relacionados a Serviços de Armazanagem ou Industrialização por encomenda.
        let cfop_valido = [
            CFOP_DE_ARMAZENAGEM.as_slice(),
            CFOP_DE_INDUSTRIALIZACAO.as_slice(),
        ]
        .concat();
        let series: Series = cfop_valido.iter().collect();
        let literal_series: Expr = series.implode()?.into_series().lit();

        // Estes serviços são insumos com direito à crédito das Contribuições que devem ser excluídos das glosas.
        let cfop_de_insumos: Expr = col(cfop).is_in(literal_series, true);

        let filter: Expr = nfe.and(cfop_de_insumos.not());

        // Adicionar coluna temporária
        let lazyframe: LazyFrame = dataframe01
            .lazy()
            .with_column(
                when(filter.clone())
                    .then(columns[0]) // keep original value: "Alíquota Zero"
                    .otherwise(lit(NULL)) // replace by null
                    .alias(columns[1]), // Coluna Temporária
            )
            .with_column(
                when(filter)
                    .then(columns[2]) // keep original value: "Incidência Monofásica"
                    .otherwise(lit(NULL)) // replace by null
                    .alias(columns[3]), // Coluna Temporária
            );

        let situacao_03: Expr = col(columns[1])
            .is_not_null()
            .or(col(columns[3]).is_not_null());

        println!("situacao_03: {situacao_03:?}");

        let mensagem: Expr = concat_str(
            [
                col(glosar),
                lit("Situação 03:"),
                lit("Aquisição de bens ou serviços não sujeitos ao pagamento da contribuição."),
                lit(
                    "De acordo com o inciso II do § 2º do art. 3º das Leis 10.637/2002 e 10.833/2003, não dará direito",
                ),
                lit(
                    "a crédito o valor da aquisição de bens ou serviços não sujeitos ao pagamento da contribuição.",
                ),
                col(columns[1]),
                col(columns[3]),
                lit("&"),
            ],
            " ",
            true,
        );

        let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_03, mensagem, lit(0))?;

        let dataframe02: DataFrame = lf_result.collect()?;

        println!("dataframe02: {dataframe02}\n");

        // Get columns from dataframe
        let bcal_values: &Column = dataframe02.column(valor_bc)?;

        // Get columns with into_iter()
        let vec_bcal_values: Vec<f64> = bcal_values.f64()?.into_iter().flatten().collect();

        // Get columns from dataframe
        let glosar: &Column = dataframe02.column(glosar)?;
        let glosar_col: Vec<&str> = glosar.str()?.into_iter().flatten().collect();

        println!("glosar_col: {glosar_col:#?}\n");

        assert_eq!(vec_bcal_values, vec![0.0, 0.3, 10.0, 89.01, -3.41, 0.0]);

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output test_analisar_situacao10`
    fn test_analisar_situacao10() -> JoinResult<()> {
        configure_the_environment();

        let top: &str = coluna(Left, "tipo_operacao");
        let natureza: &str = coluna(Left, "natureza");
        let cst: &str = coluna(Left, "cst");
        let origem_do_item: &str = coluna(Right, "origem"); // "Registro de Origem do Item : NF Item (Todos)"
        let descricao_cfop: &str = coluna(Right, "descricao_cfop"); // "Descrição CFOP : NF Item (Todos)"
        let valor_bc: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições";
        let glosar: &str = coluna(Middle, "glosar");

        let dataframe: DataFrame = df! [
            top            => [1, 1, 1, 1, 1, 1],
            natureza       => [1, 2, 3, 4, 5, 6],
            cst            => [50, 51, 52, 53, 60, 56],
            origem_do_item => ["CTe", "cte", "CTE", "CTe", "CTE", "CTe"],
            descricao_cfop => ["Anula", "Amostra", "Venda sem brinde", "Anula", "anulação", "Anula da Compra"],
            "optional"     => [Some(28), Some(300), None, Some(2), Some(-30), None],
            valor_bc       => [23.6, 0.3, 10.0, 89.01, -3.41, 52.07],
            glosar         => ["Situação 01 &", "", "Situação 03 &", "", "", "Situação 02 &"],
        ]?;

        println!("dataframe: {dataframe}\n");

        let lazyframe: LazyFrame = dataframe.lazy();

        let lf_itens_de_docs_fiscais_result: LazyFrame = analisar_situacao10(lazyframe)?;

        let df_itens_de_docs_fiscais_result: DataFrame =
            lf_itens_de_docs_fiscais_result.collect()?;

        println!("df_itens_de_docs_fiscais_result: {df_itens_de_docs_fiscais_result}\n");

        // Get columns from dataframe
        let bcal_values: &Column = df_itens_de_docs_fiscais_result.column(valor_bc)?;

        // Get columns with into_iter()
        let vec_opt_bcal_values: Vec<Option<f64>> = bcal_values.f64()?.into_iter().collect();

        println!("vec_opt_bcal_values: {vec_opt_bcal_values:?}\n");

        // Get columns from dataframe
        let glosar: &Column = df_itens_de_docs_fiscais_result.column(glosar)?;
        let glosar_values: Vec<Option<&str>> = glosar.str()?.into_iter().collect();
        println!("glosar_values: {glosar_values:#?}\n");

        assert_eq!(
            vec_opt_bcal_values,
            vec![
                Some(0.0),
                Some(0.0),
                Some(10.0),
                Some(0.0),
                Some(0.0),
                Some(52.07)
            ]
        );

        Ok(())
    }
}
