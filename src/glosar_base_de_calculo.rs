use crate::{
    Arguments, DataFrameExtension, MyResult,
    Side::{Left, Middle, Right},
    adicionar_coluna_de_aliquota_zero, adicionar_coluna_de_credito_presumido,
    adicionar_coluna_de_incidencia_monofasica,
    adicionar_coluna_periodo_de_apuracao_inicial_e_final, coluna, cst_50_a_56, equal,
    get_cnpj_base, operacoes_de_credito, round_column, unequal,
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

/// Trait extension to LazyFrame
pub trait LazyFrameExtension {
    /// Format LazyFrame values
    ///
    /// Substituir multiple_whitespaces " " por apenas um " "
    ///
    /// Remover multiple_whitespaces " " e/or "&" das extremidades da linha
    fn format_values(self) -> Self;

    /// Adicionar colunas auxiliares das situações de glosa.
    ///
    /// Adicionar 3 colunas contendo CNPJ Base
    fn adicionar_colunas_auxiliares(self) -> Self;

    /// Remover colunas auxiliares das situações de glosa.
    #[allow(dead_code)]
    fn remover_colunas_auxiliares(self) -> Self;
}

impl LazyFrameExtension for LazyFrame {
    fn format_values(self) -> Self {
        // Column names:
        let glosar: &str = coluna(Middle, "glosar");
        let valor_bc: &str = coluna(Left, "valor_bc");

        self.with_columns([
            col(valor_bc).apply(
                |series| round_column(series, 2),
                GetOutput::from_type(DataType::Float64),
            ),
            col(glosar)
                // Substituir multiple_whitespaces " " por apenas um " "
                .str()
                .replace_all(lit(r"\s{2,}"), lit(" "), false)
                // Remover multiple_whitespaces " " e/or "&" das extremidades da linha
                .str()
                .replace_all(lit(r"^[\s&]+|[\s&]+$"), lit(""), false),
        ])
    }

    fn adicionar_colunas_auxiliares(self) -> Self {
        let columns: Vec<&str> = vec![
            coluna(Left, "contribuinte_cnpj"), // "CNPJ dos Estabelecimentos do Contribuinte"
            "CNPJ Base do Contribuinte",       // Coluna auxiliar
            coluna(Right, "remetente_cnpj2"), // "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe"
            "CNPJ Base do Remetente",         // Coluna auxiliar
            coluna(Right, "destinatario_cnpj"), // "CTe - Informações do Destinatário do CT-e: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes"
            "CNPJ Base do Destinatário",        // Coluna auxiliar
            coluna(Right, "chave_de_acesso"), // "Inf. NFe - Chave de acesso da NF-e : ConhecimentoInformacaoNFe"
            "Valor Total de Documentos Vinculados", // Coluna auxiliar
        ];

        // CTe: 123, 2 NFes: [123, 123] de valor total = 345.85
        // NFe: 123, 2 CTes: [123, 123] de valor total = 217.01
        // https://docs.pola.rs/user-guide/expressions/strings/#extract-a-pattern
        let pattern: Expr = lit(r"(?i)valor total = (.*)"); // regex

        self.with_columns([
            // Adicionar 3 colunas contendo CNPJ Base
            col(columns[0])
                .apply(get_cnpj_base, GetOutput::from_type(DataType::String))
                .alias(columns[1]), // Coluna auxiliar
            col(columns[2])
                .apply(get_cnpj_base, GetOutput::from_type(DataType::String))
                .alias(columns[3]), // Coluna auxiliar
            col(columns[4])
                .apply(get_cnpj_base, GetOutput::from_type(DataType::String))
                .alias(columns[5]), // Coluna auxiliar
            col(columns[6])
                .str()
                .extract(pattern, 1)
                .cast(DataType::Float64)
                .alias(columns[7]), // Coluna auxiliar
        ])
    }

    fn remover_colunas_auxiliares(self) -> Self {
        let columns: Vec<&str> = vec![
            "Alíquota Zero",
            "Crédito Presumido",
            "Incidência Monofásica",
            "CNPJ Base do Contribuinte",
            "CNPJ Base do Remetente",
            "CNPJ Base do Destinatário",
            "Valor Total de Documentos Vinculados",
        ];

        // Remover coluna temporária
        self.drop(by_name(columns, true))
    }
}

pub fn glosar_bc(dataframe: &DataFrame, args: &Arguments) -> MyResult<DataFrame> {
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
    let lazyframe: LazyFrame = analisar_situacao06(lazyframe)?;
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

fn analisar_situacao01(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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

fn analisar_situacao02(lazyframe: LazyFrame, args: &Arguments) -> MyResult<LazyFrame> {
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
    let lf_result: LazyFrame = lf_result.drop(by_name([pa_ini, pa_fim], true));

    Ok(lf_result)
}

fn analisar_situacao03(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
    let lf_result: LazyFrame = lf_result.drop(by_name([columns[1], columns[3]], true));

    Ok(lf_result)
}

fn analisar_situacao04(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
            col(valor_bc).apply(
                |col| round_column(col, 2),
                GetOutput::from_type(DataType::Float64),
            ),
            lit("-"),
            col(valor_cte_vinculado).apply(
                |col| round_column(col, 2),
                GetOutput::from_type(DataType::Float64),
            ),
            lit("="),
            valor_justo.clone().apply(
                |col| round_column(col, 2),
                GetOutput::from_type(DataType::Float64),
            ),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_04, mensagem, valor_justo)?;

    Ok(lf_result)
}

#[allow(dead_code)]
fn analisar_situacao05(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
            col(valor_bc).apply(
                |col| round_column(col, 2),
                GetOutput::from_type(DataType::Float64),
            ),
            lit("para"),
            delta.clone().apply(
                |col| round_column(col, 2),
                GetOutput::from_type(DataType::Float64),
            ),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_05, mensagem, delta)?;

    Ok(lf_result)
}

fn analisar_situacao06(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
    let glosar: &str = coluna(Middle, "glosar");
    let cnpj_particip: &str = coluna(Left, "cnpj_particip"); // "CNPJ do Participante",
    let num_doc: &str = coluna(Left, "num_doc"); // "Nº do Documento Fiscal",

    let series: Series = Series::new("c".into(), ["12.345.678/0009-01"]);
    let literal_series: Expr = series.implode()?.into_series().lit();
    let cnpj: Expr = col(cnpj_particip).is_in(literal_series, true);

    let series: Series = Series::new("n".into(), [654321]);
    let literal_series: Expr = series.implode()?.into_series().lit();
    let num_doc: Expr = col(num_doc).is_in(literal_series, true);

    let situacao_06: Expr = operacoes_de_credito()?.and(cnpj).and(num_doc);

    println!("situacao_06: {situacao_06:?}\n");

    let mensagem: Expr = concat_str(
        [
            col(glosar),
            lit("Situação 06:"),
            lit("Item de Documento Fiscal usado em duplicidade."),
            lit("&"),
        ],
        " ",
        true,
    );

    let lf_result: LazyFrame = aplicar_situacao(lazyframe, situacao_06, mensagem, lit(0))?;

    Ok(lf_result)
}

fn analisar_situacao07(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
    let lf_result: LazyFrame = lf_result.drop(by_name([columns[3], columns[5]], true));

    Ok(lf_result)
}

fn analisar_situacao08(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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

fn analisar_situacao09(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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

fn analisar_situacao10(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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

fn analisar_situacao11(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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

fn analisar_situacao12(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
fn analisar_situacao13(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
fn analisar_situacao14(lazyframe: LazyFrame) -> MyResult<LazyFrame> {
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
) -> MyResult<LazyFrame> {
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
    fn test_analisar_situacao02() -> MyResult<()> {
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
    fn test_analisar_situacao03() -> MyResult<()> {
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
    fn test_analisar_situacao10() -> MyResult<()> {
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
