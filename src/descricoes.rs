use polars::{datatypes::DataType, prelude::*};
use std::{collections::HashMap, sync::LazyLock as Lazy};

/// Example:
///
/// <https://docs.rs/once_cell/latest/once_cell/sync/struct.Lazy.html>
static DESCRICAO_DO_INDICADOR_DE_ORIGEM: Lazy<HashMap<i64, &'static str>> = Lazy::new(|| {
    HashMap::from([
        (0, "Operação no Mercado Interno"),
        (1, "Operação de Importação"),
    ])
});

static DESCRICAO_DO_TIPO_DE_OPERACAO: Lazy<HashMap<i64, &'static str>> = Lazy::new(|| {
    HashMap::from([
        (1, "Entrada"),
        (2, "Saída"),
        (3, "Ajuste"),   // "Ajuste de Acréscimo"
        (4, "Ajuste"),   // "Ajuste de Redução"
        (5, "Desconto"), // "Desconto da Contribuição Apurada no Próprio Período"
        (6, "Desconto"), // "Desconto Efetuado em Período Posterior"
        (7, "Detalhamento"),
    ])
});

static DESCRICAO_DO_TIPO_DE_CREDITO: Lazy<HashMap<i64, &'static str>> = Lazy::new(|| {
    HashMap::from([
        (1, "Alíquota Básica"),
        (2, "Alíquotas Diferenciadas"),
        (3, "Alíquota por Unidade de Produto"),
        (4, "Estoque de Abertura"),
        (5, "Aquisição Embalagens para Revenda"),
        (6, "Presumido da Agroindústria"),
        (7, "Outros Créditos Presumidos"),
        (8, "Importação"),
        (9, "Atividade Imobiliária"),
        (99, "Outros"),
        (100, ""),
    ])
});

static DESCRICAO_DO_MES: Lazy<HashMap<i64, &'static str>> = Lazy::new(|| {
    HashMap::from([
        (1, "janeiro"),
        (2, "fevereiro"),
        (3, "março"),
        (4, "abril"),
        (5, "maio"),
        (6, "junho"),
        (7, "julho"),
        (8, "agosto"),
        (9, "setembro"),
        (10, "outubro"),
        (11, "novembro"),
        (12, "dezembro"),
        (13, ""), // utilizado para acumulação de valores trimestrais
    ])
});

/// 4.3.7 - Tabela Base de Cálculo do Crédito (versão 1.0.1)
static NATUREZA_DA_BASE_DE_CALCULO_DOS_CREDITOS: Lazy<HashMap<i64, &'static str>> =
    Lazy::new(|| {
        HashMap::from([
            (1, "Aquisição de Bens para Revenda"),
            (2, "Aquisição de Bens Utilizados como Insumo"),
            (3, "Aquisição de Serviços Utilizados como Insumo"),
            (
                4,
                "Energia Elétrica e Térmica, Inclusive sob a Forma de Vapor",
            ),
            (5, "Aluguéis de Prédios"),
            (6, "Aluguéis de Máquinas e Equipamentos"),
            (7, "Armazenagem de Mercadoria e Frete na Operação de Venda"),
            (8, "Contraprestações de Arrendamento Mercantil"),
            (
                9,
                "Máquinas, Equipamentos ... (Crédito sobre Encargos de Depreciação)",
            ),
            (
                10,
                "Máquinas, Equipamentos ... (Crédito com Base no Valor de Aquisição)",
            ),
            (
                11,
                "Amortizacao e Depreciação de Edificações e Benfeitorias em Imóveis",
            ),
            (
                12,
                "Devolução de Vendas Sujeitas à Incidência Não-Cumulativa",
            ),
            (13, "Outras Operações com Direito a Crédito"),
            (14, "Atividade de Transporte de Cargas - Subcontratação"),
            (
                15,
                "Atividade Imobiliária - Custo Incorrido de Unidade Imobiliária",
            ),
            (
                16,
                "Atividade Imobiliária - Custo Orçado de Unidade não Concluída",
            ),
            (
                17,
                "Atividade de Prestação de Serviços de Limpeza, Conservação e Manutenção",
            ),
            (18, "Estoque de Abertura de Bens"),
            // Ajustes
            (31, "Ajuste de Acréscimo (PIS/PASEP)"),
            (35, "Ajuste de Acréscimo (COFINS)"),
            (41, "Ajuste de Redução (PIS/PASEP)"),
            (45, "Ajuste de Redução (COFINS)"),
            // Descontos
            (
                51,
                "Desconto da Contribuição Apurada no Próprio Período (PIS/PASEP)",
            ),
            (
                55,
                "Desconto da Contribuição Apurada no Próprio Período (COFINS)",
            ),
            (61, "Desconto Efetuado em Período Posterior (PIS/PASEP)"),
            (65, "Desconto Efetuado em Período Posterior (COFINS)"),
            // Percentuais do Rateio
            (80, "Receita Bruta (valores)"),
            (81, "Receita Bruta (percentuais)"),
            (90, "Base de Cálculo de Débitos Omitidos"),
            (
                91,
                "Débitos: Revenda de Mercadorias de NCM 2309.90 (PIS/PASEP)",
            ),
            (
                95,
                "Débitos: Revenda de Mercadorias de NCM 2309.90 (COFINS)",
            ),
            // Base de Cálculo dos Créditos
            (101, "Base de Cálculo dos Créditos - Alíquota Básica (Soma)"),
            (
                102,
                "Base de Cálculo dos Créditos - Alíquotas Diferenciadas (Soma)",
            ),
            (
                103,
                "Base de Cálculo dos Créditos - Alíquota por Unidade de Produto (Soma)",
            ),
            (
                104,
                "Base de Cálculo dos Créditos - Estoque de Abertura (Soma)",
            ),
            (
                105,
                "Base de Cálculo dos Créditos - Aquisição Embalagens para Revenda (Soma)",
            ),
            (
                106,
                "Base de Cálculo dos Créditos - Presumido da Agroindústria (Soma)",
            ),
            (
                107,
                "Base de Cálculo dos Créditos - Outros Créditos Presumidos (Soma)",
            ),
            (108, "Base de Cálculo dos Créditos - Importação (Soma)"),
            (
                109,
                "Base de Cálculo dos Créditos - Atividade Imobiliária (Soma)",
            ),
            (199, "Base de Cálculo dos Créditos - Outros (Soma)"),
            // Valor Total do Crédito Apurado no Período
            (201, "Crédito Apurado no Período (PIS/PASEP)"),
            (205, "Crédito Apurado no Período (COFINS)"),
            // Crédito Disponível após Ajustes
            (211, "Crédito Disponível após Ajustes (PIS/PASEP)"),
            (215, "Crédito Disponível após Ajustes (COFINS)"),
            // Crédito Disponível após Descontos
            (221, "Crédito Disponível após Descontos (PIS/PASEP)"),
            (225, "Crédito Disponível após Descontos (COFINS)"),
            // Saldo de Crédito Passível de Desconto ou Ressarcimento
            (300, "Base de Cálculo dos Créditos - Valor Total (Soma)"),
            (
                301,
                "Saldo de Crédito Passível de Desconto ou Ressarcimento (PIS/PASEP)",
            ),
            (
                305,
                "Saldo de Crédito Passível de Desconto ou Ressarcimento (COFINS)",
            ),
        ])
    });

pub fn descricao_da_origem(col: Column) -> Result<Option<Column>, PolarsError> {
    let result_option_series = match col.dtype() {
        DataType::Int64 => indicador_da_origem(col),
        _ => {
            eprintln!("fn descricao_da_origem()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!(
                    "Not supported for Series with DataType {:?}",
                    col.dtype()
                )
                .into(),
            ))
        }
    };

    result_option_series
}

fn indicador_da_origem(col: Column) -> Result<Option<Column>, PolarsError> {
    let new_col: Column = col
        .i64()?
        .into_iter()
        .map(|option_i64: Option<i64>| {
            option_i64.map(|number| {
                let opt_descricao = DESCRICAO_DO_INDICADOR_DE_ORIGEM.get(&number);
                match opt_descricao {
                    Some(&descricao) => descricao.to_string(),
                    None => format!("{number}: Sem descrição"),
                }
            })
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn descricao_do_tipo_de_operacao(col: Column) -> Result<Option<Column>, PolarsError> {
    let result_option_cols = match col.dtype() {
        DataType::Int64 => tipo_de_operacao(col),
        _ => {
            eprintln!("fn descricao_do_tipo_de_operacao()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!(
                    "Not supported for Series with DataType {:?}",
                    col.dtype()
                )
                .into(),
            ))
        }
    };

    result_option_cols
}

fn tipo_de_operacao(col: Column) -> Result<Option<Column>, PolarsError> {
    let new_col: Column = col
        .i64()?
        .into_iter()
        .map(|option_i64: Option<i64>| {
            option_i64.map(|number| {
                let opt_descricao = DESCRICAO_DO_TIPO_DE_OPERACAO.get(&number);
                match opt_descricao {
                    Some(&descricao) => descricao.to_string(),
                    None => format!("{number}: Sem descrição"),
                }
            })
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn descricao_do_tipo_de_credito(col: Column) -> Result<Option<Column>, PolarsError> {
    let result_option_series = match col.dtype() {
        DataType::Int64 => tipo_descricao(col),
        _ => {
            eprintln!("fn descricao_do_tipo_de_credito()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!(
                    "Not supported for Series with DataType {:?}",
                    col.dtype()
                )
                .into(),
            ))
        }
    };

    result_option_series
}

fn tipo_descricao(col: Column) -> Result<Option<Column>, PolarsError> {
    let new_col: Column = col
        .i64()?
        .into_iter()
        .map(|option_i64: Option<i64>| {
            option_i64.map(|number| match DESCRICAO_DO_TIPO_DE_CREDITO.get(&number) {
                Some(&descricao) => descricao.to_string(),
                None => format!("{number}: Sem descrição"),
            })
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn descricao_do_mes(col: Column) -> Result<Option<Column>, PolarsError> {
    let result_option_cols = match col.dtype() {
        DataType::Int64 => tipo_mes(col),
        _ => {
            eprintln!("fn descricao_do_mes()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!(
                    "Not supported for Series with DataType {:?}",
                    col.dtype()
                )
                .into(),
            ))
        }
    };

    result_option_cols
}

fn tipo_mes(col: Column) -> Result<Option<Column>, PolarsError> {
    let new_col: Column = col
        .i64()?
        .into_iter()
        .map(|option_i64: Option<i64>| {
            option_i64.map(|number| match DESCRICAO_DO_MES.get(&number) {
                Some(&descricao) => descricao.to_string(),
                None => format!("{number}: Sem descrição"),
            })
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}

pub fn descricao_da_natureza_da_bc_dos_creditos(
    col: Column
) -> Result<Option<Column>, PolarsError> {
    let result_option_series = match col.dtype() {
        DataType::Int64 => natureza_da_bc_dos_creditos(col),
        _ => {
            eprintln!("fn descricao_da_natureza_da_bc_dos_creditos()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!(
                    "Not supported for Series with DataType {:?}",
                    col.dtype()
                )
                .into(),
            ))
        }
    };

    result_option_series
}

fn natureza_da_bc_dos_creditos(col: Column) -> Result<Option<Column>, PolarsError> {
    let new_col: Column = col
        .i64()?
        .into_iter()
        .map(|opt_int64: Option<i64>| {
            opt_int64.map(|integer: i64| {
                match NATUREZA_DA_BASE_DE_CALCULO_DOS_CREDITOS.get(&integer) {
                    Some(&descricao) => {
                        if integer <= 18 {
                            format!("{integer:02} - {descricao}")
                        } else {
                            //format!("{integer:03} - {descricao}")
                            descricao.to_string()
                        }
                    }
                    None => format!("{integer}: Sem descrição"),
                }
            })
        })
        .collect::<StringChunked>()
        .into_column();

    Ok(Some(new_col))
}
