use polars::prelude::*;

use chrono::{
    Datelike, // let year = naive_date.year(); let month = naive_date.month();
    Months,
    NaiveDate,
};

use crate::{
    Arguments, JoinResult, Side::Left, coluna, get_output_as_date, operacoes_de_entrada_ou_saida,
};

/// Colunas temporárias: `Período de Apuração Inicial` e `Período de Apuração Final`.
///
/// Adicionar estas 2 colunas temporárias para analisar_situacao02.
pub fn adicionar_coluna_periodo_de_apuracao_inicial_e_final(
    lazyframe: LazyFrame,
    args: &Arguments,
) -> JoinResult<LazyFrame> {
    let periodo_de_apuracao: &str = coluna(Left, "pa"); // "Período de Apuração",
    let pa_ini: &str = "Período de Apuração Inicial";
    let pa_fim: &str = "Período de Apuração Final";

    let dt_start: Option<u32> = args.date_start;
    let dt_final: Option<u32> = args.date_final;

    // O Polars ignora valores nulos em funções de agregação como .min() e .max().
    // Portanto, ao adicionar as colunas de período inicial e final:

    let lf_result: LazyFrame = lazyframe
        .with_column(
            when(operacoes_de_entrada_ou_saida()?)
                // O .min() ignorará os nulos que criamos para as somas,
                // pegando apenas os meses reais (1 a 12).
                .then(col(periodo_de_apuracao).min())
                .otherwise(lit(NULL)) // replace by null
                .alias(pa_ini),
        )
        .with_column(
            when(operacoes_de_entrada_ou_saida()?)
                .then(col(periodo_de_apuracao).max())
                .otherwise(lit(NULL)) // replace by null
                .alias(pa_fim),
        )
        .with_column(
            // Subtrair 62 dias, aproximadamente dois meses
            // col(pa_ini) - chrono::Duration::days(62).lit()
            col(pa_ini).apply(
                move |col: Column| subtrair_meses(col, 2, dt_start),
                // GetOutput::from_type(DataType::Date),
                get_output_as_date,
            ),
        )
        .with_column(
            // Adicionar 31 dias, aproximadamente um mês
            // col(pa_fim) + chrono::Duration::days(31).lit()
            col(pa_fim).apply(
                move |col: Column| adicionar_meses(col, 1, dt_final),
                // GetOutput::from_type(DataType::Date),
                get_output_as_date,
            ),
        );

    //println!("lazyframe: {:?}", lz.clone().collect()?);

    Ok(lf_result)
}

/// Subtrair numero_de_mes de Series compostas de datas
pub fn subtrair_meses(
    col: Column,
    numero_de_mes: u32,
    dt: Option<u32>,
) -> Result<Column, PolarsError> {
    match col.dtype() {
        DataType::Date => sub_month(col, numero_de_mes, dt),
        _ => {
            eprintln!("fn subtrair_meses()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!("Not supported for Series with DataType {:?}", col.dtype()).into(),
            ))
        }
    }
}

// Polars does not have a FromIterator implementation on Series from an iterator of NaiveDate's.
// https://stackoverflow.com/questions/76297868/convert-str-to-naivedate-datatype-in-rust-polars
// https://stackoverflow.com/questions/75074357/filter-a-polars-dataframe-by-date-in-rust
fn sub_month(col: Column, numero_de_mes: u32, dt: Option<u32>) -> Result<Column, PolarsError> {
    let date: Vec<Option<NaiveDate>> = col
        .date()?
        .as_date_iter()
        .map(|opt_naive_date: Option<NaiveDate>| {
            opt_naive_date.and_then(|naive_date| {
                let (year, month) = get_year_and_month(naive_date, dt);

                // Fixar uma data específica:
                //let year = 2022;
                //let month = 3;

                NaiveDate::from_ymd_opt(year, month, 1)
                    .and_then(|dt| dt.checked_sub_months(Months::new(numero_de_mes)))
            })
        })
        .collect();

    Ok(Column::new("a".into(), date))
}

pub fn adicionar_meses(
    col: Column,
    numero_de_mes: u32,
    dt: Option<u32>,
) -> Result<Column, PolarsError> {
    match col.dtype() {
        DataType::Date => add_month(col, numero_de_mes, dt),
        _ => {
            eprintln!("fn adicionar_meses()");
            eprintln!("Column: {col:?}");
            Err(PolarsError::InvalidOperation(
                format!("Not supported for Series with DataType {:?}", col.dtype()).into(),
            ))
        }
    }
}

fn add_month(col: Column, numero_de_mes: u32, dt: Option<u32>) -> Result<Column, PolarsError> {
    let date: Vec<Option<NaiveDate>> = col
        .date()?
        .as_date_iter()
        .map(|opt_naive_date: Option<NaiveDate>| {
            opt_naive_date.and_then(|naive_date| {
                let (year, month) = get_year_and_month(naive_date, dt);

                // Fixar uma data específica:
                //let year = 2023;
                //let month = 6;

                NaiveDate::from_ymd_opt(year, month, 1)
                    .and_then(|dt| dt.checked_add_months(Months::new(numero_de_mes)))
            })
        })
        .collect();

    Ok(Column::new("a".into(), date))
}

fn get_year_and_month(naive_date: NaiveDate, dt: Option<u32>) -> (i32, u32) {
    match dt {
        Some(number) => {
            // number format: yyyymm
            // Example: 202308
            let year: i32 = (number / 100) as i32;
            let month: u32 = number % 100;
            // println!("year: {year} ; month: {month}");
            (year, month)
        }
        None => {
            let year: i32 = naive_date.year();
            let month: u32 = naive_date.month();
            // println!("year: {year} ; month: {month}");
            (year, month)
        }
    }
}
