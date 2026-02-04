use chrono::{DateTime, Local};
use claudiofsr_lib::clear_terminal_screen;
use execution_time::ExecutionTime;
use join_with_assignments::*;
use polars::prelude::*;

/*
Example of use:
    // Padronizar dados iniciais:
    unique -eitwcdknv 'Info da Receita sobre o Contribuinte.csv' > nfe_float64.csv

    clear && cargo test -- --nocapture
    clear && cargo run -- --help
    cargo fmt --all -- --check
    rustfmt src/excel.rs
    cargo doc --open
    cargo b -r && cargo install --path=.

    cat ~/.config/join_with_assignments/default-config.toml
    rm -v  ~/.config/join_with_assignments/default-config.toml
    join_with_assignments -1 'Info do Contribuinte EFD Contribuicoes.csv' -2 nfe_float64.csv -a '|' -b ';' -d ';' -c true -p true

    cargo run -- -1 'Dados - An/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - An/nfe_float64.csv' -c true -p true -r true -s 201601 -f 201612
    cargo run -- -1 'Dados - Ar/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Ar/nfe_float64.csv' -c true -p true -r true -s 202110 -f 202309

    join_with_assignments -1 'Dados - An/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - An/nfe_float64.csv' -c true -p true -r true -s 201601 -f 201612
    join_with_assignments -1 'Dados - Ar/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Ar/nfe_float64.csv' -c true -p true -r true -s 202110 -f 202309

    // Verificação dos arquivos:
    sha512sum df*.csv "Dados - Nd/"df*.csv
    b3sum df*.csv "Dados - Nd/"df*.csv

    file1="df_itens_de_docs_fiscais_result.csv"
    file2="Dados - Nd/df_itens_de_docs_fiscais_result.csv"
    diff $file1 $file2
    line=1
    diff <(head -n $line $file1 | tail -n 1) <(head -n $line $file2 | tail -n 1)
    meld $file1 $file2&

    // Boring Data Tool (bdt): <https://github.com/andygrove/bdt>
    bdt schema df_itens_de_docs_fiscais_result.parquet
    bdt schema df_consolidacao_natureza_da_bcalc.parquet
    bdt count --table df_itens_de_docs_fiscais_result.parquet
*/
fn main() -> JoinResult<()> {
    clear_terminal_screen();
    configure_the_environment();
    show_sysinfo();

    let args: Arguments = Arguments::build()?;
    let timer = ExecutionTime::start();

    let df_itens_de_docs_fiscais: DataFrame = get_dataframe_after_assignments(&args)?;

    let df_consolidacao_natureza_da_bcalc: DataFrame =
        obter_consolidacao_nat(&df_itens_de_docs_fiscais, false)?;

    let df_itens_de_docs_fiscais_result: DataFrame = glosar_bc(&df_itens_de_docs_fiscais, &args)?;

    let df_consolidacao_natureza_da_bcalc_result: DataFrame =
        obter_consolidacao_nat(&df_itens_de_docs_fiscais_result, true)?;

    // Add column from one dataframe to another.
    let df_joined: DataFrame =
        integrate_and_sort_column(df_itens_de_docs_fiscais, df_itens_de_docs_fiscais_result)?;

    let df_filtered = apply_filter(df_joined, &args)?;

    let df_itens_de_docs_fiscais_result = conditionally_remove_null_columns(df_filtered, &args)?;

    let dataframes: Vec<DataFrame> = [
        df_itens_de_docs_fiscais_result,
        df_consolidacao_natureza_da_bcalc,
        df_consolidacao_natureza_da_bcalc_result,
    ]
    .into_iter()
    .map(|mut df| {
        // Necessário antes de usar PolarsXlsxWriter::new()
        df.rechunk_mut();
        df
    })
    .collect();

    let basenames: [&str; 3] = [
        "df_itens_de_docs_fiscais_result",
        "df_consolidacao_natureza_da_bcalc",
        "df_consolidacao_natureza_da_bcalc_result",
    ];

    let iterator = dataframes.iter().zip(basenames.iter());

    if args.print_parquet == Some(true) {
        for (dataframe, basename) in iterator.clone() {
            write_pqt(dataframe, basename)?;
        }
    }

    if args.print_csv == Some(true) {
        let delimiter: char = args.delimiter_output.unwrap_or(';');
        for (dataframe, basename) in iterator {
            write_csv(dataframe, basename, delimiter)?;
        }
    }

    if args.print_excel == Some(true) {
        write_xlsx(&dataframes)?;
    }

    let dt_local_now: DateTime<Local> = Local::now();
    println!("Location date: {}", dt_local_now.format("%d/%m/%Y"));
    println!("Total Execution Time: {}\n", timer.get_elapsed_time());

    Ok(())
}
