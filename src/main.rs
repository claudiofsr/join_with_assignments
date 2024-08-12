use chrono::{DateTime, Local};
use claudiofsr_lib::clear_terminal_screen;
use join_with_assignments::*;
use polars::prelude::*;
use std::{error::Error, time::Instant};

/*
Example of use:
    // Padronizar dados iniciais:
    unique -eitwcdknv 'Info da Receita sobre o Contribuinte.csv' > nfe_float64.csv

    clear && cargo test -- --nocapture
    clear && cargo run -- --help
    cargo b -r && cargo install --path=.
    rustfmt src/excel.rs

    cat ~/.config/join_with_assignments/default-config.toml
    rm -v  ~/.config/join_with_assignments/default-config.toml
    join_with_assignments -1 'Info do Contribuinte EFD Contribuicoes.csv' -2 nfe_float64.csv -a '|' -b ';' -d ';' -c true -p true

    cargo run -- -1 'Dados - Ar/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Ar/nfe_float64.csv' -c true -p true -r true -s 202110 -f 202309
    cargo run -- -1 'Dados - Au/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Au/nfe_float64.csv' -c true -p true -r true -s 201708 -f 202312
    cargo run -- -1 'Dados - Bo/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Bo/nfe_float64.csv' -c true -p true -r true -s 202010 -f 202012
    cargo run -- -1 'Dados - Br/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Br/nfe_float64.csv' -c true -p true -r true -s 202109 -f 202303
    cargo run -- -1 'Dados - Da/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Da/nfe_float64.csv' -c true -p true -r true -s 202201 -f 202206
    cargo run -- -1 'Dados - Le/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Le/nfe_float64.csv' -c true -p true -r true -s 202204 -f 202306
    cargo run -- -1 'Dados - Nd/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Nd/nfe_float64.csv' -c true -p true -r true -s 201301 -f 201812
    cargo run -- -1 'Dados - Pg/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Pg/nfe_float64.csv' -c true -p true -r true -s 201907 -f 202206
    cargo run -- -1 'Dados - Tc/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Tc/nfe_float64.csv' -c true -p true -r true -s 201604 -f 201812

    join_with_assignments -1 'Dados - Ar/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Ar/nfe_float64.csv' -c true -p true -r true -s 202110 -f 202309
    join_with_assignments -1 'Dados - Au/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Au/nfe_float64.csv' -c true -p true -r true -s 201708 -f 202312
    join_with_assignments -1 'Dados - Bo/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Bo/nfe_float64.csv' -c true -p true -r true -s 202010 -f 202012
    join_with_assignments -1 'Dados - Br/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Br/nfe_float64.csv' -c true -p true -r true -s 202109 -f 202303
    join_with_assignments -1 'Dados - Da/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Da/nfe_float64.csv' -c true -p true -r true -s 202201 -f 202206
    join_with_assignments -1 'Dados - Le/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Le/nfe_float64.csv' -c true -p true -r true -s 202204 -f 202306
    join_with_assignments -1 'Dados - Nd/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Nd/nfe_float64.csv' -c true -p true -r true -s 201301 -f 201812
    join_with_assignments -1 'Dados - Pg/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Pg/nfe_float64.csv' -c true -p true -r true -s 201907 -f 202206
    join_with_assignments -1 'Dados - Tc/Info do Contribuinte EFD Contribuicoes.csv' -2 'Dados - Tc/nfe_float64.csv' -c true -p true -r true -s 201604 -f 201812

    // Verificação dos arquivos:
    sha512sum df*.csv "Dados - Bo/"df*.csv
    b3sum df*.csv "Dados - Da/"df*.csv

    file1="df_itens_de_docs_fiscais_result.csv"
    file2="Dados - Da/df_itens_de_docs_fiscais_result.csv"
    diff $file1 $file2
    line=1
    diff <(head -n $line $file1 | tail -n 1) <(head -n $line $file2 | tail -n 1)
    meld $file1 $file2&

    // Boring Data Tool (bdt): <https://github.com/andygrove/bdt>
    bdt schema df_itens_de_docs_fiscais_result.parquet
    bdt schema df_consolidacao_natureza_da_bcalc.parquet
    bdt count --table df_itens_de_docs_fiscais_result.parquet
*/
fn main() -> Result<(), Box<dyn Error>> {
    clear_terminal_screen();
    configure_the_environment();
    show_sysinfo();

    let args: Arguments = Arguments::build()?;
    let time = Instant::now();

    let df_itens_de_docs_fiscais: DataFrame = get_dataframe_after_assignments(&args)?;

    let df_consolidacao_natureza_da_bcalc: DataFrame =
        obter_consolidacao_nat(&df_itens_de_docs_fiscais, false)?;

    let df_itens_de_docs_fiscais_result: DataFrame = glosar_bc(&df_itens_de_docs_fiscais, &args)?;

    let df_consolidacao_natureza_da_bcalc_result: DataFrame =
        obter_consolidacao_nat(&df_itens_de_docs_fiscais_result, true)?;

    let dataframes: [DataFrame; 4] = [
        df_itens_de_docs_fiscais,
        df_itens_de_docs_fiscais_result,
        df_consolidacao_natureza_da_bcalc,
        df_consolidacao_natureza_da_bcalc_result,
    ];

    let basenames: [&str; 4] = [
        "df_itens_de_docs_fiscais",
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

    write_xlsx(&args, &dataframes)?;

    let dt_local_now: DateTime<Local> = Local::now();
    println!("Location date: {}", dt_local_now.format("%d/%m/%Y"));
    println!("Total Execution Time: {:?}\n", time.elapsed());

    Ok(())
}
