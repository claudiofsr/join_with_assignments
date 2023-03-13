use chrono::{DateTime, Local};
use polars::prelude::*;
use polars::datatypes::DataType;
use rayon::prelude::*;

use std::{
    env,
    process, // process::exit(1)
    error::Error,
    time::Instant,
};

use join_with_assignments::{
    Config,
    VecTuples,
    show_sysinfo,
    clear_terminal_screen,
    get_lazyframe_from_csv,
    round_series,
    get_opt_vectuples,
    get_option_assignments,
    formatar_chave_eletronica,
    write_csv,
    write_pqt,
};

mod my_table;
use my_table::MyTable;

fn main() -> Result<(), Box<dyn Error>> {

    configure_the_environment();
    clear_terminal_screen();
    show_sysinfo();

    let my_table = MyTable::default();
    let args: Vec<String> = env::args().collect();
    let config = Config::build(&args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    let time = Instant::now();

    // Read LazyFrame from CSV file
    let lf_a: LazyFrame = get_lazyframe_from_csv(config.csv_a, config.dlm_a, "left" )?
        .with_row_count(my_table.side_a.column_number, Some(0u32));
    let lf_b: LazyFrame = get_lazyframe_from_csv(config.csv_b, config.dlm_b, "right")?
        .with_row_count(my_table.side_b.column_number, Some(0u32));

    // Formatar colunas a fim de realizar comparações e somas de valores.
    // Lazy operations don’t execute until we call .collect()?.
    let lf_a: LazyFrame = format_fazyframe_a(lf_a, &my_table).collect()?.lazy();
    let lf_b: LazyFrame = format_fazyframe_b(lf_b, &my_table).collect()?.lazy();

    // Groupby column
    let lazy_groupby_a: LazyFrame = groupby_fazyframe_a(lf_a.clone(), &my_table)?;
    let lazy_groupby_b: LazyFrame = groupby_fazyframe_b(lf_b.clone(), &my_table)?;

    let dataframe_joinned: DataFrame = join_lazyframes(lazy_groupby_a, lazy_groupby_b, &my_table)?;

    //print_column_and_schema(dataframe_joinned.clone());

    let vec_opt_vec_tuples: Vec<Option<VecTuples>> = get_vec_from_assignments(dataframe_joinned, &my_table)?;
    let df_correlation: DataFrame = make_df_correlation(vec_opt_vec_tuples, &my_table)?;

    let lf_c: LazyFrame = join_with_interline_correlations(lf_a, lf_b, df_correlation, &my_table)?;
    let mut dfd_output: DataFrame = check_correlation_between_dataframes(lf_c, &my_table)?;

    //println!("dfd_output:\n{dfd_output}\n");
    write_csv(&mut dfd_output, ';', "output.csv")?;
    write_pqt(&mut dfd_output, "output.parquet")?;
    //let df_parquet: DataFrame = read_pqt("output.parquet")?;

    let dt_local_now: DateTime<Local> = Local::now();
    println!("Data Local: {}", dt_local_now.format("%d/%m/%Y"));
    println!("Tempo de Execução Total: {:?}\n",time.elapsed());

    Ok(())
}

fn configure_the_environment() {
    // https://stackoverflow.com/questions/70830241/rust-polars-how-to-show-all-columns/75675569#75675569
    // https://pola-rs.github.io/polars/polars/index.html#config-with-env-vars
    // Config with ENV vars
    env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
    //env::set_var("POLARS_FMT_MAX_COLS", "20"); // maximum number of columns shown when formatting DataFrames.
    env::set_var("POLARS_FMT_MAX_ROWS", "10");   // maximum number of rows shown when formatting DataFrames.
    env::set_var("POLARS_FMT_STR_LEN", "50");    // maximum number of characters printed per string value.
}

/// Formatar colunas a fim de realizar comparações e somas de valores.
fn format_fazyframe_a (lazyframe: LazyFrame, my_table: &MyTable) -> LazyFrame {

    lazyframe // Formatar colunas
    .with_column(
        col(my_table.side_a.column_number).cast(DataType::UInt64)
    )
    .with_column(
        col(my_table.side_a.column_chave)
        .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::Utf8))
    )
    .with_column(
        col(my_table.side_a.column_value)
        .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
    )
}

/// Formatar colunas a fim de realizar comparações e somas de valores.
fn format_fazyframe_b (lazyframe: LazyFrame, my_table: &MyTable) -> LazyFrame {

    lazyframe // Formatar colunas
    .with_column(
        col(my_table.side_b.column_number).cast(DataType::UInt64)
    )
    .with_column(
        col(my_table.side_b.column_chave)
        .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::Utf8))
    )
    .with_column(
        col(my_table.side_b.column_value)
        .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
    )
    .with_column(
        col(my_table.side_b.column_bc_icms)
        .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
    )
}

fn groupby_fazyframe_a (lazyframe: LazyFrame, my_table: &MyTable) -> Result<LazyFrame, PolarsError> {

    let lf_groupby: LazyFrame = lazyframe
    .filter(
             col(my_table.side_a.column_chave).is_not_null()
        .and(col(my_table.side_a.column_value).is_not_null())
    )
    .groupby([col(my_table.side_a.column_chave)])
    .agg([
        col(my_table.side_a.column_number),
        col(my_table.side_a.column_value).alias("Valores dos Itens da Nota Fiscal EFD"),
    ]);

    println!("Group information according to column '{}'", my_table.side_a.column_chave);
    println!("groupby_fazyframe_a:\n{}\n", lf_groupby.clone().collect()?);

    Ok(lf_groupby)
}

fn groupby_fazyframe_b (lazyframe: LazyFrame, my_table: &MyTable) -> Result<LazyFrame, PolarsError> {

    let lf_groupby: LazyFrame = lazyframe
    .filter(
             col(my_table.side_b.column_chave).is_not_null()
        .and(col(my_table.side_b.column_value).is_not_null())
    )
    .filter(
        when(col(my_table.side_b.column_registro).eq(lit("NFe")))
        .then(col(my_table.side_b.column_value).gt(0))
        .otherwise(true)
    )
    .groupby([col(my_table.side_b.column_chave)])
    .agg([
        col(my_table.side_b.column_number),
        col(my_table.side_b.column_value).alias("Valores dos Itens da Nota Fiscal NFE"),
    ]);

    println!("Group information according to column '{}'", my_table.side_b.column_chave);
    println!("groupby_fazyframe_b:\n{}\n", lf_groupby.clone().collect()?);

    Ok(lf_groupby)
}

fn join_lazyframes (lazyframe_a: LazyFrame, lazyframe_b: LazyFrame, my_table: &MyTable) -> Result<DataFrame, PolarsError> {

    let dataframe: DataFrame = lazyframe_a
    .join(lazyframe_b, [col(my_table.side_a.column_chave)], [col(my_table.side_b.column_chave)], JoinType::Inner)
    // An inner join produces a DataFrame that contains only the rows where the join key exists in both DataFrames.
    // Caso fosse utilizado JoinType::left, dado uma chave EFD de 44 digitos, estas seriam as chaves NFE de mesmos digitos não encontradas!
    //.filter(col("Valores dos Itens da Nota Fiscal NFE").not_null())
    //.slice(220, 5) // range
    //.limit(4)
    // https://pola-rs.github.io/polars-book/user-guide/dsl/custom_functions.html?highlight=apply#to-apply
    .with_column(
        // pack to struct to get access to multiple fields in a custom `apply/map`
        // polars-plan-0.26.1/src/dsl/functions.rs ; features = ["dtype-struct"]
        as_struct(&[
            col("Valores dos Itens da Nota Fiscal EFD"),
            col("Valores dos Itens da Nota Fiscal NFE"),
            ])
            .apply(
                |s| {
                    // Downcast to struct
                    let struct_chunked: &StructChunked = s.struct_()?;

                    // Get the fields as Series
                    let ser_list_efd: &Series = &struct_chunked.field_by_name("Valores dos Itens da Nota Fiscal EFD")?;
                    let ser_list_nfe: &Series = &struct_chunked.field_by_name("Valores dos Itens da Nota Fiscal NFE")?;

                    // Get rows from columns with into_iter()
                    let vec_opt_ser_efd: Vec<Option<Series>> = ser_list_efd.list()?.into_iter().collect();
                    let vec_opt_ser_nfe: Vec<Option<Series>> = ser_list_nfe.list()?.into_iter().collect();

                    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
                    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
                    let vec_series: Vec<Option<Series>> = (vec_opt_ser_efd, vec_opt_ser_nfe)
                        .into_par_iter() // rayon: parallel iterator
                        .map(|(opt_ser_efd, opt_ser_nfe)| {
                            match (opt_ser_efd, opt_ser_nfe) {
                                (Some(ser_efd), Some(ser_nfe)) => get_option_assignments(ser_efd, ser_nfe),
                                _ => None,
                            }
                        })
                        .collect();

                    let new_series = Series::new("New", vec_series);

                    Ok(Some(new_series))
                },
                GetOutput::from_type(DataType::UInt64),
            )
            .alias("Munkres Assignments"),
    )
    .collect()?;

    println!("dataframe_joinned = lazyframe_a.join(lazyframe_b, [...], JoinType::Inner)\n{dataframe}\n");

    Ok(dataframe)
}

#[allow(dead_code)]
fn print_column_and_schema (dataframe: DataFrame) {
    let column_names = dataframe.get_column_names();
    let schema = dataframe.schema();
    println!("column_names: {column_names:#?}");
    println!("schema: {schema:#?}");
}

fn get_vec_from_assignments (dataframe: DataFrame, my_table: &MyTable) -> Result<Vec<Option<VecTuples>>, PolarsError> {

    // Get columns from dataframe
    let column_chave_doc: &Series = dataframe.column(my_table.side_a.column_chave)?;
    let column_lines_efd: &Series = dataframe.column(my_table.side_a.column_number)?;
    let column_lines_nfe: &Series = dataframe.column(my_table.side_b.column_number)?;
    let column_assignmen: &Series = dataframe.column("Munkres Assignments")?;

    // Get rows from columns with into_iter()
    let vec_opt_chave_doc: Vec<Option<&str>> = column_chave_doc.utf8()?.into_iter().collect();
    let vec_opt_ser_efd: Vec<Option<Series>> = column_lines_efd.list()?.into_iter().collect();
    let vec_opt_ser_nfe: Vec<Option<Series>> = column_lines_nfe.list()?.into_iter().collect();
    let vec_opt_ser_asg: Vec<Option<Series>> = column_assignmen.list()?.into_iter().collect();

    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
    let vec_opt_vec_tuples: Vec<Option<VecTuples>> = (vec_opt_chave_doc, vec_opt_ser_efd, vec_opt_ser_nfe, vec_opt_ser_asg)
        .into_par_iter() // rayon: parallel iterator
        .map(|(opt_chave_doc, opt_ser_efd, opt_ser_nfe, opt_ser_asg)| {
            match (opt_chave_doc, opt_ser_efd, opt_ser_nfe, opt_ser_asg) {
                (Some(chave_doc), Some(ser_efd), Some(ser_nfe), Some(ser_asg)) => get_opt_vectuples(chave_doc, ser_efd, ser_nfe, ser_asg),
                _ => None
            }
        })
        .collect();

    drop(dataframe);

    Ok(vec_opt_vec_tuples)
}

fn make_df_correlation(vec_opt_vec_tuples:Vec<Option<VecTuples>>, my_table: &MyTable) -> Result<DataFrame, PolarsError> {

    // Transform a vector of tuples into many vectors
    let mut col_chaves: Vec<String> = Vec::new();
    let mut col_lines_efd: Vec<u64> = Vec::new();
    let mut col_lines_nfe: Vec<u64> = Vec::new();

    for vec_tuples in vec_opt_vec_tuples.into_iter().flatten() {
        for (chave, line_efd, line_nfe) in vec_tuples {
            col_chaves.push(chave);
            col_lines_efd.push(line_efd);
            col_lines_nfe.push(line_nfe);
        }
    }

    let df_correlation: DataFrame = df! {
        my_table.side_a.column_chave => &col_chaves,
        my_table.side_a.column_number => &col_lines_efd,
        my_table.side_b.column_number => &col_lines_nfe,
    }?;

    println!("df_correlation:\n{df_correlation}\n");
    //write_csv(&mut df_correlation, ';', "output_correlation.csv")?;

    Ok(df_correlation)
}

fn join_with_interline_correlations (lf_a: LazyFrame, lf_b: LazyFrame, df_correlation: DataFrame, my_table: &MyTable) -> Result<LazyFrame, PolarsError> {

    let columns = (my_table.side_a.column_chave, my_table.side_b.column_number);
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_b = lf_b // Duplicate columns before join()
    .with_column(
        col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)").alias(columns.0),
    );

    let lf_b_solution: LazyFrame = df_correlation.lazy().join(lf_b, common_a, common_b, JoinType::Left)
    .drop_columns([my_table.side_b.column_number]);


    // add two empty columns to lazyframe
    let empty_column1 = "Verificação dos Valores: EFD x Docs Fiscais";
    let empty_column2 = "Glosar Crédito de PIS/PASEP e COFINS";
    let lf_a = lf_a
        .with_columns(
            vec![
                lit("").alias(empty_column1),
                lit("").alias(empty_column2),
            ]
        );

    let columns = (my_table.side_a.column_chave, my_table.side_a.column_number);
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_c: LazyFrame = lf_a.join(lf_b_solution, common_a, common_b, JoinType::Left)
    .drop_columns([my_table.side_a.column_number]);

    Ok(lf_c)
}

fn check_correlation_between_dataframes (lazyframe: LazyFrame, my_table: &MyTable) -> Result<DataFrame, PolarsError> {

    let coluna_deverificacao: &str = "Verificação dos Valores: EFD x Docs Fiscais";
    let valor_do_item_da_efd: &str = my_table.side_a.column_value;             // "Valor Total do Item";
    let valor_da_nota_proporcional_nfe: &str = my_table.side_b.column_value;   // "Valor da Nota Proporcional : NF Item (Todos) SOMA";
    let valor_da_base_calculo_icms_nfe: &str = my_table.side_b.column_bc_icms; // "ICMS: Base de Cálculo : NF Item (Todos) SOMA"

    let valores_iguais_nota_prop = col(valor_do_item_da_efd).eq(col(valor_da_nota_proporcional_nfe));
    let valores_iguais_base_icms = col(valor_do_item_da_efd).eq(col(valor_da_base_calculo_icms_nfe));

    let dataframe: DataFrame = lazyframe
        .with_column(
            when(valores_iguais_nota_prop)
            .then(lit("valores iguais: Nota Proporcional"))
            .otherwise(
                when(valores_iguais_base_icms)
                .then(lit("valores iguais: Base de Cálculo do ICMS"))
                .otherwise(lit(""))
            )
            .alias(coluna_deverificacao)
        )
        .collect()?;

    Ok(dataframe)
}
