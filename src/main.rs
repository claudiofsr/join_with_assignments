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
    clear_terminal_screen,
    get_lazyframe_from_csv,
    round_series,
    get_opt_vectuples,
    get_option_assignments,
    formatar_chave_eletronica,
    write_csv,
    write_pqt,
};

fn main() -> Result<(), Box<dyn Error>> {

    configure_the_environment();
    clear_terminal_screen();

    let args: Vec<String> = env::args().collect();
    let config = Config::build(&args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    let time = Instant::now();

    // Read LazyFrame from CSV file
    let lf_a: LazyFrame = get_lazyframe_from_csv(config.csv_a, config.dlm_a, "left" )?.with_row_count("Linhas EFD", Some(0u32));
    let lf_b: LazyFrame = get_lazyframe_from_csv(config.csv_b, config.dlm_b, "right")?.with_row_count("Linhas NFE", Some(0u32));

    // Formatar colunas para f64 a fim de realizar somas de valores.
    let lf_a: LazyFrame = format_fazyframe_a(lf_a);
    let lf_b: LazyFrame = format_fazyframe_b(lf_b);

    // Groupby column
    let lazy_groupby_a: LazyFrame = groupby_fazyframe_a(lf_a.clone())?;
    let lazy_groupby_b: LazyFrame = groupby_fazyframe_b(lf_b.clone())?;

    let dataframe_joinned: DataFrame = join_lazyframes(lazy_groupby_a, lazy_groupby_b)?;

    //print_column_and_schema(dataframe_joinned.clone());

    let vec_opt_vec_tuples: Vec<Option<VecTuples>> = get_vec_from_assignments(dataframe_joinned)?;
    let df_correlation: DataFrame = make_df_correlation(vec_opt_vec_tuples)?;

    let lf_c: LazyFrame = join_with_interline_correlations(lf_a, lf_b, df_correlation)?;
    let mut dfd_output: DataFrame = check_correlation_between_dataframes(lf_c)?;

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

fn format_fazyframe_a (lazyframe: LazyFrame) -> LazyFrame {

    lazyframe // Formatar colunas
    .with_column(
        col("Linhas EFD").cast(DataType::UInt64)
    )
    .with_column(
        col("Chave do Documento")
        .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::Utf8))
    )
    .with_column(
        col("Valor Total do Item")
        .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
    )
}

fn format_fazyframe_b (lazyframe: LazyFrame) -> LazyFrame {

    lazyframe // Formatar colunas
    .with_column(
        col("Linhas NFE").cast(DataType::UInt64)
    )
    .with_column(
        col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)")
        .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::Utf8))
    )
    .with_column(
        col("Valor da Nota Proporcional : NF Item (Todos) SOMA")
        .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
    )
    .with_column(
        col("ICMS: Base de Cálculo : NF Item (Todos) SOMA")
        .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
    )
}

fn groupby_fazyframe_a (lazyframe: LazyFrame) -> Result<LazyFrame, PolarsError> {

    let column_name:   &str = "Chave do Documento";
    let column_number: &str = "Linhas EFD";
    let column_value:  &str = "Valor Total do Item";

    let lf_groupby: LazyFrame = lazyframe
    .groupby([col(column_name)])
    .agg([
        col(column_number),
        col(column_value).alias("Valores dos Itens da Nota Fiscal EFD"),
    ])
    .filter(col(column_name).is_not_null());

    println!("Group information according to column '{column_name}'");
    println!("groupby_fazyframe_a:\n{}\n", lf_groupby.clone().collect()?);

    Ok(lf_groupby)
}

fn groupby_fazyframe_b (lazyframe: LazyFrame) -> Result<LazyFrame, PolarsError> {

    let column_name:   &str = "Chave da Nota Fiscal Eletrônica : NF Item (Todos)";
    let column_number: &str = "Linhas NFE";
    let column_value:  &str = "Valor da Nota Proporcional : NF Item (Todos) SOMA";

    let lf_groupby: LazyFrame = lazyframe
    .filter(
        when(col("Registro de Origem do Item : NF Item (Todos)").eq(lit("NFe")))
        .then(col(column_value).gt(0))
        .otherwise(true)
    )
    .groupby([col(column_name)])
    .agg([
        col(column_number),
        col(column_value).alias("Valores dos Itens da Nota Fiscal NFE"),
    ])
    .filter(col(column_name).is_not_null());

    println!("Group information according to column '{column_name}'");
    println!("groupby_fazyframe_b:\n{}\n", lf_groupby.clone().collect()?);

    Ok(lf_groupby)
}

fn join_lazyframes (lazyframe_a: LazyFrame, lazyframe_b: LazyFrame) -> Result<DataFrame, PolarsError> {

    let dataframe: DataFrame = lazyframe_a
    .join(lazyframe_b, [col("Chave do Documento")], [col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)")], JoinType::Inner)
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

fn get_vec_from_assignments (dataframe: DataFrame) -> Result<Vec<Option<VecTuples>>, PolarsError> {

    // Get columns from dataframe
    let column_chave_doc: &Series = dataframe.column("Chave do Documento")?;
    let column_lines_efd: &Series = dataframe.column("Linhas EFD")?;
    let column_lines_nfe: &Series = dataframe.column("Linhas NFE")?;
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

fn make_df_correlation(vec_opt_vec_tuples:Vec<Option<VecTuples>>) -> Result<DataFrame, PolarsError> {

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
        "Chave do Documento" => &col_chaves,
        "Linhas EFD" => &col_lines_efd,
        "Linhas NFE" => &col_lines_nfe,
    }?;

    println!("df_correlation:\n{df_correlation}\n");
    //write_csv(&mut df_correlation, ';', "output_correlation.csv")?;

    Ok(df_correlation)
}

fn join_with_interline_correlations (lf_a: LazyFrame, lf_b: LazyFrame, df_correlation: DataFrame) -> Result<LazyFrame, PolarsError> {

    let columns = ("Chave do Documento", "Linhas NFE");
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_b = lf_b // Duplicate columns before join()
    .with_column(
        col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)").alias(columns.0),
    );

    let lf_b_solution: LazyFrame = df_correlation.lazy().join(lf_b, common_a, common_b, JoinType::Left)
    .drop_columns(["Linhas NFE"]);


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

    let columns = ("Chave do Documento", "Linhas EFD");
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_c: LazyFrame = lf_a.join(lf_b_solution, common_a, common_b, JoinType::Left)
    .drop_columns(["Linhas EFD"]);

    Ok(lf_c)
}

fn check_correlation_between_dataframes (lazyframe: LazyFrame) -> Result<DataFrame, PolarsError> {

    let coluna_deverificacao = "Verificação dos Valores: EFD x Docs Fiscais";
    let valor_do_item_da_efd = "Valor Total do Item";
    let valor_da_nota_proporcional_nfe = "Valor da Nota Proporcional : NF Item (Todos) SOMA";
    let valor_da_base_calculo_icms_nfe = "ICMS: Base de Cálculo : NF Item (Todos) SOMA";

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
