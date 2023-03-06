use chrono::{DateTime, Local};
use polars::prelude::*;
//use polars_core::utils::{ _split_offsets, split_ca, split_series};
use polars::datatypes::DataType;
use rayon::prelude::*;
use pathfinding::prelude::{Matrix, kuhn_munkres_min};
use std::{
    env,
    process, // process::exit(1)
    error::Error,
    time::Instant,
    //collections::BTreeMap,
};

use join_with_assignments::{
    Config,
    clear_terminal_screen,
    get_matrix,
    convert_to_square_matrix,
    get_lazyframe_from_csv,
    datatype_to_f64,
    formatar_chave_eletronica,
    write_csv,
    write_pqt,
    /*
    get_width,
    print_matrix,
    display_bipartite_matching,
    */
};

fn main() -> Result<(), Box<dyn Error>> {

    clear_terminal_screen();
    let args: Vec<String> = env::args().collect();
    let config = Config::build(&args).unwrap_or_else(|err| {
        println!("Problem parsing arguments: {err}");
        process::exit(1);
    });

    let time = Instant::now();
    let lf_a: LazyFrame = get_lazyframe_from_csv(&config.csv_a, ';', "left" )?.with_row_count("Linhas EFD", Some(0u32));
    let lf_b: LazyFrame = get_lazyframe_from_csv(&config.csv_b, ';', "right")?.with_row_count("Linhas NFE", Some(0u32));

    // Formatar colunas para f64 a fim de realizar somas de valores.
    let lf_a = lf_a // Formatar colunas
    .with_column(
        col("Linhas EFD").cast(DataType::UInt64)
    )
    .with_column(
        col("Chave do Documento")
        .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::Utf8))
    )
    .with_column(
        col("Valor Total do Item")
        .apply(datatype_to_f64, GetOutput::from_type(DataType::Float64))
    );
    

    let lf_b = lf_b // Formatar colunas
    .with_column(
        col("Linhas NFE").cast(DataType::UInt64)
    )
    .with_column(
        col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)")
        .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::Utf8))
    )
    .with_column(
        col("Valor da Nota Proporcional : NF Item (Todos) SOMA")
        .apply(datatype_to_f64, GetOutput::from_type(DataType::Float64))
    )
    .with_column(
        col("ICMS: Base de Cálculo : NF Item (Todos) SOMA")
        .apply(datatype_to_f64, GetOutput::from_type(DataType::Float64))
    );


    // --- lazy_groupby_a ---
    // ------ Start -------

    let lazy_groupby_a = lf_a.clone()
    .groupby([col("Chave do Documento")])
    .agg([
        col("Linhas EFD"),
        col("Valor Total do Item").alias("Valores dos Itens da Nota Fiscal EFD"),
    ]);

    println!("lazy_groupby_a:\n{}\n", lazy_groupby_a.clone().collect()?);

    // ------ Final -------
    // --- lazy_groupby_a ---


    // --- lazy_groupby_b ---
    // ------ Start -------

    let lazy_groupby_b = lf_b.clone()
    .filter(
        when(col("Registro de Origem do Item : NF Item (Todos)").eq(lit("NFe")))
        .then(col("Valor da Nota Proporcional : NF Item (Todos) SOMA").gt(0))
        .otherwise(true)
    )
    .groupby([col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)")])
    .agg([
        col("Linhas NFE"),
        col("Valor da Nota Proporcional : NF Item (Todos) SOMA").alias("Valores dos Itens da Nota Fiscal NFE"),
    ]);

    println!("lazy_groupby_b:\n{}\n", lazy_groupby_b.clone().collect()?);

    // ------ Final -------
    // --- lazy_groupby_b ---


    let df_groupby_c: DataFrame = lazy_groupby_a
    .join(lazy_groupby_b, [col("Chave do Documento")], [col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)")], JoinType::Inner)
    // An inner join produces a DataFrame that contains only the rows where the join key exists in both DataFrames.
    //.filter(col("Valores dos Itens da Nota Fiscal NFE").not_null()) // Dado uma chave EFD de 44 digitos, estas são as chaves NFE de mesmos digitos não encontradas! JoinType::left
    //.slice(220, 5) // range
    //.limit(2)
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
                    // downcast to struct
                    let struct_chunked: &StructChunked = s.struct_()?;

                    // get the fields as Series
                    let series_list_efd: &Series = &struct_chunked.field_by_name("Valores dos Itens da Nota Fiscal EFD")?;
                    let series_list_nfe: &Series = &struct_chunked.field_by_name("Valores dos Itens da Nota Fiscal NFE")?;

                    let chunked_array_efd: &ChunkedArray<ListType> = series_list_efd.list()?;
                    let chunked_array_nfe: &ChunkedArray<ListType> = series_list_nfe.list()?;

                    let vec_opt_series_efd: Vec<Option<Series>> = chunked_array_efd.into_iter().collect();
                    let vec_opt_series_nfe: Vec<Option<Series>> = chunked_array_nfe.into_iter().collect();

                    let vec_vecf64_efd: Vec<Vec<f64>> = get_vec_vecf64(vec_opt_series_efd)?;
                    let vec_vecf64_nfe: Vec<Vec<f64>> = get_vec_vecf64(vec_opt_series_nfe)?;

                    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
                    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
                    let vec_series: Vec<Series> = (vec_vecf64_efd, vec_vecf64_nfe)
                        .into_par_iter() // rayon: parallel iterator
                        .map(|(vecf64_efd, vecf64_nfe)| munkres_assignments(vecf64_efd, vecf64_nfe))
                        .collect();

                    let new_series = Series::new("New", vec_series);

                    Ok(Some(new_series))
                },
                GetOutput::from_type(DataType::UInt64),
            )
            .alias("Munkres Assignments"),
    )
    .collect()?;

    fn get_vec_vecf64(vec_opt_series: Vec<Option<Series>>) -> Result<Vec<Vec<f64>>, PolarsError> {

        // https://stackoverflow.com/questions/71376935/how-to-get-a-vec-from-polars-series-or-chunkedarray

        let vec: Vec<Vec<f64>> = vec_opt_series
        .into_iter()
        .map(|opt_series| opt_series
            .map( |series| series
                .f64()
                .unwrap()
                //.into_no_null_iter() // if we are certain we don't have missing values
                .into_iter()
                .map(|opt_f64| opt_f64.unwrap())
                .collect::<Vec<f64>>()
            )
            .unwrap()
        )
        .collect();

        Ok(vec)
    }

    fn munkres_assignments(vec_a: Vec<f64>, vec_b: Vec<f64>) -> Series {

        let array_1: Vec<i128> = vec_a.iter().map(|&v| (v * 100.0).round() as i128).collect();
        let array_2: Vec<i128> = vec_b.iter().map(|&v| (v * 100.0).round() as i128).collect();
    
        //let width: usize = get_width(&array_1, &array_2);
        //println!("\nFind the minimum bipartite matching:");
        //println!("array_1: {array_1:width$?}");
        //println!("array_2: {array_2:width$?}");
    
        let mut matrix: Vec<Vec<i128>> = get_matrix(&array_1, &array_2);
    
        convert_to_square_matrix(&mut matrix);
    
        // Assign weights to everybody choices
        let weights: Matrix<i128> = Matrix::from_rows(matrix.clone()).unwrap();
        let (_sum, assignments): (i128, Vec<usize>) = kuhn_munkres_min(&weights);
    
        //display_bipartite_matching(width, &matrix, &array_1, &array_2, &assignments, false);
        //print_matrix(width, &matrix[..], &array_1, &array_2, &assignments, true);

        // convert Vec<usize> to Vec<u64>
        let assignments_u64: Vec<u64> = assignments.iter().map(|&val| u64::try_from(val).unwrap() ).collect();

        Series::new("New", assignments_u64)
    }

    println!("df_groupby_c = lz_groupby_a.join(&lz_groupby_b)\n{df_groupby_c}\n");

    /*
    let column_names = lz_groupby_c.collect()?.get_column_names();
    let schema = lz_groupby_c.collect()?.schema();
    println!("column_names: {column_names:#?}");
    println!("schema: {schema:#?}");
    */


    let column_chave_doc: &Series = df_groupby_c.column("Chave do Documento")?;
    let column_lines_efd: &Series = df_groupby_c.column("Linhas EFD")?;
    let column_lines_nfe: &Series = df_groupby_c.column("Linhas NFE")?;
    let column_assignmen: &Series = df_groupby_c.column("Munkres Assignments")?;

    let vec_opt_chave_doc: Vec<Option<&str>>   = column_chave_doc.utf8()?.into_iter().collect();
    let vec_opt_lines_efd: Vec<Option<Series>> = column_lines_efd.list()?.into_iter().collect();
    let vec_opt_lines_nfe: Vec<Option<Series>> = column_lines_nfe.list()?.into_iter().collect();
    let vec_opt_assignmen: Vec<Option<Series>> = column_assignmen.list()?.into_iter().collect();
    
    fn get_vec_vecu64(vec_opt_series: Vec<Option<Series>>) -> Result<Vec<Vec<u64>>, PolarsError> {

        // https://stackoverflow.com/questions/71376935/how-to-get-a-vec-from-polars-series-or-chunkedarray

        let vec: Vec<Vec<u64>> = vec_opt_series
        .into_iter()
        .map(|opt_series| opt_series
            .map( |series| series
                .u64()
                .unwrap()
                .into_iter()
                .map(|opt_u64| opt_u64.unwrap())
                .collect::<Vec<u64>>()
            )
            .unwrap()
        )
        .collect();

        Ok(vec)
    }

    let vec_chave_doc: Vec<&str>     = vec_opt_chave_doc.iter().map(|&opt_str| opt_str.unwrap()).collect();
    let vec_lines_efd: Vec<Vec<u64>> = get_vec_vecu64(vec_opt_lines_efd)?;
    let vec_lines_nfe: Vec<Vec<u64>> = get_vec_vecu64(vec_opt_lines_nfe)?;
    let vec_assignmen: Vec<Vec<u64>> = get_vec_vecu64(vec_opt_assignmen)?;

    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
    let vec_vec_tuples: Vec<Vec<(String, u64, u64)>> = (vec_chave_doc, vec_lines_efd, vec_lines_nfe, vec_assignmen)
        .into_par_iter() // rayon: parallel iterator
        .map(|(chave_doc, lines_efd, lines_nfe, assignmen)| get_vec_tuples(chave_doc, &lines_efd, &lines_nfe, &assignmen))
        .collect();

    fn get_vec_tuples(chave_doc: &str, lines_efd: &[u64], lines_nfe: &[u64], assignments: &[u64]) -> Vec<(String, u64, u64)> {

        let mut chaves_valores_itens: Vec<(String, u64, u64)> = Vec::new();

        for (row, &col) in assignments.iter().enumerate() {

            let opt_line_efd: Option<&u64> = lines_efd.get(row);
            let opt_line_nfe: Option<&u64> = lines_nfe.get(col as usize);
            
            if let (Some(&l_efd), Some(&l_nfe)) = (opt_line_efd, opt_line_nfe) {
                let tuple = (chave_doc.to_string(), l_efd, l_nfe);
                //println!("row: {row} ; tuple: {tuple:?}");
                chaves_valores_itens.push(tuple);
            }
        }

        chaves_valores_itens
    }

    // Transform a vector of tuples into many vectors
    let mut col_chaves: Vec<String> = Vec::new();
    let mut col_lines_efd: Vec<u64> = Vec::new();
    let mut col_lines_nfe: Vec<u64> = Vec::new();

    for vec_tuples in vec_vec_tuples {
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



    let columns = ("Chave do Documento", "Linhas NFE");
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_b = lf_b // Duplicate columns before join()
    .with_column(
        col("Chave da Nota Fiscal Eletrônica : NF Item (Todos)").alias(columns.0),
    );

    let lf_b_solution: LazyFrame = df_correlation.lazy().join(lf_b, common_a, common_b, JoinType::Left)
    .drop_columns(["Linhas NFE"]);

    //let mut df_solution = lf_b_solution.clone().collect()?;
    //write_csv(&mut df_solution, ';', "output_solution.csv")?;




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

    let valor_do_item_da_efd = "Valor Total do Item";
    let valor_do_basecal_nfe = "Valor da Nota Proporcional : NF Item (Todos) SOMA";
    let valor_do_baseicm_nfe = "ICMS: Base de Cálculo : NF Item (Todos) SOMA";

    let valores_iguais_basecal = col(valor_do_item_da_efd).eq(col(valor_do_basecal_nfe));
    let valores_iguais_baseicm = col(valor_do_item_da_efd).eq(col(valor_do_baseicm_nfe));

    let mut dfd_output = lf_c
        .with_column(
            when(valores_iguais_basecal)
            .then(lit("valores iguais: Nota Proporcional"))
            .otherwise(
                when(valores_iguais_baseicm)
                .then(lit("valores iguais: Base de Cálculo do ICMS"))
                .otherwise(lit(""))
            )
            .alias(empty_column1)
        )
        .collect()?;




    //println!("dfd_output:\n{dfd_output}\n");
    write_csv(&mut dfd_output, ';', "output.csv")?;
    write_pqt(&mut dfd_output, "output.parquet")?;

    //let df_parquet: DataFrame = read_pqt("output.parquet")?;

    let dt_local_now: DateTime<Local> = Local::now();
    println!("Data Local: {}", dt_local_now.format("%d/%m/%Y"));
    println!("Tempo de Execução Total: {:?}\n",time.elapsed());

    Ok(())
}
