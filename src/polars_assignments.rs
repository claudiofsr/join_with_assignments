use rayon::prelude::*;
use std::error::Error;

use polars::{
    prelude::*,
    datatypes::DataType,
};

use crate::{
    get_lazyframe_from_csv,
    round_series,
    //round_float64_columns,
    get_opt_vectuples,
    get_option_assignments,
    formatar_chave_eletronica,
    VecTuples,
    args::Arguments,
    coluna,
    Column,
    columns::Extensions,
    Side::{Left, Middle, Right},
};

/// Use Polars to get dataframe after Munkres assignments
///
/// A `DataFrame` is built upon a `Vec<Series>` where the `Series` have the same length.
///
/// [polars-core-version/src/frame/mod.rs]
pub fn get_dataframe_after_assignments(args: &Arguments) -> Result<DataFrame, Box<dyn Error>> {

    println!("Read LazyFrame from CSV files.");
    let lazyframe_a: LazyFrame = get_lazyframe_from_csv(args.file1.clone(), args.delimiter_input_1, Left)?
        .with_row_index(coluna(Left, "count_lines"), Some(0u32));
    let lazyframe_b: LazyFrame = get_lazyframe_from_csv(args.file2.clone(), args.delimiter_input_2, Right)?
        .with_row_index(coluna(Right, "count_lines"), Some(0u32));

    println!("Format the columns to perform comparisons and sum values.");
    let lazyframe_a: LazyFrame = format_fazyframe_a(lazyframe_a)?;
    let lazyframe_b: LazyFrame = format_fazyframe_b(lazyframe_b)?;

    let lazy_groupby_a: LazyFrame = groupby_fazyframe_a(lazyframe_a.clone())?;
    let lazy_groupby_b: LazyFrame = groupby_fazyframe_b(lazyframe_b.clone())?;

    let dataframe_joinned: DataFrame = join_lazyframes(lazy_groupby_a, lazy_groupby_b)?;

    let vec_opt_vec_tuples: Vec<Option<VecTuples>> = get_vec_from_assignments(dataframe_joinned)?;
    let df_correlation: DataFrame = make_df_correlation(vec_opt_vec_tuples)?;

    let lazyframe_c: LazyFrame = join_with_interline_correlations(lazyframe_a, lazyframe_b, df_correlation)?;
    let dfd_output: DataFrame = check_correlation_between_dataframes(lazyframe_c)?;

    /*
    // Add filter to reduce dataframe
    let dfd_output: DataFrame = dfd_output
        .lazy()
        .filter(col("Ano do Período de Apuração").eq(lit(2022)))
        .filter(col("Mês do Período de Apuração").eq(lit(6)))
        .collect()?;
    */

    Ok(dfd_output)
}

/// Formatar colunas a fim de realizar comparações e somas de valores.
fn format_fazyframe_a(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {

    let columns_with_float64: Vec<&str> = vec![
        coluna(Left, "valor_item"),
        coluna(Left, "valor_bc"),
    ];

    let count_lines = coluna(Left, "count_lines");
    let chave = coluna(Left, "chave");
    //let valor_item = coluna(Left, "valor_item");



    /*
    println!("df_a 1: {}", lazyframe.clone().collect()?);
    println!("[chave]: {}", lazyframe.clone().collect()?[chave]);
    println!("[valor_item]: {}", lazyframe.clone().collect()?[valor_item]);

    // Get columns from dataframe
    let valores: Series = lazyframe.clone().collect()?.column(valor_item)?.clone();

    // Get columns with into_iter()
    let vec_opt_valores: Vec<Option<f64>> = valores.f64()?.into_iter().collect();

    let vec_valores: Vec<f64> = vec_opt_valores.into_iter().flatten().filter(|v| *v > 1.0).take(100).collect();
    println!("valores: {:?}\n", vec_valores);
    */



    let lz = lazyframe // Formatar colunas
        .with_column(col(count_lines).cast(DataType::UInt64))
        .with_column(
            col(chave)
            .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::String))
        )
        .with_columns([
            cols(columns_with_float64)
            .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
        ]);

    // Lazy operations don’t execute until we call .collect()?.
    // Using the select method is the recommended way to sort columns in polars.
    let lazyframe: LazyFrame = lz
        .collect()?
        .select(Column::get_columns().get_names(Left))? // sort columns
        .lazy();

    Ok(lazyframe)
}

/// Formatar colunas a fim de realizar comparações e somas de valores.
fn format_fazyframe_b(lazyframe: LazyFrame) -> Result<LazyFrame, Box<dyn Error>> {

    let columns_with_float64: Vec<&str> = vec![
        coluna(Right, "valor_total"),
        coluna(Right, "valor_item"),
        coluna(Right, "valor_bc_icms"),
        coluna(Right, "valor_icms"),
    ];

    let count_lines = coluna(Right, "count_lines");
    let chave = coluna(Right, "chave");
    //let valor_item = coluna(Right, "valor_item");



    /*
    println!("df_b 1: {}", lazyframe.clone().collect()?);
    println!("[chave]: {}", lazyframe.clone().collect()?[chave]);
    println!("[valor_item]: {}", lazyframe.clone().collect()?[valor_item]);

    // Get columns from dataframe
    let valores: Series = lazyframe.clone().collect()?.column(valor_item)?.clone();

    // Get columns with into_iter()
    let vec_opt_valores: Vec<Option<f64>> = valores.f64()?.into_iter().collect();

    let vec_valores: Vec<f64> = vec_opt_valores.into_iter().flatten().filter(|v| *v > 1.0).take(100).collect();
    println!("valores: {:?}\n", vec_valores);
    */



    let lz = lazyframe // Formatar colunas
        .with_column(col(count_lines).cast(DataType::UInt64))
        .with_column(
            col(chave)
            .apply(formatar_chave_eletronica, GetOutput::from_type(DataType::String))
        )
        .with_columns([
            cols(columns_with_float64)
            .apply(|series| round_series(series, 2), GetOutput::from_type(DataType::Float64))
            //all()
            //.apply(|series| round_float64_columns(series, 2), GetOutput::same_type())
        ]);
    
    // Lazy operations don’t execute until we call .collect()?.
    // Using the select method is the recommended way to sort columns in polars.
    let lazyframe: LazyFrame = lz
        .collect()?
        .select(Column::get_columns().get_names(Right))? // sort columns
        .lazy();

    Ok(lazyframe)
}

fn groupby_fazyframe_a(lazyframe: LazyFrame) -> Result<LazyFrame, PolarsError> {

    let count_lines = coluna(Left, "count_lines");
    let chave = coluna(Left, "chave");
    let valor_item = coluna(Left, "valor_item");

    let lf_groupby: LazyFrame = lazyframe
        .filter(
            col(chave).is_not_null()
            .and(col(valor_item).is_not_null())
        )
        .group_by([col(chave)])
        .agg([
            //count(),
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal EFD"),
        ]);

    println!("Group information according to column '{}'", chave);
    println!("groupby_fazyframe_a:\n{}\n", lf_groupby.clone().collect()?);

    Ok(lf_groupby)
}

fn groupby_fazyframe_b(lazyframe: LazyFrame) -> Result<LazyFrame, PolarsError> {

    let count_lines = coluna(Right, "count_lines");
    let chave = coluna(Right, "chave");
    let valor_item = coluna(Right, "valor_item");
    let origem = coluna(Right, "origem");

    let lf_groupby: LazyFrame = lazyframe
        .filter(
            col(chave).is_not_null()
            .and(col(valor_item).is_not_null())
        )
        .filter(
            when(col(origem).eq(lit("NFe")))
            .then(col(valor_item).gt(0))
            .otherwise(true)
        )
        .group_by([col(chave)])
        .agg([
            //count(),
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal NFE"),
        ]);

    println!("Group information according to column '{}'", chave);
    println!("groupby_fazyframe_b:\n{}\n", lf_groupby.clone().collect()?);

    Ok(lf_groupby)
}

fn join_lazyframes(lazyframe_a: LazyFrame, lazyframe_b: LazyFrame) -> Result<DataFrame, PolarsError> {

    let dataframe: DataFrame = lazyframe_a
        .join(lazyframe_b, [col(coluna(Left, "chave"))], [col(coluna(Right, "chave"))], JoinType::Inner.into())
        // An inner join produces a DataFrame that contains only the rows where the join key exists in both DataFrames.
        // https://pola-rs.github.io/polars-book/user-guide/expressions/user-defined-functions/#combining-multiple-column-values
        .with_column(
            // pack to struct to get access to multiple fields in a custom `apply/map`
            // polars-plan-0.26.1/src/dsl/functions.rs ; features = ["dtype-struct"]
            as_struct([
                col("Valores dos Itens da Nota Fiscal EFD"),
                col("Valores dos Itens da Nota Fiscal NFE"),
                ].to_vec())
                .apply(
                    |s| {
                        // Downcast to struct
                        let struct_chunked: &StructChunked = s.struct_()?;

                        // Get the fields as Series
                        let ser_list_efd: &Series = &struct_chunked.field_by_name("Valores dos Itens da Nota Fiscal EFD")?;
                        let ser_list_nfe: &Series = &struct_chunked.field_by_name("Valores dos Itens da Nota Fiscal NFE")?;

                        // Get columns with into_iter()
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

fn get_vec_from_assignments(dataframe: DataFrame) -> Result<Vec<Option<VecTuples>>, PolarsError> {

    // Get columns from dataframe
    let aggregation: &Series = dataframe.column(coluna(Left, "chave"))?;
    let lines_efd: &Series = dataframe.column(coluna(Left, "count_lines"))?;
    let lines_nfe: &Series = dataframe.column(coluna(Right, "count_lines"))?;
    let assignmen: &Series = dataframe.column("Munkres Assignments")?;

    // Get columns with into_iter()
    let vec_opt_aggregation: Vec<Option<&str>> = aggregation.str()?.into_iter().collect();
    let vec_opt_ser_efd: Vec<Option<Series>> = lines_efd.list()?.into_iter().collect();
    let vec_opt_ser_nfe: Vec<Option<Series>> = lines_nfe.list()?.into_iter().collect();
    let vec_opt_ser_asg: Vec<Option<Series>> = assignmen.list()?.into_iter().collect();

    // https://docs.rs/rayon/latest/rayon/iter/struct.MultiZip.html
    // MultiZip is an iterator that zips up a tuple of parallel iterators to produce tuples of their items.
    let vec_opt_vec_tuples: Vec<Option<VecTuples>> = (vec_opt_aggregation, vec_opt_ser_efd, vec_opt_ser_nfe, vec_opt_ser_asg)
        .into_par_iter() // rayon: parallel iterator
        .map(|(opt_aggregation, opt_ser_efd, opt_ser_nfe, opt_ser_asg)| {
            match (opt_aggregation, opt_ser_efd, opt_ser_nfe, opt_ser_asg) {
                (Some(aggregation), Some(ser_efd), Some(ser_nfe), Some(ser_asg)) => get_opt_vectuples(aggregation, ser_efd, ser_nfe, ser_asg),
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
        coluna(Left, "chave") => &col_chaves,
        coluna(Left, "count_lines") => &col_lines_efd,
        coluna(Right, "count_lines") => &col_lines_nfe,
    }?;

    println!("Dataframe with correlations between rows of two tables.");
    println!("df_correlation:\n{df_correlation}\n");
    //write_csv(&mut df_correlation, ';', "output_correlation.csv")?;

    Ok(df_correlation)
}

fn join_with_interline_correlations(lf_a: LazyFrame, lf_b: LazyFrame, df_correlation: DataFrame) -> Result<LazyFrame, PolarsError> {

    let columns = (coluna(Left, "chave"), coluna(Right, "count_lines"));
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_b = lf_b // Duplicate columns before join()
        .with_column(
            col(coluna(Right, "chave")).alias(columns.0),
        );

    let lf_b_solution: LazyFrame = df_correlation.lazy().join(lf_b, common_a, common_b, JoinType::Left.into())
        .drop([coluna(Right, "count_lines")]);

    // add two empty columns to lazyframe
    let lf_a = lf_a
        .with_columns([
            lit(NULL).alias(coluna(Middle, "verificar")).cast(DataType::String),
            lit(NULL).alias(coluna(Middle, "glosar")).cast(DataType::String),
        ]);

    let columns = (coluna(Left, "chave"), coluna(Left, "count_lines"));
    let common_a = [col(columns.0), col(columns.1)];
    let common_b = [col(columns.0), col(columns.1)];

    let lf_c: LazyFrame = lf_a.join(lf_b_solution, common_a, common_b, JoinType::Left.into())
        .drop([coluna(Left, "count_lines")]);

    Ok(lf_c)
}

fn check_correlation_between_dataframes(lazyframe: LazyFrame) -> Result<DataFrame, PolarsError> {

    let delta: f64 = 0.05;
    let chave_is_null: Expr = col(coluna(Right, "chave")).is_null();

    let valor_da_bcal_da_efd: &str = coluna(Left, "valor_bc");                 // "Valor da Base de Cálculo das Contribuições";
    let valor_do_item_da_efd: &str = coluna(Left, "valor_item");               // "Valor Total do Item",

    let coluna_de_verificacao: &str = coluna(Middle, "verificar");             // "Verificação dos Valores: EFD x Docs Fiscais";

    let valor_da_nota_proporcional_nfe: &str = coluna(Right, "valor_item");    // "Valor da Nota Proporcional : NF Item (Todos) SOMA";
    let valor_da_base_calculo_icms_nfe: &str = coluna(Right, "valor_bc_icms"); // "ICMS: Base de Cálculo : NF Item (Todos) SOMA"

    let valores_iguais_base_prop: Expr = (col(valor_da_bcal_da_efd) - col(valor_da_nota_proporcional_nfe)).abs().lt(lit(delta));
    let valores_iguais_base_icms: Expr = (col(valor_da_bcal_da_efd) - col(valor_da_base_calculo_icms_nfe)).abs().lt(lit(delta));
    let valores_iguais_item_prop: Expr = (col(valor_do_item_da_efd) - col(valor_da_nota_proporcional_nfe)).abs().lt(lit(delta));
    let valores_iguais_item_icms: Expr = (col(valor_do_item_da_efd) - col(valor_da_base_calculo_icms_nfe)).abs().lt(lit(delta));

    let dataframe: DataFrame = lazyframe
        .with_column(
            when(chave_is_null)
                .then(lit(NULL))
                .when(valores_iguais_base_prop)
                .then(lit("Base de Cálculo das Contribuições == Nota Proporcional"))
                .when(valores_iguais_base_icms)
                .then(lit("Base de Cálculo das Contribuições == Base de Cálculo do ICMS"))
                .when(valores_iguais_item_prop)
                .then(lit("Valor Total do Item == Nota Proporcional"))
                .when(valores_iguais_item_icms)
                .then(lit("Valor Total do Item == Base de Cálculo do ICMS"))
                .otherwise(lit(NULL))
                .alias(coluna_de_verificacao)
        )
        .collect()?;

    Ok(dataframe)
}

#[cfg(test)]
mod test_assignments {
    use std::env;
    use super::*;
    use crate::{
        round_float64_columns,
        configure_the_environment, glosar_base_de_calculo::LazyFrameExtension,
    };

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// `cargo test -- --show-output get_number_of_rows`
    fn get_number_of_rows() -> Result<(), Box<dyn Error>> {
        configure_the_environment();

        let dataframe_01: DataFrame = df!(
            "strings" => &["aa", "bb", "cc", "dd", "ee","ff"],
            "float64"  => [23.654, 0.319, 10.0049, 89.01999, -3.41501, 52.0766],
            "options"  => [Some(28), Some(300), None, Some(2), Some(-30), None],
        )?;

        println!("original: {dataframe_01}\n");

        let dataframe_02: DataFrame = dataframe_01
            .lazy()
            .with_row_index("count lines", Some(1u32))
            .collect()?;

        println!("with new column: {dataframe_02}\n");

        let new_col: Vec<u32> = dataframe_02
            .column("count lines")?
            .u32()?
            .into_iter()
            .map(|opt_u32| opt_u32.unwrap())
            .collect();

        println!("new_col: {new_col:?}");

        Ok(())
    }

    #[test]
    /**
    `cargo test -- --show-output concat_str_with_nulls`

    <https://github.com/pola-rs/polars/issues/8750>

    Add ignore_nulls for concat_str (#13877)

    geany polars-plan-0.37.0/src/dsl/functions/concat.rs&
    */
    fn concat_str_with_nulls() -> Result<(), Box<dyn Error>> {
        configure_the_environment();

        let dataframe_01: DataFrame = df!(
            "str_1" => [Some("Food"), None, Some("April"),  None],
            "str_2" => [Some("Trick"), Some("Or"), Some("Treat"),  None],
            "str_3" => [None::<&str>, None, None,  None],
            "str_4" => [Some("aa"), Some("bb"), Some("cc"),  None],
        )?;

        println!("dataframe_01: {dataframe_01}\n");

        let mensagem_ignore_nulls_true: Expr = concat_str([
            col("str_1"),
            col("str_2"),
            col("str_3"),
            col("str_4"),
        ], "*", true);

        // Need add .fill_null(lit(""))
        let mensagem_ignore_nulls_false: Expr = concat_str([
            col("str_1").fill_null(lit("")),
            col("str_2").fill_null(lit("")),
            col("str_3").fill_null(lit("")),
            col("str_4").fill_null(lit("")),
        ], "*", false);

        let dataframe_02: DataFrame = dataframe_01
            .lazy()
            .with_columns([
                mensagem_ignore_nulls_true.alias("concat ignore_nulls_true"),
                mensagem_ignore_nulls_false.alias("concat ignore_nulls_false"),
            ])
            .collect()?;

        println!("dataframe02: {dataframe_02}\n");

        let col_a: Series = Series::new("concat ignore_nulls_true", &["Food*Trick*aa", "Or*bb", "April*Treat*cc", ""]);
        let col_b: Series = Series::new("concat ignore_nulls_false", &["Food*Trick**aa", "*Or**bb", "April*Treat**cc", "***"]);

        assert_eq!(dataframe_02.column("concat ignore_nulls_true")?, &col_a);
        assert_eq!(dataframe_02.column("concat ignore_nulls_false")?, &col_b);

        /*
        Output:

        dataframe_01: shape: (4, 4)
        ╭───────┬───────┬───────┬───────╮
        │ str_1 ┆ str_2 ┆ str_3 ┆ str_4 │
        │ ---   ┆ ---   ┆ ---   ┆ ---   │
        │ str   ┆ str   ┆ str   ┆ str   │
        ╞═══════╪═══════╪═══════╪═══════╡
        │ Food  ┆ Trick ┆ null  ┆ aa    │
        │ null  ┆ Or    ┆ null  ┆ bb    │
        │ April ┆ Treat ┆ null  ┆ cc    │
        │ null  ┆ null  ┆ null  ┆ null  │
        ╰───────┴───────┴───────┴───────╯

        dataframe02: shape: (4, 6)
        ╭───────┬───────┬───────┬───────┬──────────────────────────┬───────────────────────────╮
        │ str_1 ┆ str_2 ┆ str_3 ┆ str_4 ┆ concat ignore_nulls_true ┆ concat ignore_nulls_false │
        │ ---   ┆ ---   ┆ ---   ┆ ---   ┆ ---                      ┆ ---                       │
        │ str   ┆ str   ┆ str   ┆ str   ┆ str                      ┆ str                       │
        ╞═══════╪═══════╪═══════╪═══════╪══════════════════════════╪═══════════════════════════╡
        │ Food  ┆ Trick ┆ null  ┆ aa    ┆ Food*Trick*aa            ┆ Food*Trick**aa            │
        │ null  ┆ Or    ┆ null  ┆ bb    ┆ Or*bb                    ┆ *Or**bb                   │
        │ April ┆ Treat ┆ null  ┆ cc    ┆ April*Treat*cc           ┆ April*Treat**cc           │
        │ null  ┆ null  ┆ null  ┆ null  ┆                          ┆ ***                       │
        ╰───────┴───────┴───────┴───────┴──────────────────────────┴───────────────────────────╯
        */

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output filter_even_numbers`
    ///
    /// Add argument to pl.concat_str() to treat Null as empty string
    ///
    /// <https://github.com/pola-rs/polars/issues/3534>
    ///
    /// <https://github.com/pola-rs/polars/issues/8750>
    fn filter_even_numbers() -> Result<(), Box<dyn Error>> {
        configure_the_environment();

        // Column names:
        let glosar: &str = coluna(Middle, "glosar");
        let verificar: &str = coluna(Middle, "verificar");
        let valor_bc: &str = coluna(Left, "valor_bc");

        let dataframe01: DataFrame = df!(
            "integers" => &[1, 2, 3, 4, 5],
            valor_bc   => [23.654, 0.319, 10.0049, 89.01999, -3.41501],
            "options"  => [Some(28), Some(300), None, Some(2), Some(-30)],
        )?;

        println!("dataframe01: {dataframe01}\n");

        let lazyframe: LazyFrame = dataframe01
            .lazy()
            .with_columns([
                lit(NULL).alias(verificar).cast(DataType::String),
                lit(NULL).alias(glosar).cast(DataType::String),
            ]);

        println!("dataframe02: {}\n", lazyframe.clone().collect()?);

        // modulo operation returns the remainder of a division
        // `a % b = a - b * floor(a / b)`
        let modulo: Expr = col("integers") % lit(2);
        let situacao: Expr = modulo.eq(lit(0)); // Even Number

        let mensagem: Expr = concat_str([
            col(glosar),
            lit("Situação 01:"),
            col("integers"),
            lit("is an even"),
            lit("number"),
            lit("&"),
        ], " ", true);

        let lazyframe: LazyFrame = lazyframe
            .with_column(
                when(situacao)
                    .then(mensagem)
                    .otherwise(col(glosar))
                .alias(glosar)
            )
            .format_values();

        let dataframe03: DataFrame = lazyframe.collect()?;

        println!("dataframe03: {dataframe03}\n");

        let series: Series = Series::new(glosar, &[
            None,
            Some("Situação 01: 2 is an even number"),
            None,
            Some("Situação 01: 4 is an even number"),
            None,
        ]);

        assert_eq!(dataframe03.column(glosar)?, &series);

        Ok(())
    }

    // How to apply a function to multiple columns of a polars DataFrame in Rust
    // https://stackoverflow.com/questions/72372821/how-to-apply-a-function-to-multiple-columns-of-a-polars-dataframe-in-rust
    // https://pola-rs.github.io/polars/polars_lazy/index.html

    #[test]
    /// `cargo test -- --show-output apply_a_function_to_multiple_columns`
    fn apply_a_function_to_multiple_columns() -> Result<(), Box<dyn Error>> {
        configure_the_environment();

        let dataframe01: DataFrame = df!(
            "integers"  => &[1, 2, 3, 4, 5, 6],
            "float64 A" => [23.654, 0.319, 10.0049, 89.01999, -3.41501, 52.0766],
            "options"   => [Some(28), Some(300), None, Some(2), Some(-30), None],
            "float64 B" => [9.9999, 0.399, 10.0061, 89.0105, -3.4331, 52.099999],
        )?;

        println!("dataframe01: {dataframe01}\n");

        // let selected: Vec<&str> = vec!["float64 A", "float64 B"];

        // Example 1:
        // Format only the columns with float64
        // input: two columns --> output: two columns

        let lazyframe: LazyFrame = dataframe01
            .lazy()
            .with_columns([
                //cols(selected)
                all()
                .apply(|series|
                    round_float64_columns(series, 2),
                    GetOutput::same_type()
                 )
             ]);

        let dataframe02: DataFrame = lazyframe.clone().collect()?;

        println!("dataframe02: {dataframe02}\n");

        let series_a: Series = Series::new("float64 A", &[23.65, 0.32, 10.00, 89.02, -3.42, 52.08]);
        let series_b: Series = Series::new("float64 B", &[10.00,  0.4, 10.01, 89.01, -3.43, 52.1]);

        assert_eq!(dataframe02.column("float64 A")?, &series_a);
        assert_eq!(dataframe02.column("float64 B")?, &series_b);

        // Example 2:
        // input1: two columns --> output: one new column
        // input2: one column  --> output: one new column

        let lazyframe: LazyFrame = lazyframe
            .with_columns([
                apuracao1("float64 A", "float64 B", "New Column 1"),
                apuracao2("float64 A", "New Column 2"),
                (col("integers") * lit(10) + col("options")).alias("New Column 3"),
             ]);

        println!("dataframe03: {}\n", lazyframe.collect()?);

        Ok(())
    }

    fn apuracao1(name_a: &str, name_b: &str, new: &str) -> Expr {
        (col(name_a) * col(name_b) / lit(100))
        //.over("some_group")
        .alias(new)
    }

    fn apuracao2(name_a: &str, new: &str) -> Expr {
        (lit(10) * col(name_a) - lit(2))
        //.over("some_group")
        .alias(new)
    }

    #[test]
    /// `cargo test -- --show-output read_csv_file`
    fn read_csv_file_v1() -> Result<(), Box<dyn Error>> {

        env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
        env::set_var("POLARS_FMT_MAX_COLS", "60"); // maximum number of columns shown when formatting DataFrames.
        env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
        env::set_var("POLARS_FMT_STR_LEN", "52");  // maximum number of characters printed per string value.

        let delimiter = ';';
        let file = "src/tests/csv_file01";
        let valor_item = "Valor da Nota Proporcional : NF Item (Todos) SOMA";

        // --- with_infer_schema_length --- //
        println!("\n### --- with_infer_schema_length --- ###\n");

        let result_lazyframe: PolarsResult<LazyFrame> = LazyCsvReader::new(file)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(true)
            .with_separator(delimiter as u8)
            .with_quote_char(Some(b'"'))
            .has_header(true)
            //.with_has_header(true)
            .with_ignore_errors(true)
            .with_null_values(None)
            .with_missing_is_null(true)
            .with_infer_schema_length(Some(10))
            .finish();

        let df_a = result_lazyframe?.collect()?;
        println!("df_a: {df_a}\n");

        // Get columns from dataframe
        let valores_a: &Series = df_a.column(valor_item)?;

        // Get columns with into_iter()
        let vec_valores_a: Vec<f64> = valores_a.f64()?.into_iter().flatten().collect();
        println!("valores_a: {:?}\n", vec_valores_a);

       // --- with_schema --- //
       println!("\n### --- with_schema --- ###\n");

        let name_dtype = [
            ("Linhas NFE", DataType::UInt64),
            ("Número da Nota : NF Item (Todos)", DataType::Int64),
            ("Dia da Emissão : NF Item (Todos)", DataType::String),
            ("Código CFOP : NF Item (Todos)", DataType::Int64),
            ("COFINS: Alíquota ad valorem - Atributo : NF Item (Todos)", DataType::Float64),
            ("PIS: Alíquota ad valorem - Atributo : NF Item (Todos)", DataType::Float64),
            ("CST COFINS Descrição : NF Item (Todos)", DataType::String),
            ("CST PIS Descrição : NF Item (Todos)", DataType::String),
            ("Valor Total : NF (Todos) SOMA", DataType::Float64),
            ("Valor da Nota Proporcional : NF Item (Todos) SOMA", DataType::Float64),
            ("IPI: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
            ("ISS: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
        ];

        let mut schema: Schema = Schema::new();
        name_dtype
            .into_iter()
            .for_each(|(name, dtype)| {
                schema.with_column(name.into(), dtype);
            });
        
        let result_lazyframe: PolarsResult<LazyFrame> = LazyCsvReader::new(file)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(false) // use regex
            .with_separator(delimiter as u8)
            .with_quote_char(Some(b'"'))
            .has_header(true)
            //.with_has_header(true)
            .with_ignore_errors(true)
            .with_null_values(None)
            .with_missing_is_null(true)
            .with_schema(Some(Arc::new(schema)))
            .finish();

        let options = StrptimeOptions {
            format: Some("%-d/%-m/%Y".into()),
            strict: false, // If set then polars will return an error if any date parsing fails
            exact: true,   // If polars may parse matches that not contain the whole string e.g. “foo-2021-01-01-bar” could match “2021-01-01”
            cache: true,   // use a cache of unique, converted dates to apply the datetime conversion.
        };

        // Format date
        let lazyframe: LazyFrame = result_lazyframe?
            .with_column(
                col("^(Período|Data|Dia).*$") // regex
                .str()
                .to_date(options)
            );

        let lazyframe_b: LazyFrame = lazyframe
            .with_row_index("Linhas NFE", Some(0u32));

        let df_b = lazyframe_b.clone().collect()?;
        println!("df_b: {df_b}\n");

        // Print column names and their respective types
        // Iterates over the `(&name, &dtype)` pairs in this schema
        lazyframe_b
            .schema()?
            .iter()
            .enumerate()
            .for_each(|(index, (column_name, data_type))|{
                println!("column {:02}: (\"{column_name}\", DataType::{data_type}),", index + 1);
            });
        
        println!();

        // Get columns from dataframe
        let valores_b: &Series = df_b.column(valor_item)?;

        // Get columns with into_iter()
        let vec_valores_b: Vec<f64> = valores_b.f64()?.into_iter().flatten().collect();
        println!("valores_b: {:?}\n", vec_valores_b);

        assert_eq!(vec_valores_a, [3623.56, 7379.51, 6783.56, 106.34, 828.98]);
        assert_eq!(vec_valores_a, vec_valores_b);

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output read_csv_file`
    fn read_csv_file_v2() -> Result<(), Box<dyn Error>> {

        env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
        env::set_var("POLARS_FMT_MAX_COLS", "60"); // maximum number of columns shown when formatting DataFrames.
        env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
        env::set_var("POLARS_FMT_STR_LEN", "52");  // maximum number of characters printed per string value.

        let delimiter = ';';
        let file = "src/tests/csv_file01";
        let valor_item = coluna(Right, "valor_item"); // "Valor da Nota Proporcional : NF Item (Todos) SOMA"

        // --- with_infer_schema_length --- //
        println!("\n### --- with_infer_schema_length --- ###\n");

        let result_lazyframe: PolarsResult<LazyFrame> = LazyCsvReader::new(file)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(true)
            .with_separator(delimiter as u8)
            .with_quote_char(Some(b'"'))
            .has_header(true)
            //.with_has_header(true)
            .with_ignore_errors(true)
            //.with_null_values(Some(NullValues::AllColumns(null_values)))
            .with_null_values(None)
            .with_missing_is_null(true)
            .with_infer_schema_length(Some(10))
            //.with_schema(Some(Arc::new(schema)))
            .finish();

        let df_a = result_lazyframe?.collect()?;
        println!("df_a: {df_a}\n");

        // Get columns from dataframe
        let valores_a: &Series = df_a.column(valor_item)?;

        // Get columns with into_iter()
        let vec_valores_a: Vec<f64> = valores_a.f64()?.into_iter().flatten().collect();
        println!("valores_a: {:?}\n", vec_valores_a);

        // --- with_schema --- //
        println!("\n### --- with_schema --- ###\n");

        let lazyframe_b: LazyFrame = get_lazyframe_from_csv(Some(file.into()), Some(delimiter), Right)?
            .with_row_index(coluna(Right, "count_lines"), Some(0u32));

        let df_b = lazyframe_b.collect()?;

        // Get columns from dataframe
        let valores_b: &Series = df_b.column(valor_item)?;

        // Get columns with into_iter()
        let vec_valores_b: Vec<f64> = valores_b.f64()?.into_iter().flatten().collect();
        println!("valores_b: {:?}\n", vec_valores_b);

        assert_eq!(vec_valores_a, [3623.56, 7379.51, 6783.56, 106.34, 828.98]);
        assert_eq!(vec_valores_a, vec_valores_b);

        Ok(())
    }
}
