use polars::prelude::*;
// use rayon::prelude::*; // For parallel processing of rows

use crate::{
    AllCorrelations, DataFrameExtension, JoinResult, LazyFrameExtension,
    Side::{Left, Middle, Right},
    args::Arguments,
    coluna, formatar_ncm_expr, get_lazyframe_from_csv, get_opt_vectuples, get_option_assignments,
    get_output_as_uint64, retain_only_digits,
};

/// Use Polars to get dataframe after Munkres assignments
///
/// A `DataFrame` is built upon a `Vec<Series>` where the `Series` have the same length.
///
/// [polars-core-version/src/frame/mod.rs]
pub fn get_dataframe_after_assignments(args: &Arguments) -> JoinResult<DataFrame> {
    let count_lines_left = coluna(Left, "count_lines");
    let count_lines_right = coluna(Right, "count_lines");

    println!("Read LazyFrame from CSV files.");
    let lazyframe_a: LazyFrame =
        get_lazyframe_from_csv(args.file1.clone(), args.delimiter_input_1, Left)?
            .with_row_index(count_lines_left, Some(0u32));
    let lazyframe_b: LazyFrame =
        get_lazyframe_from_csv(args.file2.clone(), args.delimiter_input_2, Right)?
            .with_row_index(count_lines_right, Some(0u32));

    println!("Format the columns to perform comparisons and sum values.\n");
    let lazyframe_a: LazyFrame = format_fazyframe_a(lazyframe_a)?;
    let lazyframe_b: LazyFrame = format_fazyframe_b(lazyframe_b)?;

    let lazy_groupby_a: LazyFrame = groupby_fazyframe_a(lazyframe_a.clone())?;
    let lazy_groupby_b: LazyFrame = groupby_fazyframe_b(lazyframe_b.clone())?;

    let dataframe_joinned: DataFrame = join_lazyframes(lazy_groupby_a.clone(), lazy_groupby_b)?;

    let all_correlations: AllCorrelations = get_vec_from_assignments(&dataframe_joinned)?;
    let df_correlation: DataFrame = make_df_correlation(all_correlations)?;

    let lazyframe_c: LazyFrame =
        join_with_interline_correlations(lazyframe_a, lazyframe_b, df_correlation)?;

    let df_final: DataFrame = check_correlation_between_dataframes(lazyframe_c)?;

    println!("df_final: {df_final}\n");

    /*
    // Add filter to reduce dataframe
    let df_filtered: DataFrame = df_final
        .lazy()
        .filter(col("Ano do Período de Apuração").eq(lit(2022)))
        .filter(col("Mês do Período de Apuração").eq(lit(6)))
        .collect()?;
    */

    Ok(df_final.sort_by_columns(None)?)
}

/// Formatar colunas a fim de realizar comparações e somas de valores.
fn format_fazyframe_a(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let count_lines = coluna(Left, "count_lines");
    let chave = coluna(Left, "chave");
    let ncm = coluna(Left, "ncm");
    //let valor_item = coluna(Left, "valor_item");

    /*
    println!("df_a 1: {}", lazyframe.clone().collect()?);
    println!("[chave]: {}", lazyframe.clone().collect()?[chave]);
    println!("[valor_item]: {}", lazyframe.clone().collect()?[valor_item]);

    // Get columns from dataframe
    let valores: Series = lazyframe.clone().collect()?.column(valor_item)?.clone();

    // Get columns with into_iter()
    let vec_opt_valores: Vec<Option<f64>> = valores.f64()?.into_iter().collect();

    let vec_valores: Vec<f64> = vec_opt_valores.into_iter()
    .flatten().filter(|v| *v > 1.0).take(100).collect();

    println!("valores: {:?}\n", vec_valores);
    */

    let lz = lazyframe // Formatar colunas
        .with_column(col(count_lines).cast(DataType::UInt64))
        .with_column(retain_only_digits(chave))
        .with_column(formatar_ncm_expr(ncm))
        .round_float_columns(2);

    // Lazy operations don’t execute until we call .collect()?.
    Ok(lz.collect()?.lazy())
}

/// Formatar colunas a fim de realizar comparações e somas de valores.
fn format_fazyframe_b(lazyframe: LazyFrame) -> JoinResult<LazyFrame> {
    let count_lines = coluna(Right, "count_lines");
    let chave = coluna(Right, "chave");
    let ncm = coluna(Right, "ncm");
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
        .with_column(retain_only_digits(chave))
        .with_column(formatar_ncm_expr(ncm))
        .round_float_columns(2);

    // Lazy operations don’t execute until we call .collect()?.
    Ok(lz.collect()?.lazy())
}

/// Análise de Chaves (NFe, CTe) ou da união de (CNPJ + Num Doc Fiscal)
/// de arquivos da EFD escriturado pelo contribuinte.
fn groupby_fazyframe_a(lazyframe: LazyFrame) -> PolarsResult<LazyFrame> {
    let periodo_de_apuracao: &str = coluna(Left, "pa");
    let chave = coluna(Left, "chave");
    let count_lines = coluna(Left, "count_lines");
    let valor_item = coluna(Left, "valor_item");
    let period_count = "Nº de Períodos";

    let lf_groupby_chave_efd: LazyFrame = lazyframe
        .clone()
        .select([
            col(periodo_de_apuracao),
            col(chave),
            col(count_lines),
            col(valor_item),
        ])
        .filter(col(periodo_de_apuracao).is_not_null())
        .filter(col(chave).is_not_null())
        .filter(col(count_lines).is_not_null())
        .filter(col(valor_item).is_not_null())
        // Obter possível escrituração na EFD da mesma chave em múltiplos períodos.
        // Agrupar por col(periodo_de_apuracao) e col(chave).
        .group_by([col(periodo_de_apuracao), col(chave)])
        .agg([
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal EFD"),
        ]);

    println!("Group information according to column '{periodo_de_apuracao}' and '{chave}'");
    println!(
        "groupby_fazyframe_a (informações da EFD):\n{}\n",
        lf_groupby_chave_efd.clone().collect()?
    );

    println!(
        "groupby_fazyframe_a (chaves utilizadas em múltiplos períodos):\n{}\n",
        lf_groupby_chave_efd
            .clone()
            .group_by([col(chave)])
            .agg([
                // Count how many unique accounting periods each key appears in
                col(periodo_de_apuracao)
                    .unique()
                    .count()
                    .alias(period_count),
                col("Valores dos Itens da Nota Fiscal EFD").explode(),
                col("Valores dos Itens da Nota Fiscal EFD")
                    .explode()
                    .sum()
                    .alias("Soma dos Valores dos Itens"),
            ])
            .filter(col(period_count).gt(1))
            .collect()?
    );

    Ok(lf_groupby_chave_efd)
}

/*
/// Análise de Chaves (NFe, CTe) ou da união de (CNPJ + Num Doc Fiscal)
/// de arquivos da EFD escriturado pelo contribuinte.
fn groupby_fazyframe_a(lazyframe: LazyFrame) -> PolarsResult<LazyFrame> {
    let chave = coluna(Left, "chave");
    let periodo_de_apuracao: &str = coluna(Left, "pa");
    let period_count = "Nº de Períodos";

    let lf_from_chave = get_lazyframe_from_chave_efd(&lazyframe)?;
    let lf_from_cnpj = get_lazyframe_from_cnpj_efd(&lazyframe)?;

    // União vertical dos dois LazyFrames
    let concatenated_lf = concat(
        &[lf_from_chave, lf_from_cnpj],
        UnionArgs::default(), // Use default arguments for union behavior
    )?;

    println!("Group information according to column '{periodo_de_apuracao}' and '{chave}'");
    println!(
        "groupby_fazyframe_a (informações da EFD):\n{}\n",
        concatenated_lf.clone().collect()?
    );

    println!(
        "groupby_fazyframe_a (chaves utilizadas em múltiplos períodos):\n{}\n",
        concatenated_lf
            .clone()
            .group_by([col(chave)])
            .agg([
                // Count how many unique accounting periods each key appears in
                col(periodo_de_apuracao)
                    .unique()
                    .count()
                    .alias(period_count),
                col("Valores dos Itens da Nota Fiscal EFD").explode(),
                col("Valores dos Itens da Nota Fiscal EFD")
                    .explode()
                    .sum()
                    .alias("Soma dos Valores dos Itens"),
            ])
            .filter(col(period_count).gt(1))
            .collect()?
    );

    Ok(concatenated_lf)
}

fn get_lazyframe_from_chave_efd(lazyframe: &LazyFrame) -> PolarsResult<LazyFrame> {
    let periodo_de_apuracao: &str = coluna(Left, "pa");
    let chave = coluna(Left, "chave");
    let count_lines = coluna(Left, "count_lines");
    let valor_item = coluna(Left, "valor_item");

    let lf_groupby_chave_efd: LazyFrame = lazyframe
        .clone()
        .select([
            col(periodo_de_apuracao),
            col(chave),
            col(valor_item),
            col(count_lines),
        ])
        .filter(col(periodo_de_apuracao).is_not_null())
        .filter(col(chave).is_not_null())
        .filter(col(valor_item).is_not_null())
        // Obter possível escrituração na EFD da mesma chave em múltiplos períodos.
        // Agrupar por col(periodo_de_apuracao) e col(chave).
        .group_by([col(periodo_de_apuracao), col(chave)])
        .agg([
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal EFD"),
        ]);

    println!(
        "lazyframe_from_chave_efd:\n{}\n",
        lf_groupby_chave_efd.clone().collect()?
    );

    Ok(lf_groupby_chave_efd)
}

fn get_lazyframe_from_cnpj_efd(lazyframe: &LazyFrame) -> PolarsResult<LazyFrame> {
    let count_lines = coluna(Left, "count_lines");
    let chave = coluna(Left, "chave");
    let valor_item = coluna(Left, "valor_item");
    let periodo_de_apuracao: &str = coluna(Left, "pa");
    let registro: &str = coluna(Left, "registro");

    let cnpj_particip = coluna(Left, "cnpj_particip");
    let num_doc = coluna(Left, "num_doc");

    // Expressão regular para remover caracteres não numéricos
    // [^0-9] irá reter qualquer caractere que não seja um dígito (0-9)
    let pattern: Expr = lit(r"[^0-9]"); // regex

    // 1. Limpa o CNPJ
    let cnpj_particip_numerico: Expr = col(cnpj_particip)
        .str()
        .replace_all(pattern, lit(""), false)
        .alias("cnpj_particip_numerico");

    // 2. Formata o num_doc
    // Transformação para num_doc: i64 para String com zfill
    let num_doc_formatado: Expr = col(num_doc)
        .cast(DataType::String) // Primeiro, converte i64 para String
        .str() // Acessa os métodos de string
        .zfill(lit(9)) // Preenche com zeros à esquerda até 9 dígitos
        .alias("num_doc_formatado"); // Alias para a nova coluna formatada

    // 3. Cria a nova coluna combinada
    let chave_cnpj_numdoc: Expr =
        (col("cnpj_particip_numerico") + col("num_doc_formatado")).alias(chave);

    let lf_groupby_cnpj_efd: LazyFrame = lazyframe
        .clone()
        //.filter(col(chave).is_null())
        .select([
            col(periodo_de_apuracao),
            col(registro),
            col(cnpj_particip),
            col(num_doc),
            col(valor_item),
            col(count_lines),
        ])
        .filter(col(periodo_de_apuracao).is_not_null())
        .filter(col(registro).is_not_null())
        .filter(col(cnpj_particip).is_not_null())
        .filter(col(num_doc).is_not_null())
        .filter(col(valor_item).is_not_null())
        .with_columns([
            cnpj_particip_numerico, // Adiciona a coluna cnpj limpo
            num_doc_formatado,      // Adiciona a coluna de número formatada
        ])
        .with_column(chave_cnpj_numdoc) // Adiciona a coluna combinada
        .group_by([
            col(periodo_de_apuracao),
            //col(registro),
            col(cnpj_particip),
            col(num_doc),
            col(chave),
        ])
        .agg([
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal EFD"),
        ]);

    println!(
        "lazyframe_from_cnpj_efd:\n{}\n",
        lf_groupby_cnpj_efd.clone().collect()?
    );

    lf_groupby_cnpj_efd.drop_columns(&[registro, cnpj_particip, num_doc])
}
*/

/// Análise de Chaves (NFe, CTe) de arquivos da RFB que podem ser consultados
/// em `www.nfe.fazenda.gov.br` ou em `www.cte.fazenda.gov.br`.
fn groupby_fazyframe_b(lazyframe: LazyFrame) -> PolarsResult<LazyFrame> {
    let chave = coluna(Right, "chave");
    let origem = coluna(Right, "origem");
    let count_lines = coluna(Right, "count_lines");
    let valor_item = coluna(Right, "valor_item");

    let pattern: Expr = lit(r"(?i)NFe"); // regex
    let is_nfe: Expr = col(origem).str().contains(pattern, false);

    let lf_groupby_chave_nfe: LazyFrame = lazyframe
        .select([col(chave), col(origem), col(count_lines), col(valor_item)])
        .filter(col(chave).is_not_null())
        .filter(col(count_lines).is_not_null())
        .filter(col(valor_item).is_not_null())
        .filter(when(is_nfe).then(col(valor_item).gt(0)).otherwise(true))
        .group_by([col(chave)])
        .agg([
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal NFE"),
        ]);

    println!("Group information according to column '{chave}'");
    println!(
        "groupby_fazyframe_b (informações da RFB):\n{}\n",
        lf_groupby_chave_nfe.clone().collect()?
    );

    Ok(lf_groupby_chave_nfe)
}

/*
/// Análise de Chaves (NFe, CTe) de arquivos da RFB que podem ser consultados
/// em `www.nfe.fazenda.gov.br` ou em `www.cte.fazenda.gov.br`.
fn groupby_fazyframe_b(lazyframe: LazyFrame) -> PolarsResult<LazyFrame> {
    let chave = coluna(Right, "chave");
    let origem = coluna(Right, "origem");
    let count_lines = coluna(Right, "count_lines");
    let valor_item = coluna(Right, "valor_item");

    let pattern: Expr = lit(r"(?i)NFe"); // regex
    let is_nfe: Expr = col(origem).str().contains(pattern, false);

    let lf_groupby_chave_nfe: LazyFrame = lazyframe
        .select([col(chave), col(origem), col(valor_item), col(count_lines)])
        .filter(col(chave).is_not_null())
        .filter(col(valor_item).is_not_null())
        .filter(when(is_nfe).then(col(valor_item).gt(0)).otherwise(true))
        .group_by([col(chave)])
        .agg([
            col(count_lines),
            col(valor_item).alias("Valores dos Itens da Nota Fiscal NFE"),
        ]);

    println!(
        "lf_groupby_chave_nfe:\n{}\n",
        lf_groupby_chave_nfe.clone().collect()?
    );

    let chave_len = 44; // chave de 44 dígitos
    let cnpj: Expr = col(chave).str().slice(lit(6), lit(14)); // Pegar 14 dígitos a partir do 7º (índice 6)
    let num: Expr = col(chave).str().slice(lit(25), lit(9)); // Pegar 9 dígitos a partir do 26º (índice 25)

    let lf_groupby_cnpj_nfe = lf_groupby_chave_nfe
        .clone()
        .filter(col(chave).str().len_bytes().eq(chave_len))
        .with_column(
            concat_list([cnpj, num])?
                .list()
                .join(lit(""), true)
                //.alias("Chave CNPJ Emitente + NumDoc"),
                .alias(chave),
        );

    println!(
        "lf_groupby_cnpj_nfe: {}\n",
        lf_groupby_cnpj_nfe.clone().collect()?
    );

    // União vertical dos dois LazyFrames
    let concatenated_lf = concat(
        &[lf_groupby_chave_nfe, lf_groupby_cnpj_nfe],
        UnionArgs::default(), // Use default arguments for union behavior
    )?;

    println!("Group information according to column '{chave}'");
    println!(
        "groupby_fazyframe_b (informações da RFB):\n{}\n",
        concatenated_lf.clone().collect()?
    );

    Ok(concatenated_lf)
}
*/

/// Joins two LazyFrames, applies a custom UDF for Munkres assignments, and collects the result into a DataFrame.
///
/// This function performs an inner join on `lazyframe_a` and `lazyframe_b` based on a common "chave" (key) column.
/// After the join, it calculates Munkres assignments between list-type columns
/// "Valores dos Itens da Nota Fiscal EFD" and "Valores dos Itens da Nota Fiscal NFE"
/// and adds the results as a new column "Munkres Assignments".
///
/// # Arguments
/// * `lazyframe_a` - The left LazyFrame.
/// * `lazyframe_b` - The right LazyFrame.
///
/// # Returns
/// A `Result` containing the joined and processed DataFrame or a `PolarsError` if any operation fails.
fn join_lazyframes(lazyframe_a: LazyFrame, lazyframe_b: LazyFrame) -> PolarsResult<DataFrame> {
    let chave_efd: &str = coluna(Left, "chave");
    let chave_nfe: &str = coluna(Right, "chave");

    let dataframe: DataFrame = lazyframe_a
        .join(
            lazyframe_b,
            [col(chave_efd)], // Join key from the left DataFrame
            [col(chave_nfe)], // Join key from the right DataFrame
            // Retornar apenas as linhas que têm correspondências em AMBOS os LazyFrames
            JoinType::Inner.into(),
        )
        .with_column(apply_munkres_assignments(
            "Valores dos Itens da Nota Fiscal EFD",
            "Valores dos Itens da Nota Fiscal NFE",
            "Munkres Assignments",
        )?)
        .collect()?;

    println!(
        "dataframe_joinned = lazyframe_a.join(lazyframe_b, [...], JoinType::Inner)\n{dataframe}\n"
    );

    Ok(dataframe)
}

/// Aplica a lógica de "Munkres Assignments" entre duas colunas de Series List.
/// Retorna uma expressão que pode ser usada em `with_column`.
fn apply_munkres_assignments(
    column_name_efd: &str,
    column_name_nfe: &str,
    output_alias: &str,
) -> PolarsResult<Expr> {
    // Clone as strings para que a closure possa possuí-las.
    // Isso garante que elas estarão disponíveis quando a closure for executada,
    // mesmo que 'apply_munkres_assignments' já tenha retornado.
    let col_efd_owned = column_name_efd.to_string();
    let col_nfe_owned = column_name_nfe.to_string();
    let output_alias_owned = output_alias.to_string();

    Ok(
        as_struct([col(column_name_efd), col(column_name_nfe)].to_vec())
            .apply(
                // Use 'move' para transferir a posse das strings clonadas para a closure.
                move |col: Column| -> PolarsResult<Column> {
                    // Downcast to struct
                    let struct_chunked: &StructChunked = col.struct_()?;

                    // Get the individual Series (columns) from the struct by their names.
                    let ser_efd: Series = struct_chunked.field_by_name(&col_efd_owned)?;
                    let ser_nfe: Series = struct_chunked.field_by_name(&col_nfe_owned)?;

                    let list_efd = ser_efd.list()?;
                    let list_nfe = ser_nfe.list()?;

                    let vec_series: Vec<Option<Series>> = list_efd
                        .into_iter()
                        .zip(list_nfe)
                        .map(
                            |(opt_ser_efd, opt_ser_nfe)| match (opt_ser_efd, opt_ser_nfe) {
                                (Some(ser_efd), Some(ser_nfe)) => {
                                    // If both Series are present, calculate Munkres assignments.
                                    get_option_assignments(&ser_efd, &ser_nfe)
                                }
                                _ => None,
                            },
                        )
                        .collect();

                    // Create a new Series from the calculated Munkres assignments.
                    let new_series = Series::new("New".into(), vec_series);
                    Ok(new_series.into_column())
                },
                // Define the output data type for the new column.
                // GetOutput::from_type(DataType::UInt64),
                get_output_as_uint64,
            )
            .alias(&output_alias_owned),
    )
}

/**
Retrieves correlated line tuples from a DataFrame containing assignments.

This function processes a DataFrame where each row potentially represents a key
and associated lists of 'lines_efd', 'lines_nfe', and 'Munkres Assignments'.

It aims to correlate lines between EFD and NFe based on the Munkres assignments.
*/
pub fn get_vec_from_assignments(dataframe: &DataFrame) -> PolarsResult<AllCorrelations> {
    // Define column names using the helper function.
    let chave_col_name = coluna(Left, "chave");
    let count_lines_efd_col_name = coluna(Left, "count_lines");
    let count_lines_nfe_col_name = coluna(Right, "count_lines");
    let assignments_col_name = "Munkres Assignments";

    // Extract Series for each column from the DataFrame.
    // The `?` operator propagates any PolarsError if a column is not found or has an incorrect type.
    let aggregation_str = dataframe.column(chave_col_name)?.str()?; // StringChunked iterator
    let lines_efd_list = dataframe.column(count_lines_efd_col_name)?.list()?; // ListChunked iterator
    let lines_nfe_list = dataframe.column(count_lines_nfe_col_name)?.list()?; // ListChunked iterator
    let assignmen_list = dataframe.column(assignments_col_name)?.list()?; // ListChunked iterator

    /*
    // Collect the iterators into Vecs first.
    // This makes them concrete types that Rayon can parallelize easily.
    // This assumes the collected Vecs fit in memory.
    let opt_keys: Vec<Option<&str>> = aggregation_str.into_iter().collect();
    let opt_efd_series_vec: Vec<Option<Series>> = lines_efd_list.into_iter().collect();
    let opt_nfe_series_vec: Vec<Option<Series>> = lines_nfe_list.into_iter().collect();
    let opt_asg_series_vec: Vec<Option<Series>> = assignmen_list.into_iter().collect();

    // Parallel iteration over the zipped Series.
    // Using Rayon's MultiZip to iterate in parallel over the Options yielded by Polars Series iterators.
    let all_correlations: Vec<Option<CorrelatedLineTuples>> = (
        opt_keys,
        opt_efd_series_vec,
        opt_nfe_series_vec,
        opt_asg_series_vec,
    )
        .into_par_iter() // rayon: parallel iterator enables parallel processing of each row
        .map(|(opt_key, opt_efd_ser, opt_nfe_ser, opt_asg_ser)| {
            // Delegate the row-wise processing to a separate function for clarity.
            // This function handles the conversion of Series to Vec<u64> and the correlation logic.
            get_opt_vectuples(opt_key, opt_efd_ser, opt_nfe_ser, opt_asg_ser)
        })
        .collect();
    */

    // Zip iterators from the Series. This performs a row-wise, sequential iteration.
    // Each `.into_iter()` on a Polars ChunkedArray returns a boxed iterator (Box<dyn PolarsIterator<Item = Option<T>>>).
    // The `zip` method is called on these iterators, creating nested tuples for each row.
    let all_correlations: AllCorrelations = aggregation_str
        .into_iter() // Starts with `Box<dyn PolarsIterator<Item = Option<&str>>>`
        .zip(lines_efd_list) // Zips with `Box<dyn PolarsIterator<Item = Option<Series>>>`
        .zip(lines_nfe_list) // Zips with another `Box<dyn PolarsIterator<Item = Option<Series>>>`
        .zip(assignmen_list) // Zips with the last `Box<dyn PolarsIterator<Item = Option<Series>>>`
        .map(|(((opt_key, opt_efd_ser), opt_nfe_ser), opt_asg_ser)| {
            // For each row, call the helper function to process the optional Series data.
            // Note: `get_opt_vectuples` from the previous context is assumed to be
            // `get_opt_correlated_tuples_for_row` in the refactored versions.
            get_opt_vectuples(opt_key, opt_efd_ser, opt_nfe_ser, opt_asg_ser)
        })
        .collect();

    Ok(all_correlations)
}

/// Creates a Polars DataFrame from a collection of optional vectors of correlated lines.
///
/// This function flattens the input structure and organizes the correlated data
/// into three columns: 'chave', 'efd_line_number', and 'nfe_line_number'.
///
/// # Arguments
///
/// * `all_correlations` - A collection (using the `AllCorrelations` type alias)
///   where each element is an `Option` containing a vector of `CorrelatedLines`.
///   This allows handling cases where some groups of correlations might be absent.
///
/// # Returns
///
/// A `PolarsResult<DataFrame>` containing the resulting DataFrame if successful,
/// or a PolarsError if DataFrame creation fails.
fn make_df_correlation(all_correlations: AllCorrelations) -> PolarsResult<DataFrame> {
    let chave = coluna(Left, "chave");
    let efd_line_number = coluna(Left, "count_lines");
    let nfe_line_number = coluna(Right, "count_lines");

    // Pre-allocate vectors with a reasonable capacity to reduce reallocations.
    // Correctly estimate the total number of individual CorrelatedLines.
    let estimated_total_correlations: usize = all_correlations
        .iter()
        .flatten() // Filters out None and unwraps Option<&Vec> to &Vec
        .map(|vec_correlated_lines| vec_correlated_lines.len()) // Gets the length of each inner Vec
        .sum(); // Sums up all lengths

    // Transform a vector of tuples into many vectors
    let mut col_chaves: Vec<String> = Vec::with_capacity(estimated_total_correlations);
    let mut col_lines_efd: Vec<u64> = Vec::with_capacity(estimated_total_correlations);
    let mut col_lines_nfe: Vec<u64> = Vec::with_capacity(estimated_total_correlations);

    // Iterate efficiently, flattening the `Option` and `Vec` structures.
    for correlated_lines_vec in all_correlations.into_iter().flatten() {
        for correlated_lines in correlated_lines_vec {
            col_chaves.push(correlated_lines.chave);
            col_lines_efd.push(correlated_lines.line_efd);
            col_lines_nfe.push(correlated_lines.line_nfe);
        }
    }

    // Create the DataFrame using the df! macro with correct column names.
    let df_correlation: DataFrame = df! {
        chave => &col_chaves,
        efd_line_number => &col_lines_efd,
        nfe_line_number => &col_lines_nfe,
    }?;

    println!("Dataframe with correlations between rows of two tables.");
    println!("df_correlation:\n{df_correlation}\n");
    //write_csv(&mut df_correlation, ';', "output_correlation.csv")?;

    Ok(df_correlation)
}

fn join_with_interline_correlations(
    lf_a: LazyFrame,
    lf_b: LazyFrame,
    df_correlation: DataFrame,
) -> PolarsResult<LazyFrame> {
    let chave_a: &str = coluna(Left, "chave");
    let chave_b: &str = coluna(Right, "chave");

    let count_lines_a: &str = coluna(Left, "count_lines");
    let count_lines_b: &str = coluna(Right, "count_lines");

    // Com JoinType::Left, a coluna do lado esquerdo (chave_a de df_correlation) é mantida,
    // e a coluna correspondente do lado direito (chave_b de lf_b) é descartada.

    // First join: correlation data with lf_b
    let lf_b_with_correlation: LazyFrame = df_correlation
        .lazy()
        .join(
            // Duplicate columns before join()
            lf_b.with_column(col(chave_b).alias(chave_a)),
            [col(chave_a), col(count_lines_b)], // Join key(s) for the left side (df_correlation)
            [col(chave_a), col(count_lines_b)], // Join key(s) for the right side (lf_b)
            JoinType::Left.into(),
        )
        .drop_columns(&[count_lines_b])?;

    // Add two empty string columns to lf_a
    let lf_a_prepared = lf_a.with_columns([
        lit(NULL)
            .alias(coluna(Middle, "verificar"))
            .cast(DataType::String),
        lit(NULL)
            .alias(coluna(Middle, "glosar"))
            .cast(DataType::String),
    ]);

    // Second join: prepared lf_a with the result of the first join
    // We join on `chave_a` and `count_lines_a` from both sides.
    let final_lf: LazyFrame = lf_a_prepared
        .join(
            lf_b_with_correlation,
            [col(chave_a), col(count_lines_a)],
            [col(chave_a), col(count_lines_a)],
            JoinType::Left.into(),
        )
        .drop_columns(&[count_lines_a])?;

    Ok(final_lf)
}

fn check_correlation_between_dataframes(lazyframe: LazyFrame) -> PolarsResult<DataFrame> {
    let delta: f64 = 0.05;
    let chave_is_null: Expr = col(coluna(Right, "chave")).is_null();

    let valor_da_bcal_da_efd: &str = coluna(Left, "valor_bc"); // "Valor da Base de Cálculo das Contribuições";
    let valor_do_item_da_efd: &str = coluna(Left, "valor_item"); // "Valor Total do Item",

    let coluna_de_verificacao: &str = coluna(Middle, "verificar"); // "Verificação dos Valores: EFD x Docs Fiscais";

    let valor_da_nota_proporcional_nfe: &str = coluna(Right, "valor_item"); // "Valor da Nota Proporcional : NF Item (Todos) SOMA";
    let valor_da_base_calculo_icms_nfe: &str = coluna(Right, "valor_bc_icms"); // "ICMS: Base de Cálculo : NF Item (Todos) SOMA"

    let valores_iguais_base_prop: Expr = (col(valor_da_bcal_da_efd)
        - col(valor_da_nota_proporcional_nfe))
    .abs()
    .lt(lit(delta));
    let valores_iguais_base_icms: Expr = (col(valor_da_bcal_da_efd)
        - col(valor_da_base_calculo_icms_nfe))
    .abs()
    .lt(lit(delta));
    let valores_iguais_item_prop: Expr = (col(valor_do_item_da_efd)
        - col(valor_da_nota_proporcional_nfe))
    .abs()
    .lt(lit(delta));
    let valores_iguais_item_icms: Expr = (col(valor_do_item_da_efd)
        - col(valor_da_base_calculo_icms_nfe))
    .abs()
    .lt(lit(delta));

    let dataframe: DataFrame = lazyframe
        .with_column(
            when(chave_is_null)
                .then(lit(NULL))
                .when(valores_iguais_base_prop)
                .then(lit(
                    "Base de Cálculo das Contribuições == Nota Proporcional",
                ))
                .when(valores_iguais_base_icms)
                .then(lit(
                    "Base de Cálculo das Contribuições == Base de Cálculo do ICMS",
                ))
                .when(valores_iguais_item_prop)
                .then(lit("Valor Total do Item == Nota Proporcional"))
                .when(valores_iguais_item_icms)
                .then(lit("Valor Total do Item == Base de Cálculo do ICMS"))
                .otherwise(lit(NULL))
                .alias(coluna_de_verificacao),
        )
        .collect()?;

    Ok(dataframe)
}

#[cfg(test)]
mod test_assignments {
    use super::*;
    use crate::{
        CorrelatedLines, ExprExtension, LazyFrameExtension, Side, apply_custom_schema_rules,
        configure_the_environment,
    };
    use std::{collections::HashMap, env};

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// `cargo test -- --show-output make_df_correlation_basic`
    fn make_df_correlation_basic() -> PolarsResult<()> {
        let chave = coluna(Left, "chave");
        let efd_line_number = coluna(Left, "count_lines");
        let nfe_line_number = coluna(Right, "count_lines");

        let correlations1 = Some(vec![
            CorrelatedLines {
                chave: "itemA".to_string(),
                line_efd: 1,
                line_nfe: 101,
            },
            CorrelatedLines {
                chave: "itemB".to_string(),
                line_efd: 2,
                line_nfe: 102,
            },
        ]);
        let correlations2 = None;
        let correlations3 = Some(vec![CorrelatedLines {
            chave: "itemA".to_string(),
            line_efd: 3,
            line_nfe: 103,
        }]);

        let all_correlations = vec![correlations1, correlations2, correlations3];
        let df = make_df_correlation(all_correlations)?;

        assert_eq!(df.height(), 3);
        // Check column names and values using the new names
        assert_eq!(df.column(chave)?.str()?.get(0), Some("itemA"));
        assert_eq!(df.column(efd_line_number)?.u64()?.get(1), Some(2));
        assert_eq!(df.column(nfe_line_number)?.u64()?.get(2), Some(103));

        let chave_col = Column::new(chave.into(), ["itemA", "itemB", "itemA"]);
        assert_eq!(df.column(chave)?, &chave_col);

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output get_number_of_rows`
    fn get_number_of_rows() -> JoinResult<()> {
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
    fn concat_str_with_nulls() -> JoinResult<()> {
        configure_the_environment();

        let dataframe_01: DataFrame = df!(
            "str_1" => [Some("Food"), None, Some("April"),  None],
            "str_2" => [Some("Trick"), Some("Or"), Some("Treat"),  None],
            "str_3" => [None::<&str>, None, None,  None],
            "str_4" => [Some("aa"), Some("bb"), Some("cc"),  None],
        )?;

        println!("dataframe_01: {dataframe_01}\n");

        let mensagem_ignore_nulls_true: Expr = concat_str(
            [col("str_1"), col("str_2"), col("str_3"), col("str_4")],
            "*",
            true,
        );

        // Need add .fill_null(lit(""))
        let mensagem_ignore_nulls_false: Expr = concat_str(
            [
                col("str_1").fill_null(lit("")),
                col("str_2").fill_null(lit("")),
                col("str_3").fill_null(lit("")),
                col("str_4").fill_null(lit("")),
            ],
            "*",
            false,
        );

        let dataframe_02: DataFrame = dataframe_01
            .lazy()
            .with_columns([
                mensagem_ignore_nulls_true.alias("concat ignore_nulls_true"),
                mensagem_ignore_nulls_false.alias("concat ignore_nulls_false"),
            ])
            .collect()?;

        println!("dataframe02: {dataframe_02}\n");

        let col_a = Column::new(
            "concat ignore_nulls_true".into(),
            &["Food*Trick*aa", "Or*bb", "April*Treat*cc", ""],
        );
        let col_b = Column::new(
            "concat ignore_nulls_false".into(),
            &["Food*Trick**aa", "*Or**bb", "April*Treat**cc", "***"],
        );

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
    fn filter_even_numbers() -> JoinResult<()> {
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

        let lazyframe: LazyFrame = dataframe01.lazy().with_columns([
            lit(NULL).alias(verificar).cast(DataType::String),
            lit(NULL).alias(glosar).cast(DataType::String),
        ]);

        println!("dataframe02: {}\n", lazyframe.clone().collect()?);

        // modulo operation returns the remainder of a division
        // `a % b = a - b * floor(a / b)`
        let modulo: Expr = col("integers") % lit(2);
        let situacao: Expr = modulo.eq(lit(0)); // Even Number

        let mensagem: Expr = concat_str(
            [
                col(glosar),
                lit("Situação 01:"),
                col("integers"),
                lit("is an even"),
                lit("number"),
                lit("&"),
            ],
            " ",
            true,
        );

        let lazyframe: LazyFrame = lazyframe
            .with_column(
                when(situacao)
                    .then(mensagem)
                    .otherwise(col(glosar))
                    .alias(glosar),
            )
            .format_values();

        let dataframe03: DataFrame = lazyframe.collect()?;

        println!("dataframe03: {dataframe03}\n");

        let col = Column::new(
            glosar.into(),
            &[
                None,
                Some("Situação 01: 2 is an even number"),
                None,
                Some("Situação 01: 4 is an even number"),
                None,
            ],
        );

        assert_eq!(dataframe03.column(glosar)?, &col);

        Ok(())
    }

    // How to apply a function to multiple columns of a polars DataFrame in Rust
    // https://stackoverflow.com/questions/72372821/how-to-apply-a-function-to-multiple-columns-of-a-polars-dataframe-in-rust
    // https://pola-rs.github.io/polars/polars_lazy/index.html

    #[test]
    /// `cargo test -- --show-output apply_a_function_to_multiple_columns`
    fn apply_a_function_to_multiple_columns() -> JoinResult<()> {
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

        let lazyframe: LazyFrame = dataframe01.lazy().with_columns([
            //cols(selected)
            all().as_expr().round_expr(2),
        ]);

        let dataframe02: DataFrame = lazyframe.clone().collect()?;

        println!("dataframe02: {dataframe02}\n");

        let col_a = Column::new(
            "float64 A".into(),
            &[23.65, 0.32, 10.00, 89.02, -3.42, 52.08],
        );
        let col_b = Column::new("float64 B".into(), &[10.00, 0.4, 10.01, 89.01, -3.43, 52.1]);

        assert_eq!(dataframe02.column("float64 A")?, &col_a);
        assert_eq!(dataframe02.column("float64 B")?, &col_b);

        // Example 2:
        // input1: two columns --> output: one new column
        // input2: one column  --> output: one new column

        let lazyframe: LazyFrame = lazyframe.with_columns([
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
    /// `cargo test -- --show-output read_csv_file_v1`
    fn read_csv_file_v1() -> JoinResult<()> {
        unsafe {
            env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
            env::set_var("POLARS_FMT_MAX_COLS", "10"); // maximum number of columns shown when formatting DataFrames.
            env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
            env::set_var("POLARS_FMT_STR_LEN", "20"); // maximum number of characters printed per string value.
        }

        // wget https://raw.githubusercontent.com/claudiofsr/join_with_assignments/master/src/tests/csv_file01

        let delimiter = ';';
        let file_path = "src/tests/csv_file01";
        let value_p = "Value P";
        let plpath = PlPath::from_str(file_path);

        // --- with_infer_schema_length --- //
        println!("\n### --- with_infer_schema_length --- ###\n");

        let result_lazyframe_a: PolarsResult<LazyFrame> = LazyCsvReader::new(plpath.clone())
            .with_try_parse_dates(true)
            .with_separator(delimiter as u8)
            .with_has_header(true)
            .with_ignore_errors(true)
            .with_missing_is_null(true)
            .with_infer_schema_length(Some(10))
            .finish();

        let df_a = result_lazyframe_a?.collect()?;
        println!("df_a: {df_a}\n");

        // Get columns from dataframe
        let values_pa: &Column = df_a.column(value_p)?;

        // Get columns with into_iter()
        let vec_a: Vec<f64> = values_pa.f64()?.into_iter().flatten().collect();
        println!("values_pa: {vec_a:?}");

        // --- with_schema --- //
        println!("\n### --- with_schema --- ###\n");

        // The number or order of columns in the Schema does not need to match the CSV file.
        let column_dtypes: HashMap<&str, DataType> = HashMap::from([
            ("Value T", DataType::Float64),
            ("Dia da Emissão", DataType::String),
            ("Linhas NFE", DataType::UInt64),
            ("Alíquota", DataType::Float64),
            ("Descrição", DataType::String),
            ("Descrição B", DataType::String),
            ("Value P", DataType::Float64),
            ("Tributo", DataType::Float64),
            ("Número", DataType::Int64),
        ]);

        let options = StrptimeOptions {
            format: Some("%-d/%-m/%Y".into()),
            strict: false, // If set then polars will return an error if any date parsing fails
            exact: true, // If polars may parse matches that not contain the whole string e.g. “foo-2021-01-01-bar” could match “2021-01-01”
            cache: true, // use a cache of unique, converted dates to apply the datetime conversion.
        };

        let cols_dtype: Arc<HashMap<&'static str, DataType>> = Arc::new(column_dtypes);

        let result_lazyframe_b: PolarsResult<LazyFrame> = LazyCsvReader::new(plpath)
            .with_try_parse_dates(false) // use regex
            .with_separator(delimiter as u8)
            .with_has_header(true)
            .with_ignore_errors(true)
            .with_missing_is_null(true)
            .with_infer_schema_length(Some(0)) // Infer schema length 0 reads only headers.
            .with_schema_modify(Box::new(move |schema: Schema| {
                apply_custom_schema_rules(schema, &cols_dtype, Side::Left)
            }))?
            .finish();

        let mut lazyframe_b = result_lazyframe_b?
            //.with_column(col("Alíquota").cast(DataType::Int64))
            .with_column(
                col("^(Período|Data|Dia).*$") // regex
                    .str()
                    .to_date(options),
            );

        // Print column names and their respective types
        // Iterates over the `(&name, &dtype)` pairs in this schema
        lazyframe_b.collect_schema()?.iter().enumerate().for_each(
            |(index, (column_name, data_type))| {
                println!(
                    "column {:02}: (\"{column_name}\", DataType::{data_type}),",
                    index + 1
                );
            },
        );

        println!();

        let df_b = lazyframe_b.collect()?;
        println!("df_b: {df_b}\n");

        // Get columns from dataframe
        let values_pb: &Column = df_b.column(value_p)?;

        // Get columns with into_iter()
        let vec_b: Vec<f64> = values_pb.f64()?.into_iter().flatten().collect();
        println!("values_pb: {vec_b:?}");

        assert_eq!(vec_a, [3623.56, 7379.51, 6783.56, 106.34, 828.98]);
        assert_eq!(vec_a, vec_b);
        assert_eq!(df_a, df_b);

        Ok(())
    }

    #[test]
    /// `cargo test -- --show-output read_csv_file_v2`
    fn read_csv_file_v2() -> JoinResult<()> {
        unsafe {
            env::set_var("POLARS_FMT_TABLE_ROUNDED_CORNERS", "1"); // apply rounded corners to UTF8-styled tables.
            env::set_var("POLARS_FMT_MAX_COLS", "60"); // maximum number of columns shown when formatting DataFrames.
            env::set_var("POLARS_FMT_MAX_ROWS", "10"); // maximum number of rows shown when formatting DataFrames.
            env::set_var("POLARS_FMT_STR_LEN", "52"); // maximum number of characters printed per string value.
        }

        let delimiter = ';';
        let file_path = "src/tests/csv_file02";
        let valor_item = coluna(Right, "valor_item"); // "Valor da Nota Proporcional : NF Item (Todos) SOMA"
        let plpath = PlPath::from_str(file_path);

        // --- with_infer_schema_length --- //
        println!("\n### --- with_infer_schema_length --- ###\n");

        let result_lazyframe: PolarsResult<LazyFrame> = LazyCsvReader::new(plpath)
            .with_encoding(CsvEncoding::LossyUtf8)
            .with_try_parse_dates(true)
            .with_separator(delimiter as u8)
            .with_quote_char(Some(b'"'))
            .with_has_header(true)
            .with_ignore_errors(true)
            .with_null_values(None)
            .with_missing_is_null(true)
            .with_infer_schema_length(Some(10))
            .finish();

        let df_a = result_lazyframe?.collect()?;
        println!("df_a: {df_a}\n");

        // Get columns from dataframe
        let values_pa: &Column = df_a.column(valor_item)?;

        // Get columns with into_iter()
        let vec_a: Vec<f64> = values_pa.f64()?.into_iter().flatten().collect();
        println!("values_pa: {vec_a:?}\n");

        // --- with_schema --- //
        println!("\n### --- with_schema --- ###\n");

        let lazyframe_b: LazyFrame =
            get_lazyframe_from_csv(Some(file_path.into()), Some(delimiter), Right)?
                .with_row_index(coluna(Right, "count_lines"), Some(0u32));

        let df_b = lazyframe_b.collect()?;
        println!("df_b: {df_b}\n");

        // Get columns from dataframe
        let values_pb: &Column = df_b.column(valor_item)?;

        // Get columns with into_iter()
        let vec_b: Vec<f64> = values_pb.f64()?.into_iter().flatten().collect();
        println!("values_pb: {vec_b:?}");

        assert_eq!(vec_a, [3623.56, 7379.51, 6783.56, 106.34, 828.98]);
        assert_eq!(vec_a, vec_b);

        Ok(())
    }
}
