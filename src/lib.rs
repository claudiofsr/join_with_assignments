// https://pola-rs.github.io/polars-book/user-guide/introduction.html
// https://pola-rs.github.io/polars/polars/datatypes/enum.DataType.html
// https://stackoverflow.com/questions/72276752/how-to-use-date-in-polars-in-rust
// https://pola-rs.github.io/polars/polars/prelude/struct.LazyFrame.html
// https://able.bio/haixuanTao/data-manipulation-polars-vs-rust--3def44c8
// https://stackoverflow.com/questions/72490297/rust-polars-is-it-possible-to-explode-a-list-column-into-multiple-columns
// https://stackoverflow.com/questions/75418198/rust-polars-is-it-possible-to-subtract-a-list-column
// https://stackoverflow.com/questions/72699572/compare-2-tables-in-polars-and-select-a-value-based-on-that-comparison
// https://jorgecarleitao.github.io/arrow2/main/guide/index.html
// https://stackoverflow.com/questions/73562068/get-the-original-datatype-values-from-a-vector-of-anyvalues
// https://stackoverflow.com/questions/74394222/two-lists-in-a-list-of-struct-in-polars
// https://stackoverflow.com/questions/74668242/resample-time-series-using-polars-in-rust
// https://stackoverflow.com/questions/72075847/rust-polars-par-iter-for-chunkedarrayfloat64type
// https://stackoverflow.com/questions/70959170/is-there-a-way-to-apply-a-udf-function-returning-multiple-values-in-rust-polars
// https://stackoverflow.com/questions/74521285/how-to-zip-2-list-columns-on-python-polars
// https://stackoverflow.com/questions/71376935/how-to-get-a-vec-from-polars-series-or-chunkedarray

use polars::prelude::*;
use polars::datatypes::DataType;
use std::num::ParseFloatError;
use pathfinding::prelude::{Matrix, kuhn_munkres_min};
use rayon::prelude::*;
use std::{
    cmp,
    error::Error,
    fs::File,
    process, // process::exit(1)
    process::Command,
};

pub type VecTuples = Vec<(String, u64, u64)>;

#[derive(Default, Debug, Clone)]
pub struct Config {
    // https://doc.rust-lang.org/book/ch12-03-improving-error-handling-and-modularity.html
    pub csv_a: Option<String>,
    pub csv_b: Option<String>,
    pub dlm_a: Option<String>,
    pub dlm_b: Option<String>,
}

impl Config {

    pub fn build(args: &[String]) -> Result<Config, &'static str> {

        if !(args.len() >= 3 && args.len() <= 5) {
            println!("Usage:");
            println!("\tjoin_with_assignments file1.csv file2.csv [delimiter1 delimiter2]\n");
            println!("Please insert the two CSV files: file1.csv file2.csv");
            println!("Optionally enter delimiter characters for CSV files");
            println!("delimiter1 and delimiter2 are characters that delimit the columns of csv files");
            println!("By default, the delimiter character for CSV files is ;");
            println!("Example: join_with_assignments file1.csv file2.csv \"|\" \",\"\n");
            return Err("not enough arguments!\n");
        }

        let csv_a: Option<String> = args.get(1).map(|s| s.to_string());
        let csv_b: Option<String> = args.get(2).map(|s| s.to_string());
        let dlm_a: Option<String> = args.get(3).map(|s| s.to_string());
        let dlm_b: Option<String> = args.get(4).map(|s| s.to_string());

        Ok(Config {csv_a, csv_b, dlm_a, dlm_b})
    }
}

pub fn clear_terminal_screen() {
    if cfg!(target_os = "windows") {
        Command::new("cls").status().unwrap();
    } else {
        Command::new("clear").status().unwrap();
    };
}

fn round_f64(x: f64, decimals: u32) -> f64 {
    let y = 10i32.pow(decimals) as f64;
    (x * y).round() / y
}

/// See polars-core-0.27.2/src/utils/mod.rs and macro_rules! split_array {...}
pub fn split_series(series: &Series) -> PolarsResult<Vec<Series>> {

    let vec_series: Vec<Series> = (0..series.len())
        .into_par_iter() // rayon: parallel iterator
        .map(|i| series
            .slice(i as i64, 1)
            .explode()
            .unwrap()
        )
        .collect();

    Ok(vec_series)
}

/* --- munkres --- */
/* ---- start ---- */

pub fn get_width<T>(array1: &[T], array2: &[T]) -> usize
    where T: Copy + Clone + ToString + PartialOrd
{
    let concatenated: Vec<T> = [array1, array2].concat();
    let mut max_value: T = concatenated[0];
    for value in concatenated {
        if value > max_value {
            max_value = value;
        }
    }
    let width: usize = max_value.to_string().len();
    width
}

pub fn get_matrix(array1: &[i128], array2: &[i128]) -> Vec<Vec<i128>> {
    let mut matrix = Vec::new();
    for a1 in array1 {
        let mut array = Vec::new();
        for a2 in array2 {
            let delta: i128 = a1 - a2;
            array.push(delta.pow(2));  // valor ao quadrado
            //array.push(delta.abs()); // valor absoluto
        }
        matrix.push(array);
    }
    matrix
}

// https://stackoverflow.com/questions/59314686/how-to-efficiently-create-a-large-vector-of-items-initialized-to-the-same-value
// https://stackoverflow.com/questions/29530011/creating-a-vector-of-zeros-for-a-specific-size

pub fn convert_to_square_matrix(matrix: &mut Vec<Vec<i128>>) {

    // check if the matrix is a square matrix,
    // if not convert it to square matrix by padding zeroes.

    let row_number: usize = matrix.len();
    let col_number: usize = matrix[0].len();
    let delta: usize = row_number.abs_diff(col_number);
    let _min: usize = cmp::min(row_number, col_number);

    //println!("row_number: {row_number}");
    //println!("col_number: {col_number}");
    //println!("delta: {delta}\n");

    if row_number < col_number { // Add rows
        for _ in 0 .. delta {
            let vector = vec![0; col_number];
            matrix.push(vector);
        }
    }

    if row_number > col_number { // Add columns
        for vector in &mut matrix[..] {
            let zeroes = vec![0; delta];
            vector.extend(zeroes);
        }
    }
}

pub fn display_bipartite_matching (
    width: usize,
    matrix: &[Vec<i128>],
    array1: &[i128],
    array2: &[i128],
    assignments: &[usize],
    filter: bool,
) -> i128 {

    let row_number: usize = array1.len();
    let col_number: usize = array2.len();
    let min: usize = cmp::min(row_number, col_number);
    let max: usize = cmp::max(row_number, col_number);
    let widx = max.to_string().len();

    let mut bipartite: Vec<(i128, i128, u128)> = Vec::new();
    let mut assign: Vec<usize> = Vec::new(); // assignments after filter
    let mut values: Vec<i128> = Vec::new();
    let mut sum = 0;

    // https://doc.rust-lang.org/std/vec/struct.Vec.html#method.retain
    // assignments.to_vec().retain(|&col| col < min);

    for (row, &col) in assignments.iter().enumerate() {

        if filter && ((row_number > col_number && col >= min) || (row_number < col_number && row >= min)) {
            continue;
        }

        let value = matrix[row][col];
        values.push(value);
        assign.push(col);
        sum += value;
    }

    let width_index: usize = get_width(&assign, &[]);
    let width_value: usize = get_width(&[], &values);
    let width_b: usize = width_index.max(width_value);

    println!("matrix indexes: {assign:>width_b$?}");
    println!("matrix values:  {values:>width_b$?}");
    println!("sum of values: {sum}\n");

    for (row, &col) in assignments.iter().enumerate() {

        if (row_number > col_number && col >= min) || (row_number < col_number && row >= min) {
            continue;
        }

        let delta: u128 = array1[row].abs_diff(array2[col]);
        println!("(array1[{row:widx$}], array2[{col:widx$}], abs_diff): ({:>width$}, {:>width$}, {delta:>width$})", array1[row], array2[col]);
        bipartite.push((array1[row], array2[col], delta));
    }
    println!();

    sum
}

pub fn print_matrix(
    width: usize,
    matrix: &[Vec<i128>],
    array1: &[i128],
    array2: &[i128],
    assignments: &[usize],
    filter: bool,
) {

    let row_number: usize = array1.len();
    let col_number: usize = array2.len();
    let min: usize = cmp::min(row_number, col_number);

    println!("Matriz do módulo da diferença, matriz[i][j] = abs (array1[i] - array2[j]):\n");

    print!("{:>w$}", ' ', w = width + 2);
    for val in array2 {
        print!(" {val:>width$}, ");
    }
    println!("\n");

    for (i, vec) in matrix.iter().enumerate() {
        let mut val = format!("{:>width$}", ' ');
        if i < array1.len() {
            val = format!("{:>width$}", array1[i]);
        }
        print!("{val} [");

        let mut vector: Vec<i128> = vec.to_vec();
        if filter && (row_number > col_number) {
            vector.truncate(min);
        }

        let idx = assignments[i];
        for (j, val) in vector.iter().enumerate() {
            if j == idx {
                let star: String = vec!["*".to_string(); 1 + width - val.to_string().len()].join("");
                let new_val = [star, val.to_string()].concat(); // add *
                print!("{new_val:>width$}"); // add *
            } else {
                print!(" {val:>width$}");
            }
            if j < (vector.len() - 1) {
                print!(", ");
            }
        }
        println!(" ]");

        if filter && (row_number < col_number && i >= (min - 1)) {
            // println!("i: {i} ; min: {min}\n");
            break;
        }
    }

    println!();
}

/* ---- final ---- */
/* --- munkres --- */

pub fn get_option_assignments(series_efd: Series, series_nfe: Series) -> Option<Series> {

    let result_chunckedarray_f64_efd: Result<&ChunkedArray<Float64Type>, PolarsError> = series_efd.f64();
    let result_chunckedarray_f64_nfe: Result<&ChunkedArray<Float64Type>, PolarsError> = series_nfe.f64();

    match (result_chunckedarray_f64_efd, result_chunckedarray_f64_nfe) {
        (Ok(chunckedarray_f64_efd), Ok(chunckedarray_f64_nfe)) => {
            let vec_opt_f64_efd: Vec<Option<f64>> = chunckedarray_f64_efd.into_iter().collect();
            let vec_opt_f64_nfe: Vec<Option<f64>> = chunckedarray_f64_nfe.into_iter().collect();

            let result_vec_float64_efd: Result<Vec<f64>, String> = flatten_all(vec_opt_f64_efd);
            let result_vec_float64_nfe: Result<Vec<f64>, String> = flatten_all(vec_opt_f64_nfe);

            match (result_vec_float64_efd, result_vec_float64_nfe) {
                (Ok(vec_float64_efd), Ok(vec_float64_nfe)) => {
                    let vec_assignments: Vec<u64> = munkres_assignments(&vec_float64_efd, &vec_float64_nfe);
                    Some(Series::new("New", vec_assignments))
                },
                _ => None,
            }
        },
        _ => {
            println!("Float64Type PolarsError!");
            println!("series_efd.dtype(): {} ; series_efd: {series_efd:?}", series_efd.dtype());
            println!("series_nfe.dtype(): {} ; series_nfe: {series_nfe:?}", series_nfe.dtype());
            None
        },
    }
}

/// Get Series of minimal Munkres Assignments from two f64 Slices
fn munkres_assignments(vec_a: &[f64], vec_b: &[f64]) -> Vec<u64> {

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
    let assignments_u64: Vec<u64> = assignments
        .iter()
        .map(|&val| u64::try_from(val).unwrap())
        .collect();

    assignments_u64
}

pub fn get_opt_vectuples(chave_doc: &str, series_efd: Series, series_nfe: Series, series_asg: Series) -> Option<VecTuples> {

    let result_chunckedarray_u64_efd: Result<&ChunkedArray<UInt64Type>, PolarsError> = series_efd.u64();
    let result_chunckedarray_u64_nfe: Result<&ChunkedArray<UInt64Type>, PolarsError> = series_nfe.u64();
    let result_chunckedarray_u64_asg: Result<&ChunkedArray<UInt64Type>, PolarsError> = series_asg.u64();

    match (result_chunckedarray_u64_efd, result_chunckedarray_u64_nfe, result_chunckedarray_u64_asg) {
        (Ok(chunckedarray_u64_efd), Ok(chunckedarray_u64_nfe), Ok(chunckedarray_u64_asg)) => {

            let vec_opt_u64_efd: Vec<Option<u64>> = chunckedarray_u64_efd.into_iter().collect();
            let vec_opt_u64_nfe: Vec<Option<u64>> = chunckedarray_u64_nfe.into_iter().collect();
            let vec_opt_u64_asg: Vec<Option<u64>> = chunckedarray_u64_asg.into_iter().collect();

            let result_vec_u64_efd: Result<Vec<u64>, String> = flatten_all(vec_opt_u64_efd);
            let result_vec_u64_nfe: Result<Vec<u64>, String> = flatten_all(vec_opt_u64_nfe);
            let result_vec_u64_asg: Result<Vec<u64>, String> = flatten_all(vec_opt_u64_asg);

            match (result_vec_u64_efd, result_vec_u64_nfe, result_vec_u64_asg) {
                (Ok(vec_u64_efd), Ok(vec_u64_nfe), Ok(vec_u64_asg)) => {
                    line_assignments(chave_doc, &vec_u64_efd, &vec_u64_nfe, &vec_u64_asg)
                },
                _ => None,
            }
        },
        _ => {
            println!("UInt64Type PolarsError!");
            println!("chave_doc: {chave_doc}");
            println!("series_efd.dtype(): {} ; series_efd: {series_efd:?}", series_efd.dtype());
            println!("series_nfe.dtype(): {} ; series_nfe: {series_nfe:?}", series_nfe.dtype());
            println!("series_asg.dtype(): {} ; series_asg: {series_asg:?}", series_asg.dtype());
            None
        },
    }
}

fn line_assignments(chave_doc: &str, slice_lines_efd: &[u64], slice_lines_nfe: &[u64], assignments: &[u64]) -> Option<VecTuples> {

    let mut chaves_com_linhas_correlacionadas: VecTuples = Vec::new();

    for (row, &col) in assignments.iter().enumerate() {

        let opt_line_efd: Option<&u64> = slice_lines_efd.get(row);
        let opt_line_nfe: Option<&u64> = slice_lines_nfe.get(col as usize);

        if let (Some(&line_efd), Some(&line_nfe)) = (opt_line_efd, opt_line_nfe) {
            let tuple: (String, u64, u64) = (chave_doc.to_string(), line_efd, line_nfe);
            //println!("row: {row} ; tuple: {tuple:?}");
            chaves_com_linhas_correlacionadas.push(tuple);
        }
    }

    if chaves_com_linhas_correlacionadas.is_empty() {
        None
    } else {
        Some(chaves_com_linhas_correlacionadas)
    }
}

/*
#[allow(dead_code)]
async fn download(url: &str) -> Result<Vec<u8>, reqwest::Error> {
    let response = reqwest::get(url).await?;
    println!("Response: {:?} {}", response.version(), response.status());
    println!("Headers: {:#?}\n", response.headers());
    let body = response.text().await?;
    let data: Vec<u8> = body.bytes().collect();
    Ok(data)
}

// https://georgik.rocks/how-to-download-binary-file-in-rust-by-reqwest/
#[allow(dead_code)]
async fn fetch_url(url: &str, output_file: &str) -> Result<(), Box<dyn Error>> {
    let response = reqwest::get(url).await?;
    let mut file = std::fs::File::create(output_file)?;
    let mut content =  Cursor::new(response.bytes().await?);
    std::io::copy(&mut content, &mut file)?;
    println!("download '{output_file}' from '{url}'.");
    Ok(())
}

#[allow(dead_code)]
fn download_file_from_the_internet(url: &str, output_file: &str) {
    let resp = reqwest::blocking::get(url).expect("request failed");
    let body = resp.text().expect("body invalid");
    let mut out = File::create(output_file).expect("failed to create file");
    std::io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");
    println!("download '{output_file}' from '{url}'.");
}
*/

/// flatten_all removes the intermediate option and displays error messages if None.
/// Another alternative is to use .into_iter().flatten(), but without error messages.
pub fn flatten_all<T>(vec_opt_type: Vec<Option<T>>) -> Result<Vec<T>, String>
    where T: std::default::Default + std::fmt::Debug + ?Sized
{
    let result_vec: Result<Vec<T>, String> = vec_opt_type
        .into_iter()
        .map(get_type)
        .collect();

    result_vec
}

// https://stackoverflow.com/questions/26368288/how-do-i-stop-iteration-and-return-an-error-when-iteratormap-returns-a-result
fn get_type<T>(opt_type: Option<T>) -> Result<T, String> 
    where T: std::default::Default + std::fmt::Debug + ?Sized
{
    match opt_type {
        Some(value) => Ok(value),
        None => {
            let generic_type_name: &str = std::any::type_name::<T>();
            println!("\n\tError when executing function flatten_all().");
            println!("\tAll values are expected to be Some({generic_type_name}).");
            println!("\tBut at least one value was None!\n");
            Err(format!("Found {opt_type:?} value!"))
        },
    }
}

#[allow(dead_code)]
fn make_schema(side: &str) -> Schema {

    let vec_tuple_a = vec![
        ("Linhas", DataType::Int64),
        ("Arquivo da EFD Contribuições", DataType::Utf8),
        ("Nº da Linha da EFD", DataType::Int64),
        ("CNPJ dos Estabelecimentos do Contribuinte", DataType::Utf8),
        ("Nome do Contribuinte", DataType::Utf8),
        ("Ano do Período de Apuração", DataType::Int64),
        ("Trimestre do Período de Apuração", DataType::Int64),
        ("Mês do Período de Apuração", DataType::Utf8),
        ("Tipo de Operação", DataType::Utf8),
        ("Tipo de Crédito", DataType::Utf8),
        ("Registro", DataType::Utf8),
        ("Código de Situação Tributária (CST)", DataType::Utf8),
        ("Código Fiscal de Operações e Prestações (CFOP)", DataType::Utf8),
        ("Natureza da Base de Cálculo dos Créditos", DataType::Utf8),
        ("Descrição das Operações", DataType::Utf8),
        ("CNPJ do Participante", DataType::Utf8),
        ("CPF do Participante", DataType::Utf8),
        ("Nome do Participante", DataType::Utf8),
        ("Nº do Documento Fiscal", DataType::Int64),
        ("Chave do Documento", DataType::Utf8),
        ("Modelo do Documento Fiscal", DataType::Utf8),
        ("Verificação da Chave", DataType::Utf8),
        ("Nº do Item do Documento Fiscal", DataType::Int64),
        ("Data da Emissão do Documento Fiscal", DataType::Utf8),
        ("Data da Entrada / Aquisição / Execução ou da Saída / Prestação / Conclusão", DataType::Utf8),
        ("Tipo do Item", DataType::Utf8),
        ("Descrição do Item", DataType::Utf8),
        ("Natureza do Frete Contratado", DataType::Utf8),
        ("Código NCM", DataType::Int64),
        ("Escrituração Contábil: Nome da Conta", DataType::Utf8),
        ("Informação Complementar do Documento Fiscal", DataType::Utf8),
        ("Valor Total do Item", DataType::Float64),
        ("Valor da Base de Cálculo de PIS/PASEP e COFINS", DataType::Float64),
        ("Alíquota de PIS/PASEP (em percentual)", DataType::Float64),
        ("Alíquota de COFINS (em percentual)", DataType::Float64),
        ("Valor de PIS/PASEP", DataType::Float64),
        ("Valor de COFINS", DataType::Float64),
        ("Valor de ISS", DataType::Float64),
        ("Valor da Base de Cálculo de ICMS", DataType::Float64),
        ("Alíquota de ICMS (em percentual)", DataType::Float64),
        ("Valor de ICMS", DataType::Float64),
    ];

    let vec_tuple_b = vec![
        ("CNPJ do Contribuinte : NF Item (Todos)", DataType::Utf8),
        ("Nome do Contribuinte : NF Item (Todos)", DataType::Utf8),
        ("Entrada/Saída : NF (Todos)", DataType::Utf8),
        ("CPF/CNPJ do Participante : NF (Todos)", DataType::Utf8),
        ("Nome do Participante : NF (Todos)", DataType::Utf8),
        ("CRT : NF (Todos)", DataType::Utf8),
        ("Observações : NF (Todos)", DataType::Utf8),
        ("CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("CTe - Remetente das mercadorias transportadas: Nome de Conhecimento : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("CTe - Remetente das mercadorias transportadas: Município de Conhecimento : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("CTe - UF do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Nome do Município do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - UF do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Nome do Município do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Informações do Destinatário do CT-e: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Informações do Destinatário do CT-e: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("CTe - Local de Entrega constante na Nota Fiscal: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes", DataType::Utf8),
        ("Descrição da Natureza da Operação : NF Item (Todos)", DataType::Utf8),
        ("Cancelada : NF (Todos)", DataType::Utf8),
        ("Registro de Origem do Item : NF Item (Todos)", DataType::Utf8),
        ("Natureza da Base de Cálculo do Crédito Descrição : NF Item (Todos)", DataType::Utf8),
        ("Modelo - Descrição : NF Item (Todos)", DataType::Utf8),
        ("Número da Nota : NF Item (Todos)", DataType::Int64),
        ("Chave da Nota Fiscal Eletrônica : NF Item (Todos)", DataType::Utf8),
        ("Inf. NFe - Chave de acesso da NF-e : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("CTe - Observações Gerais de Conhecimento : ConhecimentoInformacaoNFe", DataType::Utf8),
        ("Dia da Emissão : NF Item (Todos)", DataType::Utf8),
        ("Número da DI : NF Item (Todos)", DataType::Utf8),
        ("Número do Item : NF Item (Todos)", DataType::Int64),
        ("Código CFOP : NF Item (Todos)", DataType::Int64),
        ("Descrição CFOP : NF Item (Todos)", DataType::Utf8),
        ("Descrição da Mercadoria/Serviço : NF Item (Todos)", DataType::Utf8),
        ("Código NCM : NF Item (Todos)", DataType::Int64),
        ("Descrição NCM : NF Item (Todos)", DataType::Utf8),
        ("COFINS: Alíquota ad valorem - Atributo : NF Item (Todos)", DataType::Int64),
        ("PIS: Alíquota ad valorem - Atributo : NF Item (Todos)", DataType::Int64),
        ("CST COFINS Descrição : NF Item (Todos)", DataType::Utf8),
        ("CST PIS Descrição : NF Item (Todos)", DataType::Utf8),
        ("Valor Total : NF (Todos) SOMA", DataType::Float64),
        ("Valor da Nota Proporcional : NF Item (Todos) SOMA", DataType::Float64),
        ("Valor dos Descontos : NF Item (Todos) SOMA", DataType::Float64),
        ("Valor Seguro : NF (Todos) SOMA", DataType::Float64),
        ("COFINS: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
        ("PIS: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
        ("IPI: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
        ("ISS: Base de Cálculo : NF Item (Todos) SOMA", DataType::Float64),
        ("ISS: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
        ("ICMS: Alíquota : NF Item (Todos) NOISE OR", DataType::Float64),
        ("ICMS: Base de Cálculo : NF Item (Todos) SOMA", DataType::Float64),
        ("ICMS: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
        ("ICMS por Substituição: Valor do Tributo : NF Item (Todos) SOMA", DataType::Float64),
    ];

    let vec_tuple = if side == "left" {
        vec_tuple_a
    } else {
        vec_tuple_b
    };

    let mut schema: Schema = Schema::new();

    for tuple in vec_tuple {
        let (column_name, tipo) = tuple;
        schema.with_column(column_name.to_string(), tipo);
    }

    schema
}

// https://pola-rs.github.io/polars/py-polars/html/reference/lazyframe/index.html
fn read_csv_lazy(file_path: &str, delimiter_char: char, _side: &str) -> Result<LazyFrame, PolarsError> {
    //let schema: Schema = make_schema(side);

    // Set values that will be interpreted as missing/null.
    let null_values: Vec<String> = vec![
        " ".to_string(),
        "<N/D>".to_string(),
        "*DIVERSOS*".to_string(),
    ];

    let lazyframe: LazyFrame = LazyCsvReader::new(file_path)
        .with_encoding(CsvEncoding::LossyUtf8)
        .with_parse_dates(false)
        .with_delimiter(delimiter_char as u8)
        .has_header(true)
        .with_ignore_errors(true)
        .with_null_values(Some(NullValues::AllColumns(null_values)))
        .with_infer_schema_length(Some(50))
        //.with_schema(schema.into())
        .finish()?;

    Ok(lazyframe)
}

// https://docs.rs/polars/latest/polars/
// We recommend to build your queries directly with polars-lazy.
// This allows you to combine expression into powerful aggregations and column selections.
// All expressions are evaluated in parallel and your queries are optimized just in time.

pub fn get_lazyframe_from_csv(file_path: Option<String>, delimiter: Option<String>, side: &str) -> Result<LazyFrame, PolarsError> {

    let options = StrpTimeOptions {
        date_dtype: DataType::Date,
        fmt: Some("%-d/%-m/%Y".into()),
        //fmt: Some("%Y-%-m-%-d".into()),
        exact: true,
        ..Default::default()
    };

    let file_path: String = file_path.unwrap();

    let delimiter_default: String = ";".to_string();
    let delimiter_string: String = delimiter.unwrap_or(delimiter_default);
    let delimiter_char: char = delimiter_string.chars().next().unwrap();

    let lazyframe: LazyFrame = read_csv_lazy(&file_path, delimiter_char, side)?
        .with_column(
            col("^.*Data|Dia|birthday.*$")
            //cols(["Data","Date","Dia"])
            .str().strptime(options)
        );

    println!("file_path: {file_path}\n{}\n", lazyframe.clone().collect()?);

    let binding: Arc<Schema> = lazyframe.schema().unwrap();
    let columns: Vec<(usize, (&String, &DataType))> = binding.iter().enumerate().collect();

    for (index, (column_name, data_type)) in columns {
        println!("column {:02}: (\"{column_name}\", DataType::{data_type}),", index + 1);
    }
    println!();

    Ok(lazyframe)
}

pub fn write_csv(df: &mut DataFrame, delimiter_char: char, output_path: &str) -> Result<(), PolarsError> {
    let mut output_csv: File = File::create(output_path)
    .expect("could not create output.csv file!");

    println!("Write DataFrame to '{output_path}'");
    println!("{df}\n");

    CsvWriter::new(&mut output_csv)
        .with_delimiter(delimiter_char as u8)
        .has_header(true)
        .finish(df)?;

    Ok(())
}

pub fn write_pqt(df: &mut DataFrame, output_path: &str) -> Result<(), PolarsError> {
    let mut output_parquet: File = File::create(output_path)
    .expect("could not create output.parquet file!");

    println!("Write DataFrame to output.parquet\n");

    ParquetWriter::new(&mut output_parquet)
        .with_statistics(true)
        //.with_compression(ParquetCompression::Lz4Raw)
        .finish(df)?;

    Ok(())
}

#[allow(dead_code)]
fn read_pqt(output_path: &str) -> Result<DataFrame, PolarsError> {
    let parquet = File::open(output_path)?;
    let reader = ParquetReader::new(parquet);
    let df_parquet: DataFrame = reader.finish()?;

    println!("df_parquet:\n{df_parquet}\n");

    Ok(df_parquet)
}

pub fn round_series(series: Series, decimals: u32) -> Result<Option<Series>, PolarsError> {

    let result_option_series = match series.dtype() {
        DataType::Float64 => Ok(Some(round_series_float64(series, decimals))),
        DataType::Utf8    => Ok(Some(round_series_utf8(series, decimals))),
        _ => Err(PolarsError::InvalidOperation(
            format!(
                "Not supported for Series with dtype {:?}",
                series.dtype()
            )
            .into(),
        )),
    };

    result_option_series
}

fn round_series_float64(series: Series, decimals: u32) -> Series {

    let chunked_array: &ChunkedArray<Float64Type> = series.f64().unwrap();

    let vec_option_f64: Vec<Option<f64>> = chunked_array.into_iter().collect();

    let series: Series = vec_option_f64
        .par_iter() // rayon: parallel iterator
        //.into_iter()
        .map(|opt_f64|
            opt_f64.map(|f64| round_f64(f64, decimals))
        )
        .collect::<Float64Chunked>()
        .into_series();

    series
}

fn round_series_utf8(series: Series, decimals: u32) -> Series {

    let series_formatted: Series = series
        .utf8()
        .unwrap()
        .par_iter() // rayon: parallel iterator
        //.into_iter()
        .map(|opt_str: Option<&str>| {
            opt_str.map(|str: &str|
                {
                    let result: Result<f64, ParseFloatError> = str
                    .trim()
                    .replace('.', "")
                    .replace(',', ".")
                    .parse::<f64>();

                    match result {
                        Ok(float) => round_f64(float, decimals),
                        Err(why) => {
                            println!("fn round_series_utf8()");
                            println!("Error parse f64: {why}");
                            process::exit(1)
                        }
                    }
                }
            )
        })
        .collect::<Float64Chunked>()
        .into_series();

    series_formatted
}

pub fn formatar_chave_eletronica(series: Series) -> Result<Option<Series>, PolarsError> {

    let result_option_series = match series.dtype() {
        DataType::Utf8 => Ok(Some(format_digits(series))),
        _ => Err(PolarsError::InvalidOperation(
            format!(
                "Not supported for Series with dtype {:?}",
                series.dtype()
            )
            .into(),
        )),
    };

    result_option_series
}

// https://docs.rs/polars/latest/polars/prelude/string/struct.StringNameSpace.html#
fn format_digits(series: Series) -> Series {

    let formatted: Series = series
    .utf8()
    .unwrap()
    .par_iter() // rayon: parallel iterator
    //.into_iter()
    .map(retain_only_digits)
    .collect::<Utf8Chunked>()
    .into_series();

    formatted
}

fn retain_only_digits(opt_str: Option<&str>) -> Option<String> {
    let mut only_digits: String = match opt_str {
        Some(str) => str.to_string(),
        None => return None,
    };

    only_digits.retain(|current_char| current_char.is_ascii_digit());

    if !only_digits.is_empty() {
        // formatar código: '1234...89'
        let cod: String = ["'", &only_digits, "'"].concat();
        Some(cod)
    } else {
        None
    }
}

#[cfg(test)]
mod test_functions {
    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --nocapture flatten
    // cargo test -- --show-output
    // cargo test -- --show-output multiple_values
    use super::*;

    #[test]
    fn function_returning_multiple_values() -> Result<(), Box<dyn Error>> {
        df_multiple_values()?;
        Ok(())
    }

    #[test]
    fn flatten_all_versus_flatten() -> Result<(), Box<dyn Error>> {
        vec_option_u32()?;
        Ok(())
    }

    #[test]
    fn use_rayon_join() -> Result<(), Box<dyn Error>> {
        execute_closures_in_parallel()?;
        Ok(())
    }
}

// https://stackoverflow.com/questions/70959170/is-there-a-way-to-apply-a-udf-function-returning-multiple-values-in-rust-polars
pub fn df_multiple_values() -> Result<(), Box<dyn Error>> {
    let df = df![
        "a" => [1.0, 2.0, 3.0],
        "b" => [1.0, 2.0, 3.0]
    ]?;

    let df: DataFrame = df
        .lazy()
        .select([map_multiple(
            |columns| {
                Ok(Some(
                         columns[0].f64()?.into_no_null_iter()
                    .zip(columns[1].f64()?.into_no_null_iter())
                    .map(|(a, b)| {
                        let out = black_box(a, b);
                        Series::new("", [out.0, out.1, out.2])
                    })
                    .collect::<ChunkedArray<ListType>>()
                    .into_series()))
            },
            [col("a"), col("b")],
            GetOutput::from_type(DataType::Float64),
        ).alias("Multiple Values")
        ])
        .collect()?;

    //dbg!(df);
    println!("{df}");

    /*
    shape: (3, 1)
    ┌─────────────────┐
    │ Multiple Values │
    │ ---             │
    │ list[f64]       │
    ╞═════════════════╡
    │ [2.0, 3.3, 1.0] │
    │ [4.0, 6.6, 4.0] │
    │ [6.0, 9.9, 9.0] │
    └─────────────────┘
    */

    let column_multiple_values: &Series = df.column("Multiple Values")?;
    let vec_opt_lines_efd: Vec<Option<Series>> = column_multiple_values.list()?.into_iter().collect();

    // É necessário formatar o número de casas decimais
    let series_formatted: Vec<Option<Series>> = vec_opt_lines_efd
        .par_iter() // rayon: parallel iterator
        //.into_iter()
        .map(|opt_series|
            opt_series
            .as_ref()
            .map(|series| round_series(series.clone(), 1).unwrap())
            .unwrap()
        )
        .collect();

    let vec_series: Vec<Series> = flatten_all(series_formatted).unwrap();

    let vec_lines: Vec<Vec<f64>> = vec_series
        .par_iter() // rayon: parallel iterator
        //.into_iter()
        .map(|series| {
            let chunkedarray_f64: &ChunkedArray<Float64Type> = series.f64().unwrap();
            let vec_opt_f64: Vec<Option<f64>>= chunkedarray_f64.into_iter().collect();
            let vec_f64: Vec<f64>= flatten_all(vec_opt_f64).unwrap();
            vec_f64
        })
        .collect();

    let first_list = vec![2.0, 3.3, 1.0];

    assert!(
        first_list
        .into_iter()
        .zip(vec_lines[0].clone())
        .all(|(a, b)|
            {
                println!("a: {a:>3} ; b: {b:>3}");
                a == b
            }
        )
    );

    Ok(())
}

/// Your function that takes 2 argument and returns 3
fn black_box(a: f64, b: f64) -> (f64, f64, f64) {
    (a+b, 5.4 * a - 2.1 * b, a*b)
}

pub fn vec_option_u32() -> Result<(), Box<dyn Error>> {

    let options: Vec<Option<u32>> = vec![Some(123), Some(321), None, Some(231), None, Some(57)];
    println!("options: {options:?}");

    // Flattening works on any IntoIterator type, including Option and Result:
    let values_flattened: Vec<u32> = options.clone().into_iter().flatten().collect();
    println!("values_flattened: {values_flattened:?}");
    assert_eq!(values_flattened, vec![123, 321, 231, 57]);

    let result_vec: Result<Vec<u32>, String> = flatten_all(options);
    assert_eq!(result_vec.err(), Some("Found None value!".to_string()));

    Ok(())
}

// https://blog.logrocket.com/implementing-data-parallelism-rayon-rust/
pub fn execute_closures_in_parallel() -> Result<(), Box<dyn Error>> {

    // Takes two closures and potentially runs them in parallel.
    // It returns a pair of the results from those closures.

    let number = 5;

    let (a, b) = rayon::join(
        || factorial(number),
        || strings_to_num(&["12", "100", "19887870", "56", "9098"]),
    );

    println!("factorial of {number} is {a}");
    println!("numbers are {:?}", b) ;

    Ok(())
}

fn strings_to_num(slice: &[&str]) -> Vec<usize> {
    slice.iter().map(|&s| {
        s.parse::<usize>().expect("{s} is not a number")
    }).collect()
}

fn factorial(n: u128) -> u128 {
    (1..=n).reduce(|multiple, next| multiple * next).unwrap()
}
