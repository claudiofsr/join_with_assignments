use regex::Regex;
use std::sync::LazyLock as Lazy;

/// Base Legal conforme código NCM e descrição do item.
pub fn base_legal_de_aliquota_zero(codigo_ncm: u64, descricao: &str) -> Option<&'static str> {
    // Specific NCM codes that do not qualify for any exemption
    let especificos: [u64; 1] = [
        3029000, // lei_10925_art01_inciso20a()
    ];

    if especificos.contains(&codigo_ncm) {
        return None;
    }

    // Match NCM codes to specific legal articles.
    match codigo_ncm {
        31000000..=31999999 => lei_10925_art01_inciso01(),
        38080000..=38089999 | 27075000 => lei_10925_art01_inciso02(),
        12011000 => lei_10925_art01_inciso03(descricao),
        25000000..=25999999 => lei_10925_art01_inciso04(descricao),
        7133319 | 7133329 | 7133399 | 10062000..=10063099 | 11062000..=11062099 => {
            lei_10925_art01_inciso05()
        }
        30029099 => lei_10925_art01_inciso06(),
        30023000..=30023099 => lei_10925_art01_inciso07(),
        11022000..=11022099 | 11031300..=11031399 | 11041900..=11041999 => {
            lei_10925_art01_inciso09()
        }
        1051100 => lei_10925_art01_inciso10(),
        // Observe que o intervalo de ncm (4010000 ..= 4049999) é analisado
        // em diferentes condições conforme a descrição do item.
        // Decreto 8.533/2015, Art 04 ; LEI Nº 10.925/2004, Art 01 incisos 11 e 13
        ncm @ 4010000..=4049999 => condicoes_ncm_04(descricao, ncm),
        22029000 | 22029900 => lei_10925_art01_inciso11b(descricao),
        4060000..=4069999 => lei_10925_art01_inciso12(descricao),
        11010010 => lei_10925_art01_inciso14(),
        10010000..=10019999 => lei_10925_art01_inciso15(),
        19012000 | 19059090 => lei_10925_art01_inciso16(),
        19020000..=19029999 => lei_10925_art01_inciso18(),

        2010000..=2029999
        | 2062000..=2062999
        | 15021010..=15021019
        | 2061000
        | 2102000
        | 5069000
        | 5100010 => lei_10925_art01_inciso19a(),
        2030000..=2039999
        | 2064000..=2064999
        | 2070000..=2079999
        | 2090000..=2099999
        | 2101000..=2101999
        | 2063000
        | 2109900 => lei_10925_art01_inciso19b(),
        2040000..=2049999 | 2068000 => lei_10925_art01_inciso19c(),

        3020000..=3029999 => lei_10925_art01_inciso20a(),
        3030000..=3049999 => lei_10925_art01_inciso20b(),

        9010000..=9019999 | 21011000..=21011999 => lei_10925_art01_inciso21(),
        17011400 | 17019900 => lei_10925_art01_inciso22(),
        15070000..=15149999 => lei_10925_art01_inciso23(),
        4051000 => lei_10925_art01_inciso24(),
        15171000 => lei_10925_art01_inciso25(),
        34011190 => lei_10925_art01_inciso26(),
        33060000..=33069999 => lei_10925_art01_inciso27(),
        48181000 => lei_10925_art01_inciso28(),

        29000000..=29999999 => decreto_6426_art01(descricao),

        ncm @ 7000000..=8999999 | ncm @ 4070000..=4079999 => lei_10865_art28_inciso03(ncm),
        5110000..=5119999 => lei_10865_art28_inciso05(),
        87130000..=87139999 => lei_10865_art28_inciso14(),

        _ => None,
    }
}

/*
    LEI Nº 10.925, DE 23 DE JULHO DE 2004.
    http://www.planalto.gov.br/ccivil_03/_ato2004-2006/2004/lei/l10.925.htm

    Reduz as alíquotas do PIS/PASEP e da COFINS incidentes na importação e na comercialização do
    mercado interno de fertilizantes e defensivos agropecuários e dá outras providências.
*/

fn lei_10925_art01_inciso01() -> Option<&'static str> {
    // I - adubos ou fertilizantes classificados no Capítulo 31, exceto os produtos de uso veterinário, da Tabela de Incidência do Imposto sobre
    // Produtos Industrializados - TIPI, aprovada pelo Decreto nº 4.542, de 26 de dezembro de 2002, e suas matérias-primas;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso I (Adubos ou Fertilizantes).")
}

fn lei_10925_art01_inciso02() -> Option<&'static str> {
    // II - defensivos agropecuários classificados na posição 38.08 da TIPI e suas matérias-primas;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso II (Defensivos Agropecuários).")
}

fn lei_10925_art01_inciso03(descricao: &str) -> Option<&'static str> {
    // III - sementes e mudas destinadas à semeadura e plantio, em conformidade com o disposto na Lei nº 10.711,
    // de 5 de agosto de 2003, e produtos de natureza biológica utilizados em sua produção;

    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)Semente|Muda").unwrap());

    if RE.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso III (Sementes e Mudas destinadas à semeadura e plantio).",
        )
    } else {
        None
    }
}

fn lei_10925_art01_inciso04(descricao: &str) -> Option<&'static str> {
    /*
    IV - corretivo de solo de origem mineral classificado no Capítulo 25 da TIPI;

    TABELA DE INCIDÊNCIA DO IMPOSTO SOBRE PRODUTOS INDUSTRIALIZADOS (TIPI)

    Capítulo 25: Sal; enxofre; terras e pedras; gesso, cal e cimento

    CST do produto em sua entrada: 73 Operação de Aquisição a Alíquota Zero.
    CST do produto em sua   saída: 06 Operação Tributável a Alíquota Zero.
    */

    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)Corretivo|Sal Refinado|Perlita").unwrap());

    if RE.is_match(descricao) {
        Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso IV (Corretivo de Solo).")
    } else {
        None
    }
}

fn lei_10925_art01_inciso05() -> Option<&'static str> {
    // V - produtos classificados nos códigos 0713.33.19, 0713.33.29, 0713.33.99, 1006.20, 1006.30 e 1106.20 da TIPI;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso V (Arroz e Feijão).")
}

fn lei_10925_art01_inciso06() -> Option<&'static str> {
    // VI - inoculantes agrícolas produzidos a partir de bactérias fixadoras de nitrogênio,
    // classificados no código 3002.90.99 da TIPI;
    Some(
        "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso VI (Inoculantes Agrícolas, biofertilizantes).",
    )
}

fn lei_10925_art01_inciso07() -> Option<&'static str> {
    // VII - produtos classificados no Código 3002.30 da TIPI;
    Some(
        "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso VII (Vacinas para Medicina Veterinária).",
    )
}

fn lei_10925_art01_inciso09() -> Option<&'static str> {
    // IX - farinha, grumos e sêmolas, grãos esmagados ou em flocos, de milho, classificados,
    // respectivamente, nos códigos 1102.20, 1103.13 e 1104.19, todos da TIPI;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso IX (Farinha de Milho).")
}

fn lei_10925_art01_inciso10() -> Option<&'static str> {
    // X - pintos de 1 (um) dia classificados no código 0105.11 da TIPI;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso X (Pintos).")
}

fn condicoes_ncm_04(descricao: &str, ncm: u64) -> Option<&'static str> {
    static QUEIJOS_TRIBUTADOS: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)creme.*leite|sobremesa|sobr.*qj|gorgonzola|cheddar|cotage|cottage|soro")
            .unwrap()
    });
    static LEITE_FLUIDO: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)Leite\s*(Fluido Paste|Fluido Industr|Past|UHT|UAT)").unwrap()
    });
    static LEITE_EM_PO: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)Leite\s*(em Po|em Pó|Integral|Semidesnatado|Desnatado|Ferm)").unwrap()
    });
    static IOGURTE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)Beb.*Lac|Achocolatado|(Iog|Yogur|Yoghurt|Kefir|Kumys)|Coalhada").unwrap()
    });
    static SORO_FLUIDO: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)Soro").unwrap());
    static SORO_EM_PO: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?i)\b(Pó|Po)\b").unwrap()); // word boundaries: \b

    if !QUEIJOS_TRIBUTADOS.is_match(descricao)
        && (LEITE_FLUIDO.is_match(descricao)
            || LEITE_EM_PO.is_match(descricao)
            || IOGURTE.is_match(descricao))
    {
        // XI - leite fluido pasteurizado ou industrializado, na forma de ultrapasteurizado, leite em pó, integral, semidesnatado ou desnatado,
        // leite fermentado, bebidas e compostos lácteos e fórmulas infantis, assim definidas conforme previsão legal específica, destinados ao
        // consumo humano ou utilizados na industrialização de produtos que se destinam ao consumo humano;
        Some(
            "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XI (Leite Fluido, em Pó e Fermentado[inclui Iogurte] e Bebidas Lácteas).",
        )
    } else if ncm == 4041000 && SORO_FLUIDO.is_match(descricao) && !SORO_EM_PO.is_match(descricao) {
        // XIII - soro de leite fluido a ser empregado na industrialização de produtos destinados ao consumo humano.
        Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XIII (Soro de Leite Fluido).")
    } else {
        None
    }
}

fn lei_10925_art01_inciso11b(descricao: &str) -> Option<&'static str> {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)Beb.*Lac|Achocolatado|Achocolat").unwrap());

    if RE.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XI (Bebidas Lácteas e Achocolatados).",
        )
    } else {
        None
    }
}

fn lei_10925_art01_inciso12(descricao: &str) -> Option<&'static str> {
    // XII - queijos tipo mozarela, minas, prato, queijo de coalho, ricota, requeijão, queijo provolone, queijo parmesão, queijo fresco não maturado e queijo do reino;
    // queijos tipo mozarela = (mozarela|mussarela|muçarela)

    static RE01: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)creme.*leite|sobremesa|sobr.*qj|gorgonzola|cheddar").unwrap()
    });
    static RE02: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?i)(QJO|Queijo).*(moza|muss|muça|minas|prato|coalho|ricota|provolone|Parm|fresc|petit suisse|suico|reino|cotage|cottage)").unwrap()
    });
    static RE03: Lazy<Regex> = Lazy::new(|| {
        Regex::new(
            r"(?i)Req\b|Requeijão|Requeijao|C.*Cheese|(QJO|Queijo).*(cremoso|uf equil)|ricota",
        )
        .unwrap()
    });

    if !RE01.is_match(descricao) && (RE02.is_match(descricao) || RE03.is_match(descricao)) {
        Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XII (Queijos e Requeijão).")
    } else {
        None
    }
}

fn lei_10925_art01_inciso14() -> Option<&'static str> {
    // XIV - farinha de trigo classificada no código 1101.00.10 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XIV (Farinha de Trigo).")
}

fn lei_10925_art01_inciso15() -> Option<&'static str> {
    // XV - trigo classificado na posição 10.01 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XV (Trigo).")
}

fn lei_10925_art01_inciso16() -> Option<&'static str> {
    // XVI - pré-misturas próprias para fabricação de pão comum e pão comum classificados,
    // respectivamente, nos códigos 1901.20.00 Ex 01 e 1905.90.90 Ex 01 da Tipi.
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XVI (Pré Misturas de Pão).")
}

fn lei_10925_art01_inciso18() -> Option<&'static str> {
    // XVIII - massas alimentícias classificadas na posição 19.02 da Tipi.
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XVIII (Massas Alimentícias).")
}

fn lei_10925_art01_inciso19a() -> Option<&'static str> {
    // 	XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal classificados nos seguintes códigos da Tipi:
    // a) 02.01, 02.02, 0206.10.00, 0206.2, 0210.20.00, 0506.90.00, 0510.00.10 e 1502.10.1;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XIX, alínea A (Carnes).")
}

fn lei_10925_art01_inciso19b() -> Option<&'static str> {
    // b) 02.03, 0206.30.00, 0206.4, 02.07, 02.09 e 0210.1 e carne de frango classificada nos códigos 0210.99.00;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XIX, alínea B (Carnes).")
}

fn lei_10925_art01_inciso19c() -> Option<&'static str> {
    // c) 02.04 e miudezas comestíveis de ovinos e caprinos classificadas no código 0206.80.00;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XIX, alínea C (Carnes).")
}

fn lei_10925_art01_inciso20a() -> Option<&'static str> {
    // XX - peixes e outros produtos classificados nos seguintes códigos da Tipi:
    // a) 03.02, exceto 0302.90.00;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XX, alínea A (Peixes).")
}

fn lei_10925_art01_inciso20b() -> Option<&'static str> {
    // b) 03.03 e 03.04;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XX, alínea B (Peixes).")
}

fn lei_10925_art01_inciso21() -> Option<&'static str> {
    // XXI - café classificado nos códigos 09.01 e 2101.1 da Tipi;
    Some("'Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXI (Café)'.")
}

fn lei_10925_art01_inciso22() -> Option<&'static str> {
    // XXII - açúcar classificado nos códigos 1701.14.00 e 1701.99.00 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXII (Açúcar).")
}

fn lei_10925_art01_inciso23() -> Option<&'static str> {
    // XXIII - óleo de soja classificado na posição 15.07 da Tipi e outros
    // óleos vegetais classificados nas posições 15.08 a 15.14 da Tipi;
    Some(
        "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXIII (Óleo de Soja e Outros Óleos Vegetais).",
    )
}

fn lei_10925_art01_inciso24() -> Option<&'static str> {
    // XXIV - manteiga classificada no código 0405.10.00 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXIV (Manteiga).")
}

fn lei_10925_art01_inciso25() -> Option<&'static str> {
    // XXV - margarina classificada no código 1517.10.00 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXV (Margarina).")
}

fn lei_10925_art01_inciso26() -> Option<&'static str> {
    // XXVI - sabões de toucador classificados no código 3401.11.90 Ex 01 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXVI (Sabões de Toucador).")
}

fn lei_10925_art01_inciso27() -> Option<&'static str> {
    // XXVII - produtos para higiene bucal ou dentária classificados na posição 33.06 da Tipi;
    Some(
        "Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXVII (Produtos para Higiene Bucal ou Dentária).",
    )
}

fn lei_10925_art01_inciso28() -> Option<&'static str> {
    // XXVIII - papel higiênico classificado no código 4818.10.00 da Tipi;
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XXVIII (Papel Higiênico).")
}

#[allow(dead_code)]
fn lei_10925_art01_paragrafo04() -> Option<&'static str> {
    // 	§ 4º - Aplica-se a redução de alíquotas de que trata o caput também à receita bruta decorrente das saídas do
    // estabelecimento industrial, na industrialização por conta e ordem de terceiros dos bens e produtos classificados
    // nas posições 01.03, 01.05, 02.03, 02.06.30.00, 0206.4, 02.07 e 0210.1 da Tipi.
    Some("Alíquota Zero - Lei 10.925/2004, Art. 1º, §4º.")
}

fn decreto_6426_art01(descricao: &str) -> Option<&'static str> {
    /*
     --- Decreto 6.426/2008 ---
    Ver também o Art. 425 da IN RFB 1911/2019

    https://aditivosingredientes.com.br/upload_arquivos/201610/2016100943561001477573436.pdf
    A nisina é a bacteriocina com maior uso comercial, sendo a única reconhecida pela FDA e usada como conservante alimentício (bioconservante).
    EMULPUR/Emulsificante: Lecitina de Soja classificada no código 2923.20.00, veja crédito presumido no art. 31 da Lei 12.865/2013
    http://florien.com.br/wp-content/uploads/2016/06/LECITINA-DE-SOJA.pdf
    Lecitina de Soja: É uma mistura de compostos orgânicos (fosfolipídeos ou fosfatídeos) constituída por um ou
    mais ácidos graxos, ligados a um radical de glicerina, que por sua vez pode estar associado a
    um radical fosfatidilcolina, fosfatidiletanolamina, fosfatidilserina ou fosfatidilinositol.
    https://aditivosingredientes.com.br/upload_arquivos/201604/2016040610112001461594119.pdf
    A lecitina é amplamente utilizada na indústria de alimentos, seja como ingrediente, seja como coemulsificante.
    https://repositorio.ufu.br/bitstream/123456789/15152/1/Diss%20Leticia.pdf
    A Lecitina compõe-se em cerca de 60% de mistura de fostatídeos (colina, etanolamina, e inositol),
    38% de óleo e 2% de umidade segundo Moretto e Fett (1998).
    */

    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)Sucralox|LACTATO DE CALCIO|Lactulose").unwrap());

    if RE.is_match(descricao) {
        Some(
            "Alíquota Zero - Decreto 6.426/2008, Art. 1º, Inciso I e Inciso II (Produtos Químicos relacionados nos Anexos I e II).",
        )
    } else {
        None
    }
}

/**
Lei 10.865/2004 (<https://www.planalto.gov.br/ccivil_03/_ato2004-2006/2004/lei/L10.865.htm#art28>)

Art. 28. Ficam reduzidas a 0 (zero) as alíquotas da contribuição para o PIS/PASEP e da COFINS
incidentes sobre a receita bruta decorrente da venda, no mercado interno, de:

III - produtos hortícolas e frutas, classificados nos Capítulos 7 e 8, e ovos, classificados na posição 04.07, todos da TIPI; e
*/
fn lei_10865_art28_inciso03(ncm: u64) -> Option<&'static str> {
    if (8000000..=8999999).contains(&ncm) {
        Some(
            "Alíquota Zero - Lei 10.865/2004, Art. 28, Inciso III (Frutas classificadas no Capítulo 8).",
        )
    } else if (4070000..=4079999).contains(&ncm) {
        Some(
            "Alíquota Zero - Lei 10.865/2004, Art. 28, Inciso III (Ovos classificados na posição 04.07).",
        )
    } else {
        Some(
            "Alíquota Zero - Lei 10.865/2004, Art. 28, Inciso III (Produtos hortícolas classificados no Capítulo 7).",
        )
    }
}

fn lei_10865_art28_inciso05() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei 10.865/2004, Art. 28, Inciso V (Semens e Embriões da posição 05.11 da NCM).",
    )
}

fn lei_10865_art28_inciso14() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei 10.865/2004, Art. 28, Inciso XIV (Produtos classificados na posição 87.13 da NCM).",
    )
}

/*
Questões interessantes:
https://users.rust-lang.org/t/polars-null-values/63950

use polars::prelude::*;

fn main() -> Result<()> {
    let mut df = CsvReader::from_path("some_file.csv")?
        // replace non utf8 values with �
        .with_encoding(CsvEncoding::LossyString)
        .finish()?;

    // get a hold on the data types
    let dtypes = df.dtypes();

    // pattern match the datatypes and fill missing values with zero
    for (i, dt) in dtypes.iter().enumerate() {
        if let DataType::Int64 | DataType::Float64 = dt {
            df.may_apply_at_idx(i, |s| s.fill_none(FillNoneStrategy::Zero))?;
        }
    }

    Ok(())
}
*/

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

/// Run tests with:
///
/// `cargo test -- --show-output tests_aliquota_zero`
#[cfg(test)]
mod tests_aliquota_zero {
    use crate::{
        JoinResult, adicionar_coluna_de_aliquota_zero, configure_the_environment,
        get_output_as_int32_fields,
    };
    use polars::prelude::*;

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// cargo test -- --show-output test_apply_multiple_columns
    /// - Fonte: ~/.cargo/registry/src/index.crates.io-6f17d22bba15001f/polars-0.30.0/tests/it/lazy/queries.rs
    fn test_apply_multiple_columns() -> Result<(), PolarsError> {
        let df: DataFrame = df!(
            "A"=> [1, 2, 3, 4, 5],
            "fruits"=> ["banana", "banana", "apple", "apple", "banana"],
            "B"=> [5, 4, 3, 2, 1],
            "cars"=> ["beetle", "audi", "beetle", "beetle", "beetle"]
        )?;

        println!("dataframe 1: {df}\n");

        let _multiply_v1 = |s: &mut [Column]| &(&s[0] * &s[0])? * &(&s[1] - 2);

        fn multiply(s: &mut [Column]) -> Result<Column, PolarsError> {
            // Get the fields as Columns
            let col_0: &Column = &s[0];
            let col_1: &Column = &s[1];

            (col_0 * col_0)? * (col_1 - 2)
        }

        /*
        Update:
        geany polars-0.50.0/tests/it/lazy/queries.rs&
        let out = df
            .clone()
            .lazy()
            .select([map_multiple(
                multiply,
                [col("A"), col("B")],
                GetOutput::from_type(DataType::Int32),
            )])
            .collect()?;

        geany polars-0.51.0/tests/it/lazy/queries.rs&
        let out = df
            .clone()
            .lazy()
            .select([map_multiple(
                multiply,
                [col("A"), col("B")],
                |_, f| { Ok(Field::new(f[0].name().clone(), DataType::Int32)) }
            )])
            .collect()?;
        */

        let df: DataFrame = df
            .clone()
            .lazy()
            .with_columns([map_multiple(
                multiply,
                [col("A"), col("B")],
                // GetOutput::from_type(DataType::Int32),
                // |_, f| Ok(Field::new(f[0].name().clone(), DataType::Int32)),
                get_output_as_int32_fields,
            )
            .alias("Map Multiple")])
            .collect()?;

        println!("dataframe 2: {df}\n");

        let out: Vec<i32> = df
            .column("Map Multiple")?
            .i32()?
            .into_iter()
            .flatten()
            .collect();

        println!("out: {out:?}\n");
        assert_eq!(out, &[3, 8, 9, 0, -25]);

        let groupby = df
            .lazy()
            .group_by_stable([col("cars")])
            .agg([apply_multiple(
                multiply,
                [col("A"), col("B")],
                // GetOutput::from_type(DataType::Int32),
                // |_, f| Ok(Field::new(f[0].name().clone(), DataType::Int32)),
                get_output_as_int32_fields,
                false,
            )])
            .sort(["cars"], Default::default())
            .collect()?;

        println!("groupby: {groupby}\n");

        let vec_series: Vec<Series> = groupby.column("A")?.list()?.into_iter().flatten().collect();

        let vec_vec_i32: Vec<Vec<i32>> = vec_series
            .into_iter()
            .map(|series| {
                series
                    .i32()
                    .unwrap()
                    .into_iter()
                    .flatten()
                    .collect::<Vec<_>>()
            })
            .collect();

        println!("vec_vec_i32: {vec_vec_i32:?}\n");

        assert_eq!(vec_vec_i32, &[vec![8], vec![3, 9, 0, -25]]);
        Ok(())
    }

    #[test]
    fn filtrar_coluna() -> JoinResult<()> {
        // cargo test -- --show-output filtrar_coluna

        configure_the_environment();

        // "Tipo de Operação" 1: Entrada, 2: Saída, 3: Ajuste, 4:Desconto, 5: Detalhamento

        let dataframe01: DataFrame = df! [
            "Tipo de Operação" => [1, 2, 4, 1, 6, 2],
            "Valor da Base de Cálculo das Contribuições" => [23.6, 100.3, 15.0, 89.01, 45.41, 200.07],

            "Código NCM"                   => [Some("25.00.1234"), Some("25.00.1234"), Some("22.07.1000"), Some("22.07.1000"), None, None],
            "Código NCM : NF Item (Todos)" => [None, Some(31000000), None, None, Some(22071000), Some(19020000)],

            "Descrição do Item"                                 => ["Sal Grosso", "Sal Refinado", "bla", "bla", "bla", "bla"],
            "Descrição da Mercadoria/Serviço : NF Item (Todos)" => ["bla", "bla", "bla", "bla", "bla", "bla"],
        ]?;

        println!("dataframe01: {dataframe01}\n");

        let lazyframe: LazyFrame = dataframe01.lazy();
        let lazyframe: LazyFrame = adicionar_coluna_de_aliquota_zero(lazyframe)?;

        let dataframe02: DataFrame = lazyframe.collect()?;
        println!("dataframe02: {dataframe02}\n");

        let columns: Vec<&str> = vec!["Alíquota Zero"];

        // Get columns from dataframe
        let cst_values: &Column = dataframe02.column(columns[0])?;

        // Get columns values with into_iter()
        let vec_opt_str: Vec<Option<&str>> = cst_values.str()?.into_iter().collect();

        println!("vec_opt_str: {vec_opt_str:#?}");

        assert_eq!(
            vec_opt_str,
            [
                None,
                Some(
                    "NCM 25.00.1234 : Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso IV (Corretivo de Solo)."
                ),
                None,
                None,
                None,
                Some(
                    "NCM 19020000 : Alíquota Zero - Lei 10.925/2004, Art. 1º, Inciso XVIII (Massas Alimentícias)."
                ),
            ]
        );

        Ok(())
    }
}
