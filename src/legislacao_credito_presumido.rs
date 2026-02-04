use regex::Regex;
use std::sync::LazyLock as Lazy;

/// Base Legal conforme código NCM e descrição do item.
pub fn base_legal_de_credito_presumido(codigo_ncm: u64, descricao: &str) -> Option<&'static str> {
    let especificos: [u64; 1] = [
        3029000, // lei_10925_art01_inciso20a()
    ];

    if especificos.contains(&codigo_ncm) {
        return None;
    }

    match codigo_ncm {
        // Observe que o intervalo de ncm (4010000 ..= 4049999) é analisado
        // em diferentes condições conforme a descrição do item.
        // Decreto 8.533/2015, Art 04 ; LEI Nº 10.925/2004, Art 01 incisos 11 e 13
        ncm @ 4010000..=4049999 => condicoes_ncm_04(descricao, ncm),

        1020000..=1029999 | 1040000..=1049999 => lei_12058_art33(),
        1030000..=1039999 | 1050000..=1059999 => lei_12350_art55(),

        _ => None,
    }
}

fn condicoes_ncm_04(descricao: &str, _ncm: u64) -> Option<&'static str> {
    static LEITE_IN_NATURA: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(?i)Leite (In Natura|Cru)").unwrap());

    if LEITE_IN_NATURA.is_match(descricao) {
        // DECRETO Nº 8.533, DE 30 DE SETEMBRO DE 2015
        // Crédito Presumido: aquisição de leite in natura utilizado como insumo - Programa Mais Leite Saudável.
        // Crédito Presumido: aquisição sem pagamento das Constribuições que gera direito a crédito.
        Some(
            "Crédito Presumido - Decreto 8.533/2015, Art. 4º, Inciso I (Leite In Natura Utilizado como Insumo - Programa Mais Leite Saudável).",
        )
    } else {
        None
    }
}

fn lei_12058_art33() -> Option<&'static str> {
    /*
    Crédito Presumido ; artigos 32 a 34 da Lei nº 12.058/2009 ; PJ que industrializa os produtos classificados nas posições 01.02 (Boi) e 01.04 (Ovino ou Caprino) da NCM.
    Ver também IN RFB 977 de 2009 e Tabela 4.3.9
    Art. 33. As pessoas jurídicas sujeitas ao regime de apuração não cumulativa da Contribuição para o PIS/Pasep e da Cofins, inclusive cooperativas, que produzam mercadorias classificadas nos
    códigos 02.01, 02.02, 02.04, 0206.10.00, 0206.20, 0206.21, 0206.29, 0206.80.00, 0210.20.00, 0506.90.00, 0510.00.10 e 1502.00.1 da NCM, destinadas à exportação, poderão descontar da
    Contribuição para o PIS/Pasep e da Cofins devidas em cada período de apuração crédito presumido, calculado sobre o valor dos bens classificados nas posições 01.02 e 01.04 da NCM,
    adquiridos de pessoa física ou recebidos de cooperado pessoa física. (Redação dada pela Lei nº 12.839, de 2013)
    */
    Some("Crédito Presumido - Lei 12.058/2009, Art. 33 (Animais vivos: bovino, ovino ou caprino).")
}

fn lei_12350_art55() -> Option<&'static str> {
    /*
    Crédito Presumido ; artigos 54 a 56 da Lei nº 12.350/2010 ; PJ que industrializa os produtos classificados nas posições 01.03 (Suíno) e 01.05 (Frango e outras aves) da NCM.
    Ver também IN RFB 1157 de 2011 e Tabela 4.3.9
    Art. 55.  As pessoas jurídicas sujeitas ao regime de apuração não cumulativa da Contribuição para o PIS/Pasep e da Cofins, inclusive cooperativas,
    que produzam mercadorias classificadas nos códigos 02.03, 0206.30.00, 0206.4, 02.07 e 0210.1 da NCM, destinadas a exportação, poderão descontar da
    Contribuição para o PIS/Pasep e da Cofins devidas em cada período de apuração crédito presumido, calculado sobre:
    III – o valor dos bens classificados nas posições 01.03 e 01.05 da NCM, adquiridos de pessoa física ou recebidos de cooperado pessoa física.
    */
    Some("Crédito Presumido - Lei 12.350/2010, Art. 55 (Animais vivos: Suíno ou Frango).")
}
