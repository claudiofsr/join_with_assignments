//! Regras de enquadramento legal de Crédito Presumido de PIS e COFINS.
//!
//! Contempla as desonerações e regimes especiais de aproveitamento de crédito na aquisição de insumos de:
//! - Decreto nº 8.533/2015 (Programa Mais Leite Saudável)
//! - Lei nº 12.058/2009 (Cadeia de carne bovina, ovina e caprina)
//! - Lei nº 12.350/2010 (Cadeia de carne suína e avícola)

use regex::Regex;
use std::sync::LazyLock;

/// Base Legal conforme código NCM e descrição do item.
pub fn base_legal_de_credito_presumido(codigo_ncm: u64, descricao: &str) -> Option<&'static str> {
    let especificos: &[u64] = &[
        3029000, // Exceção padrão para descartar conflitos de peixes
    ];

    if especificos.contains(&codigo_ncm) {
        return None;
    }

    match codigo_ncm {
        // Capítulo 4: Leite e derivados líquidos/pó (04.01 a 04.04)
        // Quando convertidos para u64, os NCMs iniciados em '0' perdem o zero à esquerda (7 dígitos)
        ncm @ 4010000..=4049999 => condicoes_ncm_04(descricao, ncm),

        // Posição 01.02 (Bovinos) e 01.04 (Ovinos/Caprinos)
        1020000..=1029999 | 1040000..=1049999 => lei_12058_art33(),

        // Posição 01.03 (Suínos) e 01.05 (Aves)
        1030000..=1039999 | 1050000..=1059999 => lei_12350_art55(),

        _ => None,
    }
}

fn condicoes_ncm_04(descricao: &str, _ncm: u64) -> Option<&'static str> {
    static LEITE_IN_NATURA: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"(?i)Leite (In Natura|Cru)").unwrap());

    if LEITE_IN_NATURA.is_match(descricao) {
        // DECRETO Nº 8.533, DE 30 DE SETEMBRO DE 2015
        // Art. 4º A pessoa jurídica regularmente habilitada, provisória ou definitivamente, ao Programa Mais Leite Saudável poderá descontar créditos presumidos da Contribuição para o PIS/Pasep e da Cofins, (...) calculados sobre o valor do leite in natura adquirido de produtor rural ou recebido de cooperado cooperativa...
        Some(
            "Crédito Presumido - Decreto nº 8.533/2015, Art. 4º, Inciso I (Leite In Natura Utilizado como Insumo - Programa Mais Leite Saudável).",
        )
    } else {
        None
    }
}

fn lei_12058_art33() -> Option<&'static str> {
    // LEI Nº 12.058, DE 13 DE OUTUBRO DE 2009.
    // Art. 33. As pessoas jurídicas sujeitas ao regime de apuração não cumulativa da Contribuição para o PIS/Pasep e da Cofins, inclusive cooperativas,
    // que produzam mercadorias classificadas nos códigos 02.01, 02.02, 02.04, 0206.10.00, 0206.20, 0206.21, 0206.29, 0206.80.00, 0210.20.00,
    // 0506.90.00, 0510.00.10 e 1502.00.1 da NCM, destinadas à exportação, poderão descontar da Contribuição para o PIS/Pasep e da Cofins devidas
    // em cada período de apuração crédito presumido, calculado sobre o valor dos bens classificados nas posições 01.02 e 01.04 da NCM...
    Some(
        "Crédito Presumido - Lei nº 12.058/2009, Art. 33 (Animais Vivos da Posição 01.02 e 01.04: Bovinos, Ovinos ou Caprinos).",
    )
}

fn lei_12350_art55() -> Option<&'static str> {
    // LEI Nº 12.350, DE 20 DE DEZEMBRO DE 2010.
    // Art. 55. As pessoas jurídicas sujeitas ao regime de apuração não cumulativa da Contribuição para o PIS/Pasep e da Cofins, inclusive cooperativas,
    // que produzam mercadorias classificadas nos códigos 02.03, 0206.30.00, 0206.4, 02.07 e 0210.1 da NCM, destinadas a exportação, poderão descontar da
    // Contribuição para o PIS/Pasep e da Cofins devidas em cada período de apuração crédito presumido, calculado sobre:
    // III – o valor dos bens classificados nas posições 01.03 e 01.05 da NCM, adquiridos de pessoa física ou recebidos de cooperado pessoa física.
    Some(
        "Crédito Presumido - Lei nº 12.350/2010, Art. 55 (Animais Vivos da Posição 01.03 e 01.05: Suínos ou Aves).",
    )
}

// ----------------------------------------------------------------------------
// TESTS
// ----------------------------------------------------------------------------

/// Run tests with:
///
/// `cargo test -- --show-output tests_credito_presumido`
#[cfg(test)]
mod tests_credito_presumido {
    use super::*;

    #[test]
    fn test_leite_mais_leite_saudavel() {
        // NCMs iniciados em 0 perdem o zero à esquerda e resultam em 7 dígitos
        assert!(base_legal_de_credito_presumido(4011010, "Leite In Natura").is_some());
        assert!(base_legal_de_credito_presumido(4011010, "Leite Cru").is_some());
        assert!(base_legal_de_credito_presumido(4011010, "Leite UHT").is_none());
    }

    #[test]
    fn test_animais_vivos_bovinos_e_ovinos() {
        assert!(base_legal_de_credito_presumido(1022110, "").is_some()); // Bovinos (7 dígitos)
        assert!(base_legal_de_credito_presumido(1041011, "").is_some()); // Ovinos (7 dígitos)
    }

    #[test]
    fn test_animais_vivos_suinos_e_aves() {
        assert!(base_legal_de_credito_presumido(1031000, "").is_some()); // Suínos (7 dígitos)
        assert!(base_legal_de_credito_presumido(1051110, "").is_some()); // Aves (7 dígitos)
    }

    #[test]
    fn test_nao_credito_presumido() {
        assert!(base_legal_de_credito_presumido(3029000, "").is_none());
        assert!(base_legal_de_credito_presumido(31021010, "").is_none());
    }
}
