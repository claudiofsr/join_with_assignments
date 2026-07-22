//! Regras de Alíquota Zero de PIS e COFINS.
//!
//! Contempla as principais desonerações tributárias incidentes sobre a comercialização interna
//! e a importação de bens e serviços no Brasil, vigentes em julho de 2026, amparadas pelas seguintes normas:
//! - Decreto nº 6.426/2008 (Reduz a zero as alíquotas de produtos químicos e farmacêuticos relacionados)
//! - Lei nº 10.925/2004 (Desonerações de fertilizantes, sementes, leite, queijos, carnes, farinhas e produtos da cesta básica)
//! - Lei nº 10.865/2004 (Alíquota zero para produtos hortícolas, frutas, ovos, sêmens, aeronaves e embarcações)
//! - Lei nº 13.097/2015 (Desoneração de partes de aerogeradores e pneumáticos para bicicletas)
//! - Lei nº 11.945/2009 (Conversão de suspensão em alíquota zero no regime de Drawback Integrado)
//! - Lei nº 12.649/2012 (Desoneração de tecnologias assistivas e acessibilidade para pessoas com deficiência)
//!
//! **Nota de Transição Constitucional (Reforma Tributária):**
//! Este regime de desonerações vigora até 31/12/2026, sendo integralmente substituído pela
//! CBS (Contribuição sobre Bens e Serviços) e pelo IBS (Imposto sobre Bens e Serviços) em 01/01/2027,
//! conforme cronograma de transição estabelecido pela Lei Complementar nº 214/2025.

use regex::Regex;
use std::sync::LazyLock;

// ============================================================================
// CONSTANTES CENTRALIZADAS
// ============================================================================

/// Lista de NCMs soberanos e exclusivos de semeadura/plantio.
///
/// Amparado pela Lei nº 10.711/2003 e regulamentado pela Lei nº 10.925/2004, Art. 1º, Inciso III.
const NCMS_EXCLUSIVOS_SEMEADURA: &[u64] = &[
    10019100, // Trigo para semeadura (Posição SH 10.01)
    10021000, // Centeio para semeadura (Posição SH 10.02)
    10031000, // Cevada para semeadura (Posição SH 10.03)
    10041000, // Aveia para semeadura (Posição SH 10.04)
    10051000, // Milho para semeadura (Posição SH 10.05)
    10061010, // Arroz em casca para semeadura (Posição SH 10.06)
    10071000, // Sorgo granífero para semeadura (Posição SH 10.07)
    12011000, // Soja para semeadura (Posição SH 12.01)
    12023000, // Amendoim para semeadura (Posição SH 12.02)
    12040010, // Linhaça para semeadura (Posição SH 12.04)
    12051010, // Colza de baixo teor de ácido erúcico para semeadura (Posição SH 12.05)
    12059010, // Outras colzas para semeadura (Posição SH 12.05)
    12060010, // Girassol para semeadura (Posição SH 12.06)
    12072100, // Algodão para semeadura (Posição SH 12.07)
    12074010, // Gergelim para semeadura (Posição SH 12.07)
    12075010, // Mostarda para semeadura (Posição SH 12.07)
    12076010, // Cânhamo para semeadura (Posição SH 12.07)
    12079110, // Cártamo para semeadura (Posição SH 12.07)
    12079910, // Mamona para semeadura (Posição SH 12.07)
];

// ============================================================================
// EXPRESSÕES REGULARES PARA QUALIFICAÇÃO TEXTUAL (VALIDAÇÃO QUALITATIVA)
// ============================================================================

static RE_SEMENTE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Semente|Muda|Semeadura|P/\s*Seme|Sementes|Matriz|Estaca|Porta-enxerto")
        .unwrap()
});

static RE_CORRETIVO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Corretivo|Calc[áa]rio|Gesso|Filito|Enxofre|Mineral").unwrap()
});

static RE_QUEIJOS_TRIB: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(
        r"(?i)creme.*leite|sobremesa|sobr.*qj|gorgonzola|cheddar|roquefort|brie|camembert|fondue",
    )
    .unwrap()
});

static RE_LEITE_FLUIDO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Leite\s*(Fluido Paste|Fluido Industr|Past|UHT|UAT|Pasteurizado|Esterilizado)")
        .unwrap()
});

static RE_LEITE_PO: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Leite\s*(em Po|em Pó|Integral|Semidesnatado|Desnatado|Fermentado)").unwrap()
});

static RE_IOGURTE: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Beb.*Lac|Achocolatado|(Iog|Yogur|Yoghurt|Kefir|Kumys)|Coalhada").unwrap()
});

static RE_FORMULAS_INFANTIS: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Fórmula\s*Infantil|Formula\s*Infantil|F\.Infantil|Nutrição\s*Infantil")
        .unwrap()
});

static RE_SORO_FLUIDO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)Soro").unwrap());
static RE_SORO_PO: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)\b(Pó|Po)\b").unwrap());

static RE_ACHOCOLATADO: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)Beb.*Lac|Achocolatado|Achocolat").unwrap());

static RE_QUEIJOS_VALIDOS_01: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)(QJO|Queijo).*(moza|muss|muça|minas|prato|coalho|ricota|provolone|Parm|fresc|petit suisse|suico|reino|cotage|cottage)").unwrap()
});

static RE_QUEIJOS_VALIDOS_02: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"(?i)Req\b|Requeijão|Requeijao|C.*Cheese|(QJO|Queijo).*(cremoso|uf equil)|ricota")
        .unwrap()
});

static RE_QUIMICOS: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)Sucralox|Sucralose|LACTATO DE CALCIO|Lactulose").unwrap());

static RE_AEROGERADORES: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(?i)Aerogerador|Pá\s+eólica|Eólica|Wind|Hub").unwrap());

static RE_EX_01: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)Ex\s*0?1").unwrap());
static RE_DRAWBACK: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(?i)Drawback").unwrap());

// ============================================================================
// VALIDADOR PRINCIPAL
// ============================================================================

/// Executa o enquadramento de Alíquota Zero de PIS e COFINS com base no NCM e descrição.
///
/// # Argumentos
/// * `codigo_ncm` - Código numérico da Nomenclatura Comum do Mercosul (u64)
/// * `descricao` - Descrição textual do produto para validação qualitativa
///
/// # Retorno
/// Retorna `Some(&str)` contendo a fundamentação legal específica se o item fizer jus
/// ao benefício de Alíquota Zero, ou `None` caso seja tributado normalmente.
pub fn base_legal_de_aliquota_zero(codigo_ncm: u64, descricao: &str) -> Option<&'static str> {
    // Exceções de subprodutos de peixes (Fígados, ovas, barbatanas e despojos da posição 03.02)
    // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea "a" (exclui expressamente tais subprodutos).
    let excecoes_peixe: &[u64] = &[
        3029000, // Código legado: Fígados, ovas e despojos de peixe fresco (0302.90.00)
        3029100, // SH 2022: Fígados, ovas e sêmen (0302.91.00)
        3029200, // SH 2022: Barbatanas de tubarão (0302.92.00)
        3029900, // SH 2022: Outros despojos de peixe fresco (0302.99.00)
    ];

    if excecoes_peixe.contains(&codigo_ncm) {
        return None;
    }

    // Regime aduaneiro especial de Drawback Integrado
    // Base Legal: Lei nº 11.945/2009, Art. 13 (Conversão de suspensão em alíquota zero após exportação).
    if RE_DRAWBACK.is_match(descricao) {
        return lei_11945_art13_drawback();
    }

    match codigo_ncm {
        // Capítulo 31 - Adubos ou Fertilizantes
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso I.
        // Inciso I - adubos ou fertilizantes classificados no Capítulo 31, exceto os produtos de uso veterinário,
        // da Tabela de Incidência do Imposto sobre Produtos Industrializados - TIPI,
        // aprovada pelo Decreto nº 4.542, de 26 de dezembro de 2002, e suas matérias-primas;
        31000000..=31999999 => lei_10925_art01_inciso01(),

        // Posição 38.08 - Defensivos Agropecuários (Fungicidas, Herbicidas, Inseticidas) e matérias-primas
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso II.
        // Inciso II - defensivos agropecuários classificados na posição 38.08 da TIPI e suas matérias-primas;
        38080000..=38089999 | 27075000 => lei_10925_art01_inciso02(),

        // Sementes (exclusivas ou Posição 12.09) e Mudas/Plantas (Posições 06.01 e 06.02)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso III (alinhada à Lei de Sementes nº 10.711/2003).
        // Inciso III - sementes e mudas destinadas à semeadura e plantio, em conformidade com o disposto na Lei nº 10.711,
        // de 5 de agosto de 2003, e produtos de natureza biológica utilizados em sua produção;
        ncm if (6011000..=6029999).contains(&ncm)
            || (12090000..=12099999).contains(&ncm)
            || NCMS_EXCLUSIVOS_SEMEADURA.contains(&ncm) =>
        {
            lei_10925_art01_inciso03(ncm, descricao)
        }

        // Corretivos de solo de origem mineral (Capítulo 25)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso IV.
        // Inciso IV - corretivo de solo de origem mineral classificado no Capítulo 25 da TIPI;
        25000000..=25999999 => lei_10925_art01_inciso04(descricao),

        // Inoculantes agrícolas produzidos a partir de bactérias fixadoras de nitrogênio
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso VI (NCM 3002.90.99 ou 3002.49.99).
        // Inciso VI - inoculantes agrícolas produzidos a partir de bactérias fixadoras de nitrogênio,
        // classificados no código 3002.90.99 da TIPI;
        30024999 | 30029099 => lei_10925_art01_inciso06(),

        // Vacinas de uso veterinário (Posição 30.02)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso VII.
        // Inciso VII - produtos classificados no Código 3002.30 da TIPI;
        30024200 | 30023000..=30023099 => lei_10925_art01_inciso07(),

        // Pintos de um dia (NCM 0105.11)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso X.
        // Inciso X - pintos de 1 (um) dia classificados no código 0105.11 da TIPI;
        1051100..=1051199 | 10511 => lei_10925_art01_inciso10(),

        // Feijão, arroz e farinha de mandioca/sagu
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso V (NCM 0713.33, 1006.20, 1006.30 e 1106.20).
        // Inciso V - produtos classificados nos códigos 0713.33.19, 0713.33.29, 0713.33.99, 1006.20, 1006.30 e 1106.20 da TIPI;
        7133319 | 7133329 | 7133399 | 10062000..=10063099 | 11062000..=11062099 => {
            lei_10925_art01_inciso05()
        }

        // Farinha, grumos, sêmolas e flocos de milho
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso IX (NCM 1102.20, 1103.13 e 1104.19).
        // Inciso IX - farinha, grumos e sêmolas, grãos esmagados ou em flocos, de milho, classificados,
        // respectivamente, nos códigos 1102.20, 1103.13 e 1104.19, todos da TIPI;
        11022000..=11022099 | 11031300..=11031399 | 11041900..=11041999 => {
            lei_10925_art01_inciso09()
        }

        // Leite fluido, leites em pó e soro de leite (Capítulo 04 - Faixa de 7 dígitos devido ao leading zero)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XI e XIII (NCM 04.01 a 04.04).
        // Inciso XI - leite fluido pasteurizado ou industrializado, na forma de ultrapasteurizado, leite em pó, integral,
        // semidesnatado ou desnatado, leite fermentado, bebidas e compostos lácteos e fórmulas infantis,
        // assim definidas conforme previsão legal específica, destinados ao consumo humano ou utilizados na
        // industrialização de produtos que se destinam ao consumo humano;
        // Inciso XIII - soro de leite fluido a ser empregado na industrialização de produtos destinados ao consumo humano.
        ncm if (4010000..=4049999).contains(&ncm) => condicoes_ncm_04(descricao, ncm),

        // Fórmulas infantis (NCM 1901.10.10)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XI.
        // Inciso XI - leite fluido pasteurizado ou industrializado, na forma de ultrapasteurizado, leite em pó,
        // integral, semidesnatado ou desnatado, leite fermentado, bebidas e compostos lácteos e fórmulas infantis,
        // assim definidas conforme previsão legal específica, destinados ao consumo humano ou utilizados na
        // industrialização de produtos que se destinam ao consumo humano;
        19011000..=19011099 => condicoes_formula_infantil(descricao),

        // Bebidas lácteas e achocolatados (NCM 2202.90.00 ou 2202.99.00)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XI.
        // Inciso XI - leite fluido pasteurizado... leite fermentado, bebidas e compostos lácteos...
        // destinados ao consumo humano...
        22029000 | 22029900 => lei_10925_art01_inciso11b(descricao),

        // Queijos tradicionais listados (Capítulo 04 - Faixa de 7 dígitos devido ao leading zero)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XII.
        // Inciso XII - queijos tipo mozarela, minas, prato, queijo de coalho, ricota, requeijão, queijo provolone, queijo parmesão, queijo fresco não maturado e queijo do reino;
        4060000..=4069999 => lei_10925_art01_inciso12(descricao),

        // Farinha de Trigo (NCM 1101.00.10)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XIV.
        // Inciso XIV - farinha de trigo classificada no código 1101.00.10 da Tipi;
        11010010 => lei_10925_art01_inciso14(),

        // Trigo em grãos (Posição 10.01)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XV.
        // Inciso XV - trigo classificado na posição 10.01 da Tipi;
        10010000..=10019999 => lei_10925_art01_inciso15(),

        // Pré-misturas próprias para panificação e pão comum (NCM 1901.20 e 1905.90)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XVI.
        // Inciso XVI - pré-misturas próprias para fabricação de pão comum e pão comum classificados, respectivamente,
        // nos códigos 1901.20.00 Ex 01 e 1905.90.90 Ex 01 da Tipi.
        19012000..=19012099 | 19059090 => lei_10925_art01_inciso16(),

        // Massas alimentícias secas ou frescas (Posição 19.02)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso  XVIII.
        // Inciso XVIII - massas alimentícias classificadas na posição 19.02 da Tipi.
        19020000..=19029999 => lei_10925_art01_inciso18(),

        // Carnes bovinas, ovinas, caprinas e gorduras (Posições 02.01, 02.02, 02.04 e desmembramentos)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea "a" (Faixas de 7 dígitos).
        // Inciso XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal classificados nos
        // seguintes códigos da Tipi: a) 02.01, 02.02, 0206.10.00, 0206.2, 0210.20.00, 0506.90.00, 0510.00.10 e 1502.10.1;
        2010000..=2029999
        | 2062000..=2062999
        | 15021010..=15021019
        | 2061000
        | 2102000
        | 5069000
        | 5100010 => lei_10925_art01_inciso19a(),

        // Carnes suínas e de aves domésticas (Capítulo 02)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea "b" (Faixas de 7 dígitos).
        // Inciso XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal classificados nos
        // seguintes códigos da Tipi: b) 02.03, 0206.30.00, 0206.4, 02.07, 02.09 e 0210.1 e carne de frango
        // classificada nos códigos 0210.99.00;
        2030000..=2039999
        | 2064000..=2064999
        | 2070000..=2079999
        | 2090000..=2099999
        | 2101100..=2101999
        | 2063000
        | 2109900 => lei_10925_art01_inciso19b(),

        // Carnes ovinas, caprinas e miudezas (Posição 02.04)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea "c" (Faixas de 7 dígitos).
        // Inciso XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal
        // classificados nos seguintes códigos da Tipi: c) 02.04 e miudezas comestíveis de ovinos e
        // caprinos classificadas no código 0206.80.00;
        2040000..=2049999 | 2068000 => lei_10925_art01_inciso19c(),

        // Peixes frescos ou resfriados (Posição 03.02 - Faixas de 7 dígitos)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea "a".
        // Inciso XX - peixes e outros produtos classificados nos seguintes códigos da Tipi: a) 03.02, exceto 0302.90.00;
        3020000..=3029999 => lei_10925_art01_inciso20a(),

        // Peixes congelados ou filés (Posições 03.03 e 03.04 - Faixas de 7 dígitos)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea "b".
        // Inciso XX - peixes e outros produtos classificados nos seguintes códigos da Tipi: b) 03.03 e 03.04;
        3030000..=3049999 => lei_10925_art01_inciso20b(),

        // Café em grãos ou solúvel (Posições 09.01 e 21.01)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXI (Faixas de 7 dígitos).
        // Inciso XXI - café classificado nos códigos 09.01 e 2101.1 da Tipi;
        9010000..=9019999 | 21011100..=21011200 => lei_10925_art01_inciso21(),

        // Açúcar de cana ou beterraba (Posição 17.01)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXII.
        // Inciso XXII - açúcar classificado nos códigos 1701.14.00 e 1701.99.00 da Tipi;
        17011400 | 17019900 => lei_10925_art01_inciso22(),

        // Óleo de soja e óleos vegetais refinados (Posições 15.07 a 15.14)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXIII.
        // Inciso XXIII - óleo de soja classificado na posição 15.07 da Tipi e outros óleos vegetais
        // classificados nas posições 15.08 a 15.14 da Tipi;
        15070000..=15149999 => lei_10925_art01_inciso23(),

        // Manteiga industrializada (NCM 0405.10.00 - Faixa de 7 dígitos)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXIV.
        // Inciso XXIV - manteiga classificada no código 0405.10.00 da Tipi;
        4051000 => lei_10925_art01_inciso24(),

        // Margarina comestível (NCM 1517.10.00)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXV.
        // Inciso XXV - margarina classificada no código 1517.10.00 da Tipi;
        15171000 => lei_10925_art01_inciso25(),

        // Sabões de toucador (NCM 3401.11.90 Ex 01)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXVI.
        // Inciso XXVI - sabões de toucador classificados no código 3401.11.90 Ex 01 da Tipi;
        34011190 => lei_10925_art01_inciso26(),

        // Produtos para higiene bucal (Posição 33.06)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXVII.
        // Inciso XXVII - produtos para higiene bucal ou dentária classificados na posição 33.06 da Tipi;
        33060000..=33069999 => lei_10925_art01_inciso27(),

        // Papel higiênico (NCM 4818.10.00)
        // Base Legal: Lei nº 10.925/2004, Art. 1º, Inciso XXVIII.
        // Inciso XXVIII - papel higiênico classificado no código 4818.10.00 da Tipi;
        48181000 => lei_10925_art01_inciso28(),

        // Produtos hortícolas, frutas e ovos frescos (Capítulos 07, 08 e 04)
        // Base Legal: Lei nº 10.865/2004, Art. 28, Inciso III.
        // Inciso III - produtos hortícolas e frutas, classificados nos Capítulos 7 e 8, e ovos,
        // classificados na posição 04.07, todos da TIPI;
        7000000..=8999999 | 4070000..=4079999 => lei_10865_art28_inciso03(codigo_ncm),

        // Sêmens e embriões para reprodução animal (Posição 05.11)
        // Base Legal: Lei nº 10.865/2004, Art. 28, Inciso V (Faixas de 7 dígitos).
        // Inciso XI - semens e embriões da posição 05.11, da NCM.
        5110000..=5119999 => lei_10865_art28_inciso05(),

        // Preparações compostas não alcoólicas - Concentrados (NCM 2106.90.10 Ex 01)
        // Base Legal: Lei nº 10.865/2004, Art. 28, Inciso VII.
        // Inciso XIII – preparações compostas não-alcoólicas, classificadas no código 2106.90.10 Ex 01 da Tipi,
        // destinadas à elaboração de bebidas pelas pessoas jurídicas industriais...
        21069010 => lei_10865_art28_inciso07(descricao),

        // Aeronaves civis e drones profissionais (Posição 88.02 e 8806.10)
        // Base Legal: Lei nº 10.865/2004, Art. 28, Inciso IV.
        // Inciso VI - aeronaves, classificadas na posição 88.02 da NCM;
        88020000..=88029999 | 88061000..=88061099 => lei_10865_art28_inciso04(),

        // Embarcações comerciais ou cargueiras vinculadas ao REB (Capítulo 89)
        // Base Legal: Lei nº 10.865/2004, Art. 28, Inciso X.
        // Inciso I - materiais e equipamentos, inclusive partes, peças e componentes,
        // destinados ao emprego na construção, conservação, modernização, conversão ou reparo de
        // embarcações registradas ou pré-registradas no Registro Especial Brasileiro;
        89010000..=89089999 => lei_10865_art28_inciso10(),

        // Partes para aerogeradores de energia eólica (NCM 8503.00.90 Ex 01)
        // Base Legal: Lei nº 13.097/2015, Art. 1º.
        // XL - produtos classificados no Ex 01 do código 8503.00.90 da Tipi.
        85030090 => lei_13097_art01_aerogeradores(descricao),

        // Pneumáticos e câmaras de ar de borracha para bicicletas (NCM 4011.50 e 4013.20)
        // Base Legal: Lei nº 13.097/2015, Art. 147.
        // Art. 147. Ficam reduzidas a zero as alíquotas das contribuições para PIS/Pasep e Cofins incidentes
        // sobre as receitas de venda dos produtos classificados nos códigos 4011.50.00 e 4013.20.00 da Tipi.
        40115000 | 40132000 => lei_13097_art147_bicicletas(),

        // Tecnologias assistivas, cadeiras de rodas e próteses
        // Base Legal: Lei nº 10.865/2004, Art. 8º, § 12 (conforme alteração da Lei nº 12.649/2012).
        // Inciso XVIII - produtos classificados na posição 87.13 da Nomenclatura Comum do Mercosul - NCM;
        // Inciso XIX - artigos e aparelhos ortopédicos ou para fraturas classificados no código 90.21.10 da NCM;
        // Inciso XX - artigos e aparelhos de próteses classificados no código 90.21.3 da NCM;
        84433222 | 87142000 | 90213980 | 90214000 | 87130000..=87139999 => {
            lei_12649_necessidades_especiais()
        }

        // Compostos químicos orgânicos de uso farmacêutico (Capítulo 29)
        // Base Legal: Decreto nº 6.426/2008, Art. 1º, Inciso I e II.
        // Art. 1º Ficam reduzidas a zero as alíquotas da Contribuição para o PIS/PASEP e da COFINS incidentes
        // sobre a importação e a comercialização no mercado interno dos produtos químicos e farmacêuticos...
        29000000..=29999999 => decreto_6426_art01(descricao),

        _ => None,
    }
}

// ============================================================================
// METADADOS E FUNÇÕES DE FUNDAMENTAÇÃO LEGAL DETALHADA
// ============================================================================

/// Fornece o enquadramento de adubos e fertilizantes do Capítulo 31.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso I.
///
/// **Norma Transcrita:**
/// > "Inciso I - adubos ou fertilizantes classificados no Capítulo 31, exceto os produtos de uso veterinário, da Tabela de Incidência do Imposto sobre Produtos Industrializados - TIPI, aprovada pelo Decreto nº 4.542, de 26 de dezembro de 2002, e suas matérias-primas;"
fn lei_10925_art01_inciso01() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso I (Adubos ou Fertilizantes do Capítulo 31 e suas Matérias-Primas).",
    )
}

/// Fornece o enquadramento de defensivos agrícolas da Posição 38.08 e nafta petroquímica associada.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso II.
///
/// **Norma Transcrita:**
/// > "Inciso II - defensivos agropecuários classificados na posição 38.08 da TIPI e suas matérias-primas;"
fn lei_10925_art01_inciso02() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso II (Defensivos Agropecuários da Posição 38.08 e suas Matérias-Primas).",
    )
}

/// Valida e enquadra sementes e mudas destinadas ao plantio e semeadura regulamentados.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso III, combinada com
/// os parâmetros de controle de qualidade previstos na Lei nº 10.711/2003.
///
/// **Norma Transcrita:**
/// > "Inciso III - sementes e mudas destinadas à semeadura e plantio, em conformidade com o disposto na Lei nº 10.711, de 5 de agosto de 2003, e produtos de natureza biológica utilizados em sua produção;"
fn lei_10925_art01_inciso03(codigo_ncm: u64, descricao: &str) -> Option<&'static str> {
    // Validação qualitativa comum a ambos os grupos
    if !RE_SEMENTE.is_match(descricao) {
        return None;
    }

    // Diferenciação direta: se estiver na faixa do Cap. 06 são mudas, caso contrário são sementes
    if (6011000..=6029999).contains(&codigo_ncm) {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso III (Mudas destinadas ao plantio em conformidade com a Lei nº 10.711/2003).",
        )
    } else {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso III (Sementes destinadas à semeadura em conformidade com a Lei nº 10.711/2003).",
        )
    }
}

/// Valida e enquadra corretores minerais de solo do Capítulo 25.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso IV.
///
/// **Norma Transcrita:**
/// > "Inciso IV - corretivo de solo de origem mineral classificado no Capítulo 25 da TIPI;"
fn lei_10925_art01_inciso04(descricao: &str) -> Option<&'static str> {
    if RE_CORRETIVO.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso IV (Corretivos de Solo de Origem Mineral do Capítulo 25).",
        )
    } else {
        None
    }
}

/// Fornece o enquadramento de alimentos da cesta básica (Feijão, Arroz e farinha de mandioca/sagu).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso V.
///
/// **Norma Transcrita:**
/// > "Inciso V - produtos classificados nos códigos 0713.33.19, 0713.33.29, 0713.33.99, 1006.20, 1006.30 e 1106.20 da TIPI;"
fn lei_10925_art01_inciso05() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso V (Feijão, Arroz e Farinha de Sagu/Mandioca).",
    )
}

/// Fornece o enquadramento de inoculantes agrícolas bacteriológicos.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso VI.
///
/// **Norma Transcrita:**
/// > "Inciso VI - inoculantes agrícolas produzidos a partir de bactérias fixadoras de nitrogênio, classificados no código 3002.90.99 da TIPI;"
fn lei_10925_art01_inciso06() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso VI (Inoculantes Agrícolas para Fixação de Nitrogênio - Posição 30.02).",
    )
}

/// Fornece o enquadramento de vacinas e imunológicos veterinários.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso VII.
///
/// **Norma Transcrita:**
/// > "Inciso VII - produtos classificados no Código 3002.30 da TIPI;"
fn lei_10925_art01_inciso07() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso VII (Vacinas para Medicina Veterinária da Posição 30.02).",
    )
}

/// Fornece o enquadramento de milho em flocos, farinhas, grumos e sêmolas de milho.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso IX.
///
/// **Norma Transcrita:**
/// > "Inciso IX - farinha, grumos e sêmolas, grãos esmagados ou em flocos, de milho, classificados, respectivamente, nos códigos 1102.20, 1103.13 e 1104.19, todos da TIPI;"
fn lei_10925_art01_inciso09() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso IX (Farinha, grumos, sêmolas e grãos esmagados ou em flocos de Milho).",
    )
}

/// Fornece o enquadramento de reprodutores aviários vivos de um dia (Pintos).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso X.
///
/// **Norma Transcrita:**
/// > "Inciso X - pintos de 1 (um) dia classificados no código 0105.11 da TIPI;"
fn lei_10925_art01_inciso10() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso X (Pintos de Um Dia da Posição 01.05).",
    )
}

/// Valida e enquadra leites fluidos pasteurizados/industrializados, em pó ou soro de leite.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XI e XIII.
///
/// **Normas Transcritas:**
/// > "Inciso XI - leite fluido pasteurizado ou industrializado, na forma de ultrapasteurizado, leite em pó, integral, semidesnatado ou desnatado, leite fermentado, bebidas e compostos lácteos e fórmulas infantis, assim definidas conforme previsão legal específica, destinados ao consumo humano ou utilizados na industrialização de produtos que se destinam ao consumo humano;"
/// > "Inciso XIII - soro de leite fluido a ser empregado na industrialização de produtos destinados ao consumo humano."
fn condicoes_ncm_04(descricao: &str, ncm: u64) -> Option<&'static str> {
    if !RE_QUEIJOS_TRIB.is_match(descricao)
        && (RE_LEITE_FLUIDO.is_match(descricao)
            || RE_LEITE_PO.is_match(descricao)
            || RE_IOGURTE.is_match(descricao))
    {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XI (Leites, Leite Fermentado, Bebidas Lácteas e Fórmulas Infantis).",
        )
    } else if ncm == 4041000
        && RE_SORO_FLUIDO.is_match(descricao)
        && !RE_SORO_PO.is_match(descricao)
    {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XIII (Soro de Leite Fluido para Industrialização).",
        )
    } else {
        None
    }
}

/// Valida e enquadra as fórmulas alimentares infantis.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XI.
///
/// **Norma Transcrita:**
/// > "Inciso XI - leite fluido pasteurizado ou industrializado, na forma de ultrapasteurizado, leite em pó... e fórmulas infantis, assim definidas conforme previsão legal específica, destinados ao consumo humano..."
fn condicoes_formula_infantil(descricao: &str) -> Option<&'static str> {
    if RE_FORMULAS_INFANTIS.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XI (Leites, Leite Fermentado, Bebidas Lácteas e Fórmulas Infantis).",
        )
    } else {
        None
    }
}

/// Valida e enquadra compostos lácteos e achocolatados comerciais.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XI.
///
/// **Norma Transcrita:**
/// > "Inciso XI - leite fluido pasteurizado... bebidas e compostos lácteos e fórmulas infantis... destinados ao consumo humano..."
fn lei_10925_art01_inciso11b(descricao: &str) -> Option<&'static str> {
    if RE_ACHOCOLATADO.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XI (Compostos Lácteos, Bebidas Lácteas e Achocolatados).",
        )
    } else {
        None
    }
}

/// Valida e enquadra queijos da cesta básica nacional (Mozarela, Ricota, Minas, etc.).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XII.
///
/// **Norma Transcrita:**
/// > "Inciso XII - queijos tipo mozarela, minas, prato, queijo de coalho, ricota, requeijão, queijo provolone, queijo parmesão, queijo fresco não maturado e queijo do reino;"
fn lei_10925_art01_inciso12(descricao: &str) -> Option<&'static str> {
    if !RE_QUEIJOS_TRIB.is_match(descricao)
        && (RE_QUEIJOS_VALIDOS_01.is_match(descricao) || RE_QUEIJOS_VALIDOS_02.is_match(descricao))
    {
        Some(
            "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XII (Queijos Mozarela, Minas, Prato, Coalho, Ricota e Requeijão).",
        )
    } else {
        None
    }
}

/// Fornece o enquadramento de farinha de trigo (NCM 1101.00.10).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XIV.
///
/// **Norma Transcrita:**
/// > "Inciso XIV - farinha de trigo classificada no código 1101.00.10 da Tipi;"
fn lei_10925_art01_inciso14() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XIV (Farinha de Trigo - Código 1101.00.10).",
    )
}

/// Fornece o enquadramento de trigo em grãos (Posição 10.01).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XV.
///
/// **Norma Transcrita:**
/// > "Inciso XV - trigo classificado na posição 10.01 da Tipi;"
fn lei_10925_art01_inciso15() -> Option<&'static str> {
    Some("Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XV (Trigo da Posição 10.01).")
}

/// Fornece o enquadramento de pré-misturas para panificação e pão francês/comum.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XVI.
///
/// **Norma Transcrita:**
/// > "Inciso XVI - pré-misturas próprias para fabricação de pão comum e pão comum classificados, respectivamente, nos códigos 1901.20.00 Ex 01 e 1905.90.90 Ex 01 da Tipi."
fn lei_10925_art01_inciso16() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XVI (Pré-Misturas para Fabricação de Pão Comum e Pão Comum).",
    )
}

/// Fornece o enquadramento de massas alimentícias da Posição 19.02.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XVIII.
///
/// **Norma Transcrita:**
/// > "Inciso XVIII - massas alimentícias classificadas na posição 19.02 da Tipi."
fn lei_10925_art01_inciso18() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XVIII (Massas Alimentícias da Posição 19.02).",
    )
}

/// Fornece o enquadramento de carnes bovinas, caprinas e ovinas e miudezas (Cesta Básica).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XIX, alínea "a".
///
/// **Norma Transcrita:**
/// > "Inciso XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal classificados nos seguintes códigos da Tipi: a) 02.01, 02.02, 0206.10.00, 0206.2, 0210.20.00, 0506.90.00, 0510.00.10 e 1502.10.1;"
fn lei_10925_art01_inciso19a() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea 'a' (Carnes Bovinas, Ovinas, Caprinas, Miudezas e Gorduras).",
    )
}

/// Fornece o enquadramento de carnes suínas e de aves domésticas (Cesta Básica).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XIX, alínea "b".
///
/// **Norma Transcrita:**
/// > "Inciso XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal classificados nos seguintes códigos da Tipi: b) 02.03, 0206.30.00, 0206.4, 02.07, 02.09 e 0210.1 e carne de frango classificada nos códigos 0210.99.00;"
fn lei_10925_art01_inciso19b() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea 'b' (Carnes Suínas e de Aves de Produção Própria).",
    )
}

/// Fornece o enquadramento de carnes ovinas e caprinas e despojos (Cesta Básica).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XIX, alínea "c".
///
/// **Norma Transcrita:**
/// > "Inciso XIX - carnes bovina, suína, ovina, caprina e de aves e produtos de origem animal classificados nos seguintes códigos da Tipi: c) 02.04 e miudezas comestíveis de ovinos e caprinos classificadas no código 0206.80.00;"
fn lei_10925_art01_inciso19c() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea 'c' (Carnes Ovinas, Caprinas e Miudezas Relacionadas).",
    )
}

/// Fornece o enquadramento de peixes frescos e resfriados da Posição 03.02.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XX, alínea "a".
///
/// **Norma Transcrita:**
/// > "Inciso XX - peixes e outros produtos classificados nos seguintes códigos da Tipi: a) 03.02, exceto 0302.90.00;"
fn lei_10925_art01_inciso20a() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea 'a' (Peixes Frescos ou Resfriados da Posição 03.02).",
    )
}

/// Fornece o enquadramento de peixes congelados ou filés sob as Posições 03.03 e 03.04.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XX, alínea "b".
///
/// **Norma Transcrita:**
/// > "Inciso XX - peixes e outros produtos classificados nos seguintes códigos da Tipi: b) 03.03 e 03.04;"
fn lei_10925_art01_inciso20b() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea 'b' (Peixes Congelados ou Filés das Posições 03.03 e 03.04).",
    )
}

/// Fornece o enquadramento de café torrado, moído ou solúvel.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXI.
///
/// **Norma Transcrita:**
/// > "Inciso XXI - café classificado nos códigos 09.01 e 2101.1 da Tipi;"
fn lei_10925_art01_inciso21() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXI (Café Não Torrado, Torrado ou Moído e Solúvel).",
    )
}

/// Fornece o enquadramento de açúcar refinado ou bruto (NCM 1701.14.00 ou 1701.99.00).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXII.
///
/// **Norma Transcrita:**
/// > "Inciso XXII - açúcar classificado nos códigos 1701.14.00 e 1701.99.00 da Tipi;"
fn lei_10925_art01_inciso22() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXII (Açúcar de Cana ou de Beterraba).",
    )
}

/// Fornece o enquadramento de óleos vegetais destinados à alimentação (soja, canola, etc.).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXIII.
///
/// **Norma Transcrita:**
/// > "Inciso XXIII - óleo de soja classificado na posição 15.07 da Tipi e outros óleos vegetais classificados nas posições 15.08 a 15.14 da Tipi;"
fn lei_10925_art01_inciso23() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXIII (Óleo de Soja e Outros Óleos Vegetais das Posições 15.07 a 15.14).",
    )
}

/// Fornece o enquadramento de manteiga de uso culinário (NCM 0405.10.00).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXIV.
///
/// **Norma Transcrita:**
/// > "Inciso XXIV - manteiga classificada no código 0405.10.00 da Tipi;"
fn lei_10925_art01_inciso24() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXIV (Manteiga do Código 0405.10.00).",
    )
}

/// Fornece o enquadramento de margarina (NCM 1517.10.00).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXV.
///
/// **Norma Transcrita:**
/// > "Inciso XXV - margarina classificada no código 1517.10.00 da Tipi;"
fn lei_10925_art01_inciso25() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXV (Margarina do Código 1517.10.00).",
    )
}

/// Fornece o enquadramento de sabões de toucador sanitários (NCM 3401.11.90 Ex 01).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXVI.
///
/// **Norma Transcrita:**
/// > "Inciso XXVI - sabões de toucador classificados no código 3401.11.90 Ex 01 da Tipi;"
fn lei_10925_art01_inciso26() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXVI (Sabões de Toucador do Código 3401.11.90 Ex 01).",
    )
}

/// Fornece o enquadramento de pastas de dentes e produtos de higiene bucal da Posição 33.06.
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXVII.
///
/// **Norma Transcrita:**
/// > "Inciso XXVII - produtos para higiene bucal ou dentária classificados na posição 33.06 da Tipi;"
fn lei_10925_art01_inciso27() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXVII (Produtos para Higiene Bucal ou Dentária da Posição 33.06).",
    )
}

/// Fornece o enquadramento de papel higiênico de uso doméstico (NCM 4818.10.00).
///
/// **Base Legal:** Lei nº 10.925, de 23 de julho de 2004, Art. 1º, Inciso XXVIII.
///
/// **Norma Transcrita:**
/// > "Inciso XXVIII - papel higiênico classificado no código 4818.10.00 da Tipi;"
fn lei_10925_art01_inciso28() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.925/2004, Art. 1º, Inciso XXVIII (Papel Higiênico do Código 4818.10.00).",
    )
}

/// Valida e enquadra produtos químicos especiais listados.
///
/// **Base Legal:** Decreto nº 6.426/2008, Art. 1º, Inciso I e II.
///
/// **Norma Transcrita:**
/// > "Art. 1º Ficam reduzidas a zero as alíquotas da Contribuição para o PIS/PASEP e da COFINS incidentes sobre a importação e a comercialização no mercado interno dos produtos químicos e farmacêuticos..."
fn decreto_6426_art01(descricao: &str) -> Option<&'static str> {
    if RE_QUIMICOS.is_match(descricao) {
        Some(
            "Alíquota Zero - Decreto nº 6.426/2008, Art. 1º, Inciso I e Inciso II (Produtos Químicos relacionados nos Anexos I e II).",
        )
    } else {
        None
    }
}

/// Fornece o enquadramento de matérias-primas importadas vinculadas ao Drawback Integrado.
///
/// **Base Legal:** Lei nº 11.945, de 2009, Art. 13 (Conversão de suspensão em Alíquota Zero).
///
/// **Norma Transcrita:**
/// > "Art. 12. A aquisição no mercado interno ou a importação, de forma combinada ou não, de mercadoria para emprego ou consumo na industrialização de produto a ser exportado poderá ser realizada com suspensão do Imposto de Importação e do Imposto sobre Produtos Industrializados - IPI."
fn lei_11945_art13_drawback() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 11.945/2009, Art. 13 (Conversão de Suspensão em Alíquota Zero - Drawback Integrado).",
    )
}

/// Valida e enquadra partes e pás eólicas para montagem de aerogeradores.
///
/// **Base Legal:** Lei nº 13.097, de 19 de janeiro de 2015, Art. 1º.
///
/// **Norma Transcrita:**
/// > "Art. 1º ... XL - produtos classificados no Ex 01 do código 8503.00.90 da Tipi, exceto pás eólicas."
fn lei_13097_art01_aerogeradores(descricao: &str) -> Option<&'static str> {
    if RE_AEROGERADORES.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei nº 13.097/2015, Art. 1º (Partes Utilizadas em Aerogeradores - Ex 01 do Código 8503.00.90).",
        )
    } else {
        None
    }
}

/// Fornece o enquadramento de pneumáticos e câmaras de borracha para bicicletas (NCM 4011.50 e 4013.20).
///
/// **Base Legal:** Lei nº 13.097, de 19 de janeiro de 2015, Art. 147.
///
/// **Norma Transcrita:**
/// > "Art. 147. Ficam reduzidas a zero as alíquotas das contribuições para PIS/Pasep e Cofins incidentes sobre as receitas de venda dos produtos classificados nos códigos 4011.50.00 e 4013.20.00 da Tipi."
fn lei_13097_art147_bicicletas() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 13.097/2015, Art. 147 (Pneumáticos e Câmaras de ar de Borracha para Bicicletas).",
    )
}

/// Determina e enquadra produtos hortícolas do Capítulo 07, frutas do Capítulo 08 e ovos da Posição 04.07.
///
/// **Base Legal:** Lei nº 10.865, de 30 de abril de 2004, Art. 28, Inciso III.
///
/// **Norma Transcrita:**
/// > "Inciso III - produtos hortícolas e frutas, classificados nos Capítulos 7 e 8, e ovos, classificados na posição 04.07, todos da TIPI;"
fn lei_10865_art28_inciso03(ncm: u64) -> Option<&'static str> {
    if (8000000..=8999999).contains(&ncm) {
        Some(
            "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso III (Frutas classificadas no Capítulo 8).",
        )
    } else if (4070000..=4079999).contains(&ncm) {
        Some(
            "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso III (Ovos classificados na posição 04.07).",
        )
    } else {
        Some(
            "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso III (Produtos hortícolas classificados no Capítulo 7).",
        )
    }
}

/// Fornece o enquadramento de aeronaves civis e drones comerciais (Posição 88.02 e 8806.10).
///
/// **Base Legal:** Lei nº 10.865, de 30 de abril de 2004, Art. 28, Inciso IV.
///
/// **Norma Transcrita:**
/// > "Inciso VI - aeronaves, classificadas na posição 88.02 da NCM;"
fn lei_10865_art28_inciso04() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso IV (Aeronaves das Posições 88.02 e 8806.10, suas Partes, Peças e Serviços).",
    )
}

/// Fornece o enquadramento de sêmens e embriões congelados para fins agropecuários (Posição 05.11).
///
/// **Base Legal:** Lei nº 10.865, de 30 de abril de 2004, Art. 28, Inciso V.
///
/// **Norma Transcrita:**
/// > "Inciso XI - semens e embriões da posição 05.11, da NCM."
fn lei_10865_art28_inciso05() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso V (Sêmens e Embriões da posição 05.11 da NCM).",
    )
}

/// Validade e enquadra xaropes e extratos concentrados destinados à preparação de refrigerantes (NCM 2106.90.10 Ex 01).
///
/// **Base Legal:** Lei nº 10.865, de 30 de abril de 2004, Art. 28, Inciso VII.
///
/// **Norma Transcrita:**
/// > "Inciso XIII – preparações compostas não-alcoólicas, classificadas no código 2106.90.10 Ex 01 da Tipi, destinadas à elaboração de bebidas pelas pessoas jurídicas industriais..."
fn lei_10865_art28_inciso07(descricao: &str) -> Option<&'static str> {
    if RE_EX_01.is_match(descricao) {
        Some(
            "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso VII (Preparações Compostas Não-Alcoólicas - Ex 01 da Posição 2106.90.10).",
        )
    } else {
        None
    }
}

/// Fornece o enquadramento de peças de estaleiro e insumos navais de embarcações do REB.
///
/// **Base Legal:** Lei nº 10.865, de 30 de abril de 2004, Art. 28, Inciso X.
///
/// **Norma Transcrita:**
/// > "Inciso I - materiais e equipamentos, inclusive partes, peças e componentes, destinados ao emprego na construção, conservação, modernização, conversão ou reparo de embarcações registradas ou pré-registradas no Registro Especial Brasileiro;"
fn lei_10865_art28_inciso10() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.865/2004, Art. 28, Inciso X (Materiais e Equipamentos para Emprego em Embarcações do REB).",
    )
}

/// Fornece o enquadramento de cadeiras de rodas, aparelhos e soluções para fins assistivos a pessoas com necessidades especiais.
///
/// **Base Legal:** Lei nº 10.865, de 30 de abril de 2004, Art. 8º, § 12 (conforme alteração da Lei nº 12.649/2012).
///
/// **Normas Transcritas:**
/// > "Inciso XVIII - produtos classificados na posição 87.13 da Nomenclatura Comum do Mercosul - NCM;"
/// > "Inciso XIX - artigos e aparelhos ortopédicos ou para fraturas classificados no código 90.21.10 da NCM;"
/// > "Inciso XX - artigos e aparelhos de próteses classificados no código 90.21.3 da NCM;"
fn lei_12649_necessidades_especiais() -> Option<&'static str> {
    Some(
        "Alíquota Zero - Lei nº 10.865/2004, Art. 8º, § 12 (Bens destinados a Pessoas com Deficiência Física, Visual ou Auditiva).",
    )
}

// ----------------------------------------------------------------------------
// TESTS
// ----------------------------------------------------------------------------

/// Run tests with:
///
/// `cargo test -- --show-output tests_aliquota_zero`
#[cfg(test)]
mod tests_aliquota_zero {
    use super::*;

    #[test]
    fn test_insumos_agropecuary_e_limites() {
        // Capítulo 31 - Fertilizante (8 dígitos)
        assert!(
            base_legal_de_aliquota_zero(31021010, "").is_some(),
            "FALHA: NCM 31021010 de Fertilizante deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso I."
        );

        // Posição 38.08 - Defensivos Agrícolas
        assert!(
            base_legal_de_aliquota_zero(38089111, "").is_some(),
            "FALHA: Defensivo Agrícola (38089111) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso II."
        );

        // Sementes destinadas à semeadura (Lei nº 10.925/2004, Art. 1º, Inciso III)
        assert!(
            base_legal_de_aliquota_zero(12011000, "Semente de Soja").is_some(),
            "FALHA: Semente de Soja (12011000) não foi enquadrada. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10019100, "Semente de Trigo p/ plantio").is_some(),
            "FALHA: Semente de Trigo (10019100) deve ser enquadrada no benefício. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10051000, "Sementes de Milho Híbrido").is_some(),
            "FALHA: Sementes de Milho para plantio (10051000) devem ser enquadradas. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10031000, "Sementes de Cevada p/ semeadura").is_some(),
            "FALHA: Sementes de Cevada (10031000) devem ser enquadradas. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10041000, "Sementes de Aveia p/ plantio").is_some(),
            "FALHA: Sementes de Aveia (10041000) devem ser enquadradas. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10021000, "Semente de Centeio selecionada").is_some(),
            "FALHA: Sementes de Centeio (10021000) devem ser enquadradas. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10061010, "Arroz em casca para semeadura").is_some(),
            "FALHA: Arroz para semeadura (10061010) deve ser enquadrado. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(10071000, "Sorgo granífero para semeadura").is_some(),
            "FALHA: Sorgo para semeadura (10071000) deve ser enquadrado. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );
        assert!(
            base_legal_de_aliquota_zero(12092100, "Mudas de alfalfa para semeadura").is_some(),
            "FALHA: Mudas/Sementes sob a posição 12.09 (12092100) devem receber o benefício de plantio. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );

        // Descaracterização de Grãos como Sementes de Semeadura
        assert_eq!(
            base_legal_de_aliquota_zero(12011000, "Grãos comerciais de Soja para refino"),
            None,
            "FALHA: Grãos comerciais ordinários não devem receber Alíquota Zero de Sementes. Lei nº 10.925/2004, Art. 1º, Inciso III."
        );

        // Corretivos de Solo (Capítulo 25)
        assert!(
            base_legal_de_aliquota_zero(25210000, "Corretivo de solo calcário").is_some(),
            "FALHA: Corretor calcário do Cap 25 não foi enquadrado. Lei nº 10.925/2004, Art. 1º, Inciso IV."
        );
        assert_eq!(
            base_legal_de_aliquota_zero(25010011, "Sal de cozinha refinado"),
            None,
            "FALHA: Sal de cozinha puro não deve herdar Alíquota Zero de corretor mineral."
        );
    }

    #[test]
    fn test_cesta_basica_graos_e_derivados() {
        // Arroz (8 dígitos)
        assert!(
            base_legal_de_aliquota_zero(10062000, "").is_some(),
            "FALHA: Arroz integral (10062000) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso V."
        );

        // Feijão (7 dígitos)
        assert!(
            base_legal_de_aliquota_zero(7133319, "").is_some(),
            "FALHA: Feijão (0713.33.19 -> 7133319) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso V."
        );

        // Farinha de Mandioca / Sagu
        assert!(
            base_legal_de_aliquota_zero(11062000, "").is_some(),
            "FALHA: Farinha de Mandioca (11062000) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso V."
        );
    }

    #[test]
    fn test_leite_derivados_e_exclusoes_queijo() {
        // Leite Líquido Fluido (7 dígitos)
        assert!(
            base_legal_de_aliquota_zero(4011010, "Leite Fluido Pasteurizado Integral").is_some(),
            "FALHA: Leite fluido (0401.10.10 -> 4011010) não foi enquadrado. Lei nº 10.925/2004, Art. 1º, Inciso XI."
        );

        // Leite em Pó (NCM 0402.10.10 -> 4021010)
        assert!(
            base_legal_de_aliquota_zero(4021010, "Leite em Pó Desnatado").is_some(),
            "FALHA: Leite em pó (0402.10.10 -> 4021010) não foi enquadrado. Lei nº 10.925/2004, Art. 1º, Inciso XI."
        );

        // Fórmulas Infantis (NCM 1901.10.10)
        assert!(
            base_legal_de_aliquota_zero(19011010, "Fórmula Infantil em pó").is_some(),
            "FALHA: Formula Infantil de nutrição humana (19011010) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XI."
        );
        assert_eq!(
            base_legal_de_aliquota_zero(19011010, "Outro de r_derivado lácteo não infantil"),
            None,
            "FALHA: Produtos do Capítulo 19 não qualificados como fórmulas infantis por descrição não possuem Alíquota Zero."
        );

        // Queijo Prato (NCM 0406.10.10 -> 4061010)
        assert!(
            base_legal_de_aliquota_zero(4061010, "Queijo Prato Fatiado").is_some(),
            "FALHA: Queijo Prato (0406.10.10 -> 4061010) não foi enquadrado. Lei nº 10.925/2004, Art. 1º, Inciso XII."
        );

        // Queijo Gorgonzola - Excluído do benefício da cesta básica
        assert_eq!(
            base_legal_de_aliquota_zero(4061010, "Queijo Gorgonzola Tipo Azul"),
            None,
            "FALHA: Queijos nobres como Gorgonzola são excluídos da Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XII."
        );
    }

    #[test]
    fn test_trigo_e_massas_alimenticias() {
        // Farinha de Trigo
        assert!(
            base_legal_de_aliquota_zero(11010010, "").is_some(),
            "FALHA: Farinha de Trigo (11010010) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XIV."
        );

        // Trigo em grão
        assert!(
            base_legal_de_aliquota_zero(10019900, "").is_some(),
            "FALHA: Trigo em grão (10019900) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XV."
        );

        // Massas Alimentícias (Macarrão)
        assert!(
            base_legal_de_aliquota_zero(19021900, "").is_some(),
            "FALHA: Macarrão/Massas (19021900) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso  XVIII."
        );
    }

    #[test]
    fn test_carnes_e_peixes_fronteiras() {
        // Carne Bovina Fresca (NCM 0201.30.00 -> 2013000)
        assert!(
            base_legal_de_aliquota_zero(2013000, "").is_some(),
            "FALHA: Carne Bovina (0201.30.00 -> 2013000) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea 'a'."
        );

        // Carne Suína Congelada (NCM 0203.29.00 -> 2032900)
        assert!(
            base_legal_de_aliquota_zero(2032900, "").is_some(),
            "FALHA: Carne Suína (0203.29.00 -> 2032900) deve possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XIX, alínea 'b'."
        );

        // Peixe Fresco (NCM 0302.11.00 -> 3021100)
        assert!(
            base_legal_de_aliquota_zero(3021100, "").is_some(),
            "FALHA: Peixes Frescos (0302.11.00 -> 3021100) devem possuir Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea 'a'."
        );

        // Peixe Excluído Expressamente (NCM 0302.90.00 -> 3029000)
        assert_eq!(
            base_legal_de_aliquota_zero(3029000, ""),
            None,
            "FALHA: Subprodutos/Desperdícios de Peixe (3029000) são excluídos da Alíquota Zero. Lei nº 10.925/2004, Art. 1º, Inciso XX, alínea 'a'."
        );
    }

    #[test]
    fn test_quimicos_excesso_e_limites() {
        // Sucralose (NCM 2932.14.00)
        assert!(
            base_legal_de_aliquota_zero(29321400, "Sucralox puro").is_some(),
            "FALHA: Sucralose (29321400) não foi enquadrada. Decreto nº 6.426/2008, Art. 1º."
        );

        // Lactato de Cálcio (NCM 2918.11.00)
        assert!(
            base_legal_de_aliquota_zero(29181100, "LACTATO DE CALCIO").is_some(),
            "FALHA: Lactato de Cálcio não foi enquadrado. Decreto nº 6.426/2008, Art. 1º."
        );

        // Químico Genérico do Cap 29 não listado
        assert_eq!(
            base_legal_de_aliquota_zero(29181100, "Outro composto genérico"),
            None,
            "FALHA: Composto químico genérico não listado no anexo do Decreto nº 6.426/2008 não deve ter Alíquota Zero."
        );
    }

    #[test]
    fn test_aerogeradores_e_bicicletas() {
        // Pás eólicas (NCM 8503.00.90 Ex 01)
        assert!(
            base_legal_de_aliquota_zero(85030090, "Pá eólica de aerogerador wind").is_some(),
            "FALHA: Pás de aerogeradores (85030090 Ex 01) devem possuir Alíquota Zero. Lei nº 13.097/2015, Art. 1º."
        );
        assert_eq!(
            base_legal_de_aliquota_zero(85030090, "Partes genéricas de motores elétricos"),
            None,
            "FALHA: Partes genéricas que não pertencem ao Ex 01 de aerogeradores são tributadas normalmente."
        );

        // Pneus de bicicletas (NCM 4011.50.00)
        assert!(
            base_legal_de_aliquota_zero(40115000, "").is_some(),
            "FALHA: Pneus de bicicletas (40115000) devem possuir Alíquota Zero. Lei nº 13.097/2015, Art. 147."
        );

        // Câmaras de ar de bicicletas (NCM 4013.20.00)
        assert!(
            base_legal_de_aliquota_zero(40132000, "").is_some(),
            "FALHA: Câmaras de ar de bicicletas (40132000) devem possuir Alíquota Zero. Lei nº 13.097/2015, Art. 147."
        );
    }

    #[test]
    fn test_frutas_horticolas_e_ovos() {
        // Hortícolas (Capítulo 7 -> 7 dígitos)
        assert!(
            base_legal_de_aliquota_zero(7011000, "Batata fresca").is_some(),
            "FALHA: Batatas frescas/hortícolas (0701.10.00 -> 7011000) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso III."
        );

        // Batata-Doce (0714.20.00 -> 7142000)
        assert!(
            base_legal_de_aliquota_zero(7142000, "Batata-doce in natura").is_some(),
            "FALHA: Batata-doce (0714.20.00 -> 7142000) deve possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso III."
        );

        // Frutas (Capítulo 8 -> 7 dígitos)
        assert!(
            base_legal_de_aliquota_zero(8081000, "Maçã fresca").is_some(),
            "FALHA: Maçãs frescas/frutas (0808.10.00 -> 8081000) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso III."
        );

        // Ovos frescos (NCM 0407.21.00 -> 4072100)
        assert!(
            base_legal_de_aliquota_zero(4072100, "").is_some(),
            "FALHA: Ovos frescos (0407.21.00 -> 4072100) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso III."
        );
    }

    #[test]
    fn test_aeronaves_e_embarcacoes() {
        // Helicóptero comercial (NCM 8802.11.00)
        assert!(
            base_legal_de_aliquota_zero(88021100, "").is_some(),
            "FALHA: Aeronaves (88021100) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso IV."
        );

        // Drone / VANT (NCM 8806.10.00)
        assert!(
            base_legal_de_aliquota_zero(88061000, "").is_some(),
            "FALHA: Drones de uso profissional (88061000) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso IV."
        );

        // Navios e Embarcações (NCM 8901.20.00)
        assert!(
            base_legal_de_aliquota_zero(89012000, "").is_some(),
            "FALHA: Embarcações (89012000) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso X."
        );
    }

    #[test]
    fn test_bebidas_frias_preparacoes_compostas() {
        // Preparações compostas Ex 01 (NCM 2106.90.10 Ex 01)
        assert!(
            base_legal_de_aliquota_zero(21069010, "Concentrado para refrigerante Ex 01").is_some(),
            "FALHA: Preparações Ex 01 devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso VII."
        );

        // Preparações compostas Ex 02 (Tributada na monofasia)
        assert_eq!(
            base_legal_de_aliquota_zero(21069010, "Bebida pronta Ex 02"),
            None,
            "FALHA: Preparações Ex 02 não possuem Alíquota Zero. Lei nº 10.865/2004, Art. 28, Inciso VII."
        );
    }

    #[test]
    fn test_bens_de_acessibilidade() {
        // Impressoras braille (NCM 8443.32.22)
        assert!(
            base_legal_de_aliquota_zero(84433222, "").is_some(),
            "FALHA: Impressoras Braille (84433222) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 8º, § 12."
        );

        // Aparelhos auditivos (NCM 9021.40.00)
        assert!(
            base_legal_de_aliquota_zero(90214000, "").is_some(),
            "FALHA: Aparelhos auditivos (90214000) devem possuir Alíquota Zero. Lei nº 10.865/2004, Art. 8º, § 12."
        );
    }

    #[test]
    fn test_drawback_integrado() {
        assert!(
            base_legal_de_aliquota_zero(
                99999999,
                "Importação de chapas sob amparo do Drawback integrado"
            )
            .is_some(),
            "FALHA: Operações vinculadas ao Drawback devem herdar o enquadramento de suspensão convertida em Alíquota Zero. Lei nº 11.945/2009, Art. 13."
        );
    }
}
