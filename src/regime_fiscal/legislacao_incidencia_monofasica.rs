//! Regras de enquadramento legal da Incidência Monofásica (Tributação Concentrada) de PIS e COFINS.
//!
//! Vigente em julho de 2026.
//! Nota de Transição: Este regime será extinto em 01/01/2027 pela LC nº 214/2025, sendo substituído pela CBS/IBS.
//!
//! Contempla as cadeias de incidência concentrada com base nas normas federais consolidadas:
//! - Lei nº 9.718/1998 (Combustíveis e Álcool - com alterações da LC nº 214/2025)
//! - Lei nº 10.147/2000 (Produtos Farmacêuticos, Cosméticos e Higiene Pessoal)
//! - Lei nº 10.485/2002 (Máquinas, Veículos e Autopeças)
//! - Lei nº 10.560/2002 (Querosene de Aviação)
//! - Lei nº 11.116/2005 (Biodiesel)
//! - Medida Provisória nº 2.158-35/2001, Art. 43 (Motocicletas)
//! - Lei nº 13.097/2015, Art. 14 (Bebidas Frias) e Art. 147 (Incentivo a Bicicletas)

/// Avalia o código NCM, retornando a fundamentação legal e descrição da regra monofásica correspondente.
pub fn base_legal_de_incidencia_monofasica(
    codigo_ncm: u64,
    _descricao: &str,
) -> Option<&'static str> {
    // ====================================================================
    // EXCEÇÕES LEGAIS EXPLÍCITAS (DESCARACTERIZAÇÃO DO REGIME MONOFÁSICO)
    // ====================================================================
    // Determinados códigos, embora inseridos em posições monofásicas, são excluídos
    // expressamente pelo caput das leis de regência, retornando ao regime geral de apuração:
    // - Posições 30.03 e 30.04: Medicamentos com função carboxiamida (Lei nº 10.147/2000, Art. 1º, caput)
    // - Código 2710.12.51: Gasolina de aviação (excluída pelo Art. 4º, I, da Lei nº 9.718/1998)
    let exclusoes_monofasico: &[u64] = &[
        30039056, // Exceção da posição 30.03 (Medicamentos contendo outros produtos)
        30049046, // Exceção da posição 30.04 (Medicamentos contendo outros produtos)
        27101251, // Exclusão expressa de 2710.12.51 (Gasolina de aviação, Lei nº 9.718/1998)
    ];

    if exclusoes_monofasico.contains(&codigo_ncm) {
        return None;
    }

    match codigo_ncm {
        // ====================================================================
        // 1. COMBUSTÍVEIS E DERIVADOS DE PETRÓLEO (Lei nº 9.718/1998)
        // ====================================================================
        // Regula a tributação concentrada na ponta das refinarias, usinas e importadores.
        // O varejo opera com alíquota zero nas operações subsequentes.

        // Gasolinas e suas correntes, exceto de aviação (Art. 4º, I)
        27101259 => Some(
            "Incidência Monofásica - Lei nº 9.718/1998, Art. 4º, Inciso I (Gasolinas e suas correntes, exceto gasolina de aviação).",
        ),

        // Óleo Diesel e suas correntes (Art. 4º, II)
        // Abrange o óleo diesel fóssil padrão e as misturas contendo biodiesel (NCM 2710.20.00)
        27101921 | 27102000 => Some(
            "Incidência Monofásica - Lei nº 9.718/1998, Art. 4º, Inciso II (Óleo Diesel e suas correntes).",
        ),

        // Gás Liquefeito de Petróleo (GLP) e Gás Liquefeito de Gás Natural (GLGN) (Art. 4º, III)
        // Inclui o gás derivado de xisto/petróleo (2711.19.10) e o gás natural liquefeito (2711.11.00)
        27111910 | 27111100 => Some(
            "Incidência Monofásica - Lei nº 9.718/1998, Art. 4º, Inciso III (Gás Liquefeito de Petróleo - GLP / GLGN).",
        ),

        // Querosene de Aviação (Lei nº 10.560/2002, Art. 2º)
        27101911 => Some(
            "Incidência Monofásica - Lei nº 10.560/2002, Art. 2º (Querosene de Aviação).",
        ),

        // Biodiesel (Lei nº 11.116/2005, Art. 3º)
        // Aplicável sobre o biodiesel puro (B100) classificado na posição 3826.00.00
        38260000 => Some("Incidência Monofásica - Lei nº 11.116/2005, Art. 3º (Biodiesel)."),

        // Álcool/Etanol, inclusive para fins carburantes (Art. 5º - Redação dada pela LC nº 214/2025)
        // Abrange álcool etílico desnaturalizado/não desnaturalizado com qualquer teor (posições 22.07 e 22.08)
        22071000..=22072099 | 22089000 => Some(
            "Incidência Monofásica - Lei nº 9.718/1998, Art. 5º (Etanol hidratado ou anidro, conforme redação da Lei Complementar nº 214/2025).",
        ),

        // ====================================================================
        // 2. PRODUTOS FARMACÊUTICOS (Lei nº 10.147/2000, Art. 1º, I, "a")
        // ====================================================================
        // DIDÁTICA FISCAL: O legislador definiu que os medicamentos acabados das posições
        // 30.01, 30.03 e 30.04 têm tributação concentrada no fabricante/importador, permitindo
        // a desoneração da cadeia varejista e de distribuição através da Alíquota Zero (Art. 2º).

        // Medicamentos sob as posições inteiras 30.01, 30.03 e 30.04 (exceto as exclusões do caput)
        30010000..=30019999 | 30030000..=30039999 | 30040000..=30049999 => Some(
            "Incidência Monofásica - Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'a' (Produtos Farmacêuticos).",
        ),

        // DIDÁTICA DE ATUALIZAÇÃO NCM: Com a extinção do desdobramento "3002.10" na NCM/SH,
        // os subprodutos imunológicos foram migrados para subposições específicas como 3002.12 a 3002.15.
        // Da mesma forma, os reagentes de diagnóstico que figuravam em 30.06 migraram para a posição 38.22.
        // O mapeamento abaixo garante a integridade histórica (pré-2022) e moderna (pós-2022) do direito fiscal.

        // Antigos itens 3002.10.1, 3002.10.2 e 3002.10.3 (Antissoros e frações)
        30021010..=30021039
        // Novas subposições correspondentes criadas após extinção de 3002.10 (NCM 2022)
        | 30021211..=30021239 // Imunoglobulinas e frações de sangue modificadas
        | 30021300            // Anticorpos monoclonais
        | 30021411..=30021490 // Produtos imunológicos misturados
        | 30021511..=30021590 // Produtos imunológicos não misturados
        // Vacinas antigas (itens 3002.20.1 e 3002.20.2) e novas (subposições 3002.41 e 3002.42)
        | 30022010..=30022029
        | 30024111..=30024190 // Vacinas para medicina humana
        | 30024210..=30024290 // Vacinas para medicina veterinária
        // Outros itens específicos da posição 30.02
        | 30029020            // Toxinas
        | 30029092            // Culturas de microrganismos
        | 30029099            // Outros produtos biológicos
        // Itens específicos das posições 30.05 e 30.06
        | 30051010            // Pensos adesivos
        | 30063011..=30063029 // Preparações opacas e reagentes de diagnóstico
        | 30066000            // Preparações químicas anticoncepcionais
        // Reagentes de diagnóstico migrados de 30.06 para a posição 38.22 na NCM atualizada
        | 38221100..=38221990 => Some(
            "Incidência Monofásica - Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'a' (Medicamentos, Vacinas, Reagentes e Imunológicos específicos).",
        ),

        // ====================================================================
        // 3. PERFUMARIA, TOUCADOR E HIGIENE PESSOAL (Lei nº 10.147/2000, Art. 1º, I, "b")
        // ====================================================================
        // DIDÁTICA FISCAL: Estão sujeitos ao regime os produtos de beleza, maquiagem e cuidados
        // com a pele (33.03 a 33.07). No entanto, a posição 33.06 (Higiene bucal/fio dental) foi
        // expressamente excetuada pelo caput da lei, retornando ao regime geral de tributação.

        // Cosméticos e Perfumaria (33.03 a 33.07, exceto 33.06 - Higiene bucal)
        33030000..=33059999 | 33070000..=33079999 => Some(
            "Incidência Monofásica - Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'b' (Perfumaria e Cosméticos).",
        ),

        // Sabonetes, Sabões de toucador e Escovas de dentes
        // (Observação: o sabonete medicinal classificado no desdobramento 3401.11.90 Ex 01 é tributação geral)
        34011190 | 34012010 | 96032100 => Some(
            "Incidência Monofásica - Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'b' (Higiene Pessoal).",
        ),

        // ====================================================================
        // 4. PNEUMÁTICOS E CÂMARAS DE AR (Lei nº 10.485/2002 e Lei nº 13.097/2015)
        // ====================================================================
        //
        // TRATAMENTO TRIBUTÁRIO DE ACORDO COM A GEOGRAFIA E CONDIÇÕES DE PRODUÇÃO:
        //
        // TRATAMENTO A) REGRA GERAL (OUTRAS REGIÕES / SEM REQUISITOS ESPECÍFICOS DA REGIÃO NORTE)
        // -> REGIME: Incidência Monofásica (Tributação Concentrada).
        // -> EMBASAMENTO LEGAL:
        //    - Lei nº 10.485, de 3 de julho de 2002, Art. 5º, caput e parágrafo único.
        // -> ALÍQUOTAS DO REGIME MONOFÁSICO:
        //    - Fabricante/Importador (Origem): 2% de PIS e 9,5% de COFINS (tributação concentrada).
        //    - Revendedores (Atacado e Varejo): Alíquota reduzida a 0% (zero por cento) por força do
        //      parágrafo único do Art. 5º da Lei nº 10.485/2002.
        // -> APLICAÇÃO: Aplica-se a todos os pneumáticos novos (posição 40.11) e câmaras de ar
        //    (posição 40.13) fabricados fora das condições específicas da Região Norte, incluindo
        //    modelos de bicicletas (4011.50.00 e 4013.20.00) produzidos em outras regiões.
        //
        // TRATAMENTO B) REGIME INCENTIVADO DA REGIÃO NORTE (ZONA FRANCA DE MANAUS)
        // -> REGIME: Alíquota Zero na Origem (Tratado fora desta função monofásica).
        // -> EMBASAMENTO LEGAL:
        //    - Lei nº 13.097, de 19 de janeiro de 2015, Art. 147, caput e parágrafo único.
        // -> REQUISITOS ACUMULATIVOS DE APLICAÇÃO (Parágrafo Único do Art. 147):
        //    1. Venda realizada por pessoa jurídica fabricante com estabelecimento implantado
        //       na Zona Franca de Manaus (ZFM);
        //    2. Atendimento ao Processo Produtivo Básico (PPB) fixado em legislação específica;
        //    3. Utilização obrigatória de borracha natural produzida por extrativismo não madeireiro
        //       na Região Norte.
        // -> ALÍQUOTAS: Reduzidas a 0% (zero por cento) na própria saída da indústria (caput do Art. 147),
        //    descaracterizando o recolhimento das alíquotas monofásicas concentradas de 2% e 9,5% na origem.

        // Pneus novos de borracha em geral (abrange subposições da posição 40.11 da TIPI)
        40110000..=40119999 => Some(
            "Incidência Monofásica - Lei nº 10.485/2002, Art. 5º (Pneus novos de borracha).",
        ),

        // Câmaras-de-ar de borracha em geral (abrange subposições da posição 40.13 da TIPI)
        40130000..=40139999 => Some(
            "Incidência Monofásica - Lei nº 10.485/2002, Art. 5º (Câmaras-de-ar de borracha).",
        ),

        // ====================================================================
        // 5. BEBIDAS FRIAS (Lei nº 13.097/2015, Art. 14)
        // ====================================================================
        // DIDÁTICA FISCAL: O novo regime de bebidas frias unificou as regras e concentrou
        // os pagamentos na saída do fabricante ou do importador. O comércio atacadista e varejista
        // usufrui de alíquota zero nas operações de revenda (Art. 28 da Lei nº 13.097/2015).
        22010000..=22039999 | 21069010 => Some(
            "Incidência Monofásica - Lei nº 13.097/2015, Art. 14 (Bebidas Frias: Águas, Cervejas, Refrigerantes e Energéticos).",
        ),

        // ====================================================================
        // 6. VEÍCULOS, TRATORES E MÁQUINAS AUTOPROPULSADAS (Lei nº 10.485/2002, Art. 1º)
        // ====================================================================
        // DIDÁTICA FISCAL: Incidência monofásica com alíquotas de 2% (PIS) e 9,6% (COFINS) no
        // fabricante/importador de veículos pesados, tratores, ônibus e automóveis de passeio.
        // Os demais revendedores do varejo automotivo aplicam alíquota zero nas operações de revenda.
        73090000..=73090099   // Reservatórios de ferro ou aço (> 300 litros)
        | 73102900..=73102990 // Reservatórios de ferro ou aço (< 300 litros)
        | 76129012            // Recipientes de alumínio
        | 84248111..=84248129 // Antigos aparelhos agrícolas/hortícolas de pulverização (NCM pré-2022)
        | 84248200..=84248290 // Aparelhos agrícolas/hortícolas de pulverização atualizados (NCM 2022)
        | 84290000..=84299999 // Bulldozers, niveladoras, escavadeiras
        | 84306990            // Outras máquinas de terraplenagem não autopropulsadas
        | 84320000..=84379999 // Máquinas agrícolas, hortícolas ou florestais (Abrange partes 8432.90.00 e 8433.90.90)
        | 87010000..=87069999 // Tratores, veículos automóveis de passageiros, carga e chassis com motor
        | 87162000 => Some(
            "Incidência Monofásica - Lei nº 10.485/2002, Art. 1º (Máquinas, Implementos e Veículos Autoveiculares).",
        ),

        // Motocicletas (Medida Provisória nº 2.158-35/2001, Art. 43)
        // Enquadramento concentrado específico da cadeia de duas rodas da posição 87.11
        87110000..=87119999 => Some(
            "Incidência Monofásica - Medida Provisória nº 2.158-35/2001, Art. 43 (Motocicletas e Ciclomotores da Posição 87.11).",
        ),

        // ====================================================================
        // 7. AUTOPEÇAS (Lei nº 10.485/2002, Art. 3º e Anexos I e II)
        // ====================================================================
        // DIDÁTICA FISCAL: O legislador estabeleceu um regime monofásico diferenciado para autopeças.
        // Nas vendas destinadas a montadoras de veículos (Art. 1º da Lei nº 10.485/2002), aplica-se
        // o regime de alíquotas reduzidas (1,65% de PIS / 7,6% de COFINS) para evitar o acúmulo de créditos na cadeia.
        // No entanto, nas vendas destinadas à reposição comercial direta (atacadistas, varejistas e oficinas),
        // as alíquotas monofásicas cheias são aplicadas na origem (2,3% de PIS / 10,8% de COFINS), e a revenda goza
        // de alíquota zero (§ 2º do Art. 3º).
        //
        // Nota Técnica de Omissão: As partes de máquinas agrícolas dos códigos 8432.90.00 e 8433.90.90 foram
        // omitidas desta lista porque já se encontram devidamente englobadas pelo intervalo abrangente
        // 84320000..=84379999 da Seção 6.
        40090000..=40099999   // Tubos de borracha vulcanizada (Anexo II, Item 1)
        | 40161010            // Partes de veículos de borracha alveolar
        | 40169990            // Outras obras de borracha vulcanizada (Ex 03 e 05)
        | 68130000..=68139999 // Guarnições de fricção (pastilhas, lonas) para freios/embreagens
        | 70071100            // Vidros temperados para veículos terrestres, aeronaves ou embarcações
        | 70072100            // Vidros laminados para veículos terrestres, aeronaves ou embarcações
        | 70091000            // Espelhos retrovisores externos e internos para veículos
        | 73201000            // Molas de folhas e suas folhas, de ferro ou aço (Ex 01)
        | 83012000            // Fechaduras do tipo utilizado em veículos automotores
        | 83023000            // Guarnições, ferragens e artigos semelhantes para veículos automotores
        | 84073390            // Motores alternativos de ignição por centelha (> 250cc e <= 1000cc)
        | 84073490            // Motores alternativos de ignição por centelha (> 1000cc)
        | 84082000..=84082090 // Motores diesel/semidiesel para propulsão de veículos do Cap. 87
        | 84089090            // Outros motores diesel (utilizados em colheitadeiras/tratores - Anexo II, Item 3)
        | 84099100..=84099199 // Partes reconhecíveis como exclusivas para motores de ignição por centelha
        | 84099900..=84099999 // Outras partes para motores alternativos a pistão (diesel ou gás)
        | 84122110            // Cilindros hidráulicos (utilizados em máquinas agrícolas - Anexo II, Item 4)
        | 84122190            // Outros motores hidráulicos lineares (utilizados em máquinas agrícolas - Anexo II, Item 5)
        | 84123110            // Cilindros pneumáticos (utilizados em caminhões/ônibus - Anexo II, Item 6)
        | 84133000..=84133090 // Bombas de combustível, óleo ou arrefecimento para motores alternativos
        | 84136019            // Bombas volumétricas rotativas (utilizadas em tratores - Anexo II, Item 7)
        | 84139100            // Partes de bombas de líquidos (Ex 01 do Anexo I)
        | 84148019            // Compressores de ar alternativos (utilizados em máquinas agrícolas - Anexo II, Item 8)
        | 84148021            // Turbocompressores de ar para motores alternativos de ignição por centelha
        | 84148022            // Outros turbocompressores de ar para motores alternativos
        | 84149039            // Partes de compressores/exaustores (caixas de ventilação - Anexo II, Item 9)
        | 84152000..=84152090 // Aparelhos de ar-condicionado para veículos automotores
        | 84212300            // Filtros de óleo ou de combustível para motores alternativos
        | 84213100            // Filtros de entrada de ar para motores de combustão interna
        | 84314100            // Caçambas, garras, colheres e tenazes para escavadeiras (Anexo II, Item 2)
        | 84314200            // Lâminas para bulldozers ou angledozer (Anexo II, Item 2)
        | 84811000            // Válvulas redutoras de pressão (Anexo II, Item 11)
        | 84812090            // Válvulas para transmissões óleo-hidráulicas ou pneumáticas (Anexo II, Item 12)
        | 84818092            // Válvulas solenoides (Anexo II, Item 13)
        | 84818099            // Outros dispositivos de canalizações (Ex 01 e 02 do Anexo I)
        | 84831000..=84835090 // Árvores de transmissão, mancais, engrenagens, volantes e polias do Anexo I
        | 84836011..=84836019 // Embreagens de fricção (para máquinas agrícolas - Anexo II, Item 14)
        | 85011019            // Outros motores de corrente contínua (para acionamento elétrico de vidros - Anexo II, Item 15)
        | 85052000            // Embreagens, acoplamentos e freios eletromagnéticos de uso veicular
        | 85071000..=85071090 // Acumuladores de chumbo (baterias) para arranque de motores alternativos
        | 85110000..=85129999 // Aparelhos de ignição, arranque, iluminação e sinalização para motores e veículos
        | 85272100..=85272900 // Aparelhos receptores de radiodifusão para veículos automotores (rádios de painel)
        | 85365090            // Interruptores e comutadores elétricos para painéis (Ex 01 do Anexo I)
        | 85391000..=85391090 // Projetores do tipo "projectores monobloco" (sealed beam)
        | 85443000            // Jogos de fios para velas de ignição e chicotes para veículos e aeronaves
        | 90292010            // Indicadores de velocidade (velocímetros) e tacômetros (contagiros)
        | 90299010            // Partes e acessórios de velocímetros e tacômetros
        | 90303921            // Aparelhos de medição/controle de grandezas elétricas para veículos
        | 90318040            // Aparelhos para controle de rodas ou eixos (alinhar/balancear)
        | 90328921..=90328929 // Reguladores de voltagem elétricos ou eletrônicos (veiculares)
        | 91040000            // Relógios para painéis de instrumentos de veículos
        | 94012000 => Some(
            "Incidência Monofásica - Lei nº 10.485/2002, Art. 3º (Autopeças Relacionadas nos Anexos I e II).",
        ),

        _ => None,
    }
}

// ----------------------------------------------------------------------------
// TESTS
// ----------------------------------------------------------------------------

/// Suíte de testes unitários dedicada à validação de limites, exceções e minúcias do regime.
/// Em caso de falha, os testes retornam a fundamentação legal e o dispositivo violado.
///
/// Run tests with:
/// `cargo test -- --show-output tests_incidencia_monofasica`
#[cfg(test)]
mod tests_incidencia_monofasica {
    use super::*;

    #[test]
    fn test_exclusao_gasolina_aviacao() {
        // NCM de Gasolina de Aviação deve retornar None pelo bloco de exclusões
        assert_eq!(base_legal_de_incidencia_monofasica(27101251, ""), None);
    }

    #[test]
    fn test_validacao_gasolina_automotiva() {
        // NCM de Gasolina automotiva deve ser devidamente enquadrado
        let result = base_legal_de_incidencia_monofasica(27101259, "");
        assert!(result.is_some());
        assert!(
            result
                .unwrap()
                .contains("Lei nº 9.718/1998, Art. 4º, Inciso I")
        );
    }

    #[test]
    fn test_exclusoes_farma() {
        // NCMs com carboxiamida sob as posições 30.03 e 30.04 devem retornar None
        assert_eq!(base_legal_de_incidencia_monofasica(30039056, ""), None);
        assert_eq!(base_legal_de_incidencia_monofasica(30049046, ""), None);
    }

    #[test]
    fn test_etanol_2025_2027() {
        let result = base_legal_de_incidencia_monofasica(22071010, "");
        assert!(result.is_some());
        assert!(result.unwrap().contains("Lei Complementar nº 214/2025"));
    }

    #[test]
    fn test_combustiveis() {
        assert!(base_legal_de_incidencia_monofasica(27101259, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(27101921, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(27111910, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(27101911, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(38260000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(22071000, "").is_some());
    }

    #[test]
    fn test_farmaceuticos_posicoes_inteiras() {
        assert!(base_legal_de_incidencia_monofasica(30010000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(30019090, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(30032000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(30041010, "").is_some());
    }

    #[test]
    fn test_farmaceuticos_excecoes() {
        assert!(base_legal_de_incidencia_monofasica(30039056, "").is_none());
        assert!(base_legal_de_incidencia_monofasica(30049046, "").is_none());
    }

    #[test]
    fn test_farmaceuticos_itens_especificos_modernos() {
        // Teste de compatibilidade histórica e migração NCM 2022
        assert!(base_legal_de_incidencia_monofasica(30021015, "").is_some()); // Antigo
        assert!(base_legal_de_incidencia_monofasica(30021215, "").is_some()); // Novo (Imunoglobulinas)
        assert!(base_legal_de_incidencia_monofasica(30021300, "").is_some()); // Novo (Anticorpos Monoclonais)
        assert!(base_legal_de_incidencia_monofasica(30024115, "").is_some()); // Novo (Vacina Humana)
        assert!(base_legal_de_incidencia_monofasica(30051010, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(30066000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(38221100, "").is_some()); // Reagentes em suporte
    }

    #[test]
    fn test_perfumaria_e_cosmeticos() {
        assert!(base_legal_de_incidencia_monofasica(33030010, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(33049990, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(33051000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(33061000, "").is_none()); // Excluída pelo caput (Higiene bucal)
        assert!(base_legal_de_incidencia_monofasica(33072010, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(34011190, "").is_some());
    }

    #[test]
    fn test_pneumaticos_excecoes() {
        // Pneus e câmaras de uso geral devem retornar o enquadramento monofásico
        assert!(base_legal_de_incidencia_monofasica(40111000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(40131000, "").is_some());

        // Pneus e câmaras de bicicletas também devem ser enquadrados aqui na regra geral
        assert!(base_legal_de_incidencia_monofasica(40115000, "").is_some());
        assert!(base_legal_de_incidencia_monofasica(40132000, "").is_some());
    }

    #[test]
    fn test_bebidas_frias() {
        assert!(base_legal_de_incidencia_monofasica(22011000, "").is_some()); // Águas minerais
        assert!(base_legal_de_incidencia_monofasica(22021000, "").is_some()); // Refrigerantes
        assert!(base_legal_de_incidencia_monofasica(22030000, "").is_some()); // Cervejas
        assert!(base_legal_de_incidencia_monofasica(21069010, "").is_some()); // Preparações compostas
    }

    #[test]
    fn test_veiculos_e_maquinas() {
        assert!(base_legal_de_incidencia_monofasica(87032100, "").is_some()); // Carros de passeio
        assert!(base_legal_de_incidencia_monofasica(87042110, "").is_some()); // Caminhões
        assert!(base_legal_de_incidencia_monofasica(87111000, "").is_some()); // Motos
        assert!(base_legal_de_incidencia_monofasica(84295119, "").is_some()); // Escavadeiras
        assert!(base_legal_de_incidencia_monofasica(84248221, "").is_some()); // Pulverizador NCM 2022
    }

    #[test]
    fn test_autopecas_estrito() {
        assert!(base_legal_de_incidencia_monofasica(85114000, "").is_some()); // Motores de arranque
        assert!(base_legal_de_incidencia_monofasica(84133010, "").is_some()); // Bomba de combustível de veículo
        assert!(base_legal_de_incidencia_monofasica(84137010, "").is_none()); // Bomba centrífuga genérica (Não-monofásica)
        assert!(base_legal_de_incidencia_monofasica(84151011, "").is_none()); // Ar-condicionado de parede (Não-monofásico)
    }

    #[test]
    fn test_nao_monofasicos() {
        assert!(base_legal_de_incidencia_monofasica(10063011, "").is_none()); // Arroz
        assert!(base_legal_de_incidencia_monofasica(0, "").is_none());
    }

    // ====================================================================
    // 1. TESTES DE COMBUSTÍVEIS E DERIVADOS (Lei nº 9.718/1998)
    // ====================================================================

    #[test]
    fn test_limites_gasolina_e_aviacao() {
        // Gasolina automotiva padrão (NCM 2710.12.59)
        let gasolina = base_legal_de_incidencia_monofasica(27101259, "");
        assert!(
            gasolina.is_some(),
            "FALHA: NCM de Gasolina automotiva (27101259) deve ser enquadrado como monofásico. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso I."
        );
        assert!(
            gasolina.unwrap().contains("Art. 4º, Inciso I"),
            "FALHA: Mensagem de retorno incorreta para Gasolina automotiva. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso I."
        );

        // Gasolina de Aviação (NCM 2710.12.51) - Exclusão expressa do regime
        assert_eq!(
            base_legal_de_incidencia_monofasica(27101251, ""),
            None,
            "FALHA: Gasolina de aviação (27101251) deve ser desconsiderada do regime monofásico. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso I (exclui expressamente a gasolina de aviação)."
        );

        // Limites frios adjacentes
        assert_eq!(
            base_legal_de_incidencia_monofasica(27101258, ""),
            None,
            "FALHA: Código limite inferior adjacente (27101258) não deve ser enquadrado como monofásico."
        );
        assert_eq!(
            base_legal_de_incidencia_monofasica(27101260, ""),
            None,
            "FALHA: Código limite superior adjacente (27101260) não deve ser enquadrado como monofásico."
        );
    }

    #[test]
    fn test_limites_diesel_e_misturas() {
        // Óleo diesel convencional (NCM 2710.19.21)
        assert!(
            base_legal_de_incidencia_monofasica(27101921, "").is_some(),
            "FALHA: Óleo diesel fóssil padrão (27101921) deve ser enquadrado como monofásico. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso II."
        );

        // Óleo diesel contendo biodiesel (NCM 2710.20.00)
        assert!(
            base_legal_de_incidencia_monofasica(27102000, "").is_some(),
            "FALHA: Óleo diesel com mistura de biodiesel (27102000) deve ser enquadrado como monofásico. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso II."
        );

        // Limites adjacentes de refino
        assert_eq!(
            base_legal_de_incidencia_monofasica(27101920, ""),
            None,
            "FALHA: Óleo lubrificante ou outra fração adjacente (27101920) não deve herdar o enquadramento do diesel."
        );
        assert_eq!(
            base_legal_de_incidencia_monofasica(27101922, ""),
            None,
            "FALHA: Fração adjacente pesada (27101922) não deve herdar o enquadramento do diesel."
        );
    }

    #[test]
    fn test_gases_liquefeitos_glp_glgn() {
        // Gás Liquefeito de Petróleo - GLP (NCM 2711.19.10)
        assert!(
            base_legal_de_incidencia_monofasica(27111910, "").is_some(),
            "FALHA: GLP convencional (27111910) deve ser enquadrado como monofásico. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso III."
        );

        // Gás Liquefeito de Gás Natural - GLGN / GNL (NCM 2711.11.00)
        assert!(
            base_legal_de_incidencia_monofasica(27111100, "").is_some(),
            "FALHA: GLGN (27111100) deve ser enquadrado como monofásico por equiparação de origem. Legislação: Lei nº 9.718/1998, Art. 4º, Inciso III."
        );

        // Gás Propano Liquefeito Isolado (NCM 2711.12.00) - Fora da regra geral de mistura GLP comercial
        assert_eq!(
            base_legal_de_incidencia_monofasica(27111200, ""),
            None,
            "FALHA: Gás propano liquefeito puro (27111200) não deve ser enquadrado diretamente como GLP monofásico nesta função."
        );
    }

    #[test]
    fn test_etanol_fronteiras_lc_214() {
        // Limite inferior do intervalo (NCM 2207.10.00) - Etanol não desnaturalizado
        let limite_inf = base_legal_de_incidencia_monofasica(22071000, "");
        assert!(
            limite_inf.is_some(),
            "FALHA: Limite inferior de etanol (22071000) deve ser enquadrado. Legislação: Lei nº 9.718/1998, Art. 5º, com redação dada pela Lei Complementar nº 214/2025."
        );
        assert!(
            limite_inf.unwrap().contains("Lei Complementar nº 214/2025"),
            "FALHA: Enquadramento do limite inferior deve referenciar a atualização da LC 214/2025."
        );

        // Limite superior do intervalo (NCM 2207.20.99) - Etanol desnaturalizado
        assert!(
            base_legal_de_incidencia_monofasica(22072099, "").is_some(),
            "FALHA: Limite superior de etanol desnaturalizado (22072099) deve ser enquadrado. Legislação: Lei nº 9.718/1998, Art. 5º, com redação dada pela Lei Complementar nº 214/2025."
        );

        // Teste de ultrapassagem de limite de intervalo (NCM 2207.21.00 - Inexistente, mas serve como teste de barreira)
        assert_eq!(
            base_legal_de_incidencia_monofasica(22072100, ""),
            None,
            "FALHA: Transposição do limite superior (22072100) deve retornar None."
        );

        // Teste de limite inferior imediato (NCM 2207.09.99 - Inexistente, serve como barreira)
        assert_eq!(
            base_legal_de_incidencia_monofasica(22070999, ""),
            None,
            "FALHA: Transposição do limite inferior (22070999) deve retornar None."
        );
    }

    // ====================================================================
    // 2. TESTES DE PRODUTOS FARMACÊUTICOS (Lei nº 10.147/2000)
    // ====================================================================

    #[test]
    fn test_medicamentos_e_exclusoes_carboxiamida() {
        // Exclusão sob a posição 30.03 (NCM 3003.90.56)
        assert_eq!(
            base_legal_de_incidencia_monofasica(30039056, ""),
            None,
            "FALHA: Medicamento com função carboxiamida da posição 30.03 (30039056) deve ser excluído da tributação monofásica. Legislação: Lei nº 10.147/2000, Art. 1º, caput (exclui expressamente o código 3003.90.56)."
        );

        // Código vizinho na posição 30.03 (NCM 3003.90.55) - Deve ser monofásico
        assert!(
            base_legal_de_incidencia_monofasica(30039055, "").is_some(),
            "FALHA: Medicamento adjacente (30039055) deve ser enquadrado no regime monofásico. Legislação: Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'a'."
        );

        // Exclusão sob a posição 30.04 (NCM 3004.90.46)
        assert_eq!(
            base_legal_de_incidencia_monofasica(30049046, ""),
            None,
            "FALHA: Medicamento com função carboxiamida da posição 30.04 (30049046) deve ser excluído da tributação monofásica. Legislação: Lei nº 10.147/2000, Art. 1º, caput (exclui expressamente o código 3004.90.46)."
        );

        // Código vizinho na posição 30.04 (NCM 3004.90.45) - Deve ser monofásico
        assert!(
            base_legal_de_incidencia_monofasica(30049045, "").is_some(),
            "FALHA: Medicamento adjacente (30049045) deve ser enquadrado no regime monofásico. Legislação: Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'a'."
        );
    }

    #[test]
    fn test_migracao_reagentes_diagnostico_ncm_2022() {
        // NCM antiga sob a posição 30.06 (NCM 3006.30.11)
        assert!(
            base_legal_de_incidencia_monofasica(30063011, "").is_some(),
            "FALHA: Reagente de diagnóstico legado (30063011) deve ser enquadrado como monofásico. Legislação: Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'a'."
        );

        // Nova posição após reforma do Sistema Harmonizado (NCM 3822.11.00) - Reagentes em suporte plástico/papel
        assert!(
            base_legal_de_incidencia_monofasica(38221100, "").is_some(),
            "FALHA: Reagente migrado pós-2022 (38221100) deve ser mapeado como monofásico. Legislação: Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'a' (adequação de nomenclatura)."
        );

        // Limite superior da nova posição de reagentes (NCM 3822.19.90)
        assert!(
            base_legal_de_incidencia_monofasica(38221990, "").is_some(),
            "FALHA: Limite superior de reagente migrado pós-2022 (38221990) deve ser enquadrado."
        );

        // Excesso de limite de reagentes migrados (NCM 3822.90.00) - Fora da lista de diagnósticos humanos
        assert_eq!(
            base_legal_de_incidencia_monofasica(38229000, ""),
            None,
            "FALHA: Código de reagentes gerais (38229000) não deve herdar o regime de diagnósticos humanos. Legislação: Lei nº 10.147/2000."
        );
    }

    // ====================================================================
    // 3. TESTES DE COSMÉTICOS E HIGIENE (Lei nº 10.147/2000)
    // ====================================================================

    #[test]
    fn test_exclusao_higiene_bucal_3306() {
        // Pasta de dentes / Dentifrícios (NCM 3306.10.00)
        assert_eq!(
            base_legal_de_incidencia_monofasica(33061000, ""),
            None,
            "FALHA: Produtos de higiene bucal/dentifrícios (33061000) são tributados no regime geral. Legislação: Lei nº 10.147/2000, Art. 1º, caput (exclui de forma expressa a totalidade da posição 33.06)."
        );

        // Produtos de cabelo de toucador da posição adjacente 33.05 (NCM 3305.10.00 - Shampoos)
        assert!(
            base_legal_de_incidencia_monofasica(33051000, "").is_some(),
            "FALHA: Shampoos (33051000) devem possuir incidência monofásica. Legislação: Lei nº 10.147/2000, Art. 1º, Inciso I, alínea 'b'."
        );
    }

    // ====================================================================
    // 4. TESTES DE PNEUMÁTICOS (Lei nº 10.485/2002)
    // ====================================================================

    #[test]
    fn test_pneumaticos_limites_e_bicicletas() {
        // Pneus de automóvel (NCM 4011.10.00)
        assert!(
            base_legal_de_incidencia_monofasica(40111000, "").is_some(),
            "FALHA: Pneus novos de carro de passeio (40111000) devem ser enquadrados como monofásicos. Legislação: Lei nº 10.485/2002, Art. 5º, caput."
        );

        // Pneus de bicicleta (NCM 4011.50.00) - Regra geral monofásica (quando fora do incentivo regional da Região Norte)
        assert!(
            base_legal_de_incidencia_monofasica(40115000, "").is_some(),
            "FALHA: Pneus de bicicleta fora da ZFM devem retornar a regra de incidência monofásica geral. Legislação: Lei nº 10.485/2002, Art. 5º, caput."
        );

        // Pneus recauchutados/usados (NCM 4012.11.00) - Não deve ser monofásico
        assert_eq!(
            base_legal_de_incidencia_monofasica(40121100, ""),
            None,
            "FALHA: Pneus recauchutados (40121100) pertencem à posição 40.12, que não é monofásica. Legislação: Lei nº 10.485/2002, Art. 5º (restringe-se exclusivamente às posições 40.11 e 40.13)."
        );
    }

    // ====================================================================
    // 5. TESTES DE AUTOPEÇAS (Lei nº 10.485/2002, Art. 3º)
    // ====================================================================

    #[test]
    fn test_discriminacao_autopecas_vs_genericos() {
        // Bomba de combustível veicular (NCM 8413.30.10) - Autopeça legítima (Anexo I)
        assert!(
            base_legal_de_incidencia_monofasica(84133010, "").is_some(),
            "FALHA: Bomba de combustível automotiva (84133010) deve ser monofásica. Legislação: Lei nº 10.485/2002, Art. 3º, Anexo I."
        );

        // Bomba d'água centrífuga industrial (NCM 8413.70.10) - Fora da lista do Anexo I
        assert_eq!(
            base_legal_de_incidencia_monofasica(84137010, ""),
            None,
            "FALHA: Bombas d'água genéricas industriais (84137010) não pertencem ao regime monofásico de autopeças. Legislação: Lei nº 10.485/2002, Art. 3º (somente as subposições indicadas no Anexo I e II são monofásicas)."
        );

        // Aparelhos de ar-condicionado de uso veicular (NCM 8415.20.10) - Autopeça (Anexo I)
        assert!(
            base_legal_de_incidencia_monofasica(84152010, "").is_some(),
            "FALHA: Ar-condicionado veicular (84152010) deve ser monofásico. Legislação: Lei nº 10.485/2002, Art. 3º, Anexo I."
        );

        // Aparelhos de ar-condicionado de parede/split comum (NCM 8415.10.11) - Não listado como autopeça
        assert_eq!(
            base_legal_de_incidencia_monofasica(84151011, ""),
            None,
            "FALHA: Ar-condicionado split residencial (84151011) não é autopeça. Legislação: Lei nº 10.485/2002, Art. 3º."
        );
    }

    #[test]
    fn test_maquinas_agricolas_e_omissao_propositada() {
        // Partes de colheitadeiras e semeadoras (NCMs 8432.90.00 e 8433.90.90)
        // Devem ser capturadas pelo intervalo amplo de máquinas agrícolas (84320000..=84379999 da Seção 6)

        let partes_semeadora = base_legal_de_incidencia_monofasica(84329000, "");
        assert!(
            partes_semeadora.is_some(),
            "FALHA: Partes de semeadoras (84329000) devem ser enquadradas como monofásicas. Legislação: Lei nº 10.485/2002, Art. 1º (abrange toda a posição 84.32)."
        );
        assert!(
            partes_semeadora.unwrap().contains("Art. 1º"),
            "FALHA: Enquadramento de partes de semeadoras deve referenciar o Art. 1º de máquinas agrícolas, e não o Art. 3º de autopeças comuns."
        );

        let partes_colheitadeira = base_legal_de_incidencia_monofasica(84339090, "");
        assert!(
            partes_colheitadeira.is_some(),
            "FALHA: Partes de colheitadeiras (84339090) devem ser enquadradas como monofásicas. Legislação: Lei nº 10.485/2002, Art. 1º (abrange toda a posição 84.33)."
        );
        assert!(
            partes_colheitadeira.unwrap().contains("Art. 1º"),
            "FALHA: Enquadramento de partes de colheitadeiras deve referenciar o Art. 1º de máquinas agrícolas, e não o Art. 3º de autopeças comuns."
        );
    }
}
