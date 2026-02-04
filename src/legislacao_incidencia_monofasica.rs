/// Base Legal conforme código NCM e descrição do item.
pub fn base_legal_de_incidencia_monofasica(
    codigo_ncm: u64,
    _descricao: &str,
) -> Option<&'static str> {
    let especificos: [u64; 2] = [
        30039056, // exceção em Incidência Monofásica
        30049046, // exceção em Incidência Monofásica
    ];

    if especificos.contains(&codigo_ncm) {
        return None;
    }

    match codigo_ncm {
        27101259 =>  Some("Incidência Monofásica - Lei 9.718/1998, Art. 4º, Inciso I (Gasolinas, exceto Gasolina de Aviação)."),
        27101921 =>  Some("Incidência Monofásica - Lei 9.718/1998, Art. 4º, Inciso II (Óleo Diesel)."),
        27111910 =>  Some("Incidência Monofásica - Lei 9.718/1998, Art. 4º, Inciso III (Gás Liquefeito de Petróleo - GLP)."),
        27101911 =>  Some("Incidência Monofásica - Lei 10.560/2002, Art. 2º (Querosene de Aviação)."),
        38260000 =>  Some("Incidência Monofásica - Lei 11.116/2005, Art. 3º (Biodiesel)."),
        22071000 ..= 22071099 | 22072010 ..= 22072019 | 22089000
                  => Some("Incidência Monofásica - Lei 9.718/1998, Art. 5º (Álcool, Inclusive para Fins Carburantes)."),
        30010000 ..= 30019999 | 30030000 ..= 30039999 | 30040000 ..= 30049999 |
        30021010 ..= 30021039 | 30022010 ..= 30022029 | 30063010 ..= 30063029 |
        30029020 | 30029092 | 30051010 | 30066000 // | 30029099: lei_10925_art01_inciso06()
                  => Some("Incidência Monofásica - Lei 10.147/2000, Art. 1º, Inciso I, alínea A (Produtos Farmacêuticos)."),
        33030000 ..= 33059999 | 33070000 ..= 33079999 | 34012010 | 96032100
                  => Some("Incidência Monofásica - Lei 10.147/2000, Art. 1º, Inciso I, alínea B (Produtos de Perfumaria ou de Higiene Pessoal)."),
        40110000 ..= 40119999 | 40130000 ..= 40139999
                  => Some("Incidência Monofásica - Lei 10.485/2002, Art. 5º (Pneumáticos)."),
        _ => None,
    }
}
