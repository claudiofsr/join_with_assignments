use polars::datatypes::DataType;
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::LazyLock as Lazy,
};

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub enum Side {
    Left,
    Middle,
    Right,
}

// Implementação de Display para o enum Side
impl fmt::Display for Side {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        // Usamos o formato de depuração (:?) aqui, que é convenientemente
        // o que já queríamos (ex: "Left", "Right", "Middle").
        // Alternativamente, poderíamos usar um `match self` para ter controle total.
        write!(f, "{:?}", self)
        // Se quiséssemos um controle mais fino:
        // match self {
        //     Side::Left => write!(f, "Left"),
        //     Side::Right => write!(f, "Right"),
        //     Side::Middle => write!(f, "Middle"),
        // }
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MyColumn {
    pub side: Side,
    pub nick: &'static str, // apelido, abreviação
    pub name: &'static str,
    pub dtype: DataType,
}

impl MyColumn {
    // Left

    pub fn set_columns_left() -> [MyColumn; 44] {
        let side = Side::Left;
        [
            MyColumn {
                side,
                nick: "count_lines",
                name: "Linhas EFD",
                dtype: DataType::UInt64,
            }, // Coluna Temporária
            MyColumn {
                side,
                nick: "num_linha",
                name: "Linhas",
                dtype: DataType::UInt64,
            },
            MyColumn {
                side,
                nick: "efd_arquivo",
                name: "Arquivo da EFD Contribuições",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "efd_linha",
                name: "Nº da Linha da EFD",
                dtype: DataType::UInt64,
            },
            MyColumn {
                side,
                nick: "contribuinte_cnpj",
                name: "CNPJ dos Estabelecimentos do Contribuinte",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "contribuinte_nome",
                name: "Nome do Contribuinte",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "pa",
                name: "Período de Apuração",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "pa_ano",
                name: "Ano do Período de Apuração",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "pa_trim",
                name: "Trimestre do Período de Apuração",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "pa_mes",
                name: "Mês do Período de Apuração",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "tipo_operacao",
                name: "Tipo de Operação",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "origem",
                name: "Indicador de Origem",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "cod_cred",
                name: "Código do Tipo de Crédito",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "tipo_cred",
                name: "Tipo de Crédito",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "registro",
                name: "Registro",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cst",
                name: "Código de Situação Tributária (CST)",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "cfop",
                name: "Código Fiscal de Operações e Prestações (CFOP)",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "natureza",
                name: "Natureza da Base de Cálculo dos Créditos",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "cnpj_particip",
                name: "CNPJ do Participante",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cpf_particip",
                name: "CPF do Participante",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "nome_particip",
                name: "Nome do Participante",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "num_doc",
                name: "Nº do Documento Fiscal",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "chave",
                name: "Chave do Documento",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "doc_modelo",
                name: "Modelo do Documento Fiscal",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "item_num",
                name: "Nº do Item do Documento Fiscal",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "item_tipo",
                name: "Tipo do Item",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "item_desc",
                name: "Descrição do Item",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "ncm",
                name: "Código NCM",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "nat_operacao",
                name: "Natureza da Operação/Prestação",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "informacao",
                name: "Informação Complementar do Documento Fiscal",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "contabil",
                name: "Escrituração Contábil: Nome da Conta",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "data_emissao",
                name: "Data da Emissão do Documento Fiscal",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "data_entrada",
                name: "Data da Entrada / Aquisição / Execução ou da Saída / Prestação / Conclusão",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "valor_item",
                name: "Valor Total do Item",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_bc",
                name: "Valor da Base de Cálculo das Contribuições",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_bc_auditado",
                name: "Valor da Base de Cálculo das Contribuições (após auditoria)",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "aliq_pis",
                name: "Alíquota de PIS/PASEP (em percentual)",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "aliq_cof",
                name: "Alíquota de COFINS (em percentual)",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_pis",
                name: "Valor de PIS/PASEP",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_cof",
                name: "Valor de COFINS",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_iss",
                name: "Valor de ISS",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_bc_icms",
                name: "Valor da Base de Cálculo de ICMS",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "aliq_icms",
                name: "Alíquota de ICMS (em percentual)",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_icms",
                name: "Valor de ICMS",
                dtype: DataType::Float64,
            },
        ]
    }

    // Middle

    pub fn set_columns_middle() -> [MyColumn; 2] {
        let side = Side::Middle;
        [
            MyColumn {
                side,
                nick: "verificar",
                name: "Verificação dos Valores: EFD x Docs Fiscais",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "glosar",
                name: "Glosar Base de Cálculo de PIS/PASEP e COFINS",
                dtype: DataType::String,
            },
        ]
    }

    // Right

    pub fn set_columns_right() -> [MyColumn; 64] {
        let side = Side::Right;
        [
            MyColumn {
                side,
                nick: "count_lines",
                name: "Linhas NFE",
                dtype: DataType::UInt64,
            }, // Coluna Temporária
            MyColumn {
                side,
                nick: "contribuinte_cnpj",
                name: "CNPJ do Contribuinte : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "contribuinte_nome",
                name: "Nome do Contribuinte : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "entrada_ou_saida",
                name: "Entrada/Saída : NF (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "participante_cnpj",
                name: "CPF/CNPJ do Participante : NF (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "participante_nome",
                name: "Nome do Participante : NF (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "regime_tributario",
                name: "CRT : NF (Todos)",
                dtype: DataType::Int16,
            },
            MyColumn {
                side,
                nick: "observacoes",
                name: "Observações : NF (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "remetente_cnpj1",
                name: "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "remetente_cnpj2",
                name: "CTe - Remetente das mercadorias transportadas: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "remetente_nome",
                name: "CTe - Remetente das mercadorias transportadas: Nome de Conhecimento : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "remetente_municipio",
                name: "CTe - Remetente das mercadorias transportadas: Município de Conhecimento : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "tomador_papel1",
                name: "Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "tomador_papel2",
                name: "Descrição CTe - Indicador do 'papel' do tomador do serviço de Conhecimento : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "tomador_cnpj1",
                name: "CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "tomador_cnpj2",
                name: "CTe - Outro tipo de Tomador: CNPJ/CPF de Conhecimento : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "inicio_estado",
                name: "CTe - UF do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "inicio_municipio",
                name: "CTe - Nome do Município do início da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "termino_estado",
                name: "CTe - UF do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "termino_municipio",
                name: "CTe - Nome do Município do término da prestação de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "destinatario_cnpj",
                name: "CTe - Informações do Destinatário do CT-e: CNPJ/CPF de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "destinatario_nome",
                name: "CTe - Informações do Destinatário do CT-e: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "local_entrega",
                name: "CTe - Local de Entrega constante na Nota Fiscal: Nome de Conhecimento : ConhecimentoValoresPrestacaoServico-Componentes",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "descricao_natureza",
                name: "Descrição da Natureza da Operação : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cancelada",
                name: "Cancelada : NF (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "origem",
                name: "Registro de Origem do Item : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "natureza",
                name: "Natureza da Base de Cálculo do Crédito Descrição : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "modelo",
                name: "Modelo - Descrição : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "num_doc",
                name: "Número da Nota : NF Item (Todos)",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "chave",
                name: "Chave da Nota Fiscal Eletrônica : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "chave_de_acesso",
                name: "Inf. NFe - Chave de acesso da NF-e : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "valor_docs_viculados",
                name: "Valor Total de Documentos Vinculados",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "observacoes_gerais",
                name: "CTe - Observações Gerais de Conhecimento : ConhecimentoInformacaoNFe",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "dia_emissao",
                name: "Dia da Emissão : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "numero_di",
                name: "Número da DI : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "numero_item",
                name: "Número do Item : NF Item (Todos)",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "cfop",
                name: "Código CFOP : NF Item (Todos)",
                dtype: DataType::Int64,
            },
            MyColumn {
                side,
                nick: "descricao_cfop",
                name: "Descrição CFOP : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "descricao_mercadoria",
                name: "Descrição da Mercadoria/Serviço : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "ncm",
                name: "Código NCM : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "descricao_ncm",
                name: "Descrição NCM : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cst_descricao_pis",
                name: "CST PIS Descrição : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cst_descricao_cof",
                name: "CST COFINS Descrição : NF Item (Todos)",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "valor_total",
                name: "Valor Total : NF (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_item",
                name: "Valor da Nota Proporcional : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_desconto",
                name: "Valor dos Descontos : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_seguro",
                name: "Valor Seguro : NF (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "aliq_pis",
                name: "PIS: Alíquota ad valorem - Atributo : NF Item (Todos)",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "aliq_cof",
                name: "COFINS: Alíquota ad valorem - Atributo : NF Item (Todos)",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_pis",
                name: "PIS: Valor do Tributo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_cof",
                name: "COFINS: Valor do Tributo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_bc_iss",
                name: "ISS: Base de Cálculo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_ipi",
                name: "IPI: Valor do Tributo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_iss",
                name: "ISS: Valor do Tributo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "aliq_icms",
                name: "ICMS: Alíquota : NF Item (Todos) NOISE OR",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_bc_icms",
                name: "ICMS: Base de Cálculo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_icms",
                name: "ICMS: Valor do Tributo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            MyColumn {
                side,
                nick: "valor_icms_sub",
                name: "ICMS por Substituição: Valor do Tributo : NF Item (Todos) SOMA",
                dtype: DataType::Float64,
            },
            // Colunas Auxiliares adicionadas
            MyColumn {
                side,
                nick: "aliquota_zero",
                name: "Alíquota Zero",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "credito_presumido",
                name: "Crédito Presumido",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "incidencai_monofasica",
                name: "Incidência Monofásica",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cnpj_base_contribuinte",
                name: "CNPJ Base do Contribuinte",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cnpj_base_remetente",
                name: "CNPJ Base do Remetente",
                dtype: DataType::String,
            },
            MyColumn {
                side,
                nick: "cnpj_base_destinatario",
                name: "CNPJ Base do Destinatário",
                dtype: DataType::String,
            },
        ]
    }

    /// Get all columns after checking uniqueness.
    ///
    /// <https://stackoverflow.com/questions/67041830/how-to-concatenate-arrays-of-known-lengths>
    pub fn get_columns() -> Vec<MyColumn> {
        let columns_left = MyColumn::set_columns_left();
        let columns_middle = MyColumn::set_columns_middle();
        let columns_right = MyColumn::set_columns_right();

        let cols: Vec<MyColumn> =
            [&columns_left[..], &columns_middle[..], &columns_right[..]].concat();

        // Verificar unicidade de todas as colunas
        cols.uniqueness();

        cols
    }

    /// Get (column_name, dtype) map.
    pub fn get_cols_dtype(side: Side) -> HashMap<&'static str, DataType> {
        let cols: Vec<MyColumn> = match side {
            Side::Left => MyColumn::set_columns_left().to_vec(),
            Side::Middle => MyColumn::set_columns_middle().to_vec(),
            Side::Right => MyColumn::set_columns_right().to_vec(),
        };

        cols.iter()
            .map(|col| (col.name, col.dtype.clone()))
            .collect()
    }
}

pub trait ColumnExtension {
    /// Get MyColumn names
    #[allow(dead_code)]
    fn get_names(&self, side: Side) -> Vec<&str>;
    /**
    HashMap<key, value>

    key: (side, nick)

    value: column name
    */
    fn get_hash(&self) -> HashMap<(Side, &'static str), &'static str>;

    /// Verify uniqueness
    ///
    /// Verificar Unicidade
    fn uniqueness(&self);
}

impl ColumnExtension for [MyColumn] {
    fn get_names(&self, side: Side) -> Vec<&str> {
        self.iter()
            .filter(|col| col.side == side)
            .map(|col| col.name)
            .collect()
    }

    fn get_hash(&self) -> HashMap<(Side, &'static str), &'static str> {
        self.iter()
            .map(|col| {
                let key = (col.side, col.nick);
                let value = col.name;
                (key, value)
            })
            .collect()
    }

    fn uniqueness(&self) {
        let mut unique_name = HashSet::new();
        let mut unique_key = HashSet::new();

        self.iter().for_each(|col| {
            let name = col.name;
            let key = (col.side, col.nick);

            if !unique_name.insert(name) {
                eprintln!("col: {col:?}");
                eprintln!("MyColumn '{name}' is not unique.");
                panic!("The column name must be unique!");
            }

            if !unique_key.insert(key) {
                eprintln!("col: {col:?}");
                eprintln!("key: {key:?} is not unique.");
                panic!("The key must be unique!");
            }
        });
    }
}

static KEY_NAME: Lazy<HashMap<(Side, &'static str), &'static str>> =
    Lazy::new(|| MyColumn::get_columns().get_hash());

/// Get MyColumn name from NAMES.get(&key)
///
/// Such that
///
/// key: (side, nick)
pub fn coluna(side: Side, nick: &str) -> &str {
    match KEY_NAME.get(&(side, nick)) {
        Some(&name) => name,
        None => {
            eprintln!("fn coluna()");
            eprintln!("side: {side:?}");
            eprintln!("nick: {nick}");
            panic!("Error: Invalid key: (side, nick)!");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Side::{Left, Middle, Right};

    // cargo test -- --help
    // cargo test -- --nocapture
    // cargo test -- --show-output

    #[test]
    /// `cargo test -- --show-output column_names`
    fn column_names() {
        let cols: Vec<MyColumn> = MyColumn::get_columns();

        let names_left: Vec<&str> = cols.get_names(Left);
        println!("names_left: {names_left:#?}");

        let names_right: Vec<&str> = cols.get_names(Right);
        println!("names_right: {names_right:#?}");
    }

    #[test]
    /// `cargo test -- --show-output  column_hash`
    fn column_hash() {
        let hash = MyColumn::get_columns().get_hash();
        println!("hash: {hash:#?}");

        assert_eq!(hash[&(Left, "chave")], "Chave do Documento");
        assert_eq!(
            hash[&(Right, "chave")],
            "Chave da Nota Fiscal Eletrônica : NF Item (Todos)"
        );
    }

    #[test]
    /// `cargo test -- --show-output sort_names`
    fn sort_names() {
        println!("Not sorted:");

        for (key, value) in KEY_NAME.iter() {
            let k = format!("{key:?}");
            println!("key: {k:32} ; value: {value}");
        }

        println!("\nSorted:");

        let mut hash_vec: Vec<((Side, &str), &str)> = KEY_NAME.clone().into_iter().collect();

        hash_vec.sort_by_key(|tuple| (tuple.0.0, tuple.0.1, tuple.1));

        for (key, value) in &hash_vec {
            let k = format!("{key:?}");
            println!("key: {k:32} ; value: {value}");
        }

        let columns_len = KEY_NAME.len();
        println!("KEY_NAME_len: {columns_len}");

        assert_eq!(
            KEY_NAME[&(Left, "efd_arquivo")],
            "Arquivo da EFD Contribuições"
        );
        assert_eq!(
            KEY_NAME[&(Middle, "glosar")],
            "Glosar Base de Cálculo de PIS/PASEP e COFINS"
        );
        assert_eq!(KEY_NAME[&(Left, "chave")], "Chave do Documento");
        assert_eq!(
            KEY_NAME[&(Right, "chave")],
            "Chave da Nota Fiscal Eletrônica : NF Item (Todos)"
        );
        assert_eq!(KEY_NAME[&(Left, "count_lines")], "Linhas EFD");
        assert_eq!(KEY_NAME[&(Right, "count_lines")], "Linhas NFE");
        assert_eq!(KEY_NAME.get(&(Right, "count_lines")), Some(&"Linhas NFE"));
        assert_eq!(columns_len, 44 + 2 + 64);
    }

    #[test]
    /// `cargo test -- --show-output get_column_from_hash`
    fn get_column_from_hash() {
        assert_eq!(coluna(Left, "efd_arquivo"), "Arquivo da EFD Contribuições");
        assert_eq!(
            coluna(Middle, "glosar"),
            "Glosar Base de Cálculo de PIS/PASEP e COFINS"
        );
        assert_eq!(coluna(Left, "chave"), "Chave do Documento");
        assert_eq!(
            coluna(Right, "chave"),
            "Chave da Nota Fiscal Eletrônica : NF Item (Todos)"
        );
        assert_eq!(coluna(Left, "count_lines"), "Linhas EFD");
        assert_eq!(coluna(Right, "count_lines"), "Linhas NFE");
    }
}
