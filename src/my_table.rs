// https://doc.rust-lang.org/rust-by-example/scope/lifetime/static_lifetime.html
// https://practice.rs/lifetime/static.html

// As a reference lifetime 'static indicates that the data pointed to 
// by the reference lives for the entire lifetime of the running program.

#[derive(Debug, Default)]
pub struct MyTable {
    pub side_a: SideA,
    pub side_b: SideB,
}

#[derive(Debug)]
pub struct SideA {
    pub column_aggregation: &'static str,
    pub column_count_lines: &'static str,
    pub column_item_values: &'static str,
}

#[derive(Debug)]
pub struct SideB {
    pub column_aggregation: &'static str,
    pub column_count_lines: &'static str,
    pub column_item_values: &'static str,
    pub column_bscalc_icms: &'static str,
    pub column_origem_regi: &'static str,
}

impl Default for SideA
{
    fn default() -> Self { 
        SideA {
            column_aggregation: "Chave do Documento",
            column_count_lines: "Linhas EFD",
            column_item_values: "Valor Total do Item",
        }
    }
}

impl Default for SideB {
    fn default() -> Self { 
        SideB {
            column_aggregation: "Chave da Nota Fiscal Eletrônica : NF Item (Todos)",
            column_count_lines: "Linhas NFE",
            column_item_values: "Valor da Nota Proporcional : NF Item (Todos) SOMA",
            column_bscalc_icms: "ICMS: Base de Cálculo : NF Item (Todos) SOMA",
            column_origem_regi: "Registro de Origem do Item : NF Item (Todos)",
        }
    }
}
