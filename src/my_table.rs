// https://doc.rust-lang.org/rust-by-example/scope/lifetime/static_lifetime.html
// https://practice.rs/lifetime/static.html

// As a reference lifetime 'static indicates that the data pointed to 
// by the reference lives for the entire lifetime of the running program.

#[derive(Debug, Default)]
pub struct MyTable {
    pub columns_a: ColumnsA,
    pub columns_b: ColumnsB,
}

#[derive(Debug)]
pub struct ColumnsA {
    pub column_chave: &'static str,
    pub column_number: &'static str,
    pub column_value: &'static str,
}

#[derive(Debug)]
pub struct ColumnsB {
    pub column_chave: &'static str,
    pub column_number: &'static str,
    pub column_value: &'static str,
    pub column_bc_icms: &'static str,
    pub column_registro: &'static str,
}

impl Default for ColumnsA
{
    fn default() -> Self { 
        ColumnsA {
        column_chave: "Chave do Documento",
        column_number: "Linhas EFD",
        column_value: "Valor Total do Item",
        }
    }
}

impl Default for ColumnsB {
    fn default() -> Self { 
        ColumnsB {
            column_chave: "Chave da Nota Fiscal Eletrônica : NF Item (Todos)",
            column_number: "Linhas NFE",
            column_value: "Valor da Nota Proporcional : NF Item (Todos) SOMA",
            column_bc_icms: "ICMS: Base de Cálculo : NF Item (Todos) SOMA",
            column_registro: "Registro de Origem do Item : NF Item (Todos)",
        }
    }
}
