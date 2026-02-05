use polars::prelude::*;
use std::fmt::Write;

// ============================================================================
// 1. INDICADOR DE ORIGEM
// ============================================================================

/// Representa o Indicador de Origem (0 - Mercado Interno, 1 - Importação).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndicadorOrigem {
    MercadoInterno = 0,
    Importacao = 1,
}

impl IndicadorOrigem {
    /// Tenta converter um `i64` para `IndicadorOrigem`.
    ///
    /// Retorna `Some(IndicadorOrigem)` se o valor for válido (0 ou 1),
    /// caso contrário, retorna `None`.
    pub const fn from_i64(v: i64) -> Option<Self> {
        match v {
            0 => Some(Self::MercadoInterno),
            1 => Some(Self::Importacao),
            _ => None,
        }
    }

    /// Retorna a descrição completa do Indicador de Origem como uma string estática.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::MercadoInterno => "Operação no Mercado Interno",
            Self::Importacao => "Operação de Importação",
        }
    }
}

/// Transforma uma coluna de `i64` (códigos de Indicador de Origem) em uma coluna de `String`
/// com as descrições correspondentes.
///
/// Valores nulos na entrada resultam em nulos na saída.
/// Valores numéricos sem correspondência no enum resultam em "{código}: Sem descrição".
pub fn descricao_da_origem(col: Column) -> PolarsResult<Column> {
    col.cast(&DataType::Int64)?
        .i64()?
        .try_apply_into_string_amortized(|n, buf| {
            buf.clear();
            let descricao = IndicadorOrigem::from_i64(n)
                .map(|e| e.as_str())
                .unwrap_or("Sem descrição");

            write!(buf, "{descricao}").map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .map(|ca| ca.into_column())
}

// ============================================================================
// 2. TIPO DE OPERAÇÃO
// ============================================================================

/// Representa o Tipo de Operação (Entrada, Saída, Ajuste, Desconto, Detalhamento).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipoOperacao {
    Entrada = 1,
    Saida = 2,
    AjusteAcrescimo = 3,
    AjusteReducao = 4,
    DescontoProprio = 5,
    DescontoPosterior = 6,
    Detalhamento = 7,
}

impl TipoOperacao {
    /// Tenta converter um `i64` para `TipoOperacao`.
    ///
    /// Retorna `Some(TipoOperacao)` se o valor for válido (1-7),
    /// caso contrário, retorna `None`.
    pub const fn from_i64(v: i64) -> Option<Self> {
        match v {
            1 => Some(Self::Entrada),
            2 => Some(Self::Saida),
            3 => Some(Self::AjusteAcrescimo),
            4 => Some(Self::AjusteReducao),
            5 => Some(Self::DescontoProprio),
            6 => Some(Self::DescontoPosterior),
            7 => Some(Self::Detalhamento),
            _ => None,
        }
    }

    /// Retorna a descrição consolidada do Tipo de Operação.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Entrada => "Entrada",
            Self::Saida => "Saída",
            Self::AjusteAcrescimo | Self::AjusteReducao => "Ajuste",
            Self::DescontoProprio | Self::DescontoPosterior => "Desconto",
            Self::Detalhamento => "Detalhamento",
        }
    }
}

/// Transforma uma coluna de `i64` (códigos de Tipo de Operação) em uma coluna de `String`
/// com as descrições correspondentes.
///
/// Valores nulos na entrada resultam em nulos na saída.
/// Valores numéricos sem correspondência no enum resultam em "{código}: Sem descrição".
pub fn descricao_do_tipo_de_operacao(col: Column) -> PolarsResult<Column> {
    col.cast(&DataType::Int64)?
        .i64()?
        .try_apply_into_string_amortized(|n, buf| {
            buf.clear();
            let descricao = TipoOperacao::from_i64(n)
                .map(|e| e.as_str())
                .unwrap_or("Sem descrição");

            write!(buf, "{descricao}").map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .map(|ca| ca.into_column())
}

// ============================================================================
// 3. TIPO DE CRÉDITO
// ============================================================================

/// Representa o Tipo de Crédito para PIS/COFINS.
/// Mnemônicos são usados para variantes com mais de 20 caracteres.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TipoCredito {
    AliquotaBasica = 1,
    AliquotasDiferenciadas = 2,
    AliquotaUnidade = 3,
    EstoqueAbertura = 4,
    AquisicaoEmbalagens = 5,
    PresumidoAgroindustria = 6,
    OutrosPresumidos = 7,
    Importacao = 8,
    AtividadeImobiliaria = 9,
    Outros = 99,
    Vazio = 100,
}

impl TipoCredito {
    /// Tenta converter um `i64` para `TipoCredito`.
    ///
    /// Retorna `Some(TipoCredito)` se o valor for válido,
    /// caso contrário, retorna `None`.
    pub const fn from_i64(v: i64) -> Option<Self> {
        match v {
            1 => Some(Self::AliquotaBasica),
            2 => Some(Self::AliquotasDiferenciadas),
            3 => Some(Self::AliquotaUnidade),
            4 => Some(Self::EstoqueAbertura),
            5 => Some(Self::AquisicaoEmbalagens),
            6 => Some(Self::PresumidoAgroindustria),
            7 => Some(Self::OutrosPresumidos),
            8 => Some(Self::Importacao),
            9 => Some(Self::AtividadeImobiliaria),
            99 => Some(Self::Outros),
            100 => Some(Self::Vazio),
            _ => None,
        }
    }

    /// Retorna a descrição completa do Tipo de Crédito.
    #[rustfmt::skip]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::AliquotaBasica         => "Alíquota Básica",
            Self::AliquotasDiferenciadas => "Alíquotas Diferenciadas",
            Self::AliquotaUnidade        => "Alíquota por Unidade de Produto",
            Self::EstoqueAbertura        => "Estoque de Abertura",
            Self::AquisicaoEmbalagens    => "Aquisição Embalagens para Revenda",
            Self::PresumidoAgroindustria => "Presumido da Agroindústria",
            Self::OutrosPresumidos       => "Outros Créditos Presumidos",
            Self::Importacao             => "Importação",
            Self::AtividadeImobiliaria   => "Atividade Imobiliária",
            Self::Outros                 => "Outros",
            Self::Vazio                  => "",
        }
    }
}

/// Transforma uma coluna de `i64` (códigos de Tipo de Crédito) em uma coluna de `String`
/// com as descrições correspondentes.
///
/// Valores nulos na entrada resultam em nulos na saída.
/// Valores numéricos sem correspondência no enum resultam em "{código}: Sem descrição".
pub fn descricao_do_tipo_de_credito(col: Column) -> PolarsResult<Column> {
    col.cast(&DataType::Int64)?
        .i64()?
        .try_apply_into_string_amortized(|n, buf| {
            buf.clear();
            let descricao = TipoCredito::from_i64(n)
                .map(|e| e.as_str())
                .unwrap_or("Sem descrição");

            write!(buf, "{descricao}").map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .map(|ca| ca.into_column())
}

// ============================================================================
// 4. MÊS
// ============================================================================

/// Representa os meses do ano e um valor acumulado.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mes {
    Jan = 1,
    Fev = 2,
    Mar = 3,
    Abr = 4,
    Mai = 5,
    Jun = 6,
    Jul = 7,
    Ago = 8,
    Set = 9,
    Out = 10,
    Nov = 11,
    Dez = 12,
    Acumulado = 13,
}

impl Mes {
    /// Tenta converter um `i64` para `Mes`.
    ///
    /// Retorna `Some(Mes)` se o valor for válido (1-13),
    /// caso contrário, retorna `None`.
    pub const fn from_i64(v: i64) -> Option<Self> {
        match v {
            1 => Some(Self::Jan),
            2 => Some(Self::Fev),
            3 => Some(Self::Mar),
            4 => Some(Self::Abr),
            5 => Some(Self::Mai),
            6 => Some(Self::Jun),
            7 => Some(Self::Jul),
            8 => Some(Self::Ago),
            9 => Some(Self::Set),
            10 => Some(Self::Out),
            11 => Some(Self::Nov),
            12 => Some(Self::Dez),
            13 => Some(Self::Acumulado),
            _ => None,
        }
    }

    /// Retorna o nome completo do mês. Para `Acumulado`, retorna uma string vazia.
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Jan => "janeiro",
            Self::Fev => "fevereiro",
            Self::Mar => "março",
            Self::Abr => "abril",
            Self::Mai => "maio",
            Self::Jun => "junho",
            Self::Jul => "julho",
            Self::Ago => "agosto",
            Self::Set => "setembro",
            Self::Out => "outubro",
            Self::Nov => "novembro",
            Self::Dez => "dezembro",
            Self::Acumulado => "",
        }
    }
}

/// Transforma uma coluna de `i64` (códigos de Mês) em uma coluna de `String`
/// com as descrições correspondentes.
///
/// Valores nulos na entrada resultam em nulos na saída.
/// Valores numéricos sem correspondência no enum resultam em "{código}: Sem descrição".
pub fn descricao_do_mes(col: Column) -> PolarsResult<Column> {
    col.cast(&DataType::Int64)?
        .i64()?
        .try_apply_into_string_amortized(|n, buf| {
            buf.clear();
            let descricao = Mes::from_i64(n)
                .map(|e| e.as_str())
                .unwrap_or("Sem descrição");

            write!(buf, "{descricao}").map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .map(|ca| ca.into_column())
}

// ============================================================================
// 5. NATUREZA DA BC DOS CRÉDITOS
// ============================================================================

/// Representa a Natureza da Base de Cálculo dos Créditos PIS/COFINS.
/// Mnemônicos são usados para variantes com mais de 20 caracteres.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NaturezaBC {
    AqBensRevenda = 1,           // Aquisição de Bens para Revenda
    AqBensInsumo = 2,            // Aquisição de Bens Utilizados como Insumo
    AqServInsumo = 3,            // Aquisição de Serviços Utilizados como Insumo
    EnergEletrica = 4,           // Energia Elétrica
    AlugPredios = 5,             // Aluguéis de Prédios
    AlugMaquinas = 6,            // Aluguéis de Máquinas e Equipamentos
    ArmazFrete = 7,              // Armazenagem de Mercadoria e Frete
    ArrendMercantil = 8,         // Contraprestações de Arrendamento Mercantil
    MaquinasDeprec = 9,          // Máquinas, Equipamentos (Depreciação)
    MaquinasAq = 10,             // Máquinas, Equipamentos (Aquisição)
    EdificBenfeit = 11,          // Edificações e Benfeitorias
    DevolVendas = 12,            // Devolução de Vendas
    OutrasOper = 13,             // Outras Operações
    TranspSubcontr = 14,         // Transporte de Cargas - Subcontratação
    ImobCustoIncorr = 15,        // Imobiliária - Custo Incorrido
    ImobCustoOrcado = 16,        // Imobiliária - Custo Orçado
    ServLimpeza = 17,            // Serviços de Limpeza, Conservação e Manutenção
    EstoqueAberturaBens = 18,    // Estoque de Abertura de Bens
    AjusteAcrescPis = 31,        // Ajuste de Acréscimo (PIS/PASEP)
    AjusteAcrescCofins = 35,     // Ajuste de Acréscimo (COFINS)
    AjusteReducPis = 41,         // Ajuste de Redução (PIS/PASEP)
    AjusteReducCofins = 45,      // Ajuste de Redução (COFINS)
    DescProprioPis = 51,         // Desconto da Contribuição Apurada no Próprio Período (PIS)
    DescProprioCofins = 55,      // Desconto da Contribuição Apurada no Próprio Período (COFINS)
    DescPostPis = 61,            // Desconto Efetuado em Período Posterior (PIS)
    DescPostCofins = 65,         // Desconto Efetuado em Período Posterior (COFINS)
    RecBrutaValores = 80,        // Receita Bruta (valores)
    RecBrutaPerc = 81,           // Receita Bruta (percentuais)
    BcDebOmitidos = 90,          // Base de Cálculo de Débitos Omitidos
    DebRevNcmPis = 91,           // Débitos: Revenda de Mercadorias de NCM (PIS)
    DebRevNcmCofins = 95,        // Débitos: Revenda de Mercadorias de NCM (COFINS)
    BcAliqBasica = 101,          // Base de Cálculo dos Créditos - Alíquota Básica
    BcAliqDif = 102,             // Base de Cálculo dos Créditos - Alíquotas Diferenciadas
    BcAliqUnidade = 103,         // Base de Cálculo dos Créditos - Alíquota por Unidade
    BcEstoqueAbertura = 104,     // Base de Cálculo dos Créditos - Estoque de Abertura
    BcAquisicaoEmbalagens = 105, // Base de Cálculo dos Créditos - Aquisição Embalagens
    BcPresumAgroind = 106,       // Base de Cálculo dos Créditos - Presumido Agroindústria
    BcOutrosPresum = 107,        // Base de Cálculo dos Créditos - Outros Créditos Presumidos
    BcImportacao = 108,          // Base de Cálculo dos Créditos - Importação
    BcAtivImob = 109,            // Base de Cálculo dos Créditos - Atividade Imobiliária
    BcOutros = 199,              // Base de Cálculo dos Créditos - Outros
    CredApuradoPis = 201,        // Crédito Apurado no Período (PIS/PASEP)
    CredApuradoCofins = 205,     // Crédito Apurado no Período (COFINS)
    CredDispAjustePis = 211,     // Crédito Disponível após Ajustes (PIS/PASEP)
    CredDispAjusteCofins = 215,  // Crédito Disponível após Ajustes (COFINS)
    CredDispDescPis = 221,       // Crédito Disponível após Descontos (PIS/PASEP)
    CredDispDescCofins = 225,    // Crédito Disponível após Descontos (COFINS)
    BcValorTotal = 300,          // Base de Cálculo dos Créditos - Valor Total
    SaldoCredPis = 301,          // Saldo de Crédito Passível de Desconto ou Ressarcimento (PIS)
    SaldoCredCofins = 305,       // Saldo de Crédito Passível de Desconto ou Ressarcimento (COFINS)
}

impl NaturezaBC {
    /// Tenta converter um `i64` para `NaturezaBC`.
    ///
    /// Retorna `Some(NaturezaBC)` se o valor for válido,
    /// caso contrário, retorna `None`.
    pub const fn from_i64(v: i64) -> Option<Self> {
        match v {
            1 => Some(Self::AqBensRevenda),
            2 => Some(Self::AqBensInsumo),
            3 => Some(Self::AqServInsumo),
            4 => Some(Self::EnergEletrica),
            5 => Some(Self::AlugPredios),
            6 => Some(Self::AlugMaquinas),
            7 => Some(Self::ArmazFrete),
            8 => Some(Self::ArrendMercantil),
            9 => Some(Self::MaquinasDeprec),
            10 => Some(Self::MaquinasAq),
            11 => Some(Self::EdificBenfeit),
            12 => Some(Self::DevolVendas),
            13 => Some(Self::OutrasOper),
            14 => Some(Self::TranspSubcontr),
            15 => Some(Self::ImobCustoIncorr),
            16 => Some(Self::ImobCustoOrcado),
            17 => Some(Self::ServLimpeza),
            18 => Some(Self::EstoqueAberturaBens),
            31 => Some(Self::AjusteAcrescPis),
            35 => Some(Self::AjusteAcrescCofins),
            41 => Some(Self::AjusteReducPis),
            45 => Some(Self::AjusteReducCofins),
            51 => Some(Self::DescProprioPis),
            55 => Some(Self::DescProprioCofins),
            61 => Some(Self::DescPostPis),
            65 => Some(Self::DescPostCofins),
            80 => Some(Self::RecBrutaValores),
            81 => Some(Self::RecBrutaPerc),
            90 => Some(Self::BcDebOmitidos),
            91 => Some(Self::DebRevNcmPis),
            95 => Some(Self::DebRevNcmCofins),
            101 => Some(Self::BcAliqBasica),
            102 => Some(Self::BcAliqDif),
            103 => Some(Self::BcAliqUnidade),
            104 => Some(Self::BcEstoqueAbertura),
            105 => Some(Self::BcAquisicaoEmbalagens),
            106 => Some(Self::BcPresumAgroind),
            107 => Some(Self::BcOutrosPresum),
            108 => Some(Self::BcImportacao),
            109 => Some(Self::BcAtivImob),
            199 => Some(Self::BcOutros),
            201 => Some(Self::CredApuradoPis),
            205 => Some(Self::CredApuradoCofins),
            211 => Some(Self::CredDispAjustePis),
            215 => Some(Self::CredDispAjusteCofins),
            221 => Some(Self::CredDispDescPis),
            225 => Some(Self::CredDispDescCofins),
            300 => Some(Self::BcValorTotal),
            301 => Some(Self::SaldoCredPis),
            305 => Some(Self::SaldoCredCofins),
            _ => None,
        }
    }

    /// Retorna a descrição completa da Natureza da BC dos Créditos.
    #[rustfmt::skip]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AqBensRevenda      => "Aquisição de Bens para Revenda",
            Self::AqBensInsumo       => "Aquisição de Bens Utilizados como Insumo",
            Self::AqServInsumo       => "Aquisição de Serviços Utilizados como Insumo",
            Self::EnergEletrica      => "Energia Elétrica e Térmica, Inclusive sob a Forma de Vapor",
            Self::AlugPredios        => "Aluguéis de Prédios",
            Self::AlugMaquinas       => "Aluguéis de Máquinas e Equipamentos",
            Self::ArmazFrete         => "Armazenagem de Mercadoria e Frete na Operação de Venda",
            Self::ArrendMercantil    => "Contraprestações de Arrendamento Mercantil",
            Self::MaquinasDeprec     => "Máquinas, Equipamentos ... (Crédito sobre Encargos de Depreciação)",
            Self::MaquinasAq         => "Máquinas, Equipamentos ... (Crédito com Base no Valor de Aquisição)",
            Self::EdificBenfeit      => "Amortizacao e Depreciação de Edificações e Benfeitorias em Imóveis",
            Self::DevolVendas        => "Devolução de Vendas Sujeitas à Incidência Não-Cumulativa",
            Self::OutrasOper         => "Outras Operações com Direito a Crédito",
            Self::TranspSubcontr     => "Atividade de Transporte de Cargas - Subcontratação",
            Self::ImobCustoIncorr    => "Atividade Imobiliária - Custo Incorrido de Unidade Imobiliária",
            Self::ImobCustoOrcado    => "Atividade Imobiliária - Custo Orçado de Unidade não Concluída",
            Self::ServLimpeza        => "Atividade de Prestação de Serviços de Limpeza, Conservação e Manutenção",
            Self::EstoqueAberturaBens=> "Estoque de Abertura de Bens",
            Self::AjusteAcrescPis    => "Ajuste de Acréscimo (PIS/PASEP)",
            Self::AjusteAcrescCofins => "Ajuste de Acréscimo (COFINS)",
            Self::AjusteReducPis     => "Ajuste de Redução (PIS/PASEP)",
            Self::AjusteReducCofins  => "Ajuste de Redução (COFINS)",
            Self::DescProprioPis     => "Desconto da Contribuição Apurada no Próprio Período (PIS/PASEP)",
            Self::DescProprioCofins  => "Desconto da Contribuição Apurada no Próprio Período (COFINS)",
            Self::DescPostPis        => "Desconto Efetuado em Período Posterior (PIS/PASEP)",
            Self::DescPostCofins     => "Desconto Efetuado em Período Posterior (COFINS)",
            Self::RecBrutaValores    => "Receita Bruta (valores)",
            Self::RecBrutaPerc       => "Receita Bruta (percentuais)",
            Self::BcDebOmitidos      => "Base de Cálculo de Débitos Omitidos",
            Self::DebRevNcmPis       => "Débitos: Revenda de Mercadorias de NCM 2309.90 (PIS/PASEP)",
            Self::DebRevNcmCofins    => "Débitos: Revenda de Mercadorias de NCM 2309.90 (COFINS)",
            Self::BcAliqBasica       => "Base de Cálculo dos Créditos - Alíquota Básica (Soma)",
            Self::BcAliqDif          => "Base de Cálculo dos Créditos - Alíquotas Diferenciadas (Soma)",
            Self::BcAliqUnidade      => "Base de Cálculo dos Créditos - Alíquota por Unidade de Produto (Soma)",
            Self::BcEstoqueAbertura  => "Base de Cálculo dos Créditos - Estoque de Abertura (Soma)",
            Self::BcAquisicaoEmbalagens => "Base de Cálculo dos Créditos - Aquisição Embalagens para Revenda (Soma)",
            Self::BcPresumAgroind    => "Base de Cálculo dos Créditos - Presumido da Agroindústria (Soma)",
            Self::BcOutrosPresum     => "Base de Cálculo dos Créditos - Outros Créditos Presumidos (Soma)",
            Self::BcImportacao       => "Base de Cálculo dos Créditos - Importação (Soma)",
            Self::BcAtivImob         => "Base de Cálculo dos Créditos - Atividade Imobiliária (Soma)",
            Self::BcOutros           => "Base de Cálculo dos Créditos - Outros (Soma)",
            Self::CredApuradoPis     => "Crédito Apurado no Período (PIS/PASEP)",
            Self::CredApuradoCofins  => "Crédito Apurado no Período (COFINS)",
            Self::CredDispAjustePis  => "Crédito Disponível após Ajustes (PIS/PASEP)",
            Self::CredDispAjusteCofins=> "Crédito Disponível após Ajustes (COFINS)",
            Self::CredDispDescPis    => "Crédito Disponível após Descontos (PIS/PASEP)",
            Self::CredDispDescCofins => "Crédito Disponível após Descontos (COFINS)",
            Self::BcValorTotal       => "Base de Cálculo dos Créditos - Valor Total (Soma)",
            Self::SaldoCredPis       => "Saldo de Crédito Passível de Desconto ou Ressarcimento (PIS/PASEP)",
            Self::SaldoCredCofins    => "Saldo de Crédito Passível de Desconto ou Ressarcimento (COFINS)",
        }
    }
}

/// Transforma uma coluna de `i64` (códigos de Natureza da BC) em uma coluna de `String`
/// com as descrições correspondentes.
///
/// Mantém a lógica de prefixo "00 - " para códigos <= 18, e apenas a descrição para outros.
/// Valores nulos na entrada resultam em nulos na saída.
/// Valores numéricos sem correspondência no enum resultam em "{código}: Sem descrição".
pub fn descricao_da_natureza_da_bc_dos_creditos(col: Column) -> PolarsResult<Column> {
    col.cast(&DataType::Int64)?
        .i64()?
        .try_apply_into_string_amortized(|n, buf| {
            buf.clear();

            match NaturezaBC::from_i64(n) {
                Some(natureza) => {
                    let descricao = natureza.as_str();
                    if n <= 18 {
                        // Lógica original para prefixo "00 - "
                        write!(buf, "{:02} - {}", n, descricao)
                            .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                    } else {
                        // Apenas a descrição para códigos > 18
                        buf.push_str(descricao);
                        Ok(())
                    }
                }
                None => {
                    // Caso o número não conste no Enum
                    write!(buf, "{:02} - Sem descrição", n)
                        .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
                }
            }
        })
        .map(|ca| ca.into_column())
}

// ============================================================================
// CÓDIGO DA SITUAÇÃO TRIBUTÁRIA (CST)
// ============================================================================

/// Código da Situação Tributária (CST) para PIS/COFINS.
///
/// **Mnemônicos Adotados:**
/// - `Operacao` -> `Oper`
/// - `Tributavel` -> `Trib`
/// - `Aliquota` -> `Aliq`
/// - `Receita` -> `Rec`
/// - `Credito` -> `Cred`
/// - `Mercado Interno` -> `MI`
/// - `Exportacao` -> `Exp`
/// - `Aquisicao` -> `Aq`
/// - `Substituicao Tributaria` -> `ST` ou `SubstiTrib`
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodigoSituacaoTributaria {
    // Saídas (01-49)
    OperTribAliqBasica = 1,
    OperTribAliqDif = 2,
    OperTribAliqUnidade = 3,
    OperTribMonofasicaRevendaAliqZero = 4,
    OperTribST = 5,
    OperTribAliqZero = 6,
    OperIsenta = 7,
    OperSemIncidencia = 8,
    OperSuspensao = 9,
    OutrasOperSaida = 49,

    // Entradas com Crédito (50-56)
    CredVincExclRecTribMI = 50,
    CredVincExclRecNTribMI = 51,
    CredVincExclRecExp = 52,
    CredVincRecTribENTribMI = 53,
    CredVincRecTribMIExp = 54,
    CredVincRecNTribMIExp = 55,
    CredVincRecTribENTribMIExp = 56,

    // Crédito Presumido (60-67)
    CredPresAqExclRecTribMI = 60,
    CredPresAqExclRecNTribMI = 61,
    CredPresAqExclRecExp = 62,
    CredPresAqRecTribENTribMI = 63,
    CredPresAqRecTribMIExp = 64,
    CredPresAqRecNTribMIExp = 65,
    CredPresAqRecTribENTribMIExp = 66,
    CredPresOutrasOper = 67,

    // Entradas sem Crédito (70-75)
    AqSemCred = 70,
    AqIsencao = 71,
    AqSuspensao = 72,
    AqAliqZero = 73,
    AqSemIncidencia = 74,
    AqSubstiTrib = 75,

    // Outros (98-99)
    OutrasOperEntrada = 98,
    OutrasOper = 99,
}

impl CodigoSituacaoTributaria {
    /// Tenta converter um `u16` para `CodigoSituacaoTributaria`.
    ///
    /// Retorna `Some(CodigoSituacaoTributaria)` se o valor for válido,
    /// caso contrário, retorna `None`.
    pub const fn from_u16(n: u16) -> Option<Self> {
        match n {
            1 => Some(Self::OperTribAliqBasica),
            2 => Some(Self::OperTribAliqDif),
            3 => Some(Self::OperTribAliqUnidade),
            4 => Some(Self::OperTribMonofasicaRevendaAliqZero),
            5 => Some(Self::OperTribST),
            6 => Some(Self::OperTribAliqZero),
            7 => Some(Self::OperIsenta),
            8 => Some(Self::OperSemIncidencia),
            9 => Some(Self::OperSuspensao),
            49 => Some(Self::OutrasOperSaida),
            50 => Some(Self::CredVincExclRecTribMI),
            51 => Some(Self::CredVincExclRecNTribMI),
            52 => Some(Self::CredVincExclRecExp),
            53 => Some(Self::CredVincRecTribENTribMI),
            54 => Some(Self::CredVincRecTribMIExp),
            55 => Some(Self::CredVincRecNTribMIExp),
            56 => Some(Self::CredVincRecTribENTribMIExp),
            60 => Some(Self::CredPresAqExclRecTribMI),
            61 => Some(Self::CredPresAqExclRecNTribMI),
            62 => Some(Self::CredPresAqExclRecExp),
            63 => Some(Self::CredPresAqRecTribENTribMI),
            64 => Some(Self::CredPresAqRecTribMIExp),
            65 => Some(Self::CredPresAqRecNTribMIExp),
            66 => Some(Self::CredPresAqRecTribENTribMIExp),
            67 => Some(Self::CredPresOutrasOper),
            70 => Some(Self::AqSemCred),
            71 => Some(Self::AqIsencao),
            72 => Some(Self::AqSuspensao),
            73 => Some(Self::AqAliqZero),
            74 => Some(Self::AqSemIncidencia),
            75 => Some(Self::AqSubstiTrib),
            98 => Some(Self::OutrasOperEntrada),
            99 => Some(Self::OutrasOper),
            _ => None,
        }
    }

    /// Retorna a descrição completa do CST como uma string estática.
    #[rustfmt::skip]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OperTribAliqBasica => "Operação Tributável com Alíquota Básica",
            Self::OperTribAliqDif => "Operação Tributável com Alíquota Diferenciada",
            Self::OperTribAliqUnidade => "Operação Tributável com Alíquota por Unidade de Medida de Produto",
            Self::OperTribMonofasicaRevendaAliqZero => "Operação Tributável Monofásica - Revenda a Alíquota Zero",
            Self::OperTribST => "Operação Tributável por Substituição Tributária",
            Self::OperTribAliqZero => "Operação Tributável a Alíquota Zero",
            Self::OperIsenta => "Operação Isenta da Contribuição",
            Self::OperSemIncidencia => "Operação sem Incidência da Contribuição",
            Self::OperSuspensao => "Operação com Suspensão da Contribuição",
            Self::OutrasOperSaida => "Outras Operações de Saída",
            Self::CredVincExclRecTribMI => "Operação com Direito a Crédito - Vinculada Exclusivamente a Receita Tributada no Mercado Interno",
            Self::CredVincExclRecNTribMI => "Operação com Direito a Crédito - Vinculada Exclusivamente a Receita Não-Tributada no Mercado Interno",
            Self::CredVincExclRecExp => "Operação com Direito a Crédito - Vinculada Exclusivamente a Receita de Exportação",
            Self::CredVincRecTribENTribMI => "Operação com Direito a Crédito - Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno",
            Self::CredVincRecTribMIExp => "Operação com Direito a Crédito - Vinculada a Receitas Tributadas no Mercado Interno e de Exportação",
            Self::CredVincRecNTribMIExp => "Operação com Direito a Crédito - Vinculada a Receitas Não Tributadas no Mercado Interno e de Exportação",
            Self::CredVincRecTribENTribMIExp => "Operação com Direito a Crédito - Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno e de Exportação",
            Self::CredPresAqExclRecTribMI => "Crédito Presumido - Operação de Aquisição Vinculada Exclusivamente a Receita Tributada no Mercado Interno",
            Self::CredPresAqExclRecNTribMI => "Crédito Presumido - Operação de Aquisição Vinculada Exclusivamente a Receita Não-Tributada no Mercado Interno",
            Self::CredPresAqExclRecExp => "Crédito Presumido - Operação de Aquisição Vinculada Exclusivamente a Receita de Exportação",
            Self::CredPresAqRecTribENTribMI => "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno",
            Self::CredPresAqRecTribMIExp => "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Tributadas no Mercado Interno e de Exportação",
            Self::CredPresAqRecNTribMIExp => "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Não-Tributadas no Mercado Interno e de Exportação",
            Self::CredPresAqRecTribENTribMIExp => "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno e de Exportação",
            Self::CredPresOutrasOper => "Crédito Presumido - Outras Operações",
            Self::AqSemCred => "Operação de Aquisição sem Direito a Crédito",
            Self::AqIsencao => "Operação de Aquisição com Isenção",
            Self::AqSuspensao => "Operação de Aquisição com Suspensão",
            Self::AqAliqZero => "Operação de Aquisição a Alíquota Zero",
            Self::AqSemIncidencia => "Operação de Aquisição sem Incidência da Contribuição",
            Self::AqSubstiTrib => "Operação de Aquisição por Substituição Tributária",
            Self::OutrasOperEntrada => "Outras Operações de Entrada",
            Self::OutrasOper => "Outras Operações",
        }
    }
}

/// Transforma códigos CST em descrições formatadas ("XX - Descrição").
///
/// Valores nulos na entrada resultam em nulos na saída.
/// Valores numéricos sem correspondência no enum resultam em "{código}: Sem descrição".
pub fn descricao_do_cst(col: Column) -> PolarsResult<Column> {
    col.cast(&DataType::Int64)? // Converte (cast) a coluna para Int64 e acesso aos dados
        .i64()?
        .try_apply_into_string_amortized(|n, buf: &mut String| -> PolarsResult<()> {
            // Essencial para reutilizar o buffer da linha anterior
            buf.clear();
            let descricao = CodigoSituacaoTributaria::from_u16(n as u16)
                .map(|codigo_cst| codigo_cst.as_str())
                .unwrap_or("Sem descrição");

            // O write! retorna std::fmt::Error.
            write!(buf, "{:02} - {}", n, descricao)
                .map_err(|e| PolarsError::ComputeError(e.to_string().into()))
        })
        .map(|ca| ca.into_column())
}
