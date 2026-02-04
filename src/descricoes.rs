use polars::prelude::*;
use std::fmt::write;

// --- 1. INDICADOR DE ORIGEM ---
#[derive(Debug, Clone, Copy)]
pub enum IndicadorOrigem {
    MercadoInterno = 0,
    Importacao = 1,
}

impl IndicadorOrigem {
    pub fn from_i64(v: i64) -> Option<Self> {
        match v {
            0 => Some(Self::MercadoInterno),
            1 => Some(Self::Importacao),
            _ => None,
        }
    }
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::MercadoInterno => "Operação no Mercado Interno",
            Self::Importacao => "Operação de Importação",
        }
    }
}

// --- 2. TIPO DE OPERAÇÃO ---
#[derive(Debug, Clone, Copy)]
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
    pub fn from_i64(v: i64) -> Option<Self> {
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
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Entrada => "Entrada",
            Self::Saida => "Saída",
            Self::AjusteAcrescimo | Self::AjusteReducao => "Ajuste",
            Self::DescontoProprio | Self::DescontoPosterior => "Desconto",
            Self::Detalhamento => "Detalhamento",
        }
    }
}

// --- 3. TIPO DE CRÉDITO ---
#[derive(Debug, Clone, Copy)]
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
    pub fn from_i64(v: i64) -> Option<Self> {
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
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AliquotaBasica => "Alíquota Básica",
            Self::AliquotasDiferenciadas => "Alíquotas Diferenciadas",
            Self::AliquotaUnidade => "Alíquota por Unidade de Produto",
            Self::EstoqueAbertura => "Estoque de Abertura",
            Self::AquisicaoEmbalagens => "Aquisição Embalagens para Revenda",
            Self::PresumidoAgroindustria => "Presumido da Agroindústria",
            Self::OutrosPresumidos => "Outros Créditos Presumidos",
            Self::Importacao => "Importação",
            Self::AtividadeImobiliaria => "Atividade Imobiliária",
            Self::Outros => "Outros",
            Self::Vazio => "",
        }
    }
}

// --- 4. MÊS ---
#[derive(Debug, Clone, Copy)]
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
    pub fn from_i64(v: i64) -> Option<Self> {
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
    pub fn as_str(&self) -> &'static str {
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

// --- 5. NATUREZA DA BC DOS CRÉDITOS ---
#[derive(Debug, Clone, Copy)]
pub enum NaturezaBC {
    AquisicaoBensRevenda = 1,
    AquisicaoBensInsumo = 2,
    AquisicaoServicosInsumo = 3,
    EnergiaEletrica = 4,
    AlugueisPredios = 5,
    AlugueisMaquinas = 6,
    ArmazenagemFrete = 7,
    ArrendamentoMercantil = 8,
    MaquinasDepreciacao = 9,
    MaquinasAquisicao = 10,
    EdificacoesBenfeitorias = 11,
    DevolucaoVendas = 12,
    OutrasOperacoes = 13,
    TransporteSubcontratacao = 14,
    ImobiliariaCustoIncorrido = 15,
    ImobiliariaCustoOrcado = 16,
    ServicosLimpeza = 17,
    EstoqueAberturaBens = 18,
    AjusteAcrescimoPis = 31,
    AjusteAcrescimoCofins = 35,
    AjusteReducaoPis = 41,
    AjusteReducaoCofins = 45,
    DescontoProprioPis = 51,
    DescontoProprioCofins = 55,
    DescontoPosteriorPis = 61,
    DescontoPosteriorCofins = 65,
    ReceitaBrutaValores = 80,
    ReceitaBrutaPercentuais = 81,
    BcDebitosOmitidos = 90,
    DebitosRevendaNcmPis = 91,
    DebitosRevendaNcmCofins = 95,
    BcAlíquotaBasica = 101,
    BcAliquotasDiferenciadas = 102,
    BcAlíquotaUnidade = 103,
    BcEstoqueAbertura = 104,
    BcAquisicaoEmbalagens = 105,
    BcPresumidoAgroindustria = 106,
    BcOutrosPresumidos = 107,
    BcImportacao = 108,
    BcAtividadeImobiliaria = 109,
    BcOutros = 199,
    CreditoApuradoPis = 201,
    CreditoApuradoCofins = 205,
    CreditoDisponivelAjustePis = 211,
    CreditoDisponivelAjusteCofins = 215,
    CreditoDisponivelDescontoPis = 221,
    CreditoDisponivelDescontoCofins = 225,
    BcValorTotal = 300,
    SaldoCreditoPis = 301,
    SaldoCreditoCofins = 305,
}

impl NaturezaBC {
    pub fn from_i64(v: i64) -> Option<Self> {
        match v {
            1 => Some(Self::AquisicaoBensRevenda),
            2 => Some(Self::AquisicaoBensInsumo),
            3 => Some(Self::AquisicaoServicosInsumo),
            4 => Some(Self::EnergiaEletrica),
            5 => Some(Self::AlugueisPredios),
            6 => Some(Self::AlugueisMaquinas),
            7 => Some(Self::ArmazenagemFrete),
            8 => Some(Self::ArrendamentoMercantil),
            9 => Some(Self::MaquinasDepreciacao),
            10 => Some(Self::MaquinasAquisicao),
            11 => Some(Self::EdificacoesBenfeitorias),
            12 => Some(Self::DevolucaoVendas),
            13 => Some(Self::OutrasOperacoes),
            14 => Some(Self::TransporteSubcontratacao),
            15 => Some(Self::ImobiliariaCustoIncorrido),
            16 => Some(Self::ImobiliariaCustoOrcado),
            17 => Some(Self::ServicosLimpeza),
            18 => Some(Self::EstoqueAberturaBens),
            31 => Some(Self::AjusteAcrescimoPis),
            35 => Some(Self::AjusteAcrescimoCofins),
            41 => Some(Self::AjusteReducaoPis),
            45 => Some(Self::AjusteReducaoCofins),
            51 => Some(Self::DescontoProprioPis),
            55 => Some(Self::DescontoProprioCofins),
            61 => Some(Self::DescontoPosteriorPis),
            65 => Some(Self::DescontoPosteriorCofins),
            80 => Some(Self::ReceitaBrutaValores),
            81 => Some(Self::ReceitaBrutaPercentuais),
            90 => Some(Self::BcDebitosOmitidos),
            91 => Some(Self::DebitosRevendaNcmPis),
            95 => Some(Self::DebitosRevendaNcmCofins),
            101 => Some(Self::BcAlíquotaBasica),
            102 => Some(Self::BcAliquotasDiferenciadas),
            103 => Some(Self::BcAlíquotaUnidade),
            104 => Some(Self::BcEstoqueAbertura),
            105 => Some(Self::BcAquisicaoEmbalagens),
            106 => Some(Self::BcPresumidoAgroindustria),
            107 => Some(Self::BcOutrosPresumidos),
            108 => Some(Self::BcImportacao),
            109 => Some(Self::BcAtividadeImobiliaria),
            199 => Some(Self::BcOutros),
            201 => Some(Self::CreditoApuradoPis),
            205 => Some(Self::CreditoApuradoCofins),
            211 => Some(Self::CreditoDisponivelAjustePis),
            215 => Some(Self::CreditoDisponivelAjusteCofins),
            221 => Some(Self::CreditoDisponivelDescontoPis),
            225 => Some(Self::CreditoDisponivelDescontoCofins),
            300 => Some(Self::BcValorTotal),
            301 => Some(Self::SaldoCreditoPis),
            305 => Some(Self::SaldoCreditoCofins),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::AquisicaoBensRevenda => "Aquisição de Bens para Revenda",
            Self::AquisicaoBensInsumo => "Aquisição de Bens Utilizados como Insumo",
            Self::AquisicaoServicosInsumo => "Aquisição de Serviços Utilizados como Insumo",
            Self::EnergiaEletrica => "Energia Elétrica e Térmica, Inclusive sob a Forma de Vapor",
            Self::AlugueisPredios => "Aluguéis de Prédios",
            Self::AlugueisMaquinas => "Aluguéis de Máquinas e Equipamentos",
            Self::ArmazenagemFrete => "Armazenagem de Mercadoria e Frete na Operação de Venda",
            Self::ArrendamentoMercantil => "Contraprestações de Arrendamento Mercantil",
            Self::MaquinasDepreciacao => {
                "Máquinas, Equipamentos ... (Crédito sobre Encargos de Depreciação)"
            }
            Self::MaquinasAquisicao => {
                "Máquinas, Equipamentos ... (Crédito com Base no Valor de Aquisição)"
            }
            Self::EdificacoesBenfeitorias => {
                "Amortizacao e Depreciação de Edificações e Benfeitorias em Imóveis"
            }
            Self::DevolucaoVendas => "Devolução de Vendas Sujeitas à Incidência Não-Cumulativa",
            Self::OutrasOperacoes => "Outras Operações com Direito a Crédito",
            Self::TransporteSubcontratacao => "Atividade de Transporte de Cargas - Subcontratação",
            Self::ImobiliariaCustoIncorrido => {
                "Atividade Imobiliária - Custo Incorrido de Unidade Imobiliária"
            }
            Self::ImobiliariaCustoOrcado => {
                "Atividade Imobiliária - Custo Orçado de Unidade não Concluída"
            }
            Self::ServicosLimpeza => {
                "Atividade de Prestação de Serviços de Limpeza, Conservação e Manutenção"
            }
            Self::EstoqueAberturaBens => "Estoque de Abertura de Bens",
            Self::AjusteAcrescimoPis => "Ajuste de Acréscimo (PIS/PASEP)",
            Self::AjusteAcrescimoCofins => "Ajuste de Acréscimo (COFINS)",
            Self::AjusteReducaoPis => "Ajuste de Redução (PIS/PASEP)",
            Self::AjusteReducaoCofins => "Ajuste de Redução (COFINS)",
            Self::DescontoProprioPis => {
                "Desconto da Contribuição Apurada no Próprio Período (PIS/PASEP)"
            }
            Self::DescontoProprioCofins => {
                "Desconto da Contribuição Apurada no Próprio Período (COFINS)"
            }
            Self::DescontoPosteriorPis => "Desconto Efetuado em Período Posterior (PIS/PASEP)",
            Self::DescontoPosteriorCofins => "Desconto Efetuado em Período Posterior (COFINS)",
            Self::ReceitaBrutaValores => "Receita Bruta (valores)",
            Self::ReceitaBrutaPercentuais => "Receita Bruta (percentuais)",
            Self::BcDebitosOmitidos => "Base de Cálculo de Débitos Omitidos",
            Self::DebitosRevendaNcmPis => {
                "Débitos: Revenda de Mercadorias de NCM 2309.90 (PIS/PASEP)"
            }
            Self::DebitosRevendaNcmCofins => {
                "Débitos: Revenda de Mercadorias de NCM 2309.90 (COFINS)"
            }
            Self::BcAlíquotaBasica => "Base de Cálculo dos Créditos - Alíquota Básica (Soma)",
            Self::BcAliquotasDiferenciadas => {
                "Base de Cálculo dos Créditos - Alíquotas Diferenciadas (Soma)"
            }
            Self::BcAlíquotaUnidade => {
                "Base de Cálculo dos Créditos - Alíquota por Unidade de Produto (Soma)"
            }
            Self::BcEstoqueAbertura => "Base de Cálculo dos Créditos - Estoque de Abertura (Soma)",
            Self::BcAquisicaoEmbalagens => {
                "Base de Cálculo dos Créditos - Aquisição Embalagens para Revenda (Soma)"
            }
            Self::BcPresumidoAgroindustria => {
                "Base de Cálculo dos Créditos - Presumido da Agroindústria (Soma)"
            }
            Self::BcOutrosPresumidos => {
                "Base de Cálculo dos Créditos - Outros Créditos Presumidos (Soma)"
            }
            Self::BcImportacao => "Base de Cálculo dos Créditos - Importação (Soma)",
            Self::BcAtividadeImobiliaria => {
                "Base de Cálculo dos Créditos - Atividade Imobiliária (Soma)"
            }
            Self::BcOutros => "Base de Cálculo dos Créditos - Outros (Soma)",
            Self::CreditoApuradoPis => "Crédito Apurado no Período (PIS/PASEP)",
            Self::CreditoApuradoCofins => "Crédito Apurado no Período (COFINS)",
            Self::CreditoDisponivelAjustePis => "Crédito Disponível após Ajustes (PIS/PASEP)",
            Self::CreditoDisponivelAjusteCofins => "Crédito Disponível após Ajustes (COFINS)",
            Self::CreditoDisponivelDescontoPis => "Crédito Disponível após Descontos (PIS/PASEP)",
            Self::CreditoDisponivelDescontoCofins => "Crédito Disponível após Descontos (COFINS)",
            Self::BcValorTotal => "Base de Cálculo dos Créditos - Valor Total (Soma)",
            Self::SaldoCreditoPis => {
                "Saldo de Crédito Passível de Desconto ou Ressarcimento (PIS/PASEP)"
            }
            Self::SaldoCreditoCofins => {
                "Saldo de Crédito Passível de Desconto ou Ressarcimento (COFINS)"
            }
        }
    }
}

// --- FUNÇÕES DE TRANSFORMAÇÃO POLARS ---

pub fn descricao_da_origem(col: Column) -> Result<Column, PolarsError> {
    let ca = col.cast(&DataType::Int64)?;
    let i64_ca = ca.i64()?;

    let new_ca = i64_ca.apply_into_string_amortized(|n, buf| match IndicadorOrigem::from_i64(n) {
        Some(e) => buf.push_str(e.as_str()),
        None => {
            let _ = write(buf, format_args!("{n}: Sem descrição"));
        }
    });
    Ok(new_ca.into_column())
}

pub fn descricao_do_tipo_de_operacao(col: Column) -> PolarsResult<Column> {
    let ca = col.cast(&DataType::Int64)?;
    let i64_ca = ca.i64()?;

    let new_ca = i64_ca.apply_into_string_amortized(|n, buf| match TipoOperacao::from_i64(n) {
        Some(e) => buf.push_str(e.as_str()),
        None => {
            let _ = write(buf, format_args!("{}: Sem descrição", n));
        }
    });
    Ok(new_ca.into_column())
}

pub fn descricao_do_tipo_de_credito(col: Column) -> PolarsResult<Column> {
    let ca = col.cast(&DataType::Int64)?;
    let i64_ca = ca.i64()?;

    let new_ca = i64_ca.apply_into_string_amortized(|n, buf| match TipoCredito::from_i64(n) {
        Some(e) => buf.push_str(e.as_str()),
        None => {
            let _ = write(buf, format_args!("{}: Sem descrição", n));
        }
    });
    Ok(new_ca.into_column())
}

pub fn descricao_do_mes(col: Column) -> PolarsResult<Column> {
    // 1. Converte a coluna para i64 de forma segura (funcional)
    // Isso elimina a necessidade do match col.dtype() manual.
    let ca = col.cast(&DataType::Int64)?;
    let i64_ca = ca.i64()?;

    let new_ca = i64_ca.apply_into_string_amortized(|n, buf| match Mes::from_i64(n) {
        Some(e) => buf.push_str(e.as_str()),
        None => {
            let _ = write(buf, format_args!("{}: Sem descrição", n));
        }
    });
    Ok(new_ca.into_column())
}

pub fn descricao_da_natureza_da_bc_dos_creditos(col: Column) -> PolarsResult<Column> {
    // 1. Resolve o erro de tipo: tenta converter qualquer entrada para i64.
    // Se a coluna já for i64, o custo é praticamente zero.
    let ca = col.cast(&DataType::Int64)?;
    let i64_ca = ca.i64()?;

    // 2. Aplicação amortizada (reutiliza o buffer de string para performance)
    let new_ca = i64_ca.apply_into_string_amortized(|n, buf| {
        // NaturezaBC::from_i64 retorna Option<NaturezaBC>
        match NaturezaBC::from_i64(n) {
            Some(natureza) => {
                let desc = natureza.as_str();
                if n <= 18 {
                    // Mantém a lógica original: prefixo "01 - " com preenchimento de zeros
                    let _ = write(buf, format_args!("{:02} - {}", n, desc));
                } else {
                    // Apenas a descrição para códigos > 18
                    buf.push_str(desc);
                }
            }
            None => {
                // Caso o número não conste no Enum
                let _ = write(buf, format_args!("{}: Sem descrição", n));
            }
        }
    });

    Ok(new_ca.into_column())
}

// ============================================================================
// Código da Situação Tributária (CST)
// ============================================================================

#[repr(u16)]
#[derive(Debug, Clone, Copy)]
pub enum CodigoSituacaoTributaria {
    OperacaoTributavelComAliquotaBasica = 1,
    OperacaoTributavelComAliquotaDiferenciada = 2,
    OperacaoTributavelComAliquotaPorUnidadeDeMedidaDeProduto = 3,
    OperacaoTributavelMonofasicaRevendaAAliquotaZero = 4,
    OperacaoTributavelPorSubstituicaoTributaria = 5,
    OperacaoTributavelAAliquotaZero = 6,
    OperacaoIsentaDaContribuicao = 7,
    OperacaoSemIncidenciaDaContribuicao = 8,
    OperacaoComSuspensaoDaContribuicao = 9,
    OutrasOperacoesDeSaida = 49,
    OperacaoComDireitoACreditoVincExclusivamenteARecTribNoMI = 50,
    OperacaoComDireitoACreditoVincExclusivamenteAReceitaSaoTributadaNoMI = 51,
    OperacaoComDireitoACreditoVincExclusivamenteAReceitaDeExportacao = 52,
    OperacaoComDireitoACreditoVincARecTribENTribNoMI = 53,
    OperacaoComDireitoACreditoVincARecTribNoMIEDeExportacao = 54,
    OperacaoComDireitoACreditoVincAReceitasNTribNoMIEDeExportacao = 55,
    OperacaoComDireitoACreditoVincARecTribENTribNoMIEDeExportacao = 56,
    CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteARecTribNoMI = 60,
    CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteAReceitaNaoTributadaNoMI = 61,
    CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteAReceitaDeExportacao = 62,
    CreditoPresumidoOperacaoDeAquisicaoVincARecTribENTribNoMI = 63,
    CreditoPresumidoOperacaoDeAquisicaoVincARecTribNoMIEDeExportacao = 64,
    CreditoPresumidoOperacaoDeAquisicaoVincAReceitasNTribNoMIEDeExportacao = 65,
    CreditoPresumidoOperacaoDeAquisicaoVincARecTribENTribNoMIEDeExportacao = 66,
    CreditoPresumidoOutrasOperacoes = 67,
    OperacaoDeAquisicaoSemDireitoACredito = 70,
    OperacaoDeAquisicaoComIsencao = 71,
    OperacaoDeAquisicaoComSuspensao = 72,
    OperacaoDeAquisicaoAAliquotaZero = 73,
    OperacaoDeAquisicaoSemIncidenciaDaContribuicao = 74,
    OperacaoDeAquisicaoPorSubstituicaoTributaria = 75,
    OutrasOperacoesDeEntrada = 98,
    OutrasOperacoes = 99,
}

impl CodigoSituacaoTributaria {
    pub const fn from_u16(cod: u16) -> Option<Self> {
        match cod {
            1 => Some(Self::OperacaoTributavelComAliquotaBasica),
            2 => Some(Self::OperacaoTributavelComAliquotaDiferenciada),
            3 => Some(Self::OperacaoTributavelComAliquotaPorUnidadeDeMedidaDeProduto),
            4 => Some(Self::OperacaoTributavelMonofasicaRevendaAAliquotaZero),
            5 => Some(Self::OperacaoTributavelPorSubstituicaoTributaria),
            6 => Some(Self::OperacaoTributavelAAliquotaZero),
            7 => Some(Self::OperacaoIsentaDaContribuicao),
            8 => Some(Self::OperacaoSemIncidenciaDaContribuicao),
            9 => Some(Self::OperacaoComSuspensaoDaContribuicao),
            49 => Some(Self::OutrasOperacoesDeSaida),
            50 => Some(Self::OperacaoComDireitoACreditoVincExclusivamenteARecTribNoMI),
            51 => Some(Self::OperacaoComDireitoACreditoVincExclusivamenteAReceitaSaoTributadaNoMI),
            52 => Some(Self::OperacaoComDireitoACreditoVincExclusivamenteAReceitaDeExportacao),
            53 => Some(Self::OperacaoComDireitoACreditoVincARecTribENTribNoMI),
            54 => Some(Self::OperacaoComDireitoACreditoVincARecTribNoMIEDeExportacao),
            55 => Some(Self::OperacaoComDireitoACreditoVincAReceitasNTribNoMIEDeExportacao),
            56 => Some(Self::OperacaoComDireitoACreditoVincARecTribENTribNoMIEDeExportacao),
            60 => Some(Self::CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteARecTribNoMI),
            61 => Some(
                Self::CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteAReceitaNaoTributadaNoMI,
            ),
            62 => Some(
                Self::CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteAReceitaDeExportacao,
            ),
            63 => Some(Self::CreditoPresumidoOperacaoDeAquisicaoVincARecTribENTribNoMI),
            64 => Some(Self::CreditoPresumidoOperacaoDeAquisicaoVincARecTribNoMIEDeExportacao),
            65 => {
                Some(Self::CreditoPresumidoOperacaoDeAquisicaoVincAReceitasNTribNoMIEDeExportacao)
            }
            66 => {
                Some(Self::CreditoPresumidoOperacaoDeAquisicaoVincARecTribENTribNoMIEDeExportacao)
            }
            67 => Some(Self::CreditoPresumidoOutrasOperacoes),
            70 => Some(Self::OperacaoDeAquisicaoSemDireitoACredito),
            71 => Some(Self::OperacaoDeAquisicaoComIsencao),
            72 => Some(Self::OperacaoDeAquisicaoComSuspensao),
            73 => Some(Self::OperacaoDeAquisicaoAAliquotaZero),
            74 => Some(Self::OperacaoDeAquisicaoSemIncidenciaDaContribuicao),
            75 => Some(Self::OperacaoDeAquisicaoPorSubstituicaoTributaria),
            98 => Some(Self::OutrasOperacoesDeEntrada),
            99 => Some(Self::OutrasOperacoes),
            _ => None,
        }
    }

    pub const fn code(self) -> u16 {
        self as u16
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::OperacaoTributavelComAliquotaBasica => "Operação Tributável com Alíquota Básica",
            Self::OperacaoTributavelComAliquotaDiferenciada => {
                "Operação Tributável com Alíquota Diferenciada"
            }
            Self::OperacaoTributavelComAliquotaPorUnidadeDeMedidaDeProduto => {
                "Operação Tributável com Alíquota por Unidade de Medida de Produto"
            }
            Self::OperacaoTributavelMonofasicaRevendaAAliquotaZero => {
                "Operação Tributável Monofásica - Revenda a Alíquota Zero"
            }
            Self::OperacaoTributavelPorSubstituicaoTributaria => {
                "Operação Tributável por Substituição Tributária"
            }
            Self::OperacaoTributavelAAliquotaZero => "Operação Tributável a Alíquota Zero",
            Self::OperacaoIsentaDaContribuicao => "Operação Isenta da Contribuição",
            Self::OperacaoSemIncidenciaDaContribuicao => "Operação sem Incidência da Contribuição",
            Self::OperacaoComSuspensaoDaContribuicao => "Operação com Suspensão da Contribuição",
            Self::OutrasOperacoesDeSaida => "Outras Operações de Saída",
            Self::OperacaoComDireitoACreditoVincExclusivamenteARecTribNoMI => {
                "Operação com Direito a Crédito - Vinculada Exclusivamente a Receita Tributada no Mercado Interno"
            }
            Self::OperacaoComDireitoACreditoVincExclusivamenteAReceitaSaoTributadaNoMI => {
                "Operação com Direito a Crédito - Vinculada Exclusivamente a Receita Não-Tributada no Mercado Interno"
            }
            Self::OperacaoComDireitoACreditoVincExclusivamenteAReceitaDeExportacao => {
                "Operação com Direito a Crédito - Vinculada Exclusivamente a Receita de Exportação"
            }
            Self::OperacaoComDireitoACreditoVincARecTribENTribNoMI => {
                "Operação com Direito a Crédito - Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno"
            }
            Self::OperacaoComDireitoACreditoVincARecTribNoMIEDeExportacao => {
                "Operação com Direito a Crédito - Vinculada a Receitas Tributadas no Mercado Interno e de Exportação"
            }
            Self::OperacaoComDireitoACreditoVincAReceitasNTribNoMIEDeExportacao => {
                "Operação com Direito a Crédito - Vinculada a Receitas Não Tributadas no Mercado Interno e de Exportação"
            }
            Self::OperacaoComDireitoACreditoVincARecTribENTribNoMIEDeExportacao => {
                "Operação com Direito a Crédito - Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno e de Exportação"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteARecTribNoMI => {
                "Crédito Presumido - Operação de Aquisição Vinculada Exclusivamente a Receita Tributada no Mercado Interno"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteAReceitaNaoTributadaNoMI => {
                "Crédito Presumido - Operação de Aquisição Vinculada Exclusivamente a Receita Não-Tributada no Mercado Interno"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincExclusivamenteAReceitaDeExportacao => {
                "Crédito Presumido - Operação de Aquisição Vinculada Exclusivamente a Receita de Exportação"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincARecTribENTribNoMI => {
                "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincARecTribNoMIEDeExportacao => {
                "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Tributadas no Mercado Interno e de Exportação"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincAReceitasNTribNoMIEDeExportacao => {
                "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Não-Tributadas no Mercado Interno e de Exportação"
            }
            Self::CreditoPresumidoOperacaoDeAquisicaoVincARecTribENTribNoMIEDeExportacao => {
                "Crédito Presumido - Operação de Aquisição Vinculada a Receitas Tributadas e Não-Tributadas no Mercado Interno e de Exportação"
            }
            Self::CreditoPresumidoOutrasOperacoes => "Crédito Presumido - Outras Operações",
            Self::OperacaoDeAquisicaoSemDireitoACredito => {
                "Operação de Aquisição sem Direito a Crédito"
            }
            Self::OperacaoDeAquisicaoComIsencao => "Operação de Aquisição com Isenção",
            Self::OperacaoDeAquisicaoComSuspensao => "Operação de Aquisição com Suspensão",
            Self::OperacaoDeAquisicaoAAliquotaZero => "Operação de Aquisição a Alíquota Zero",
            Self::OperacaoDeAquisicaoSemIncidenciaDaContribuicao => {
                "Operação de Aquisição sem Incidência da Contribuição"
            }
            Self::OperacaoDeAquisicaoPorSubstituicaoTributaria => {
                "Operação de Aquisição por Substituição Tributária"
            }
            Self::OutrasOperacoesDeEntrada => "Outras Operações de Entrada",
            Self::OutrasOperacoes => "Outras Operações",
        }
    }
}

pub fn descricao_do_cst(col: Column) -> PolarsResult<Column> {
    // 1. Converte a coluna para i64 de forma funcional e segura.
    let ca = col.cast(&DataType::Int64)?;
    let i64_ca = ca.i64()?;

    // 2. Aplicação amortizada (reutiliza o buffer interno de string)
    let new_ca = i64_ca.apply_into_string_amortized(|n, buf| {
        // Converte i64 -> u16 para bater com o Enum
        match CodigoSituacaoTributaria::from_u16(n as u16) {
            Some(cst) => {
                // Formatação idiomática: "01 - Descrição"
                let _ = write(buf, format_args!("{:02} - {}", cst.code(), cst.as_str()));
            }
            None => {
                // Fallback caso o código não exista na tabela
                let _ = write(buf, format_args!("{}: Sem descrição", n));
            }
        }
    });

    Ok(new_ca.into_column())
}
