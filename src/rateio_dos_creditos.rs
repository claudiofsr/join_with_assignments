//! # Rateio Proporcional de Créditos de PIS/COFINS
//!
//! Este módulo gerencia o particionamento e a alocação dos créditos apurados no regime
//! não cumulativo (CSTs 50 a 66) entre os diferentes grupos de receita (tributada,
//! não tributada e exportação), conforme as regras estabelecidas pelo art. 3º, § 8º,
//! inciso II da Lei nº 10.833/2003 e pela Instrução Normativa RFB nº 2.121/2022.
//!
//! ## Sistemática de Rateio
//!
//! A segregação de créditos comuns utiliza duas etapas proporcionais consecutivas:
//!
//! 1. **Fator de Rateio Global (Misto):** Identifica a parcela do insumo comum atribuível
//!    à apuração não cumulativa frente à receita bruta total do período:
//!    Fator RBNC = RecBrutaNCumulativa / ReceitaBrutaTotal
//!
//! 2. **Fator de Rateio de Subgrupo:** Para CSTs de uso comum parcial ou global,
//!    aplica a proporção interna da receita vinculada sobre o somatório do subgrupo:
//!    Proporção = Receita Destino / Soma(Receitas do Subgrupo)

use crate::{Side::Left, coluna, cst_50_a_66, receita_nao_nula};
use polars::prelude::*;

// ============================================================================
// CONSTANTES DE COLUNAS (Prevenção de Typos)
// ============================================================================
const COL_TRIB: &str = "RBNC_Tributada";
const COL_NTRIB: &str = "RBNC_NTributada";
const COL_EXP: &str = "RBNC_Exportação";
const COL_REC_BRUTA_NCUM: &str = "RecBrutaNCumulativa";
const COL_REC_BRUTA_CUM: &str = "RecBrutaCumulativa";
const COL_REC_BRUTA_TOTAL: &str = "ReceitaBrutaTotal";

/// Define de forma estrita as combinações de rateio de créditos comuns permitidas pela legislação.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrupoRateio {
    /// CST 50/60: Crédito Exclusivo de Mercado Interno Tributado.
    Trib,
    /// CST 51/61: Crédito Exclusivo de Mercado Interno Não Tributado.
    NTrib,
    /// CST 52/62: Crédito Exclusivo de Exportação.
    Exportacao,
    /// CST 53/63: Comum entre Mercado Interno Tributado e Não Tributado.
    TribNTrib,
    /// CST 54/64: Comum entre Mercado Interno Tributado e Exportação.
    TribExportacao,
    /// CST 55/65: Comum entre Mercado Interno Não Tributado e Exportação.
    NTribExportacao,
    /// CST 56/66: Comum Global (Vinculado a todas as frentes).
    Global,
}

impl GrupoRateio {
    /// Retorna se a frente tributada está ativa neste grupo de rateio.
    #[inline]
    pub fn eh_tributada(&self) -> bool {
        matches!(
            self,
            Self::Trib | Self::TribNTrib | Self::TribExportacao | Self::Global
        )
    }

    /// Retorna se a frente não tributada está ativa neste grupo de rateio.
    #[inline]
    pub fn eh_ntributada(&self) -> bool {
        matches!(
            self,
            Self::NTrib | Self::TribNTrib | Self::NTribExportacao | Self::Global
        )
    }

    /// Retorna se a frente de exportação está ativa neste grupo de rateio.
    #[inline]
    pub fn eh_exportacao(&self) -> bool {
        matches!(
            self,
            Self::Exportacao | Self::TribExportacao | Self::NTribExportacao | Self::Global
        )
    }

    /// Retorna se o grupo representa um crédito de apropriação direta (exclusivo).
    #[inline]
    pub fn eh_exclusivo(&self) -> bool {
        matches!(self, Self::Trib | Self::NTrib | Self::Exportacao)
    }
}

/// Estrutura para processar o rateio proporcional de créditos comuns e exclusivos.
///
/// # Diagrama de Fluxo do Rateio
///
/// ```text
///                       [ Base de Cálculo (CST 50 a 66) ]
///                                       │
///                       ┌───────────────┴───────────────┐
///                       ▼                               ▼
///             [ Insumo Exclusivo ]            [ Insumo Comum Geral ]
///             (Apropriação Direta)              (Fator de Rateio)
///                       │                               │
///                       ▼                               ▼
///               (Aplica 100% na)             (Multiplica a Base por:)
///             (coluna correspondente)          RecBrutaNCumulativa
///                                              ───────────────────
///                                              ReceitaBrutaTotal
/// ```
///
/// # Classificação Didática dos Créditos (CSTs 50 a 66):
///
/// * **Apropriação Direta (Exclusivo):** Créditos vinculados integralmente a um tipo de receita (Tributada, NT ou Exportação).
/// * **Rateio Proporcional Parcial:** Créditos vinculados a mais de um tipo de receita, exigindo subdivisão interna.
/// * **Rateio Proporcional Geral (Global):** Créditos comuns que devem ser distribuídos sobre a totalidade das receitas do regime.
pub struct RateioDosCreditos {
    /// Expressão que aponta para a coluna de Base de Cálculo original.
    valor_bc: Expr,
    /// Fator de proporcionalidade não cumulativo (Regime Misto).
    fator_rbnc: Expr,
    /// Fator de proporcionalidade cumulativo (Regime Misto).
    fator_rbc: Expr,
}

impl Default for RateioDosCreditos {
    fn default() -> Self {
        Self::new()
    }
}

impl RateioDosCreditos {
    /// Instancia o rateio configurando os seletores de colunas e condições fiscais.
    pub fn new() -> Self {
        let valor_bc = col(coluna(Left, "valor_bc"));
        let fator_rbnc = col(COL_REC_BRUTA_NCUM) / col(COL_REC_BRUTA_TOTAL);
        let fator_rbc = col(COL_REC_BRUTA_CUM) / col(COL_REC_BRUTA_TOTAL);

        Self {
            valor_bc,
            fator_rbnc,
            fator_rbc,
        }
    }

    // =========================================================================
    // Lógica Matemática e Métodos Auxiliares
    // =========================================================================

    /// Retorna a expressão de verificação do CST com base nas propriedades aritméticas de sua terminação.
    fn condicao_grupo(&self, grupo: GrupoRateio) -> Expr {
        let digito = match grupo {
            GrupoRateio::Trib => 0,
            GrupoRateio::NTrib => 1,
            GrupoRateio::Exportacao => 2,
            GrupoRateio::TribNTrib => 3,
            GrupoRateio::TribExportacao => 4,
            GrupoRateio::NTribExportacao => 5,
            GrupoRateio::Global => 6,
        };
        (col(coluna(Left, "cst")) % lit(10)).eq(lit(digito))
    }

    /// Filtro auxiliar para identificar se a linha corresponde a qualquer crédito de apropriação direta.
    fn condicao_exclusivo(&self) -> Expr {
        self.condicao_grupo(GrupoRateio::Trib)
            .or(self.condicao_grupo(GrupoRateio::NTrib))
            .or(self.condicao_grupo(GrupoRateio::Exportacao))
    }

    /// Filtro auxiliar para identificar se a linha corresponde a qualquer crédito de rateio comum.
    fn condicao_comum(&self) -> Expr {
        self.condicao_grupo(GrupoRateio::TribNTrib)
            .or(self.condicao_grupo(GrupoRateio::TribExportacao))
            .or(self.condicao_grupo(GrupoRateio::NTribExportacao))
            .or(self.condicao_grupo(GrupoRateio::Global))
    }

    /// Calcula a proporção de uma receita destino específica sobre o somatório das frentes ativas.
    fn calcular_proporcao_interna(&self, col_dest: &str, grupo: GrupoRateio) -> Expr {
        let soma_denominador = if grupo == GrupoRateio::Global {
            col(COL_REC_BRUTA_NCUM)
        } else {
            let mut soma = lit(0.0);
            if grupo.eh_tributada() {
                soma = soma + col(COL_TRIB);
            }
            if grupo.eh_ntributada() {
                soma = soma + col(COL_NTRIB);
            }
            if grupo.eh_exportacao() {
                soma = soma + col(COL_EXP);
            }
            soma
        };

        when(soma_denominador.clone().gt(lit(0.0)))
            .then(col(col_dest) / soma_denominador)
            .otherwise(lit(0.0))
    }

    /// Executa o cálculo proporcional de rateio de crédito comum.
    fn ratear_comum(&self, col_dest: &str, grupo: GrupoRateio) -> Expr {
        let proporcao = self.calcular_proporcao_interna(col_dest, grupo);
        self.valor_bc.clone() * self.fator_rbnc.clone() * proporcao
    }

    /// Centraliza a lógica de rateios gerando uma expressão condicional unificada.
    ///
    /// Esta função utiliza a operação de acumulação [`Iterator::fold`] para construir de forma
    /// declarativa uma árvore de expressões aninhadas (`when / then / otherwise`). Em tempo de
    /// execução, o motor do Polars avalia esse conjunto de condições para cada linha do DataFrame.
    ///
    /// ### Exclusividade das Condições
    ///
    /// As verificações de grupo são baseadas no dígito final do CST (`cst % 10`), o que torna as
    /// condições de enquadramento dos grupos ([`GrupoRateio`]) mutuamente excludentes.
    /// Como uma linha da tabela possui um único CST e, portanto, atende a apenas um grupo por vez:
    ///
    /// 1. Não existe o risco de uma linha satisfazer duas condições simultaneamente.
    /// 2. A ordem em que o `fold` aninha as ramificações de fallback no parâmetro `otherwise`
    ///    não interfere na exatidão do resultado numérico final.
    /// 3. Em tempo de execução, o Polars avalia as ramificações e interrompe a verificação
    ///    (*short-circuit*) assim que o grupo correspondente à linha é localizado.
    fn aplicar_rateios(&self, col_destino: &str, grupos: &[GrupoRateio]) -> Expr {
        grupos.iter().fold(lit(NULL), |acumulador, &grupo| {
            let valor_calculado = if grupo.eh_exclusivo() {
                self.valor_bc.clone()
            } else {
                self.ratear_comum(col_destino, grupo)
            };

            when(self.condicao_grupo(grupo))
                .then(valor_calculado)
                .otherwise(acumulador)
        })
    }

    // =========================================================================
    // Projeções Individuais de Colunas de Crédito (Retornam Expr Pura)
    // =========================================================================

    /// Retorna a expressão de rateio para a parcela atribuída a receitas tributadas no mercado interno.
    pub fn rbnc_tributada(&self) -> Expr {
        self.aplicar_rateios(
            COL_TRIB,
            &[
                GrupoRateio::Trib,
                GrupoRateio::TribNTrib,
                GrupoRateio::TribExportacao,
                GrupoRateio::Global,
            ],
        )
    }

    /// Retorna a expressão de rateio para a parcela atribuída a receitas desoneradas no mercado interno.
    pub fn rbnc_ntributada(&self) -> Expr {
        self.aplicar_rateios(
            COL_NTRIB,
            &[
                GrupoRateio::NTrib,
                GrupoRateio::TribNTrib,
                GrupoRateio::NTribExportacao,
                GrupoRateio::Global,
            ],
        )
    }

    /// Retorna a expressão de rateio para a parcela atribuída a receitas de exportação direta.
    pub fn rbnc_exportacao(&self) -> Expr {
        self.aplicar_rateios(
            COL_EXP,
            &[
                GrupoRateio::Exportacao,
                GrupoRateio::TribExportacao,
                GrupoRateio::NTribExportacao,
                GrupoRateio::Global,
            ],
        )
    }

    /*
    /// Retorna a expressão de rateio para a parcela atribuída a receitas tributadas no mercado interno.
    pub fn rbnc_tributada(&self) -> Expr {
        when(self.condicao_grupo(GrupoRateio::Trib))
            .then(self.valor_bc.clone())
            .when(self.condicao_grupo(GrupoRateio::TribNTrib))
            .then(self.ratear_comum(COL_TRIB, GrupoRateio::TribNTrib))
            .when(self.condicao_grupo(GrupoRateio::TribExportacao))
            .then(self.ratear_comum(COL_TRIB, GrupoRateio::TribExportacao))
            .when(self.condicao_grupo(GrupoRateio::Global))
            .then(self.ratear_comum(COL_TRIB, GrupoRateio::Global))
            .otherwise(lit(NULL))
    }

    /// Retorna a expressão de rateio para a parcela atribuída a receitas desoneradas no mercado interno.
    pub fn rbnc_ntributada(&self) -> Expr {
        when(self.condicao_grupo(GrupoRateio::NTrib))
            .then(self.valor_bc.clone())
            .when(self.condicao_grupo(GrupoRateio::TribNTrib))
            .then(self.ratear_comum(COL_NTRIB, GrupoRateio::TribNTrib))
            .when(self.condicao_grupo(GrupoRateio::NTribExportacao))
            .then(self.ratear_comum(COL_NTRIB, GrupoRateio::NTribExportacao))
            .when(self.condicao_grupo(GrupoRateio::Global))
            .then(self.ratear_comum(COL_NTRIB, GrupoRateio::Global))
            .otherwise(lit(NULL))
    }

    /// Retorna a expressão de rateio para a parcela atribuída a receitas de exportação direta.
    pub fn rbnc_exportacao(&self) -> Expr {
        when(self.condicao_grupo(GrupoRateio::Exportacao))
            .then(self.valor_bc.clone())
            .when(self.condicao_grupo(GrupoRateio::TribExportacao))
            .then(self.ratear_comum(COL_EXP, GrupoRateio::TribExportacao))
            .when(self.condicao_grupo(GrupoRateio::NTribExportacao))
            .then(self.ratear_comum(COL_EXP, GrupoRateio::NTribExportacao))
            .when(self.condicao_grupo(GrupoRateio::Global))
            .then(self.ratear_comum(COL_EXP, GrupoRateio::Global))
            .otherwise(lit(NULL))
    }
    */

    /// Consolida a base de cálculo total atribuível à totalidade do regime não cumulativo.
    pub fn rec_bruta_ncumulativa(&self) -> Expr {
        when(self.condicao_exclusivo())
            .then(self.valor_bc.clone())
            .when(self.condicao_comum())
            .then(self.valor_bc.clone() * self.fator_rbnc.clone())
            .otherwise(lit(NULL))
    }

    /// Identifica e quantifica os valores expurgados da base de créditos por estarem associados
    /// às saídas sujeitas ao regime cumulativo (expurgo obrigatório).
    pub fn rec_bruta_cumulativa(&self) -> Expr {
        when(self.condicao_exclusivo())
            .then(lit(NULL)) // Créditos exclusivos de RBNC não possuem parcela de expurgo cumulativo
            .when(self.condicao_comum())
            .then(self.valor_bc.clone() * self.fator_rbc.clone())
            .otherwise(lit(NULL))
    }

    /// Retorna a base de cálculo total unificada (100% da linha do insumo).
    pub fn receita_bruta_total(&self) -> Expr {
        self.valor_bc.clone()
    }

    // =========================================================================
    // Orquestração de Projeção com Envelopamento Único de Segurança
    // =========================================================================

    /// Consolida as expressões de todas as colunas aplicando a verificação de segurança de forma centralizada.
    pub fn gerar_colunas_rateio(&self) -> PolarsResult<[Expr; 6]> {
        // Avalia a condição de segurança apenas uma vez
        let condicao = cst_50_a_66()?.and(receita_nao_nula());

        // Envelopador reutilizável para aplicar a validação e fallback individual
        let wrap = |expr_calculada: Expr, nome_coluna: &str| {
            when(condicao.clone())
                .then(expr_calculada)
                .otherwise(col(nome_coluna))
                .alias(nome_coluna)
        };

        Ok([
            wrap(self.rbnc_tributada(), COL_TRIB),
            wrap(self.rbnc_ntributada(), COL_NTRIB),
            wrap(self.rbnc_exportacao(), COL_EXP),
            wrap(self.rec_bruta_ncumulativa(), COL_REC_BRUTA_NCUM),
            wrap(self.rec_bruta_cumulativa(), COL_REC_BRUTA_CUM),
            wrap(self.receita_bruta_total(), COL_REC_BRUTA_TOTAL),
        ])
    }
}

//----------------------------------------------------------------------------//
//                                   Tests                                    //
//----------------------------------------------------------------------------//

// cargo test -- --help
// cargo test -- --nocapture
// cargo test -- --show-output

/// Run tests with:
/// cargo test -- --show-output tests_ratear_creditos
#[cfg(test)]
mod tests_ratear_creditos {
    use super::*;
    use crate::{JoinResult, LazyFrameExtension, configure_the_environment};

    // =========================================================================
    // ALGORITMO LEGADO (Alternativa de Comparação para Testes de Regressão)
    // =========================================================================

    /// Rateia os créditos conforme as diretrizes de proporcionalidade da Receita Bruta (Legado).
    fn ratear_creditos(receita: &str) -> PolarsResult<Expr> {
        let cst: &str = coluna(Left, "cst");
        let valor_bc: &str = coluna(Left, "valor_bc");

        let is_50_ou_60 = (col(cst) % lit(10)).eq(lit(0));
        let is_51_ou_61 = (col(cst) % lit(10)).eq(lit(1));
        let is_52_ou_62 = (col(cst) % lit(10)).eq(lit(2));
        let is_53_ou_63 = (col(cst) % lit(10)).eq(lit(3));
        let is_54_ou_64 = (col(cst) % lit(10)).eq(lit(4));
        let is_55_ou_65 = (col(cst) % lit(10)).eq(lit(5));
        let is_56_ou_66 = (col(cst) % lit(10)).eq(lit(6));

        let cst_exclusivo = is_50_ou_60
            .clone()
            .or(is_51_ou_61.clone())
            .or(is_52_ou_62.clone());

        let fator_rbnc = col("RecBrutaNCumulativa") / col("ReceitaBrutaTotal");

        let cst_50_ou_60: Expr = is_50_ou_60.clone().and(lit(receita == "RBNC_Tributada"));
        let cst_51_ou_61: Expr = is_51_ou_61.clone().and(lit(receita == "RBNC_NTributada"));
        let cst_52_ou_62: Expr = is_52_ou_62.clone().and(lit(receita == "RBNC_Exportação"));

        let cst_53_ou_63: Expr = is_53_ou_63.and(lit(
            receita == "RBNC_Tributada" || receita == "RBNC_NTributada"
        ));
        let cst_54_ou_64: Expr = is_54_ou_64.and(lit(
            receita == "RBNC_Tributada" || receita == "RBNC_Exportação"
        ));
        let cst_55_ou_65: Expr = is_55_ou_65.and(lit(
            receita == "RBNC_NTributada" || receita == "RBNC_Exportação"
        ));

        let cst_56_ou_66: Expr = is_56_ou_66.and(lit(receita == "RBNC_Tributada"
            || receita == "RBNC_NTributada"
            || receita == "RBNC_Exportação"));

        let cst_rec_bruta_ncumulativa: Expr = lit(receita == "RecBrutaNCumulativa");
        let cst_rec_bruta_cumulativa: Expr = lit(receita == "RecBrutaCumulativa");
        let cst_rec_bruta_total: Expr = lit(receita == "ReceitaBrutaTotal");

        let expr = when(cst_50_a_66()?.and(receita_nao_nula()))
            .then(
                when(cst_56_ou_66)
                    .then(col(valor_bc) * col(receita) / col("ReceitaBrutaTotal"))
                    .when(cst_50_ou_60)
                    .then(col(valor_bc))
                    .when(cst_51_ou_61)
                    .then(col(valor_bc))
                    .when(cst_52_ou_62)
                    .then(col(valor_bc))
                    .when(cst_53_ou_63)
                    .then(
                        col(valor_bc) * fator_rbnc.clone() * col(receita)
                            / (col("RBNC_Tributada") + col("RBNC_NTributada")),
                    )
                    .when(cst_54_ou_64)
                    .then(
                        col(valor_bc) * fator_rbnc.clone() * col(receita)
                            / (col("RBNC_Tributada") + col("RBNC_Exportação")),
                    )
                    .when(cst_55_ou_65)
                    .then(
                        col(valor_bc) * fator_rbnc.clone() * col(receita)
                            / (col("RBNC_NTributada") + col("RBNC_Exportação")),
                    )
                    .when(cst_rec_bruta_ncumulativa)
                    .then(
                        when(cst_exclusivo.clone())
                            .then(col(valor_bc))
                            .otherwise(col(valor_bc) * col(receita) / col("ReceitaBrutaTotal")),
                    )
                    .when(cst_rec_bruta_cumulativa)
                    .then(
                        when(cst_exclusivo)
                            .then(lit(NULL))
                            .otherwise(col(valor_bc) * col(receita) / col("ReceitaBrutaTotal")),
                    )
                    .when(cst_rec_bruta_total)
                    .then(col(valor_bc))
                    .otherwise(lit(NULL))
                    .alias(receita),
            )
            .otherwise(col(receita));

        Ok(expr)
    }

    /// Aplica as expressões de rateio proporcional de forma totalmente Lazy sobre as colunas (Legado).
    fn ratear_bc_dos_creditos_conforme_receita_segregada_legacy(
        lazyframe: LazyFrame,
    ) -> JoinResult<LazyFrame> {
        let lazyframe = lazyframe.with_columns([
            ratear_creditos("RBNC_Tributada")?,
            ratear_creditos("RBNC_NTributada")?,
            ratear_creditos("RBNC_Exportação")?,
            ratear_creditos("RecBrutaNCumulativa")?,
            ratear_creditos("RecBrutaCumulativa")?,
            ratear_creditos("ReceitaBrutaTotal")?,
        ]);

        Ok(lazyframe)
    }

    // =========================================================================
    // TESTES UNITÁRIOS E COMPARATIVOS
    // =========================================================================

    #[test]
    /// Valida o rateio proporcional de créditos comuns (parciais e globais) e apropriações diretas
    /// utilizando parâmetros didáticos com Receita Total de R$ 100.000,00 (Fator RBNC de 90%).
    ///
    /// ### Demonstração Matemática dos Cálculos
    ///
    /// Os valores esperados na coluna de saída foram recalculados detalhadamente considerando
    /// a proporção do Fator Não-Cumulativo de 90%:
    ///
    /// Fator RBNC = 90.000,00 / 100.000,00 = 0,90 (90%)
    ///
    /// #### CST 53 (Tributado + Não Tributado no MI)
    /// * Subgrupo de rateio: 40.000,00 + 30.000,00 = 70.000,00
    /// * Tributada = 100.000,00 * 0,90 * (40.000,00 / 70.000,00) ≈ 51.428,57
    /// * Não Tributada = 100.000,00 * 0,90 * (30.000,00 / 70.000,00) ≈ 38.571,43
    ///
    /// #### CST 54 (Tributado + Exportação)
    /// * Subgrupo de rateio: 40.000,00 + 20.000,00 = 60.000,00
    /// * Tributada = 100.000,00 * 0,90 * (40.000,00 / 60.000,00) = 60.000,00
    /// * Exportação = 100.000,00 * 0,90 * (20.000,00 / 60.000,00) = 30.000,00
    ///
    /// #### CST 55 (Não Tributado + Exportação)
    /// * Subgrupo de rateio: 30.000,00 + 20.000,00 = 50.000,00
    /// * Não Tributada = 100.000,00 * 0,90 * (30.000,00 / 50.000,00) = 54.000,00
    /// * Exportação = 100.000,00 * 0,90 * (20.000,00 / 50.000,00) = 36.000,00
    ///
    /// #### CST 56 (Comum Global)
    /// * Subgrupo de rateio: RecBrutaNCumulativa = 90.000,00
    /// * Tributada = 100.000,00 * 0,90 * (40.000,00 / 90.000,00) = 40.000,00
    /// * Não Tributada = 100.000,00 * 0,90 * (30.000,00 / 90.000,00) = 30.000,00
    /// * Exportação = 100.000,00 * 0,90 * (20.000,00 / 90.000,00) = 20.000,00
    fn test_creditos_basicos() -> JoinResult<()> {
        // Configura o ambiente de exibição do console para depuração
        configure_the_environment();

        let cst_col = coluna(Left, "cst");
        let valor_bc_col = coluna(Left, "valor_bc");

        // Valores consolidados de Receita Bruta didáticos e simplificados (Fator RBNC = 90%)
        let rbnc_tributada_val = 40_000.00;
        let rbnc_ntributada_val = 30_000.00;
        let rbnc_exportacao_val = 20_000.00;
        let rec_bruta_nc_val = rbnc_tributada_val + rbnc_ntributada_val + rbnc_exportacao_val; // R$ 90.000,00
        let rec_bruta_cum_val = 10_000.00;
        let rec_bruta_total_val = rec_bruta_nc_val + rec_bruta_cum_val; // R$ 100.000,00

        // 1. Cria o DataFrame de entrada contendo as linhas de aquisições (Entradas) para cada CST:
        // - CST 50 (Exclusivo Tributada): BC de R$ 100.000,00
        // - CST 51 (Exclusivo Não Tributada): BC de R$ 200.000,00
        // - CST 52 (Exclusivo Exportação): BC de R$ 300.000,00
        // - CST 53 (Comum Parcial - Trib + NT): BC de R$ 100.000,00
        // - CST 54 (Comum Parcial - Trib + Exp): BC de R$ 100.000,00
        // - CST 55 (Comum Parcial - NT + Exp): BC de R$ 100.000,00
        // - CST 56 (Comum Global): BC de R$ 100.000,00
        let df_input = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [rbnc_tributada_val; 7],
            "RBNC_NTributada" => [rbnc_ntributada_val; 7],
            "RBNC_Exportação" => [rbnc_exportacao_val; 7],
            "RecBrutaNCumulativa" => [rec_bruta_nc_val; 7],
            "RecBrutaCumulativa" => [rec_bruta_cum_val; 7],
            "ReceitaBrutaTotal" => [rec_bruta_total_val; 7],
        ]?;

        println!("df_input:\n{}", df_input);

        // 2. Executa as projeções matemáticas através da struct RateioDosCreditos
        let rateador = RateioDosCreditos::new();
        let result_df = df_input
            .lazy()
            .with_columns(rateador.gerar_colunas_rateio()?)
            .collect()?
            // Arredonda para 2 casas decimais para validação de centavos idêntica à planilha
            .lazy()
            .round_float_columns(2)
            .collect()?;

        println!("Resultado Obtido:\n{}", result_df);

        // 3. Cria o DataFrame esperado com os novos valores calculados sob a base de R$ 100k
        // Proporções esperadas (Fator RBNC = 90%):
        // - CST 50: 100k integral na coluna Trib e na col RBNC
        // - CST 51: 200k integral na coluna NT e na col RBNC
        // - CST 52: 300k integral na coluna Exp e na col RBNC
        // - CST 53: Trib = 100k * 0.9 * (4/7) = 51.428,57 | NT = 100k * 0.9 * (3/7) = 38.571,43
        // - CST 54: Trib = 100k * 0.9 * (4/6) = 60.000,00 | Exp = 100k * 0.9 * (2/6) = 30.000,00
        // - CST 55: NT = 100k * 0.9 * (3/5) = 54.000,00   | Exp = 100k * 0.9 * (2/5) = 36.000,00
        // - CST 56: Trib = 100k * 0.9 * (4/9) = 40.000,00 | NT = 30.000,00 | Exp = 20.000,00
        let df_expected = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [Some(100_000.00), None, None, Some(51_428.57), Some(60_000.00), None, Some(40_000.00)],
            "RBNC_NTributada" => [None, Some(200_000.00), None, Some(38_571.43), None, Some(54_000.00), Some(30_000.00)],
            "RBNC_Exportação" => [None::<f64>, None, Some(300_000.00), None, Some(30_000.00), Some(36_000.00), Some(20_000.00)],
            "RecBrutaNCumulativa" => [Some(100_000.00), Some(200_000.00), Some(300_000.00), Some(90_000.00), Some(90_000.00), Some(90_000.00), Some(90_000.00)],
            "RecBrutaCumulativa" => [None, None, None, Some(10_000.00), Some(10_000.00), Some(10_000.00), Some(10_000.00)],
            "ReceitaBrutaTotal" => [Some(100_000.00), Some(200_000.00), Some(300_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00)],
        ]?;

        // 4. Compara os DataFrames para garantir exatidão absoluta
        assert_eq!(result_df, df_expected);

        Ok(())
    }

    #[test]
    /// Valida o rateio proporcional de créditos comuns (parciais e globais) e apropriações diretas
    /// utilizando parâmetros didáticos com Receita Total de R$ 100.000,00 (Fator RBNC de 75%).
    ///
    /// ### Demonstração Matemática dos Cálculos
    ///
    /// Os valores esperados na coluna de saída foram recalculados detalhadamente considerando
    /// a proporção do Fator Não-Cumulativo de 75%:
    ///
    /// Fator RBNC = 75.000,00 / 100.000,00 = 0,75 (75%)
    ///
    /// #### CST 53 (Tributado + Não Tributado no MI)
    /// * Subgrupo de rateio: 13.000,00 + 37.000,00 = 50.000,00
    /// * Tributada = 100.000,00 * 0,75 * (13.000,00 / 50.000,00) = 19.500,00
    /// * Não Tributada = 100.000,00 * 0,75 * (37.000,00 / 50.000,00) = 55.500,00
    ///
    /// #### CST 54 (Tributado + Exportação)
    /// * Subgrupo de rateio: 13.000,00 + 25.000,00 = 38.000,00
    /// * Tributada = 100.000,00 * 0,75 * (13.000,00 / 38.000,00) ≈ 25.657,89
    /// * Exportação = 100.000,00 * 0,75 * (25.000,00 / 38.000,00) ≈ 49.342,11
    ///
    /// #### CST 55 (Não Tributado + Exportação)
    /// * Subgrupo de rateio: 37.000,00 + 25.000,00 = 62.000,00
    /// * Não Tributada = 100.000,00 * 0,75 * (37.000,00 / 62.000,00) ≈ 44.758,06
    /// * Exportação = 100.000,00 * 0,75 * (25.000,00 / 62.000,00) ≈ 30.241,94
    ///
    /// #### CST 56 (Comum Global)
    /// * Subgrupo de rateio: RecBrutaNCumulativa = 75.000,00
    /// * Tributada = 100.000,00 * 0,75 * (13.000,00 / 75.000,00) = 13.000,00
    /// * Não Tributada = 100.000,00 * 0,75 * (37.000,00 / 75.000,00) = 37.000,00
    /// * Exportação = 100.000,00 * 0,75 * (25.000,00 / 75.000,00) = 25.000,00
    fn test_creditos_regime_misto() -> JoinResult<()> {
        // Configura o ambiente de exibição do console para depuração
        configure_the_environment();

        let cst_col = coluna(Left, "cst");
        let valor_bc_col = coluna(Left, "valor_bc");

        // Valores consolidados de Receita Bruta didáticos e simplificados (Fator RBNC = 75%)
        let rbnc_tributada_val = 13_000.00;
        let rbnc_ntributada_val = 37_000.00;
        let rbnc_exportacao_val = 25_000.00;
        let rec_bruta_nc_val = rbnc_tributada_val + rbnc_ntributada_val + rbnc_exportacao_val; // R$ 75.000,00
        let rec_bruta_cum_val = 25_000.00;
        let rec_bruta_total_val = rec_bruta_nc_val + rec_bruta_cum_val; // R$ 100.000,00

        // 1. Cria o DataFrame de entrada contendo as linhas de aquisições (Entradas) para cada CST:
        // - CST 50 (Exclusivo Tributada): BC de R$ 100.000,00
        // - CST 51 (Exclusivo Não Tributada): BC de R$ 200.000,00
        // - CST 52 (Exclusivo Exportação): BC de R$ 300.000,00
        // - CST 53 (Comum Parcial - Trib + NT): BC de R$ 100.000,00
        // - CST 54 (Comum Parcial - Trib + Exp): BC de R$ 100.000,00
        // - CST 55 (Comum Parcial - NT + Exp): BC de R$ 100.000,00
        // - CST 56 (Comum Global): BC de R$ 100.000,00
        let df_input = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [rbnc_tributada_val; 7],
            "RBNC_NTributada" => [rbnc_ntributada_val; 7],
            "RBNC_Exportação" => [rbnc_exportacao_val; 7],
            "RecBrutaNCumulativa" => [rec_bruta_nc_val; 7],
            "RecBrutaCumulativa" => [rec_bruta_cum_val; 7],
            "ReceitaBrutaTotal" => [rec_bruta_total_val; 7],
        ]?;

        println!("df_input:\n{}", df_input);

        // 2. Executa as projeções matemáticas através da struct RateioDosCreditos
        let rateador = RateioDosCreditos::new();
        let result_df = df_input
            .lazy()
            .with_columns(rateador.gerar_colunas_rateio()?)
            .collect()?
            // Arredonda para 2 casas decimais para validação de centavos idêntica à planilha
            .lazy()
            .round_float_columns(2)
            .collect()?;

        println!("Resultado Obtido:\n{}", result_df);

        // 3. Cria o DataFrame esperado com os novos valores calculados sob a base de R$ 100k
        // Proporções esperadas (Fator RBNC = 75%):
        // - CST 50: 100k integral na coluna Trib e na col RBNC
        // - CST 51: 200k integral na coluna NT e na col RBNC
        // - CST 52: 300k integral na coluna Exp e na col RBNC
        // - CST 53: Trib = 100k * 0.75 * (13/50) = 19.500,00 | NT = 100k * 0.75 * (37/50) = 55.500,00
        // - CST 54: Trib = 100k * 0.75 * (13/38) = 25.657,89 | Exp = 100k * 0.75 * (25/38) = 49.342,11
        // - CST 55: NT = 100k * 0.75 * (37/62) = 44.758,06   | Exp = 100k * 0.75 * (25/62) = 30.241,94
        // - CST 56: Trib = 100k * 0.75 * (13/75) = 13.000,00 | NT = 37.000,00 | Exp = 25.000,00
        let df_expected = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [Some(100_000.00), None, None, Some(19_500.00), Some(25_657.89), None, Some(13_000.00)],
            "RBNC_NTributada" => [None, Some(200_000.00), None, Some(55_500.00), None, Some(44_758.06), Some(37_000.00)],
            "RBNC_Exportação" => [None::<f64>, None, Some(300_000.00), None, Some(49_342.11), Some(30_241.94), Some(25_000.00)],
            "RecBrutaNCumulativa" => [Some(100_000.00), Some(200_000.00), Some(300_000.00), Some(75_000.00), Some(75_000.00), Some(75_000.00), Some(75_000.00)],
            "RecBrutaCumulativa" => [None, None, None, Some(25_000.00), Some(25_000.00), Some(25_000.00), Some(25_000.00)],
            "ReceitaBrutaTotal" => [Some(100_000.00), Some(200_000.00), Some(300_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00)],
        ]?;

        assert_eq!(result_df, df_expected);
        Ok(())
    }

    #[test]
    /// Valida o comportamento do rateador em cenários de exceção com divisão por zero,
    /// utilizando a mesma estrutura completa do teste de créditos básicos (7 linhas de CSTs 50 a 56)
    /// com Receita Não-Cumulativa zerada (Fator RBNC de 0%).
    ///
    /// ### Demonstração Matemática dos Cálculos (Cenário de Exceção)
    ///
    /// Fator RBNC = 0,00 / 10.000,00 = 0,00 (0%)
    ///
    /// #### CST 50 (Exclusivo Tributada)
    /// * Apropriação Direta: Ignora o fator de rateio comum do período e garante 100% de apropriação.
    /// * Tributada = 100.000,00
    ///
    /// #### CST 51 (Exclusivo Não Tributada)
    /// * Apropriação Direta: Ignora o fator de rateio comum do período e garante 100% de apropriação.
    /// * Não Tributada = 200.000,00
    ///
    /// #### CST 52 (Exclusivo Exportação)
    /// * Apropriação Direta: Ignora o fator de rateio comum do período e garante 100% de apropriação.
    /// * Exportação = 300.000,00
    ///
    /// #### CST 53 (Comum Parcial - Trib + NT)
    /// * Subgrupo de rateio: 0,00 + 0,00 = 0,00 (Divisão por zero evitada de forma segura pelo sistema)
    /// * Tributada = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    /// * Não Tributada = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    ///
    /// #### CST 54 (Comum Parcial - Trib + Exp)
    /// * Subgrupo de rateio: 0,00 + 0,00 = 0,00 (Divisão por zero evitada de forma segura pelo sistema)
    /// * Tributada = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    /// * Exportação = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    ///
    /// #### CST 55 (Comum Parcial - NT + Exp)
    /// * Subgrupo de rateio: 0,00 + 0,00 = 0,00 (Divisão por zero evitada de forma segura pelo sistema)
    /// * Não Tributada = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    /// * Exportação = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    ///
    /// #### CST 56 (Comum Global)
    /// * Fator global comum nulo: Receita Não-Cumulativa total zerada.
    /// * Tributada = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    /// * Não Tributada = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    /// * Exportação = 100.000,00 * 0,00 * (0,00 / 0,00) -> Converge de forma segura para 0,00
    fn test_divisao_por_zero() -> JoinResult<()> {
        // Configura o ambiente de exibição do console para depuração
        configure_the_environment();

        let cst_col = coluna(Left, "cst");
        let valor_bc_col = coluna(Left, "valor_bc");

        // Valores consolidados de Receita Bruta simulando ausência total de receitas não cumulativas (Fator RBNC = 0%)
        let rbnc_tributada_val = 0.00;
        let rbnc_ntributada_val = 0.00;
        let rbnc_exportacao_val = 0.00;
        let rec_bruta_nc_val = rbnc_tributada_val + rbnc_ntributada_val + rbnc_exportacao_val; // R$ 0,00
        let rec_bruta_cum_val = 10_000.00;
        let rec_bruta_total_val = rec_bruta_nc_val + rec_bruta_cum_val; // R$ 10.000,00

        // 1. Cria o DataFrame de entrada contendo as linhas de aquisições (Entradas) para cada CST:
        // - CST 50 (Exclusivo Tributada): BC de R$ 100.000,00
        // - CST 51 (Exclusivo Não Tributada): BC de R$ 200.000,00
        // - CST 52 (Exclusivo Exportação): BC de R$ 300.000,00
        // - CST 53 (Comum Parcial - Trib + NT): BC de R$ 100.000,00
        // - CST 54 (Comum Parcial - Trib + Exp): BC de R$ 100.000,00
        // - CST 55 (Comum Parcial - NT + Exp): BC de R$ 100.000,00
        // - CST 56 (Comum Global): BC de R$ 100.000,00
        let df_input = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [rbnc_tributada_val; 7],
            "RBNC_NTributada" => [rbnc_ntributada_val; 7],
            "RBNC_Exportação" => [rbnc_exportacao_val; 7],
            "RecBrutaNCumulativa" => [rec_bruta_nc_val; 7],
            "RecBrutaCumulativa" => [rec_bruta_cum_val; 7],
            "ReceitaBrutaTotal" => [rec_bruta_total_val; 7],
        ]?;

        println!("df_input:\n{}", df_input);

        // 2. Executa as projeções matemáticas através da struct RateioDosCreditos
        let rateador = RateioDosCreditos::new();
        let result_df = df_input
            .lazy()
            .with_columns(rateador.gerar_colunas_rateio()?)
            .collect()?
            // Arredonda para 2 casas decimais para validação de centavos idêntica à planilha
            .lazy()
            .round_float_columns(2)
            .collect()?;

        println!(
            "Resultado Obtido (Casos Extremos - Divisão por Zero):\n{}",
            result_df
        );

        // 3. Cria o DataFrame esperado com os novos valores calculados sob a base de R$ 100k
        // Proporções esperadas (Fator RBNC = 0%):
        // - CST 50: 100k integral na coluna Trib e na col RBNC (Apropriação Direta não rateia)
        // - CST 51: 200k integral na coluna NT e na col RBNC
        // - CST 52: 300k integral na coluna Exp e na col RBNC
        // - CST 53: Ambas as colunas (Trib e NT) devem convergir para 0.00 de forma segura.
        // - CST 54: Ambas as colunas (Trib e Exp) devem convergir para 0.00 de forma segura.
        // - CST 55: Ambas as colunas (NT e Exp) devem convergir para 0.00 de forma segura.
        // - CST 56: Todas as colunas de destino não cumulativas devem convergir para 0.00.
        let df_expected = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [Some(100_000.00), None, None, Some(0.00), Some(0.00), None, Some(0.00)],
            "RBNC_NTributada" => [None, Some(200_000.00), None, Some(0.00), None, Some(0.00), Some(0.00)],
            "RBNC_Exportação" => [None::<f64>, None, Some(300_000.00), None, Some(0.00), Some(0.00), Some(0.00)],
            "RecBrutaNCumulativa" => [Some(100_000.00), Some(200_000.00), Some(300_000.00), Some(0.00), Some(0.00), Some(0.00), Some(0.00)],
            "RecBrutaCumulativa" => [None, None, None, Some(100_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00)],
            "ReceitaBrutaTotal" => [Some(100_000.00), Some(200_000.00), Some(300_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00), Some(100_000.00)],
        ]?;

        // 4. Compara os DataFrames para garantir exatidão absoluta
        assert_eq!(result_df, df_expected);

        Ok(())
    }

    #[test]
    /// Realiza um teste de regressão estrito, comparando os resultados finais obtidos
    /// entre o novo algoritmo estruturado (`gerar_colunas_rateio`) e o algoritmo legado
    /// baseado em strings (`ratear_bc_dos_creditos_conforme_receita_segregada_legacy`).
    ///
    /// Garante que ambas as implementações comportam-se de forma idêntica do ponto de vista funcional.
    fn test_comparativo_regressao_algoritmos() -> JoinResult<()> {
        configure_the_environment();

        let cst_col = coluna(Left, "cst");
        let valor_bc_col = coluna(Left, "valor_bc");

        let rbnc_tributada_val = 14_300.00;
        let rbnc_ntributada_val = 34_500.00;
        let rbnc_exportacao_val = 26_200.00;
        let rec_bruta_nc_val = rbnc_tributada_val + rbnc_ntributada_val + rbnc_exportacao_val;
        let rec_bruta_cum_val = 25_000.00;
        let rec_bruta_total_val = rec_bruta_nc_val + rec_bruta_cum_val;

        let df_input = df![
            cst_col => [50i64, 51i64, 52i64, 53i64, 54i64, 55i64, 56i64],
            valor_bc_col => [100_000.00, 200_000.00, 300_000.00, 100_000.00, 100_000.00, 100_000.00, 100_000.00],
            "RBNC_Tributada" => [rbnc_tributada_val; 7],
            "RBNC_NTributada" => [rbnc_ntributada_val; 7],
            "RBNC_Exportação" => [rbnc_exportacao_val; 7],
            "RecBrutaNCumulativa" => [rec_bruta_nc_val; 7],
            "RecBrutaCumulativa" => [rec_bruta_cum_val; 7],
            "ReceitaBrutaTotal" => [rec_bruta_total_val; 7],
        ]?;

        // 1. Processamento pelo novo algoritmo estruturado
        let rateador_novo = RateioDosCreditos::new();
        let result_novo = df_input
            .clone()
            .lazy()
            .with_columns(rateador_novo.gerar_colunas_rateio()?)
            .collect()?
            .lazy()
            .round_float_columns(2)
            .collect()?;

        println!("result_novo: {result_novo}");

        // 2. Processamento pelo algoritmo legado baseado em strings
        let result_legacy =
            ratear_bc_dos_creditos_conforme_receita_segregada_legacy(df_input.lazy())?
                .round_float_columns(2)
                .collect()?;

        println!("result_legacy: {result_legacy}");

        // Assegura comportamento idêntico
        assert_eq!(result_novo, result_legacy);

        Ok(())
    }
}
