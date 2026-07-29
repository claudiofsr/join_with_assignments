//! Módulo responsável pela gestão e aplicação das regras tributárias brasileiras de PIS e COFINS.
//!
//! Ele centraliza as lógicas de classificação fiscal para os regimes de:
//! - Alíquota Zero (com base na Lei nº 10.925/2004, Lei nº 10.865/2004, etc.)
//! - Crédito Presumido (cadeias do agronegócio e pecuária)
//! - Incidência Monofásica (combustíveis, cosméticos, fármacos, autopeças e pneus)

pub mod aplicacao_do_regime;
pub mod legislacao_aliquota_zero;
pub mod legislacao_credito_presumido;
pub mod legislacao_incidencia_monofasica;

pub use aplicacao_do_regime::*;
