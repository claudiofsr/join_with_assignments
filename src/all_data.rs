//! # Excel Data Container
//!
//! Este módulo gerencia os conjuntos de dados que alimentarão as planilhas do Excel,
//! permitindo a orquestração de layouts paralelos ou gravação sequencial direta no workbook.

use polars::prelude::DataFrame;
use rust_xlsxwriter::{Workbook, Worksheet};

use crate::{
    JoinResult,
    excel::{ExcelMemoryMode, SheetContext, process_sheet_type, process_sheet_type_sequential},
};

/// Estrutura de contexto unificado contendo referências aos DataFrames processados.
pub struct AllData<'a> {
    pub itens: &'a DataFrame,
    pub efd_original: &'a DataFrame,
    pub efd_auditoria: &'a DataFrame,
}

impl<'a> AllData<'a> {
    /// Instancia o contêiner a partir de referências aos DataFrames de origem.
    pub fn new(
        itens: &'a DataFrame,
        efd_original: &'a DataFrame,
        efd_auditoria: &'a DataFrame,
    ) -> Self {
        Self {
            itens,
            efd_original,
            efd_auditoria,
        }
    }

    /// Constrói e retorna as planilhas geradas simultaneamente via Rayon (em memória).
    pub fn generate_worksheets_in_parallel(&self) -> JoinResult<Vec<Worksheet>> {
        let mut res_itens: JoinResult<Vec<Worksheet>> = Ok(Vec::new());
        let mut res_orig: JoinResult<Vec<Worksheet>> = Ok(Vec::new());
        let mut res_aud: JoinResult<Vec<Worksheet>> = Ok(Vec::new());

        rayon::scope(|s| {
            s.spawn(|_| {
                res_itens = process_sheet_type(self.itens, SheetContext::Itens);
            });
            s.spawn(|_| {
                res_orig = process_sheet_type(self.efd_original, SheetContext::EfdOriginal);
            });
            s.spawn(|_| {
                res_aud = process_sheet_type(self.efd_auditoria, SheetContext::EfdAuditoria);
            });
        });

        let mut worksheets = res_itens?;
        worksheets.extend(res_orig?);
        worksheets.extend(res_aud?);

        Ok(worksheets)
    }

    /// Grava as planilhas sequencialmente no Workbook (otimizado para baixo consumo de memória).
    pub fn write_sequentially_to_workbook(
        &self,
        workbook: &mut Workbook,
        memory_mode: ExcelMemoryMode,
    ) -> JoinResult<()> {
        process_sheet_type_sequential(workbook, self.itens, SheetContext::Itens, memory_mode)?;
        process_sheet_type_sequential(
            workbook,
            self.efd_original,
            SheetContext::EfdOriginal,
            memory_mode,
        )?;
        process_sheet_type_sequential(
            workbook,
            self.efd_auditoria,
            SheetContext::EfdAuditoria,
            memory_mode,
        )?;
        Ok(())
    }
}
