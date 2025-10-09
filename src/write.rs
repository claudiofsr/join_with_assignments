// excel_writer - A Polars extension to serialize dataframes to Excel xlsx files.
//
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Copyright 2022-2025, John McNamara, jmcnamara@cpan.org

use polars::prelude::*;
use rust_xlsxwriter::Format;
use std::io::{Seek, Write};

use crate::PolarsXlsxWriter;

pub struct ExcelWriter<W>
where
    W: Write,
{
    writer: W,
    xlsx_writer: PolarsXlsxWriter,
}

impl<W> SerWriter<W> for ExcelWriter<W>
where
    W: Write + Seek + Send,
{
    fn new(buffer: W) -> Self {
        ExcelWriter {
            writer: buffer,
            xlsx_writer: PolarsXlsxWriter::default(),
        }
    }

    fn finish(&mut self, df: &mut DataFrame) -> PolarsResult<()> {
        self.xlsx_writer.save_to_writer(df, &mut self.writer)?;

        Ok(())
    }
}

impl<W> ExcelWriter<W>
where
    W: Write,
{
    pub fn has_header(mut self, has_header: bool) -> Self {
        self.xlsx_writer.set_header(has_header);
        self
    }

    pub fn with_time_format(mut self, format: impl Into<Format>) -> Self {
        self.xlsx_writer.set_time_format(format);
        self
    }

    pub fn with_date_format(mut self, format: impl Into<Format>) -> Self {
        self.xlsx_writer.set_date_format(format);
        self
    }

    pub fn with_datetime_format(mut self, format: impl Into<Format>) -> Self {
        self.xlsx_writer.set_datetime_format(format);
        self
    }

    pub fn with_float_format(mut self, format: impl Into<Format>) -> Self {
        self.xlsx_writer.set_float_format(format);
        self
    }

    pub fn with_float_precision(mut self, precision: usize) -> Self {
        self.xlsx_writer.set_float_precision(precision);
        self
    }

    pub fn with_null_value(mut self, null_value: impl Into<String>) -> Self {
        self.xlsx_writer.set_null_value(null_value);
        self
    }

    pub fn with_autofit(mut self) -> Self {
        self.xlsx_writer.set_autofit(true);
        self
    }
}
