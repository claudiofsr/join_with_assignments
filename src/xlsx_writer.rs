// excel_writer - A Polars extension to serialize dataframes to Excel xlsx files.
//
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Copyright 2022-2025, John McNamara, jmcnamara@cpan.org

use std::io::{Seek, Write};
use std::path::Path;

use polars::prelude::*;
use polars_arrow::temporal_conversions::{
    date32_to_date, time64ns_to_time, timestamp_ms_to_datetime, timestamp_ns_to_datetime,
    timestamp_us_to_datetime,
};
use rust_xlsxwriter::{Format, Table, Workbook, Worksheet};

pub struct PolarsXlsxWriter {
    pub(crate) workbook: Workbook,
    pub(crate) options: WriterOptions,
}

impl Default for PolarsXlsxWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl PolarsXlsxWriter {
    pub fn new() -> PolarsXlsxWriter {
        let mut workbook = Workbook::new();
        workbook.add_worksheet();

        PolarsXlsxWriter {
            workbook,
            options: WriterOptions::default(),
        }
    }

    pub fn write_dataframe(&mut self, df: &DataFrame) -> PolarsResult<()> {
        let options = self.options.clone();
        let worksheet = self.worksheet()?;

        Self::write_dataframe_internal(df, worksheet, 0, 0, &options)?;

        Ok(())
    }

    pub fn write_dataframe_to_cell(
        &mut self,
        df: &DataFrame,
        row: u32,
        col: u16,
    ) -> PolarsResult<()> {
        let options = self.options.clone();
        let worksheet = self.worksheet()?;

        Self::write_dataframe_internal(df, worksheet, row, col, &options)?;

        Ok(())
    }

    pub fn write_dataframe_to_worksheet(
        &mut self,
        df: &DataFrame,
        worksheet: &mut Worksheet,
        row: u32,
        col: u16,
    ) -> PolarsResult<()> {
        let options = self.options.clone();

        Self::write_dataframe_internal(df, worksheet, row, col, &options)?;

        Ok(())
    }

    pub fn save<P: AsRef<Path>>(&mut self, path: P) -> PolarsResult<()> {
        self.workbook.save(path)?;

        Ok(())
    }

    pub fn set_header(&mut self, has_header: bool) -> &mut PolarsXlsxWriter {
        let table = self.options.table.clone().set_header_row(has_header);
        self.options.table = table;
        self
    }

    pub fn set_time_format(&mut self, format: impl Into<Format>) -> &mut PolarsXlsxWriter {
        self.options.time_format = format.into();
        self
    }

    pub fn set_date_format(&mut self, format: impl Into<Format>) -> &mut PolarsXlsxWriter {
        self.options.date_format = format.into();
        self
    }

    pub fn set_datetime_format(&mut self, format: impl Into<Format>) -> &mut PolarsXlsxWriter {
        self.options.datetime_format = format.into();
        self
    }

    pub fn set_float_format(&mut self, format: impl Into<Format>) -> &mut PolarsXlsxWriter {
        self.options.float_format = format.into();
        self
    }

    pub fn set_float_precision(&mut self, precision: usize) -> &mut PolarsXlsxWriter {
        if (1..=30).contains(&precision) {
            let precision = "0".repeat(precision);
            self.options.float_format = Format::new().set_num_format(format!("0.{precision}"));
        }
        self
    }

    pub fn set_null_value(&mut self, value: impl Into<String>) -> &mut PolarsXlsxWriter {
        self.options.null_value = Some(value.into());
        self
    }

    pub fn set_nan_value(&mut self, value: impl Into<String>) -> &mut PolarsXlsxWriter {
        self.options.nan_value = Some(value.into());
        self
    }

    pub fn set_infinity_value(&mut self, value: impl Into<String>) -> &mut PolarsXlsxWriter {
        self.options.infinity_value = Some(value.into());
        self
    }

    pub fn set_neg_infinity_value(&mut self, value: impl Into<String>) -> &mut PolarsXlsxWriter {
        self.options.neg_infinity_value = Some(value.into());
        self
    }

    pub fn set_autofit(&mut self, autofit: bool) -> &mut PolarsXlsxWriter {
        self.options.use_autofit = autofit;
        self
    }

    pub fn set_zoom(&mut self, zoom: u16) -> &mut PolarsXlsxWriter {
        self.options.zoom = zoom;
        self
    }

    pub fn set_screen_gridlines(&mut self, enable: bool) -> &mut PolarsXlsxWriter {
        self.options.screen_gridlines = enable;

        self
    }

    pub fn set_freeze_panes(&mut self, row: u32, col: u16) -> &mut PolarsXlsxWriter {
        self.options.freeze_cell = (row, col);

        self
    }

    pub fn set_freeze_panes_top_cell(&mut self, row: u32, col: u16) -> &mut PolarsXlsxWriter {
        self.options.top_cell = (row, col);

        self
    }

    pub fn set_autofilter(&mut self, enable: bool) -> &mut PolarsXlsxWriter {
        let table = self.options.table.clone().set_autofilter(enable);
        self.options.table = table;

        self
    }

    pub fn set_table(&mut self, table: &Table) -> &mut PolarsXlsxWriter {
        self.options.table = table.clone();
        self
    }

    pub fn set_worksheet_name(
        &mut self,
        name: impl Into<String>,
    ) -> PolarsResult<&mut PolarsXlsxWriter> {
        let worksheet = self.worksheet()?;
        worksheet.set_name(name)?;
        Ok(self)
    }

    pub fn add_worksheet(&mut self) -> &mut PolarsXlsxWriter {
        self.workbook.add_worksheet();

        self
    }

    pub fn worksheet(&mut self) -> PolarsResult<&mut Worksheet> {
        let mut last_index = self.workbook.worksheets().len();

        // Add a worksheet if there isn't one already.
        if last_index == 0 {
            self.workbook.add_worksheet();
        } else {
            last_index -= 1;
        }

        let worksheet = self.workbook.worksheet_from_index(last_index)?;

        Ok(worksheet)
    }

    // -----------------------------------------------------------------------
    // Internal functions/methods.
    // -----------------------------------------------------------------------

    // Method to support writing to ExcelWriter writer<W>.
    pub(crate) fn save_to_writer<W>(&mut self, df: &DataFrame, writer: W) -> PolarsResult<()>
    where
        W: Write + Seek + Send,
    {
        let options = self.options.clone();
        let worksheet = self.worksheet()?;

        Self::write_dataframe_internal(df, worksheet, 0, 0, &options)?;

        self.workbook.save_to_writer(writer)?;

        Ok(())
    }

    // Write the dataframe to a `rust_xlsxwriter` Worksheet. It is structured as
    // an associated method to allow it to handle external worksheets.
    #[allow(clippy::too_many_lines)]
    fn write_dataframe_internal(
        df: &DataFrame,
        worksheet: &mut Worksheet,
        row_offset: u32,
        col_offset: u16,
        options: &WriterOptions,
    ) -> Result<(), PolarsError> {
        let header_offset = u32::from(options.table.has_header_row());

        // Set NaN and Infinity values, if required.
        if let Some(nan_value) = &options.nan_value {
            worksheet.set_nan_value(nan_value);
        }
        if let Some(infinity_value) = &options.infinity_value {
            worksheet.set_infinity_value(infinity_value);
        }
        if let Some(neg_infinity_value) = &options.neg_infinity_value {
            worksheet.set_neg_infinity_value(neg_infinity_value);
        }

        // Iterate through the dataframe column by column.
        for (col_num, column) in df.columns().iter().enumerate() {
            let col_num = col_offset + col_num as u16;

            // Store the column names for use as table headers.
            if options.table.has_header_row() {
                worksheet.write(row_offset, col_num, column.name().as_str())?;
            }

            // Write the row data for each column/type.
            for (row_num, data) in column.as_materialized_series().iter().enumerate() {
                let row_num = header_offset + row_offset + row_num as u32;

                // Map the Polars Series AnyValue types to Excel/rust_xlsxwriter
                // types.
                match data {
                    AnyValue::Int8(value) => {
                        worksheet.write_number(row_num, col_num, value)?;
                    }
                    AnyValue::UInt8(value) => {
                        worksheet.write_number(row_num, col_num, value)?;
                    }
                    AnyValue::Int16(value) => {
                        worksheet.write_number(row_num, col_num, value)?;
                    }
                    AnyValue::UInt16(value) => {
                        worksheet.write_number(row_num, col_num, value)?;
                    }
                    AnyValue::Int32(value) => {
                        worksheet.write_number(row_num, col_num, value)?;
                    }
                    AnyValue::UInt32(value) => {
                        worksheet.write_number(row_num, col_num, value)?;
                    }
                    AnyValue::Int64(value) => {
                        // Allow u64 conversion within Excel's limits.
                        #[allow(clippy::cast_precision_loss)]
                        worksheet.write_number(row_num, col_num, value as f64)?;
                    }
                    AnyValue::UInt64(value) => {
                        // Allow u64 conversion within Excel's limits.
                        #[allow(clippy::cast_precision_loss)]
                        worksheet.write_number(row_num, col_num, value as f64)?;
                    }
                    AnyValue::Float32(value) => {
                        worksheet.write_number_with_format(
                            row_num,
                            col_num,
                            value,
                            &options.float_format,
                        )?;
                    }
                    AnyValue::Float64(value) => {
                        worksheet.write_number_with_format(
                            row_num,
                            col_num,
                            value,
                            &options.float_format,
                        )?;
                    }
                    AnyValue::String(value) => {
                        worksheet.write_string(row_num, col_num, value)?;
                    }
                    AnyValue::StringOwned(value) => {
                        worksheet.write_string(row_num, col_num, value.as_str())?;
                    }
                    AnyValue::Boolean(value) => {
                        worksheet.write_boolean(row_num, col_num, value)?;
                    }
                    AnyValue::Null => {
                        if let Some(null_string) = &options.null_value {
                            worksheet.write_string(row_num, col_num, null_string)?;
                        }
                    }
                    AnyValue::Datetime(value, time_units, _) => {
                        let datetime = match time_units {
                            TimeUnit::Nanoseconds => timestamp_ns_to_datetime(value),
                            TimeUnit::Microseconds => timestamp_us_to_datetime(value),
                            TimeUnit::Milliseconds => timestamp_ms_to_datetime(value),
                        };
                        worksheet.write_datetime_with_format(
                            row_num,
                            col_num,
                            datetime,
                            &options.datetime_format,
                        )?;
                        worksheet.set_column_width(col_num, 18)?;
                    }
                    AnyValue::Date(value) => {
                        let date = date32_to_date(value);
                        worksheet.write_datetime_with_format(
                            row_num,
                            col_num,
                            date,
                            &options.date_format,
                        )?;
                        worksheet.set_column_width(col_num, 10)?;
                    }
                    AnyValue::Time(value) => {
                        let time = time64ns_to_time(value);
                        worksheet.write_datetime_with_format(
                            row_num,
                            col_num,
                            time,
                            &options.time_format,
                        )?;
                    }
                    _ => {
                        polars_bail!(
                            ComputeError:
                            "Polars AnyValue data type '{}' is not supported by Excel",
                            data.dtype()
                        );
                    }
                }
            }
        }

        // Create a table for the dataframe range.
        let (mut max_row, max_col) = df.shape();
        if !options.table.has_header_row() {
            max_row -= 1;
        }
        if options.table.has_total_row() {
            max_row += 1;
        }

        // Add the table to the worksheet.
        worksheet.add_table(
            row_offset,
            col_offset,
            row_offset + max_row as u32,
            col_offset + max_col as u16 - 1,
            &options.table,
        )?;

        // Autofit the columns.
        if options.use_autofit {
            worksheet.autofit();
        }

        // Set the zoom level.
        worksheet.set_zoom(options.zoom);

        // Set the screen gridlines.
        worksheet.set_screen_gridlines(options.screen_gridlines);

        // Set the worksheet panes.
        worksheet.set_freeze_panes(options.freeze_cell.0, options.freeze_cell.1)?;
        worksheet.set_freeze_panes_top_cell(options.top_cell.0, options.top_cell.1)?;

        Ok(())
    }
}

// -----------------------------------------------------------------------
// Helper structs.
// -----------------------------------------------------------------------

// A struct for storing and passing configuration settings.
#[derive(Clone)]
pub(crate) struct WriterOptions {
    pub(crate) use_autofit: bool,
    pub(crate) date_format: Format,
    pub(crate) time_format: Format,
    pub(crate) float_format: Format,
    pub(crate) datetime_format: Format,
    pub(crate) null_value: Option<String>,
    pub(crate) nan_value: Option<String>,
    pub(crate) infinity_value: Option<String>,
    pub(crate) neg_infinity_value: Option<String>,
    pub(crate) table: Table,
    pub(crate) zoom: u16,
    pub(crate) screen_gridlines: bool,
    pub(crate) freeze_cell: (u32, u16),
    pub(crate) top_cell: (u32, u16),
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self::new()
    }
}

impl WriterOptions {
    fn new() -> WriterOptions {
        WriterOptions {
            use_autofit: false,
            time_format: "hh:mm:ss;@".into(),
            date_format: "yyyy\\-mm\\-dd;@".into(),
            datetime_format: "yyyy\\-mm\\-dd\\ hh:mm:ss".into(),
            null_value: None,
            nan_value: None,
            infinity_value: None,
            neg_infinity_value: None,
            float_format: Format::default(),
            table: Table::new(),
            zoom: 100,
            screen_gridlines: true,
            freeze_cell: (0, 0),
            top_cell: (0, 0),
        }
    }
}
