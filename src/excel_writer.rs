// excel_writer - A Polars extension to serialize dataframes to Excel xlsx files.
//
// SPDX-License-Identifier: MIT OR Apache-2.0
//
// Copyright 2022-2026, John McNamara, jmcnamara@cpan.org
//
// <https://github.com/jmcnamara/polars_excel_writer>

use std::borrow::Cow;
use std::collections::HashMap;
use std::path::Path;

use chrono::{DateTime, NaiveDate, NaiveDateTime};
use polars::prelude::*;
use rust_xlsxwriter::worksheet::IntoExcelData;
use rust_xlsxwriter::{Format, Formula, Table, TableColumn, Url, Workbook, Worksheet};

pub struct PolarsExcelWriter {
    pub(crate) workbook: Workbook,
    pub(crate) options: WriterOptions,
}

impl Default for PolarsExcelWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl PolarsExcelWriter {
    pub fn new() -> PolarsExcelWriter {
        let mut workbook = Workbook::new();
        workbook.add_worksheet();

        PolarsExcelWriter {
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

    pub fn save_to_buffer(&mut self) -> Result<Vec<u8>, PolarsError> {
        let buf = self.workbook.save_to_buffer()?;

        Ok(buf)
    }

    pub fn set_header(&mut self, has_header: bool) -> &mut PolarsExcelWriter {
        let table = self.options.table.clone().set_header_row(has_header);
        self.options.table = table;
        self
    }

    pub fn set_dtype_format(
        &mut self,
        dtype: DataType,
        format: impl Into<Format>,
    ) -> &mut PolarsExcelWriter {
        self.options.dtype_formats.insert(dtype, format.into());
        self
    }

    pub fn set_dtype_int_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        let format = format.into();

        self.set_dtype_format(DataType::Int8, format.clone());
        self.set_dtype_format(DataType::Int16, format.clone());
        self.set_dtype_format(DataType::Int32, format.clone());
        self.set_dtype_format(DataType::Int64, format.clone());
        self.set_dtype_format(DataType::UInt8, format.clone());
        self.set_dtype_format(DataType::UInt16, format.clone());
        self.set_dtype_format(DataType::UInt32, format.clone());
        self.set_dtype_format(DataType::UInt64, format.clone());

        self
    }

    pub fn set_dtype_float_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        let format = format.into();

        self.set_dtype_format(DataType::Float32, format.clone());
        self.set_dtype_format(DataType::Float64, format.clone());

        self
    }

    pub fn set_dtype_number_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        let format = format.into();

        self.set_dtype_int_format(format.clone());
        self.set_dtype_float_format(format.clone());
        self
    }

    pub fn set_dtype_datetime_format(
        &mut self,
        format: impl Into<Format>,
    ) -> &mut PolarsExcelWriter {
        let format = format.into();

        self.set_dtype_format(
            DataType::Datetime(TimeUnit::Nanoseconds, None),
            format.clone(),
        );
        self.set_dtype_format(
            DataType::Datetime(TimeUnit::Microseconds, None),
            format.clone(),
        );
        self.set_dtype_format(
            DataType::Datetime(TimeUnit::Milliseconds, None),
            format.clone(),
        );

        self
    }

    pub fn set_float_precision(&mut self, precision: usize) -> &mut PolarsExcelWriter {
        if (1..=30).contains(&precision) {
            let precision = "0".repeat(precision);
            let format = Format::new().set_num_format(format!("0.{precision}"));
            self.set_dtype_float_format(format);
        }
        self
    }

    pub fn set_column_format(
        &mut self,
        column_name: &str,
        format: impl Into<Format>,
    ) -> &mut PolarsExcelWriter {
        self.options
            .column_formats
            .insert(column_name.to_string(), format.into());
        self
    }

    pub fn set_header_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        self.options.header_format = Some(format.into());
        self
    }

    pub fn enable_column_urls(&mut self, column_name: &str) -> &mut PolarsExcelWriter {
        self.options
            .column_string_types
            .insert(column_name.to_string(), ColumnStringType::Url);
        self
    }

    pub fn enable_column_formulas(&mut self, column_name: &str) -> &mut PolarsExcelWriter {
        self.options
            .column_string_types
            .insert(column_name.to_string(), ColumnStringType::Formula);
        self
    }

    pub fn set_null_value(&mut self, value: impl Into<String>) -> &mut PolarsExcelWriter {
        self.options.null_value = Some(value.into());
        self
    }

    pub fn set_nan_value(&mut self, value: impl Into<String>) -> &mut PolarsExcelWriter {
        self.options.nan_value = Some(value.into());
        self
    }

    pub fn set_infinity_value(&mut self, value: impl Into<String>) -> &mut PolarsExcelWriter {
        self.options.infinity_value = Some(value.into());
        self
    }

    pub fn set_neg_infinity_value(&mut self, value: impl Into<String>) -> &mut PolarsExcelWriter {
        self.options.neg_infinity_value = Some(value.into());
        self
    }

    pub fn set_autofit(&mut self, autofit: bool) -> &mut PolarsExcelWriter {
        self.options.use_autofit = autofit;
        self
    }

    pub fn set_autofit_max_row(&mut self, max_row: u32) -> &mut PolarsExcelWriter {
        self.options.autofit_max_row = max_row;
        self
    }

    pub fn set_autofit_max_width(&mut self, max_width: u32) -> &mut PolarsExcelWriter {
        self.options.autofit_max_width = max_width;
        self
    }

    pub fn set_zoom(&mut self, zoom: u16) -> &mut PolarsExcelWriter {
        self.options.zoom = zoom;
        self
    }

    pub fn set_screen_gridlines(&mut self, enable: bool) -> &mut PolarsExcelWriter {
        self.options.screen_gridlines = enable;
        self
    }

    pub fn set_freeze_panes(&mut self, row: u32, col: u16) -> &mut PolarsExcelWriter {
        self.options.freeze_cell = (row, col);
        self
    }

    pub fn set_freeze_panes_top_cell(&mut self, row: u32, col: u16) -> &mut PolarsExcelWriter {
        self.options.top_cell = (row, col);
        self
    }

    pub fn set_autofilter(&mut self, enable: bool) -> &mut PolarsExcelWriter {
        let table = self.options.table.clone().set_autofilter(enable);
        self.options.table = table;
        self
    }

    pub fn set_table(&mut self, table: &Table) -> &mut PolarsExcelWriter {
        self.options.table = table.clone();
        self
    }

    pub fn set_worksheet_name(
        &mut self,
        name: impl Into<String>,
    ) -> PolarsResult<&mut PolarsExcelWriter> {
        let worksheet = self.worksheet()?;
        worksheet.set_name(name)?;
        Ok(self)
    }

    pub fn add_worksheet(&mut self) -> &mut PolarsExcelWriter {
        self.workbook.add_worksheet();
        self
    }

    pub fn worksheet(&mut self) -> PolarsResult<&mut Worksheet> {
        let mut last_index = self.workbook.worksheets().len();

        if last_index == 0 {
            self.workbook.add_worksheet();
        } else {
            last_index -= 1;
        }

        let worksheet = self.workbook.worksheet_from_index(last_index)?;

        Ok(worksheet)
    }

    // -----------------------------------------------------------------------
    // Backward compatibility helper methods for SerWriter
    // -----------------------------------------------------------------------

    pub fn set_time_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        self.set_dtype_format(DataType::Time, format);
        self
    }

    pub fn set_date_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        self.set_dtype_format(DataType::Date, format);
        self
    }

    pub fn set_datetime_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        self.set_dtype_datetime_format(format);
        self
    }

    pub fn set_float_format(&mut self, format: impl Into<Format>) -> &mut PolarsExcelWriter {
        self.set_dtype_float_format(format);
        self
    }

    // -----------------------------------------------------------------------
    // Internal functions/methods.
    // -----------------------------------------------------------------------

    #[allow(clippy::too_many_lines)]
    fn write_dataframe_internal(
        df: &DataFrame,
        worksheet: &mut Worksheet,
        row_offset: u32,
        col_offset: u16,
        options: &WriterOptions,
    ) -> Result<(), PolarsError> {
        let mut df: Cow<'_, DataFrame> = Cow::Borrowed(df);
        if df.first_col_n_chunks() > 1 {
            df.to_mut().rechunk_mut();
        }

        let header_offset = u32::from(options.table.has_header_row());
        let mut table_columns = vec![];

        if let Some(nan_value) = &options.nan_value {
            worksheet.set_nan_value(nan_value);
        }
        if let Some(infinity_value) = &options.infinity_value {
            worksheet.set_infinity_value(infinity_value);
        }
        if let Some(neg_infinity_value) = &options.neg_infinity_value {
            worksheet.set_neg_infinity_value(neg_infinity_value);
        }

        for (col_num, column) in df.columns().iter().enumerate() {
            let col = col_offset + col_num as u16;
            let column_name = column.name().to_string();

            if let Some(header_format) = &options.header_format {
                let table_column = TableColumn::new().set_header_format(header_format);
                table_columns.push(table_column);
            }

            if options.table.has_header_row() {
                worksheet.write(row_offset, col, &column_name)?;
            }

            let mut format = None;
            if let Some(dtype_format) = options.dtype_formats.get(column.dtype()) {
                format = Some(dtype_format);
            }

            if let Some(column_format) = options.column_formats.get(&column_name) {
                format = Some(column_format);
            }

            let string_type = options
                .column_string_types
                .get(&column_name)
                .unwrap_or(&ColumnStringType::Default);

            for (row_num, any_value) in column.as_materialized_series().iter().enumerate() {
                let row = header_offset + row_offset + row_num as u32;

                match any_value {
                    AnyValue::Int8(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::Int16(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::Int32(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::Int64(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::UInt8(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::UInt16(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::UInt32(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::UInt64(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::Float32(value) => write_value(worksheet, row, col, value, format)?,
                    AnyValue::Float64(value) => write_value(worksheet, row, col, value, format)?,

                    AnyValue::String(value) => match string_type {
                        ColumnStringType::Formula => {
                            let mut formula = Formula::new(value);
                            formula = formula.clone().escape_table_functions();
                            write_value(worksheet, row, col, formula, format)?;
                        }
                        ColumnStringType::Url => {
                            write_value(worksheet, row, col, Url::new(value), format)?;
                        }
                        ColumnStringType::Default => {
                            write_value(worksheet, row, col, value, format)?;
                        }
                    },

                    AnyValue::StringOwned(value) => match string_type {
                        ColumnStringType::Formula => {
                            let mut formula = Formula::new(value);
                            formula = formula.clone().escape_table_functions();
                            write_value(worksheet, row, col, formula, format)?;
                        }
                        ColumnStringType::Url => {
                            write_value(worksheet, row, col, Url::new(value), format)?;
                        }
                        ColumnStringType::Default => {
                            write_value(worksheet, row, col, value.as_str(), format)?;
                        }
                    },

                    AnyValue::Datetime(value, time_units, _) => {
                        let value = match time_units {
                            TimeUnit::Nanoseconds => timestamp_ns_to_datetime(value),
                            TimeUnit::Microseconds => timestamp_us_to_datetime(value),
                            TimeUnit::Milliseconds => timestamp_ms_to_datetime(value),
                        };

                        write_value(worksheet, row, col, &value, format)?;
                        worksheet.set_column_width(col, 18)?;
                    }

                    AnyValue::Date(value) => {
                        let value = date32_to_date(value);

                        write_value(worksheet, row, col, &value, format)?;
                        worksheet.set_column_width(col, 10)?;
                    }

                    AnyValue::Time(value) => {
                        let value = time64ns_to_time(value);

                        write_value(worksheet, row, col, &value, format)?;
                    }

                    AnyValue::Boolean(value) => write_value(worksheet, row, col, value, format)?,

                    AnyValue::Null => {
                        if let Some(value) = &options.null_value {
                            write_value(worksheet, row, col, value, format)?;
                        } else if format.is_some() {
                            write_value(worksheet, row, col, "", format)?;
                        }
                    }

                    _ => {
                        polars_bail!(
                            ComputeError:
                            "Polars AnyValue data type '{}' is not supported by Excel",
                            any_value.dtype()
                        );
                    }
                }
            }
        }

        let (mut max_row, max_col) = df.shape();
        if !options.table.has_header_row() {
            max_row -= 1;
        }
        if options.table.has_total_row() {
            max_row += 1;
        }

        let mut table = options.table.clone();
        if !table_columns.is_empty() {
            table = table.set_columns(&table_columns);
        }

        worksheet.add_table(
            row_offset,
            col_offset,
            row_offset + max_row as u32,
            col_offset + max_col as u16 - 1,
            &table,
        )?;

        if options.use_autofit {
            worksheet.set_autofit_max_width(options.autofit_max_width);
            worksheet.set_autofit_max_row(options.autofit_max_row);
            worksheet.autofit();
        }

        worksheet.set_zoom(options.zoom);
        worksheet.set_screen_gridlines(options.screen_gridlines);

        worksheet.set_freeze_panes(options.freeze_cell.0, options.freeze_cell.1)?;
        worksheet.set_freeze_panes_top_cell(options.top_cell.0, options.top_cell.1)?;

        Ok(())
    }
}

fn write_value(
    worksheet: &mut Worksheet,
    row: u32,
    col: u16,
    value: impl IntoExcelData,
    format: Option<&Format>,
) -> Result<(), PolarsError> {
    match format {
        Some(format) => worksheet.write_with_format(row, col, value, format)?,
        None => worksheet.write(row, col, value)?,
    };

    Ok(())
}

// -----------------------------------------------------------------------
// Helper structs and enums.
// -----------------------------------------------------------------------

#[derive(Clone)]
pub(crate) enum ColumnStringType {
    Default,
    Formula,
    Url,
}

#[derive(Clone)]
pub(crate) struct WriterOptions {
    pub(crate) use_autofit: bool,
    pub(crate) autofit_max_width: u32,
    pub(crate) autofit_max_row: u32,
    pub(crate) null_value: Option<String>,
    pub(crate) nan_value: Option<String>,
    pub(crate) infinity_value: Option<String>,
    pub(crate) neg_infinity_value: Option<String>,
    pub(crate) table: Table,
    pub(crate) zoom: u16,
    pub(crate) screen_gridlines: bool,
    pub(crate) freeze_cell: (u32, u16),
    pub(crate) top_cell: (u32, u16),
    pub(crate) header_format: Option<Format>,
    pub(crate) column_formats: HashMap<String, Format>,
    pub(crate) dtype_formats: HashMap<DataType, Format>,
    pub(crate) column_string_types: HashMap<String, ColumnStringType>,
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
            autofit_max_width: 300,
            autofit_max_row: 200,
            null_value: None,
            nan_value: None,
            infinity_value: None,
            neg_infinity_value: None,
            table: Table::new(),
            zoom: 100,
            screen_gridlines: true,
            freeze_cell: (0, 0),
            top_cell: (0, 0),
            header_format: None,
            column_formats: HashMap::new(),
            column_string_types: HashMap::new(),
            dtype_formats: HashMap::from([
                (DataType::Time, "hh:mm:ss;@".into()),
                (DataType::Date, "yyyy\\-mm\\-dd;@".into()),
                (
                    DataType::Datetime(TimeUnit::Nanoseconds, None),
                    "yyyy\\-mm\\-dd\\ hh:mm:ss".into(),
                ),
                (
                    DataType::Datetime(TimeUnit::Microseconds, None),
                    "yyyy\\-mm\\-dd\\ hh:mm:ss".into(),
                ),
                (
                    DataType::Datetime(TimeUnit::Milliseconds, None),
                    "yyyy\\-mm\\-dd\\ hh:mm:ss".into(),
                ),
            ]),
        }
    }
}

// -----------------------------------------------------------------------
// Helper functions for temporal conversions.
// -----------------------------------------------------------------------

/// Converts a Polars Date32 value (days since 1970-01-01) to a `chrono::NaiveDate`.
///
/// If the conversion fails due to out-of-bounds dates, it falls back to the Unix Epoch
/// (1970-01-01) via `unwrap_or_default()`.
#[inline]
fn date32_to_date(days: i32) -> NaiveDate {
    NaiveDate::from_epoch_days(days).unwrap_or_default()
}

/// Converts a Polars Time64 value (nanoseconds since midnight) to a `chrono::NaiveTime`.
///
/// This implementation safely wraps any negative or overflow values into a 24-hour cycle.
#[inline]
fn time64ns_to_time(nanos: i64) -> chrono::NaiveTime {
    const NANOS_PER_DAY: i64 = 24 * 60 * 60 * 1_000_000_000;

    // rem_euclid guarantees a positive modulo within the [0, NANOS_PER_DAY) range.
    let normalized_nanos = nanos.rem_euclid(NANOS_PER_DAY);

    let seconds = (normalized_nanos / 1_000_000_000) as u32;
    let nanoseconds = (normalized_nanos % 1_000_000_000) as u32;

    chrono::NaiveTime::from_num_seconds_from_midnight_opt(seconds, nanoseconds).unwrap_or_default()
}

/// Converts a Polars Timestamp value in nanoseconds to a `chrono::NaiveDateTime`.
///
/// Uses Euclidean division to safely support negative values (times preceding 1970).
#[inline]
fn timestamp_ns_to_datetime(ns: i64) -> NaiveDateTime {
    let seconds = ns.div_euclid(1_000_000_000);
    let nanoseconds = ns.rem_euclid(1_000_000_000) as u32;
    DateTime::from_timestamp(seconds, nanoseconds)
        .map(|dt| dt.naive_utc())
        .unwrap_or_default()
}

/// Converts a Polars Timestamp value in microseconds to a `chrono::NaiveDateTime`.
///
/// Uses Euclidean division to safely support negative values (times preceding 1970).
#[inline]
fn timestamp_us_to_datetime(us: i64) -> NaiveDateTime {
    let seconds = us.div_euclid(1_000_000);
    let nanoseconds = (us.rem_euclid(1_000_000) * 1_000) as u32;
    DateTime::from_timestamp(seconds, nanoseconds)
        .map(|dt| dt.naive_utc())
        .unwrap_or_default()
}

/// Converts a Polars Timestamp value in milliseconds to a `chrono::NaiveDateTime`.
///
/// Uses Euclidean division to safely support negative values (times preceding 1970).
#[inline]
fn timestamp_ms_to_datetime(ms: i64) -> NaiveDateTime {
    let seconds = ms.div_euclid(1_000);
    let nanoseconds = (ms.rem_euclid(1_000) * 1_000_000) as u32;
    DateTime::from_timestamp(seconds, nanoseconds)
        .map(|dt| dt.naive_utc())
        .unwrap_or_default()
}
