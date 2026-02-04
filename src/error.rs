use confy::ConfyError;
use pathfinding::matrix::MatrixFormatError;
use polars::prelude::PolarsError;
use std::{io, path::PathBuf};
use thiserror::Error;

/**
Result type to simplify function signatures.

This is a custom result type that uses our custom `JoinError` for the error type.

Functions can return `JoinResult<T>` and then use `?` to automatically propagate errors.
*/
pub type JoinResult<T> = Result<T, JoinError>;

/**
Custom error type for join_with_assignments.

This enum defines all the possible errors that can occur in the application.

We use the `thiserror` crate to derive the `Error` trait and automatically
implement `Display` using the `#[error(...)]` attribute.
*/
#[derive(Error, Debug)]
pub enum JoinError {
    #[error("Confy error: {0}")]
    Confy(#[from] ConfyError),

    #[error("Conversion error from '{from_type}' to '{to_type}': {reason}")]
    ConversionError {
        from_type: String,
        to_type: String,
        reason: String,
    },

    // Errors encountered while parsing CSV data (e.g., inconsistent columns, invalid data).
    #[error("CSV parsing error: {0} for file {1:?}")] // Adicionado PathBuf para contexto
    CSVReadError(PolarsError, PathBuf), // Alterado para PolarsError e PathBuf

    #[error(
        "Incomplete CSV read configuration: {message}. File path: {file_path:?}, Delimiter: {delimiter:?}"
    )]
    IncompleteCsvConfig {
        message: String,
        file_path: Option<PathBuf>,
        delimiter: Option<char>,
    },

    #[error(
        "Invalid 'Side' value provided: '{0}'. The middle side is not valid for this operation."
    )]
    InvalidSide(String), // Armazenará o valor de Side que foi inválido (ex: "Middle")

    #[error("Value out of i64 bounds during matrix generation: {value}")]
    I64OutOfBounds { value: f64 },

    // Wrapper for standard IO errors.
    // The #[from] attribute automatically converts io::Error to JoinError::Io.
    #[error("IO error: {0}")]
    Io(#[from] io::Error),

    #[error(
        "fn munkres_assignments(),\n\
        Matrix creation error: {source}.\n\
        slice_a_len={slice_a_len}, slice_b_len={slice_b_len}\n"
    )]
    MatrixCreationError {
        #[source] // Indica que este é o erro original
        source: MatrixFormatError,
        slice_a_len: usize,
        slice_b_len: usize,
    },

    // Wrapper for Polars errors (from the Polars library).
    // #[from] handles conversion. Handles errors from Polars operations,
    // including invalid lazy plans or errors during execution (like bad casts or regex syntax).
    #[error("Polars error: {0}")]
    Polars(#[from] PolarsError),

    #[error("TOML error: {0}")]
    Toml(#[from] toml::ser::Error),

    // A catch-all for other, less specific errors not covered by specific variants.
    // Uses a String to describe the error. Consider using this sparingly.
    #[error("Other error: {0}")]
    Other(String),
}

// Implementation of the From trait to convert a String into a JoinError.
// This allows us to easily convert generic error strings into our custom error type.
impl From<String> for JoinError {
    fn from(err: String) -> JoinError {
        // Prefer using specific error variants when possible, fallback to Other.
        JoinError::Other(err)
    }
}
