pub type Result<T> = core::result::Result<T, Error>;

use thiserror::Error;
use std::io::Error as IOError;
use pyo3::PyErr;
use pyo3::exceptions::{PyIOError, PyValueError, PyOSError};
use datafusion::error::DataFusionError;
use datafusion::arrow::error::ArrowError;
use parquet::errors::ParquetError;
use async_zip::error::ZipError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("custom error: `{0}`")]
    Custom(String),

    #[error("io error: `{0}`")]
    IOError(#[from] IOError),

    #[error("zip error: `{0}`")]
    ZipError(#[from] ZipError),

    #[error("pyo3 error: `{0}`")]
    Pyo3Error(#[from] PyErr),

    #[error("pyo3 os error: `{0}`")]
    PyosError(#[from] PyOSError),

    #[error("pyvalue error: `{0}`")]
    PyValueError(#[from] PyValueError),

    #[error("arrow error: `{0}`")]
    ArrowError(#[from] ArrowError),

    #[error("datafusion error: `{0}`")]
    DatafusionError(#[from] DataFusionError),

    #[error("parquet error: `{0}`")]
    ParquetError(#[from] ParquetError),
}

impl std::convert::From<Error> for PyErr {
    fn from(value: Error) -> Self {
        match &value {
            Error::IOError(_) => PyIOError::new_err(value.to_string()), 
            _ => PyValueError::new_err(value.to_string()),
        }
    }
}