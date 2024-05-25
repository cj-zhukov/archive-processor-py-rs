pub mod error;
pub use error::Result;

use std::sync::Arc;

use async_zip::base::read::seek::ZipFileReader;
use datafusion::arrow::array::{BinaryArray, RecordBatch, StringArray};
use datafusion::arrow::pyarrow::PyArrowType;
use datafusion::arrow::datatypes::{Schema, Field, DataType};
use datafusion::prelude::*;
use futures_lite::io::AsyncReadExt;
use parquet::arrow::AsyncArrowWriter;
use pyo3::prelude::*;
use tokio::io::AsyncWriteExt;
use tokio::{io::BufReader, fs::File};
use tokio_stream::StreamExt;
use uuid::Uuid;

#[pyfunction]
fn run_txt(py: Python<'_>, archive_name: String) -> PyResult<Vec<TxtData>> {
    pyo3_asyncio::tokio::run(py, async move {
        Ok(TxtData::processor(&archive_name).await?)
    })
}

#[pyfunction]
fn run_img(py: Python<'_>, archive_name: String) -> PyResult<PyArrowType<Vec<RecordBatch>>> {
    let batches = pyo3_asyncio::tokio::run(py, async move {
        let mut records = ImageData::processor(&archive_name).await?;
        let ctx = SessionContext::new();
        let df = ImageData::to_df(ctx, &mut records)?;
        let res = df.collect().await?;
        
        Ok(res)
    }); 

    Ok(batches?.into())
}


#[pymodule]
fn archive_processor_py_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(run_txt, m)?)?;
    m.add_function(wrap_pyfunction!(run_img, m)?)?;

    Ok(())
}

#[pyclass]
pub struct TxtData {
    #[pyo3(get)]
    pub pkey: String,
    #[pyo3(get)]
    pub file_name: String,
    #[pyo3(get)]
    pub content: String,
}

impl TxtData {
    pub fn new(pkey: &str, file_name: &str, content: &str) -> Self {
        Self {
            pkey: pkey.to_string(),
            file_name: file_name.to_string(),
            content: content.to_string(),
        }
    }

    pub async fn processor(archive_name: &str) -> Result<Vec<TxtData>> {
        let mut records: Vec<TxtData> = Vec::new();
        let mut file = BufReader::new(File::open(archive_name).await?);
        let mut zip = ZipFileReader::with_tokio(&mut file).await?;
    
        for index in 0..zip.file().entries().len() {
            let mut reader = zip.reader_with_entry(index).await?;
            let file_name = reader.entry().filename().clone().into_string()?;
            if file_name.ends_with(".txt") {
                let mut buffer = String::new();
                let _n = reader.read_to_string(&mut buffer).await?;
                let pkey = Uuid::new_v4().to_string();    
                let record = Self::new(&pkey, &file_name, &buffer);
                records.push(record);
            }
        }
            
        Ok(records)
    }

}

pub struct ImageData {
    pub pkey: String,
    pub file_name: String,
    pub data_val: Vec<u8>,
}

impl ImageData {
    pub fn new(pkey: &str, file_name: &str, data_val: Vec<u8>) -> Self {
        Self {
            pkey: pkey.to_string(),
            file_name: file_name.to_string(),
            data_val
        }
    }
}

impl ImageData {
    pub fn schema() -> Schema {
        let schema = vec![
            Field::new("pkey", DataType::Utf8, true),
            Field::new("file_name", DataType::Utf8, true),
            Field::new("data_val", DataType::Binary, true),
        ];

        Schema::new(schema)
    }

    pub async fn processor(archive_name: &str) -> Result<Vec<ImageData>> {
        let mut images: Vec<ImageData> = Vec::new();
        let mut file = BufReader::new(File::open(archive_name).await?);
        let mut zip = ZipFileReader::with_tokio(&mut file).await?;

        for index in 0..zip.file().entries().len() {
            let mut reader = zip.reader_with_entry(index).await?;
            let file_name = reader.entry().filename().clone().into_string()?;
            if file_name.ends_with(".jpg") || file_name.ends_with(".jpeg") {
                let mut buffer = Vec::new();
                let _n = reader.read_to_end(&mut buffer).await?;
                let pkey = Uuid::new_v4().to_string();    
                let image = ImageData::new(&pkey, &file_name, buffer);
                images.push(image);
            }
        }
            
        Ok(images)
    }
    
    pub fn to_df(ctx: SessionContext, records: &mut Vec<Self>) -> Result<DataFrame> {
        let mut pkey = vec![];
        let mut file_name = vec![];
        let mut data_val = vec![];

        for record in records  {
            pkey.push(record.pkey.as_str());
            file_name.push(record.file_name.as_str());
            data_val.push(&record.data_val[..]);
        }

        let batch = RecordBatch::try_new(
            Self::schema().into(),
            vec![
                Arc::new(StringArray::from(pkey)),
                Arc::new(StringArray::from(file_name)),
                Arc::new(BinaryArray::from(data_val)),
            ],
        )?;

        let df = ctx.read_batch(batch)?;    
    
        Ok(df)
    }
}

pub async fn write_df_to_file(df: DataFrame, file_path: &str) -> Result<()> {
    let mut buf = vec![];
    let schema = Schema::from(df.clone().schema());
    let mut stream = df.execute_stream().await?;
    let mut writer = AsyncArrowWriter::try_new(&mut buf, schema.into(), None)?;
    while let Some(batch) = stream.next().await.transpose()? {
        writer.write(&batch).await?;
    }
    writer.close().await?;
    let mut file = tokio::fs::File::create(file_path).await?;
    file.write_all(&buf).await?;

    Ok(())
}