use arrow::{
    csv::Reader,
    datatypes::{DataType, Field, Schema},
    record_batch::RecordBatch,
};
use datafusion::{
    datasource::MemTable,
    error::DataFusionError,
    prelude::*,
    scalar::ScalarValue,
    variable::{VarProvider, VarType},
};
use std::{fs::File, sync::Arc};
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    tracing_subscriber::registry()
        .with(stdout_log.with_filter(tracing_subscriber::filter::LevelFilter::INFO))
        .init();

    let schema = Arc::new(Schema::new(vec![
        Field::new("foo", DataType::Int64, false),
        Field::new("bar", DataType::Int64, false),
    ]));

    let file = File::open("input.csv").unwrap();
    let csv_reader = Reader::new(file, schema.clone(), true, None, 1024, None, None, None);
    let batches: Vec<RecordBatch> = csv_reader.into_iter().filter_map(|i| i.ok()).collect();
    let memtable = Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());

    let ctx = SessionContext::new();
    ctx.register_table("csv_table", memtable).unwrap();
    ctx.register_variable(VarType::UserDefined, Arc::new(HardcodedIntProvider {}));

    let dataframe = ctx
        .sql("SELECT foo FROM csv_table WHERE bar > @var")
        .await
        .unwrap();

    let _results = dataframe.collect().await.unwrap();

    Ok(())
}

// A VarProvider that alwayrs returns 1234
#[derive(Debug)]
struct HardcodedIntProvider {}

impl VarProvider for HardcodedIntProvider {
    fn get_value(&self, _var_names: Vec<String>) -> Result<ScalarValue, DataFusionError> {
        Ok(ScalarValue::Int64(Some(1234)))
    }

    fn get_type(&self, _: &[String]) -> Option<DataType> {
        Some(DataType::Int64)
    }
}
