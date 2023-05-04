use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::prelude::*;
use datafusion::scalar::ScalarValue;
use datafusion::variable::{VarProvider, VarType};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use std::collections::HashMap;
use std::sync::Arc;
use tracing_subscriber::prelude::*;

#[tokio::main]
async fn main() -> datafusion::error::Result<()> {
    const FILE_PATH: &str = "/Users/max/gitlab/ul/libs/uldatacatalog/examples/new_westminster_tube_count_autocounter_search_attributes.parquet";

    let stdout_log = tracing_subscriber::fmt::layer().pretty();
    tracing_subscriber::registry()
        .with(stdout_log.with_filter(tracing_subscriber::filter::LevelFilter::INFO))
        .init();

    let parquet_file = std::fs::File::open(FILE_PATH).unwrap();

    let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_file)
        .unwrap()
        .build()
        .unwrap();

    let batches: Vec<RecordBatch> = reader.into_iter().filter_map(|i| i.ok()).collect();

    const TABLE_NAME: &str = "parquet_table";

    let schema = batches[0].schema();
    let table = Arc::new(MemTable::try_new(schema, vec![batches]).unwrap());

    let mut map = HashMap::new();
    map.insert(
        "@v0".to_string(),
        (
            ScalarValue::TimestampMillisecond(Some(1165392000000), Some("UTC".to_string())),
            DataType::Timestamp(
                arrow::datatypes::TimeUnit::Millisecond,
                Some("UTC".to_string()),
            ),
        ),
    );

    let sql_statements = r#"SELECT "ul_node_id", "ul_observation_date" FROM parquet_table WHERE "ul_observation_date"  >=  @v0"#;

    let ctx = SessionContext::new();
    ctx.register_table(TABLE_NAME, table).unwrap();

    let provider = DataFusionVariableProvider::new(map);
    ctx.register_variable(VarType::UserDefined, Arc::new(provider));

    let df = ctx.sql(&sql_statements).await.unwrap();

    let _results = df.collect().await.unwrap();

    println!("All done!");
    Ok(())
}

#[derive(Debug)]
struct DataFusionVariableProvider {
    value_map: HashMap<String, (ScalarValue, DataType)>,
}

impl DataFusionVariableProvider {
    fn new(value_map: HashMap<String, (ScalarValue, DataType)>) -> Self {
        Self { value_map }
    }
}

impl VarProvider for DataFusionVariableProvider {
    fn get_value(&self, var_names: Vec<String>) -> Result<ScalarValue, DataFusionError> {
        if var_names.is_empty() {
            return Err(DataFusionError::Execution(
                "cannot resolve empty variable name".into(),
            ));
        }

        if let Some((value, _)) = self.value_map.get(&var_names[0]) {
            Ok(value.clone())
        } else {
            Err(DataFusionError::Execution(format!(
                "cannot resolve variable name {:?}",
                var_names
            )))
        }
    }

    fn get_type(&self, var_names: &[String]) -> Option<DataType> {
        self.value_map.get(&var_names[0]).map(|(_, ty)| ty.clone())
    }
}
