use std::collections::HashMap;

use arrow_array::BooleanArray;
use arrow_array::Int32Array;
use arrow_array::Int64Array;
use arrow_array::RecordBatch;
use arrow_array::StringArray;
use futures::StreamExt;
use iceberg::arrow::arrow_schema_to_schema;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::io::FileIO;
use iceberg::io::LocalFsStorageFactory;
use iceberg::spec::DataFileFormat;
use iceberg::spec::Schema;
use iceberg::transaction::ApplyTransactionAction;
use iceberg::transaction::Transaction;
use iceberg::writer::base_writer::equality_delete_writer::EqualityDeleteFileWriter;
use iceberg::writer::base_writer::equality_delete_writer::EqualityDeleteFileWriterBuilder;
use iceberg::writer::base_writer::equality_delete_writer::EqualityDeleteWriterConfig;
use iceberg::writer::file_writer::location_generator::DefaultFileNameGenerator;
use iceberg::writer::file_writer::location_generator::DefaultLocationGenerator;
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::IcebergWriter;
use iceberg::writer::IcebergWriterBuilder;
use iceberg::Catalog;
use sqlx::Row;

use iceberg::CatalogBuilder;
use iceberg::NamespaceIdent;
use iceberg::TableIdent;
use iceberg_catalog_sql::{
    SqlBindStyle, SqlCatalogBuilder, SQL_CATALOG_PROP_BIND_STYLE, SQL_CATALOG_PROP_URI,
    SQL_CATALOG_PROP_WAREHOUSE,
};

use sqlx::sqlite::SqlitePool;
use std::sync::Arc;

#[tokio::main]
async fn main() {
    println!("Iceberg Equality Delete Example (Rust)");

    // Create SQL catalog config pointing to SQLite
    let warehouse_path = "/tmp/warehouse";
    let catalog_uri = format!("sqlite:///{}/pyiceberg_catalog.db", warehouse_path);

    // Ensure PyIceberg database schema is compatible with Rust Iceberg
    println!("Checking database schema compatibility...");
    let pool = SqlitePool::connect(&catalog_uri).await.unwrap();

    // The following part is needed because iceberg-rust expects a different schema then PyIceberg.
    // Check if iceberg_type column exists, add it if missing
    let check_column = sqlx::query(
        "SELECT COUNT(*) as cnt FROM pragma_table_info('iceberg_tables') WHERE name='iceberg_type'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    let column_exists: i64 = check_column.get(0);

    if column_exists == 0 {
        println!("Adding missing iceberg_type column to iceberg_tables...");
        sqlx::query(
            "ALTER TABLE iceberg_tables ADD COLUMN iceberg_type VARCHAR(10) DEFAULT 'TABLE'",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE iceberg_tables SET iceberg_type = 'TABLE' WHERE iceberg_type IS NULL")
            .execute(&pool)
            .await
            .unwrap();
        println!("Schema migration complete!");
    } else {
        println!("Schema is compatible.");
    }

    pool.close().await;

    let storage_factory = LocalFsStorageFactory::default();
    let catalog = SqlCatalogBuilder::default()
        .with_storage_factory(Arc::new(storage_factory))
        .load(
            "marvel",
            HashMap::from_iter([
                (SQL_CATALOG_PROP_URI.to_string(), catalog_uri.to_string()),
                (
                    SQL_CATALOG_PROP_WAREHOUSE.to_string(),
                    warehouse_path.to_string(),
                ),
                (
                    SQL_CATALOG_PROP_BIND_STYLE.to_string(),
                    SqlBindStyle::QMark.to_string(),
                ),
            ]),
        )
        .await
        .unwrap();
    println!("Connected to catalog");

    let xmen_ns = catalog
        .get_namespace(&NamespaceIdent::from_strs(["xmen"]).unwrap())
        .await;
    println!("Found the x-men namespace:");
    dbg!(&xmen_ns);

    println!("Check what tables exist");

    for table in catalog
        .list_tables(&NamespaceIdent::from_strs(["xmen"]).unwrap())
        .await
    {
        dbg!("{}", table);
    }

    // Load the table
    let table_ident = TableIdent::from_strs(["xmen", "characters"]).unwrap();
    let table = catalog.load_table(&table_ident).await.unwrap();
    println!("Loaded table: xmen.characters");
    println!("  Location: {}", table.metadata().location());

    // Read current table data before delete
    println!("\n=== Table data BEFORE delete ===");
    let scan = table.scan().build().unwrap();
    let mut stream = scan.to_arrow().await.unwrap();
    let mut total_rows_before = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        total_rows_before += batch.num_rows();
        dbg!("{:?}", &batch);
    }
    println!("Total rows before delete: {}", total_rows_before);

    // We want to delete all inactive x-men, i.e. we want to delete by the field `active`.
    // As iceberg references fields/columns by id we need to lookup the id via the schema
    let table_schema = table.metadata().current_schema();
    let active_field = table_schema
        .field_by_name("active")
        .expect("active field not found");

    let equality_ids = vec![active_field.id];
    println!("Equality fields: active ({})", active_field.id);

    let config = EqualityDeleteWriterConfig::new(equality_ids, table_schema.clone()).unwrap();
    let delete_arrow_schema = config.projected_arrow_schema_ref().clone();

    println!("\nDelete arrow schema:");
    for field in delete_arrow_schema.fields() {
        println!("  - {} (type: {:?})", field.name(), field.data_type());
    }

    let delete_schema = arrow_schema_to_schema(&delete_arrow_schema).unwrap();

    // Use the table's file IO instead of creating a new one
    let file_io = table.file_io().clone();
    let table_location = table.metadata().location();

    let pb = ParquetWriterBuilder::new(Default::default(), Arc::new(delete_schema));
    let location_gen = DefaultLocationGenerator::with_data_location(table_location.to_string());

    let file_name_gen =
        DefaultFileNameGenerator::new("delete".to_string(), None, DataFileFormat::Parquet);

    let rolling_writer_builder = RollingFileWriterBuilder::new_with_default_file_size(
        pb,
        file_io.clone(),
        location_gen,
        file_name_gen,
    );

    let mut equality_delete_writer =
        EqualityDeleteFileWriterBuilder::new(rolling_writer_builder, config)
            .build(None)
            .await
            .unwrap();

    // Create a record batch with ALL table fields, not just the equality fields
    // The writer will project it down to only the equality field `active` automatically
    let table_arrow_schema = Arc::new(schema_to_arrow_schema(&table_schema).unwrap());

    // Create arrays for all fields in the table schema
    // We're deleting rows where active = false
    let mut arrays: Vec<Arc<dyn arrow_array::Array>> = Vec::new();

    for field in table_arrow_schema.fields() {
        let array: Arc<dyn arrow_array::Array> = match field.name().as_str() {
            "id" => Arc::new(Int64Array::from(vec![1])),
            "name" => Arc::new(StringArray::from(vec!["Scott Summers"])),
            "alias" => Arc::new(StringArray::from(vec!["Cyclops"])),
            "powers" => Arc::new(StringArray::from(vec!["Optic blasts, team leadership"])),
            "birth_year" => Arc::new(Int64Array::from(vec![1970])),
            "active" => Arc::new(BooleanArray::from(vec![false])),
            _ => panic!("Unknown field: {}", field.name()),
        };
        arrays.push(array);
    }

    let to_write = RecordBatch::try_new(table_arrow_schema.clone(), arrays).unwrap();

    println!("\nWriting delete batch with {} rows", to_write.num_rows());
    println!("This will delete all rows where active = false");

    equality_delete_writer
        .write(to_write.clone())
        .await
        .unwrap();

    println!("Delete batch written successfully!");

    // Can't commit these delete files as it is not yet supported, see
    // https://github.com/apache/iceberg-rust/issues/2269.
    //
    // $ parquet-tools inspect delete-00000.parquet

    // ############ file meta data ############
    // created_by: parquet-rs version 58.4.0
    // num_columns: 1
    // num_rows: 1
    // num_row_groups: 1
    // format_version: 1.0
    // serialized_size: 423


    // ############ Columns ############
    // active

    // ############ Column(active) ############
    // name: active
    // path: active
    // max_definition_level: 1
    // max_repetition_level: 0
    // physical_type: BOOLEAN
    // logical_type: None
    // converted_type (legacy): NONE
    // compression: UNCOMPRESSED (space_saved: 0%)


    // /tmp/warehouse/xmen/characters
    // $ parquet-tools show delete-00000.parquet
    // +----------+
    // |   active |
    // |----------|
    // |        0 |
    // +----------+
