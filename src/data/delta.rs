use deltalake::datafusion::execution::context::SessionState;
//https://github.com/delta-io/delta-rs
use deltalake::{open_table_with_storage_options, DeltaTableError, datafusion::prelude::*, Path, ObjectStore};
use deltalake::azure::register_handlers;
use std::sync::Arc;
use polars::prelude::*;
use std::io::Cursor;
use std::convert::TryFrom;
use bytes::Bytes; 
use futures;

use super::permissions::*; 
use super::metastore::*;


pub struct DeltaLakeReader {
    storage_credentials: AzureDataLakeGen2Options,
    permissions_client: Permissions,
    metastore_client: MetastoreClient,
    principal: String,

}
impl DeltaLakeReader {
    /// Creates the delta lake reader struct
    ///
    /// # Arguments
    ///
    /// * `storage_credentials` - The credentials used to authenticate against azure storage
    /// * `permissions_client` - Permissions Object to validate user permissions against unity catalog 
    /// * `metastore_client` - Metastore Client object to interact with Unity Catalog APIs for data objects. 
    /// * `principal` - The active user's username. 
    ///
    /// # Examples
    ///
    /// ```
    ///     let reader: DeltaLakeReader = DeltaLakeReader::new(storage_options, permissions_client.clone(), metastore_client.clone(), String::from(principal));
    /// ```
    pub fn new(storage_credentials: AzureDataLakeGen2Options, permissions_client: Permissions, metastore_client: MetastoreClient, principal: String) -> Self {
        let reader: DeltaLakeReader = DeltaLakeReader {
            storage_credentials,
            permissions_client,
            metastore_client,
            principal,
        };

        // Call the register_handlers function
        register_handlers(None);

        reader
    }

    /// If the user has permission to read the table, then this function returns a datafusion dataframe. 
    ///
    /// # Arguments
    ///
    /// * `table_name` - The fully qualified table name
    ///
    /// # Examples
    ///
    /// ```
    /// let table_name: &str = "my_catalog.my_schema.my_table";
    /// let df = reader.read_delta_table_as_datafusion(table_path).await.unwrap();
    /// ```
    pub async fn read_delta_table_as_datafusion(&self, table_name: &str) -> Result<deltalake::datafusion::prelude::DataFrame, DeltaTableError> {
        let table_path = self.metastore_client.get_table(table_name).await.unwrap().storage_location.unwrap_or_default();
        if !self.permissions_client.can_read(&table_name, &self.principal).await.unwrap() {
            log::error!("Permissions of Object {} Denied.", table_name);
            return Err(DeltaTableError::Generic(String::from("Permission Denied.")));
        } else {
            log::info!("Validated Permissions on Object: {}", table_name);

            log::info!("Reading Table: {}", table_path);
            let table: deltalake::DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

            let ctx: SessionContext = SessionContext::new();

            ctx.register_table("loadtable", Arc::new(table)).unwrap();

            let df: deltalake::datafusion::prelude::DataFrame = ctx.sql("SELECT * FROM loadtable").await.unwrap();
            return Ok(df);
        }

        
    }

    /// Reads a delta table in a parallel fashion
    ///
    /// # Arguments
    ///
    /// * `table_path` - The path to the table in cloud storage
    ///
    /// # Examples
    ///
    /// ```
    /// let table_path = self.metastore_client.get_table(table_name).await.unwrap().storage_location.unwrap_or_default();
    /// let table_bytes = self.parallel_read_table_as_bytes(&table_path).await?;
    /// ```
    async fn parallel_read_table_as_bytes(&self, table_path: &str) -> Result<Vec<Bytes>, DeltaTableError> {
        log::info!("Reading Table: {}", table_path);
        let table: deltalake::DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;
    
        let files: Vec<String> = table.get_file_uris().unwrap().collect();
        let object_store: Arc<dyn ObjectStore> = table.object_store();
    
        let futures: Vec<_> = files.into_iter().map(|file| {
            let object_store = Arc::clone(&object_store);
            async move {
                log::info!("Loading file: {}", file);
                let parts: Vec<&str> = file.split('/').collect();
                let file_name: &str = parts.last().unwrap();
                let file_path: Path = Path::try_from(file_name.to_string()).unwrap();
                let result = object_store.get(&file_path).await?;
                let bytes = result.bytes().await?;
                Ok::<Bytes, DeltaTableError>(bytes)
            }
        }).collect();
    
        let table_bytes: Vec<Bytes> = futures::future::join_all(futures).await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;
    
        Ok(table_bytes)
    }

    /// Reads a delta table in a serial fashion
    ///
    /// # Arguments
    ///
    /// * `table_path` - The path to the table in cloud storage
    ///
    /// # Examples
    ///
    /// ```
    /// let table_path = self.metastore_client.get_table(table_name).await.unwrap().storage_location.unwrap_or_default();
    /// let table_bytes = self.read_table_as_bytes(&table_path).await?;
    /// ```
    async fn read_table_as_bytes(&self, table_path: &str) -> Result<Vec<Bytes>, DeltaTableError> { // return bytes
        log::info!("Reading Table: {}", table_path);
        let table: deltalake::DeltaTable = open_table_with_storage_options(table_path, self.storage_credentials.to_hash_map()).await?;

        let mut table_bytes: Vec<Bytes> = Vec::default();

        // get the files and storage object
        let files: Vec<String> = table.get_file_uris().unwrap().collect();
        let object_store: Arc<dyn ObjectStore> = table.object_store();

        // foreach file we need only the file name 
        // provide it to the storage objet to download into bytes 
        // load the bytes into a Vec<Bytes>
        for file in files.iter() {
            log::info!("Loading file: {}", file);
            let parts: Vec<&str> = file.split('/').collect();
            let file_name: &str = parts[parts.len()-1];
            let file_path: Path = Path::try_from(format!("{}", file_name)).unwrap();
            let result: deltalake::storage::GetResult = object_store.get(&file_path).await.unwrap();
            let bytes: Bytes = result.bytes().await.unwrap();
            table_bytes.push(bytes);
            
        }
        Ok(table_bytes)

    }

    /// If the user has permission to read the table, then this function returns a polars dataframe. 
    ///
    /// # Arguments
    ///
    /// * `table_name` - The fully qualified table name
    /// * `parallel_read` - true/false argument to read the table serially or in parallel
    ///
    /// # Examples
    ///
    /// ```
    /// let table_name: &str = "my_catalog.my_schema.my_table";
    /// let df = reader.read_delta_table_as_polars(table_path, true).await.unwrap();
    /// ```
    pub async fn read_delta_table_as_polars(&self, table_name: &str, parallel_read: bool) -> Result<polars::prelude::DataFrame, DeltaTableError> { //Result<polars::prelude::DataFrame, DeltaTableError> {        
        // create empty DF - we will replace it later with the if/else
        let table_path = self.metastore_client.get_table(table_name).await.unwrap().storage_location.unwrap_or_default();
        let mut df: polars::prelude::DataFrame = polars::prelude::DataFrame::default();
        let mut table_bytes: Vec<Bytes> = Vec::default();

        if !self.permissions_client.can_read(&table_name, &self.principal).await.unwrap() {
            log::info!("Permissions of Object {} Denied.", table_name);
            return Ok(df);
        } else {
            log::info!("Validated Permissions on Object: {}", table_name);
            // get the table as a vector of bytes each index is a parquet file 
            if parallel_read {
                log::info!("Parallel reading table.");
                table_bytes = self.parallel_read_table_as_bytes(&table_path).await?;
            } else {
                log::info!("Seirially readin table.");
                table_bytes = self.read_table_as_bytes(&table_path).await?;
            }
            
            // foreach file we need only the file name 
            // load the bytes into a polars dataframe
            for b in table_bytes {
                let cursor = Cursor::new(b);
                let new_df: polars::prelude::DataFrame = ParquetReader::new(cursor).finish().unwrap();

                if df.is_empty() {
                    df = new_df.clone();
                } else {
                    df = match df.vstack(&new_df) {
                        Ok(stacked_df) => stacked_df,
                        Err(e) => {
                            // Handle the error if the vertical stack operation fails
                            log::error!("Error stacking DataFrames: {}", e);
                            df // Return the original DataFrame if the operation fails
                        }
                    };

                }
            }   
        }        
        Ok(df)
    }


    pub async fn print_datafusion_dataframe(&self, df: deltalake::datafusion::prelude::DataFrame) {
        let data = df.collect().await.unwrap();

        for d in data {
            log::info!("{:?}", d);

        }
    }


}

// pub struct Writer {
//     placeholder: String,
// }


