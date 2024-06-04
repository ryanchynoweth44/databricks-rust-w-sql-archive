use super::api_client::APIClient;
use crate::sql::sql_client::SqlClient as SqlClient;
use reqwest::{Error, Response};
use serde::Deserialize;
use sqlx::prelude::FromRow;


#[derive(Clone)]
pub struct MetastoreClient {
    api_client: APIClient,
    sql_client: SqlClient,
}

impl MetastoreClient {

    
    pub async fn new(workspace_name: String, db_token: String, database_url: String) -> Self {
        let api_client: APIClient = APIClient{
            db_token: db_token,
            workspace_name: workspace_name
        };
        
        let sql_client: SqlClient = SqlClient::new(&database_url).await.unwrap();

        let perms: MetastoreClient = MetastoreClient { api_client: api_client, sql_client: sql_client };

        perms
    }

    // List all catalogs in a Databricks' Unity Catalog Metastore
    // https://docs.databricks.com/api/workspace/catalogs/list
    async fn fetch_catalogs(&self) -> Result<CatalogResponse, Error> {
        let catalog_url: String = format!(
            "https://{}/api/2.1/unity-catalog/catalogs",
            &self.api_client.workspace_name
        );

        let response: Response = self.api_client.fetch(&catalog_url).await?;

        let catalogs: CatalogResponse = match response.json().await {
            Ok(catalogs) => catalogs, // If deserialization succeeds, continue with the deserialized data
            Err(e) => {
                // If deserialization fails, log the error and return an error
                log::error!("Error deserializing JSON response: {}", e);
                return Err(e.into());
            }
        };

        Ok(catalogs)
    }

    // List schemas fpr a given catalog in a Databricks' Unity Catalog Metastore
    // https://docs.databricks.com/api/workspace/schemas/list
    async fn fetch_schemas(
        &self,
        catalog_name: String,
        max_results: Option<usize>,
    ) -> Result<SchemaResponse, Error> {
        let mut schema_url = format!(
            "https://{}/api/2.1/unity-catalog/schemas?catalog_name={}",
            &self.api_client.workspace_name, catalog_name
        );

        if let Some(max) = max_results {
            schema_url.push_str(&format!("&max_results={}", max));
        }

        // Fetch schemas for the current catalog
        let response: Response = self.api_client.fetch(&schema_url).await?;
        let schemas: SchemaResponse = match response.json().await {
            Ok(schemas) => schemas, // If deserialization succeeds, continue with the deserialized data
            Err(e) => {
                // If deserialization fails, log the error and return an error
                log::error!("Error deserializing JSON response: {}", e);
                return Err(e.into());
            }
        };

        Ok(schemas)
    }

    // List all tables for a given schema/catalog in a Databricks' Unity Catalog Metastore
    // https://docs.databricks.com/api/workspace/tables/list
    async fn fetch_tables(
        &self,
        catalog_name: String,
        schema_name: String,
        max_results: Option<usize>,
    ) -> Result<TableResponse, Error> {
        let mut table_url = format!(
            "https://{}/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}",
            &self.api_client.workspace_name, catalog_name, schema_name
        );

        if let Some(max) = max_results {
            table_url.push_str(&format!("&max_results={}", max));
        }

        // Fetch tables for the current catalog/schema
        let response: Response = self.api_client.fetch(&table_url).await?;
        // let tables: TableResponse = response.json().await?;
        let tables: TableResponse = match response.json().await {
            Ok(tables) => tables, // If deserialization succeeds, continue with the deserialized data
            Err(e) => {
                // If deserialization fails, log the error and return an error
                log::error!("Error deserializing JSON response: {}", e);
                return Err(e.into());
            }
        };

        Ok(tables)
    }

    // Get an individual table object
    // https://docs.databricks.com/api/workspace/tables/get
    pub async fn get_table(&self, full_table_name: &str) -> Result<Table, Error> {
        let url: String = format!(
            "https://{}/api/2.1/unity-catalog/tables/{}",
            &self.api_client.workspace_name, full_table_name
        );

        let response: Response = self.api_client.fetch(&url).await?;
        let table: Table = response.json().await?;

        // if we get a table from the API we will update it locally
        let mut tables: Vec<Table> = Vec::new(); // Create an empty vector of tables
        tables.push(table.clone());
        let table_response = TableResponse::new(tables);
        self.sql_client.write_tables(table_response).await.unwrap();

        Ok(table)
    }

    // Get an individual schema object
    // https://docs.databricks.com/api/workspace/schemas/get
    pub async fn get_schema(&self, full_schema_name: String) -> Result<Schema, Error> {
        let url: String = format!(
            "https://{}/api/2.1/unity-catalog/schemas/{}",
            &self.api_client.workspace_name, full_schema_name
        );

        let response: Response = self.api_client.fetch(&url).await?;
        let schema: Schema = response.json().await?;

        // if we get a schema from the API we will update it locally
        let mut schemas: Vec<Schema> = Vec::new(); // Create an empty vector of schemas
        schemas.push(schema.clone());
        let schema_response = SchemaResponse::new(schemas);
        self.sql_client.write_schemas(schema_response).await.unwrap();

        Ok(schema)
    }

    // Get an individual catalog object
    // https://docs.databricks.com/api/workspace/schemas/get
    pub async fn get_catalog(&self, name: String) -> Result<Catalog, Error> {
        let url: String = format!(
            "https://{}/api/2.1/unity-catalog/catalogs/{}",
            &self.api_client.workspace_name, name
        );

        let response: Response = self.api_client.fetch(&url).await?;
        let catalog: Catalog = response.json().await?;

        // if we get a schema from the API we will update it locally
        let mut catalogs: Vec<Catalog> = Vec::new(); // Create an empty vector of Catalogs
        catalogs.push(catalog.clone());
        let catalog_response = CatalogResponse::new(catalogs);
        self.sql_client.write_catalogs(catalog_response).await.unwrap();

        Ok(catalog)
    }

    pub async fn refresh_catalogs(&self) -> Result<(), Error> {
        log::info!("Getting Catalogs. ");
        let catalogs: CatalogResponse = self.fetch_catalogs().await?;
        self.sql_client.write_catalogs(catalogs).await.unwrap();

        Ok(())
    }

    pub async fn refresh_all_schemas(&self) -> Result<(), Error> {
        let catalogs: CatalogResponse = self.fetch_catalogs().await?;
        log::info!("Getting Schemas.");
        for catalog in catalogs.catalogs {
            // exclude delta sharing catalogs for now
            if catalog.catalog_type != "DELTASHARING_CATALOG"
                && catalog.name != "__databricks_internal"
                && catalog.name != "adrian_hive_test"
            {
                let schemas: SchemaResponse = self.fetch_schemas(catalog.name, None).await?;
                self.sql_client.write_schemas(schemas).await.unwrap();
                std::thread::sleep(std::time::Duration::from_secs(1));
            }
        }
        Ok(())
    }

    pub async fn refresh_all_tables(&self) -> Result<(), Error> {
        let catalogs: CatalogResponse = self.fetch_catalogs().await?;
        let num_cats = catalogs.catalogs.len();
        let mut i = 0;
        for catalog in catalogs.catalogs {
            log::info!("Num catalogs: {} out of {}", i, num_cats);
            i = i + 1;
            if catalog.catalog_type != "DELTASHARING_CATALOG"
                && catalog.name != "__databricks_internal"
                && catalog.name != "adrian_hive_test"
            {
                let schemas: SchemaResponse =
                    self.fetch_schemas(catalog.name.clone(), None).await?;
                if let Some(schemas) = schemas.schemas {
                    for schema in schemas {
                        log::info!(
                            "----------------> Getting Tables for Schema {}.{}.",
                            schema.catalog_name,
                            schema.name
                        );
                        let table_response = self
                            .fetch_tables(catalog.name.clone(), schema.name, None)
                            .await?;
                        if let Some(ref tables) = table_response.tables {
                            log::info!("Num Tables: {}", tables.len());
                            self.sql_client.write_tables(table_response).await.unwrap();
                            // std::thread::sleep(std::time::Duration::from_secs(1));
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn refresh_tables(&self, catalog_name: String, schema_name: String) -> Result<(), Error> {
        let table_response = self.fetch_tables(catalog_name, schema_name, None).await?;
        if let Some(ref tables) = table_response.tables {
            log::info!("Num Tables: {}", tables.len());
            self.sql_client.write_tables(table_response).await.unwrap();
        }
      Ok(())
    }

    pub async fn refresh_schemas(&self, catalog_name: String) -> Result<(), Error> {
        let schema_response = self.fetch_schemas(catalog_name, None).await?;
        if let Some(ref schemas) = schema_response.schemas {
            log::info!("Num Schemas: {}", schemas.len());
            self.sql_client.write_schemas(schema_response).await.unwrap();
        }
      Ok(())
    }

}

// wrapper struct to contain a vector of catalogs
#[derive(Debug, Deserialize, Clone)]
pub struct CatalogResponse {
    pub catalogs: Vec<Catalog>,
}
impl CatalogResponse {
    pub fn new(catalogs: Vec<Catalog>) -> Self {
        CatalogResponse { catalogs: catalogs }
    }
}

// individual struct for catalogs
#[derive(Debug, Deserialize, Clone, FromRow)]
pub struct Catalog {
    pub name: String,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub provider_name: Option<String>,
    pub share_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: String,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: Option<i64>,
    pub updated_by: Option<String>,
    pub catalog_type: String,
    pub storage_location: Option<String>,
    pub isolation_mode: Option<String>,
    pub connection_name: Option<String>,
    pub full_name: String,
    pub securable_kind: Option<String>,
    pub securable_type: Option<String>,
    pub browse_only: Option<bool>,
    // excluded fields due to nesting
    // properties
    // effective_predictive_optimization_flag
    // options
    // provisioning_info
}

#[derive(Debug, Deserialize, Clone)]
pub struct SchemaResponse {
    pub schemas: Option<Vec<Schema>>,
}
impl SchemaResponse {
    pub fn new(schemas: Vec<Schema>) -> Self {
        SchemaResponse { schemas: Some(schemas) }
    }
}

#[derive(Debug, Deserialize, Clone, FromRow)]
pub struct Schema {
    pub name: String,
    pub catalog_name: String,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_root: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: String,
    pub full_name: String,
    pub storage_location: Option<String>,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: Option<i64>,
    pub updated_by: Option<String>,
    pub catalog_type: Option<String>,
    pub browse_only: Option<bool>,
    pub schema_id: String,
}

#[derive(Debug, Deserialize, Clone)]
pub struct TableResponse {
    pub tables: Option<Vec<Table>>,
}
impl TableResponse {
    pub fn new(tables: Vec<Table>) -> Self {
        TableResponse { tables: Some(tables) }
    }
}

#[derive(Debug, Deserialize, Clone, FromRow)]
pub struct Table {
    pub name: String,
    pub catalog_name: String,
    pub schema_name: String,
    pub table_type: String,
    pub data_source_format: Option<String>,
    pub storage_location: Option<String>, // full path to table
    pub view_definition: Option<String>,
    pub sql_path: Option<String>,
    pub owner: String,
    pub comment: Option<String>,
    pub storage_credential_name: Option<String>,
    pub enable_predictive_optimization: Option<String>,
    pub metastore_id: Option<String>,
    pub full_name: String,
    pub data_access_configuration_id: Option<String>,
    pub created_at: i64,
    pub created_by: String,
    pub updated_at: Option<i64>,
    pub updated_by: Option<String>,
    pub deleted_at: Option<i64>,
    pub table_id: String,
    pub access_point: Option<String>,
    pub pipeline_id: Option<String>,
    pub browse_only: Option<bool>,
    // excluded fields due to nesting
    // columns
    // dependencies
    // properties
    // table_constraints
    // row_filter
    // delta_runtime_properties_kvpairs
    // effective_predictive_optimization_flag
}
