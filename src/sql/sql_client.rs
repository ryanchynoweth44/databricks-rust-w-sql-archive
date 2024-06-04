// https://github.com/launchbadge/sqlx/tree/main/examples/sqlite/todos
use log;
use sqlx::migrate::{MigrateError, MigrateDatabase};
use crate::data::metastore::{CatalogResponse, SchemaResponse, TableResponse};
use sqlx::{Error, Sqlite, FromRow};
use sqlx::sqlite::{SqliteQueryResult, SqlitePool};



#[derive(Clone)]
pub struct SqlClient {
    pool: sqlx::Pool<Sqlite>,
}

impl SqlClient {
    pub async fn new(database_path: &str) -> Result<Self, Error> {
        // Create SQLite connection options
        if !Sqlite::database_exists(database_path).await? {
            // Sqlite::create_database(database_path).await?;
            match Sqlite::create_database(database_path).await {
                Ok(_) => log::info!("Create db success"),
                Err(error) => panic!("error: {}", error),
            }
        }
        let pool: sqlx::Pool<Sqlite> = SqlitePool::connect(database_path).await?;

        Ok(Self { pool })
    }

    pub async fn execute_sql(&self, query: &str) -> Result<SqliteQueryResult, Error> {
        log::info!("Executing SQL: {}", query);
        let result = sqlx::query(query).execute(&self.pool).await;
        match result {
            Ok(res) => {
                log::info!("--------------- {:?}", res);
                Ok(res)
            },
            Err(err) => {
                log::error!("Error executing SQL query: {}", err);
                Err(err)
            }
        }
    
    }

    pub async fn run_migrations(&self, migrations_path: &str) -> Result<(), MigrateError> {
        log::info!("-------------- Running Migrations | Path: {}", migrations_path);
        let migrations = std::path::Path::new(migrations_path);

        let migration_results = sqlx::migrate::Migrator::new(migrations)
            .await
            .unwrap()
            .run(&self.pool)
            .await;

        match migration_results {
            Ok(_) => log::info!("Migration success"),
            Err(error) => {
                panic!("error: {}", error);
            }
        }
    
        log::info!("migration: {:?}", migration_results);

        migration_results
    }

    pub async fn write_catalogs(&self, catalog_response: CatalogResponse) -> Result<(), sqlx::Error> {
        // let mut tx = self.pool.begin().await?;
        // let conn = self.pool.acquire().await?;
        // let mut tx = conn.begin().await?;

        for catalog in catalog_response.catalogs {
            if catalog.catalog_type != "DELTASHARING_CATALOG" && catalog.name != "__databricks_internal" {
                let _result: SqliteQueryResult = sqlx::query(
                    "INSERT OR REPLACE INTO catalogs (name, owner, comment, storage_root, provider_name, share_name, enable_predictive_optimization, metastore_id, created_at, created_by, updated_at, updated_by, catalog_type, storage_location, isolation_mode, connection_name, full_name, securable_kind, securable_type, browse_only)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20)"
                )
                .bind(&catalog.name)
                .bind(&catalog.owner)
                .bind(&catalog.comment)
                .bind(&catalog.storage_root)
                .bind(&catalog.provider_name)
                .bind(&catalog.share_name)
                .bind(&catalog.enable_predictive_optimization)
                .bind(&catalog.metastore_id)
                .bind(&catalog.created_at)
                .bind(&catalog.created_by)
                .bind(&catalog.updated_at)
                .bind(&catalog.updated_by)
                .bind(&catalog.catalog_type)
                .bind(&catalog.storage_location)
                .bind(&catalog.isolation_mode)
                .bind(&catalog.connection_name)
                .bind(&catalog.full_name)
                .bind(&catalog.securable_kind)
                .bind(&catalog.securable_type)
                .bind(&catalog.browse_only)
                .execute(&self.pool)
                // .execute(&mut tx)
                .await?;
            }
        }
        // tx.commit().await?;
        Ok(())
    }
    

    pub async fn write_schemas(&self, schema_response: SchemaResponse) -> Result<(), sqlx::Error> {
        if let Some(schemas) = schema_response.schemas {
            for schema in schemas {
                log::info!("Catalog: {} | Schema: {}", schema.catalog_name, schema.name);
                let _result = sqlx::query(
                    "INSERT OR REPLACE INTO schemas (name, catalog_name, owner, comment, storage_root, enable_predictive_optimization, metastore_id, full_name, storage_location, created_at, created_by, updated_at, updated_by, catalog_type, browse_only, schema_id) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16)"
                )
                .bind(&schema.name)
                .bind(&schema.catalog_name)
                .bind(&schema.owner)
                .bind(&schema.comment)
                .bind(&schema.storage_root)
                .bind(&schema.enable_predictive_optimization)
                .bind(&schema.metastore_id)
                .bind(&schema.full_name)
                .bind(&schema.storage_location)
                .bind(schema.created_at)
                .bind(&schema.created_by)
                .bind(&schema.updated_at)
                .bind(&schema.updated_by)
                .bind(&schema.catalog_type)
                .bind(&schema.browse_only)
                .bind(&schema.schema_id)
                .execute(&self.pool)
                .await?;


            }
        }    
        Ok(())
    }

    pub async fn write_tables(&self, table_response: TableResponse) -> Result<(), sqlx::Error> {
        log::info!("Writing Tables!");
        if let Some(tables) = table_response.tables {
            for table in tables {
                log::info!(" Catalog: {} | Schema: {} | Table: {}", table.catalog_name, table.schema_name, table.name);
                let result = sqlx::query(
                    "INSERT OR REPLACE INTO tables (name, catalog_name, schema_name, table_type, data_source_format, storage_location, view_definition, sql_path, owner, comment, storage_credential_name, enable_predictive_optimization, metastore_id, full_name, data_access_configuration_id, created_at, created_by, updated_at, updated_by, deleted_at, table_id, access_point, pipeline_id, browse_only) 
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)"
                )
                .bind(&table.name)
                .bind(&table.catalog_name)
                .bind(&table.schema_name)
                .bind(&table.table_type)
                .bind(&table.data_source_format)
                .bind(&table.storage_location)
                .bind(&table.view_definition)
                .bind(&table.sql_path)
                .bind(&table.owner)
                .bind(&table.comment)
                .bind(&table.storage_credential_name)
                .bind(&table.enable_predictive_optimization)
                .bind(&table.metastore_id)
                .bind(&table.full_name)
                .bind(&table.data_access_configuration_id)
                .bind(&table.created_at)
                .bind(&table.created_by)
                .bind(&table.updated_at)
                .bind(&table.updated_by)
                .bind(&table.deleted_at)
                .bind(&table.table_id)
                .bind(&table.access_point)
                .bind(&table.pipeline_id)
                .bind(&table.browse_only)
                .execute(&self.pool)
                .await;
                
                match result {
                    Ok(res) => {
                        log::info!("--------------- {:?}", res);
                    },
                    Err(err) => {
                        log::error!("Error executing SQL query: {}", err);
                        return Err(err);
                    }
                } 
            }
        }
        Ok(())
    }

    pub async fn list_catalogs(&self, search_term: Option<&str>) -> Result<Vec<ListCatalogResultSet>, sqlx::Error> {
        let mut qry: String = String::from("select name from catalogs");

        if let Some(term) = search_term {
            qry.push_str(&format!(" WHERE name LIKE '%{}%'", term));
        }


        
        let catalog_results: Vec<ListCatalogResultSet> = sqlx::query_as::<_, ListCatalogResultSet>(&qry)
            .fetch_all(&self.pool)
            .await
            .unwrap();

        for cat in &catalog_results {
            println!(
                "Catalog Name: {}", cat.name );
        }


        Ok(catalog_results)
    }

    pub async fn list_schemas(&self, catalog_name: Option<&str>, search_term: Option<&str>) -> Result<Vec<ListSchemaResultSet>, sqlx::Error> {
        let mut qry: String = String::from("select name, catalog_name from schemas");

        if catalog_name.is_some() || search_term.is_some() {
            qry.push_str(" WHERE ");
        }    
        
        if let Some(term) = search_term {
            qry.push_str(&format!("  name LIKE '%{}%'", term));
        }

        if let Some(catalog) = catalog_name {
            if search_term.is_some() {
                qry.push_str(&format!(" and catalog_name = '{}'", catalog));
            } else {
                qry.push_str(&format!("  catalog_name = '{}'", catalog));
            }
        }


        
        let schema_results: Vec<ListSchemaResultSet> = sqlx::query_as::<_, ListSchemaResultSet>(&qry)
            .fetch_all(&self.pool)
            .await
            .unwrap();

        for sc in &schema_results {
            println!(
                "Catalog Name: {} | Schema Name: {}", sc.catalog_name, sc.name );
        }


        Ok(schema_results)
    }

    pub async fn list_tables(&self, catalog_name: Option<&str>, schema_name: Option<&str>, search_term: Option<&str>) -> Result<Vec<ListTableResultSet>, sqlx::Error> {
        let mut qry: String = String::from("select name, catalog_name, schema_name from tables");

        if catalog_name.is_some() || search_term.is_some() || schema_name.is_some() {
            qry.push_str(" WHERE ");
        }    
        
        if let Some(term) = search_term {
            qry.push_str(&format!(" name LIKE '%{}%'", term));
        }

        if let Some(catalog) = catalog_name {
            if search_term.is_some() {
                qry.push_str(&format!(" and catalog_name = '{}'", catalog));
            } else {
                qry.push_str(&format!("  catalog_name = '{}'", catalog));
            }
        }

        if let Some(schema) = schema_name {
            if search_term.is_some() || catalog_name.is_some() {
                qry.push_str(&format!(" and schema_name = '{}'", schema));
            } else {
                qry.push_str(&format!("  schema_name = '{}'", schema));
            }
        }


        
        let table_results: Vec<ListTableResultSet> = sqlx::query_as::<_, ListTableResultSet>(&qry)
            .fetch_all(&self.pool)
            .await
            .unwrap();

        for t in &table_results {
            println!(
                "Catalog Name: {} | Schema Name: {} | Table Name: {}", t.catalog_name, t.schema_name, t.name );
        }


        Ok(table_results)
    }

}

#[derive(Clone, FromRow, Debug)]
pub struct ListCatalogResultSet {
    name: String,
}

#[derive(Clone, FromRow, Debug)]
pub struct ListSchemaResultSet {
    name: String,
    catalog_name: String,
}

#[derive(Clone, FromRow, Debug)]
pub struct ListTableResultSet {
    name: String,
    catalog_name: String,
    schema_name: String,
}