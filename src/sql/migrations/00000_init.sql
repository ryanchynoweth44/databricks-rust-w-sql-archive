CREATE TABLE IF NOT EXISTS catalogs (
                name TEXT PRIMARY KEY,
                owner TEXT ,
                comment TEXT,
                storage_root TEXT,
                provider_name TEXT,
                share_name TEXT,
                enable_predictive_optimization TEXT,
                metastore_id TEXT ,
                created_at INTEGER ,
                created_by TEXT ,
                updated_at INTEGER,
                updated_by TEXT,
                catalog_type TEXT,
                storage_location TEXT,
                isolation_mode TEXT,
                connection_name TEXT,
                full_name TEXT,
                securable_kind TEXT,
                securable_type TEXT,
                browse_only BOOLEAN
            ); 



CREATE TABLE IF NOT EXISTS schemas (
    schema_id TEXT PRIMARY KEY,
    name TEXT,
    catalog_name TEXT,
    owner TEXT,
    comment TEXT,
    storage_root TEXT,
    enable_predictive_optimization TEXT,
    metastore_id TEXT,
    full_name TEXT,
    storage_location TEXT,
    created_at INTEGER,
    created_by TEXT,
    updated_at INTEGER,
    updated_by TEXT,
    catalog_type TEXT,
    browse_only BOOLEAN
    -- FOREIGN KEY (catalog_name) REFERENCES Catalog(name)
);



CREATE TABLE IF NOT EXISTS tables (
    table_id TEXT PRIMARY KEY,
    name TEXT,
    catalog_name TEXT,
    schema_name TEXT,
    table_type TEXT,
    data_source_format TEXT,
    storage_location TEXT,
    view_definition TEXT,
    sql_path TEXT,
    owner TEXT,
    comment TEXT,
    storage_credential_name TEXT,
    enable_predictive_optimization TEXT,
    metastore_id TEXT,
    full_name TEXT,
    data_access_configuration_id TEXT,
    created_at INTEGER,
    created_by TEXT,
    updated_at INTEGER,
    updated_by TEXT,
    deleted_at INTEGER,
    access_point TEXT,
    pipeline_id TEXT,
    browse_only BOOLEAN--,
    -- PRIMARY KEY (name, catalog_name, schema_name),
    -- FOREIGN KEY (catalog_name) REFERENCES Catalog(name),
    -- FOREIGN KEY (schema_name, catalog_name) REFERENCES Schema(name, catalog_name)
);