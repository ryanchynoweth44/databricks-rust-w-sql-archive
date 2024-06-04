use reqwest::{Response, Error};
use serde::Deserialize;
use super::api_client::APIClient;
use std::collections::HashMap;
use std::env;





#[derive(Clone)]
pub struct Permissions {
    pub api_client: APIClient,
}

impl Permissions {

    pub fn new(workspace_name: String, db_token: String ) -> Self {
        let api_client: APIClient = APIClient{
            db_token: db_token,
            workspace_name: workspace_name
        };

        let perms: Permissions = Permissions { api_client: api_client };

        perms
    }

    /// Returns User Struct containing the princpal used for authentication. 
    ///
    /// # Arguments
    ///
    /// * `user_name` - The username associated to the token
    /// * `user_token` - The token used for authentication against Unity Catalog
    ///
    /// # Examples
    ///
    /// ```
    /// let active_user: api::permissions::User = permissions_client.authenticate_user("ryan.chynoweth@databricks.com", &api_client.db_token).await?;
    /// ```
    pub async fn authenticate_user(&self, user_name: &str, user_token: &str, workspace_name: &str) -> Result<AzureDataLakeGen2Options, Error> {
        // need to add encryption and verification of user

        // user_token will likely be required in the future as there will be a service token and a user token. 
        let auth_url: String = format!("https://{}/api/2.0/preview/scim/v2/Me", &self.api_client.workspace_name);

        let azure_storage_account_name: String = env::var("AZURE_STORAGE_ACCOUNT_NAME").expect("AZURE_STORAGE_ACCOUNT_NAME not set");
        let azure_client_id: String = env::var("AZURE_CLIENT_ID").expect("AZURE_CLIENT_ID not set");
        let azure_client_secret: String = env::var("AZURE_CLIENT_SECRET").expect("AZURE_CLIENT_SECRET not set");
        let azure_tenant_id: String = env::var("AZURE_TENANT_ID").expect("AZURE_TENANT_ID not set");
        // let workspace_name: String = env::var("WORKSPACE_NAME").expect("WORKSPACE_NAME not set");


        let storage_options: AzureDataLakeGen2Options = AzureDataLakeGen2Options::new(
            azure_storage_account_name,
            azure_client_id,
            azure_client_secret,
            azure_tenant_id
        );

        // create a user client that is different from the other services? 
        let auth_client: APIClient = APIClient {
            db_token: String::from(user_token),
            workspace_name: String::from(workspace_name)
        };

        let response: Response = self.api_client.fetch(&auth_url).await?;
        let status: bool = response.status().is_success();

        let user: User = response.json().await?;

        if !status && user.user_name == user_name {
            log::error!("Failed to authenticate user: {}", user.user_name);
        } else {
            log::info!("User {} authentication was successful.", user.user_name);
        }

        
        Ok(storage_options)
    }

    // /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}
    pub async fn fetch_permissions(&self, securable_type: SecurableType, full_name: &str, principal: &str) -> Result<PrivilegeAssignmentsResponse, Error> {

        let securable_type_str = securable_type.to_string();

        // focusing just on tables for now

        // split full name and make 3 different api calls since permissions can be delagated 
        let name_parts: Vec<&str> = full_name.split('.').collect();
        let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog        
        let schema_name = match (name_parts.get(0), name_parts.get(1)) { // may not always be a schema
            (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
            _ => "".to_string(), // handle case where parts are missing
        };        



        let mut privileges: PrivilegeAssignmentsResponse = PrivilegeAssignmentsResponse::new();

        // make calls for each part of the object name to collect permissions 
        if !name_parts.get(0).is_none() {
            let catalog_auth_url: String = format!("https://{}/api/2.1/unity-catalog/permissions/{}/{}?principal={}", &self.api_client.workspace_name, "catalog", catalog_name, principal);
            log::info!("Getting Catalog Permissions - {}", catalog_auth_url);
            let catalog_response: Response = self.api_client.fetch(&catalog_auth_url).await?;
            let catalog_perms: PrivilegeAssignmentsResponse = catalog_response.json().await?;
            privileges.add_assignment(catalog_perms, &catalog_name, SecurableType::Catalog);
        }
        if !name_parts.get(1).is_none() {
            let schema_auth_url: String = format!("https://{}/api/2.1/unity-catalog/permissions/{}/{}?principal={}", &self.api_client.workspace_name, "schema", schema_name, principal);
            log::info!("Getting Schema Permissions - '{}'", schema_auth_url);
            let schema_response: Response = self.api_client.fetch(&schema_auth_url).await?;
            let schema_perms: PrivilegeAssignmentsResponse = schema_response.json().await?;
            privileges.add_assignment(schema_perms, &schema_name, SecurableType::Schema);
        } 
        if !name_parts.get(2).is_none() {
            let obj_auth_url: String = format!("https://{}/api/2.1/unity-catalog/permissions/{}/{}?principal={}", &self.api_client.workspace_name, &securable_type_str, full_name, principal);
            log::info!("Getting Object Permissions - {}", obj_auth_url);
            let obj_response: Response = self.api_client.fetch(&obj_auth_url).await?;
            let obj_perms: PrivilegeAssignmentsResponse = obj_response.json().await?;
            privileges.add_assignment(obj_perms, &full_name, SecurableType::Table);
        }      
        
        Ok(privileges)
    }

    
    pub async fn get_object_owner(&self, securable_type: SecurableType, full_name: &str) -> Result<ObjectOwnerResponse, Error>{
        // https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/ownership.html
        // owners of objects automatically have full control of object 

        log::info!("Checking ownership on {}: {}", securable_type.to_string(), full_name);

        let url: String = format!("https://{}/api/2.1/unity-catalog/{}s/{}", &self.api_client.workspace_name, securable_type.to_string(), full_name);
        let response: Response = self.api_client.fetch(&url).await?;
        let status: bool = response.status().is_success();

        let owner_response: ObjectOwnerResponse = response.json().await?;


        if !status {
            log::error!("Failed to get owner of object - {}", full_name);
        } 

        Ok(owner_response)
    }   

    async fn check_permissions(&self, securable_type: SecurableType, full_name: &str, principal: &str, permissions: Vec<&str>) -> Result<bool, Error> {
        let mut perm_check: bool = false; // deny by default
        let object_permissions: PrivilegeAssignmentsResponse = self.fetch_permissions(securable_type.clone(), full_name, principal).await?;

        // split full name and make 3 different api calls since permissions can be delagated 
        let name_parts: Vec<&str> = full_name.split('.').collect();
        let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog        
        let schema_name = match (name_parts.get(0), name_parts.get(1)) { // may not always be a schema
            (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
            _ => "".to_string(), // handle case where parts are missing
        };

        // if they are an owner of the object or one of the parent objects then we return TRUE 
        if self.get_object_owner(securable_type.clone(), full_name).await.unwrap().owner == principal { 
            log::info!("Princpal {} is an owner of {}. ", principal, full_name);
            perm_check = true; 
        } else if !name_parts.get(1).is_none() && self.get_object_owner(SecurableType::Schema, &schema_name).await.unwrap().owner == principal { // if princpal is the owner of the schema then return True 
            log::info!("Princpal {} is an owner of {}. ", principal, schema_name);
            perm_check = true;
        } else if !name_parts.get(0).is_none() && self.get_object_owner(SecurableType::Catalog, &catalog_name).await.unwrap().owner == principal { // if princpal is the owner of the catalog then return True 
            log::info!("Princpal {} is an owner of {}. ", principal, catalog_name);
            perm_check = true;
        } 
        // if there are permissions on the object to review 
        else if let Some(assigns) = object_permissions.privilege_assignments {
            log::info!("Princpal {} not an owner of {} or any parent object. ", principal, full_name);
            for s in assigns {
                // we do not need to verify principal as we only get permissions for that principal
                // but do want to log the princpal we are mapping the current user to i.e. if they are part of a group
                if let Some(p) = &s.principal {
                    if let Some(pp) = s.privileges {
                        for value in pp {
                            // if the value is in the read_list vec then return TRUE
                            if permissions.contains(&value.as_str()) {
                                log::info!("Principal {} has {} permissions on {}.", p, value, s.object_name);
                                perm_check = true;
                            }
                        }
                    }
                }
            }
        }
        Ok(perm_check)
    }

    // /api/2.1/unity-catalog/permissions/{securable_type}/{full_name}
    pub async fn can_read(&self, full_name: &str, principal: &str) -> Result<bool, Error> {
        let readable_permissions = vec!["SELECT", "ALL_PRIVILEGES"];
        let securable_type: SecurableType = SecurableType::Table;

        // split full name and make 3 different api calls since permissions can be delagated 
        let name_parts: Vec<&str> = full_name.split('.').collect();
        let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog        
        let schema_name = match (name_parts.get(0), name_parts.get(1)) { // may not always be a schema
            (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
            _ => "".to_string(), // handle case where parts are missing
        };
        log::info!("Checking if {} can read the following objects: {} | {} | {}", principal, catalog_name, schema_name, full_name);

        let readable: bool = self.check_permissions(securable_type, full_name, principal, readable_permissions).await?; // deny by default

        Ok(readable)
    }


    pub async fn can_write(&self, full_name: &str, principal: &str) -> Result<bool, Error> {
        let writable_permissions: Vec<&str> = vec!["MODIFY", "ALL_PRIVILEGES"];
        let securable_type: SecurableType = SecurableType::Table;


        // split full name and make 3 different api calls since permissions can be delagated 
        let name_parts: Vec<&str> = full_name.split('.').collect();
        let catalog_name = name_parts.get(0).unwrap().trim_matches('"'); // always expect a catalog        
        let schema_name = match (name_parts.get(0), name_parts.get(1)) { // may not always be a schema
            (Some(part1), Some(part2)) => format!("{}.{}", part1, part2),
            _ => "".to_string(), // handle case where parts are missing
        };
        log::info!("Checking if {} can read the following objects: {} | {} | {}", principal, catalog_name, schema_name, full_name);

        let writable: bool = self.check_permissions(securable_type, full_name, principal, writable_permissions).await?; // deny by default

        Ok(writable)
    }
}


// wrapper struct to hold all permissions on an object 
#[derive(Debug, Deserialize, Clone)]
pub struct PrivilegeAssignmentsResponse {
    pub privilege_assignments: Option<Vec<PrivilegeAssignment>>,

}
impl PrivilegeAssignmentsResponse {
    // Constructor to create an empty PrivilegeAssignmentsResponse
    pub fn new() -> Self {
        PrivilegeAssignmentsResponse {
            privilege_assignments: Some(Vec::new()),
        }
    }

    // Method to extend privilege_assignments vector
    // object type needs to be "securable type"
    pub fn add_assignment(&mut self, privilege_assignments: PrivilegeAssignmentsResponse, object_name: &str, object_type: SecurableType) {
        if let Some(ref mut self_privs) = self.privilege_assignments {
            if let Some(mut privs) = privilege_assignments.privilege_assignments {
                for assignment in &mut privs {
                    assignment.object_name = object_name.to_string();
                    assignment.object_type = Some(object_type.clone());
                }
                self_privs.extend(privs);
            }
        }
    }
}

// struct to old ownership information 
#[derive(Debug, Deserialize, Clone)]
pub struct ObjectOwnerResponse {
    pub full_name: String,
    pub owner: String,
}


// objects for permissions 
#[derive(Debug, Deserialize, Clone)]
pub struct PrivilegeAssignment {
    #[serde(skip)]
    pub object_name: String,
    pub object_type: Option<SecurableType>,
    pub principal: Option<String>,
    pub privileges: Option<Vec<String>>,
}

// used to authenticate users 
#[derive(Debug, Deserialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct User {
    pub id: String,
    pub user_name: String, 
    pub display_name: String,
    pub active: bool,
}


#[derive(Debug, Deserialize, Clone)]
pub enum SecurableType {
    // https://docs.databricks.com/en/data-governance/unity-catalog/manage-privileges/privileges.html
    Catalog, // metastore ownership
    Schema, // catalog ownership
    Table, // schema ownership
    StorageCredential, // metastore ownership
    ExternalLocation, // metastore ownership 
    Function, // schema ownership 
    Share, // metastore ownership
    Provider, // metastore ownership
    Recipient, // metastore ownership
    Metastore, // account ownership - we can likely disregard as we are only working with single metastores
    Volume, // schema ownership 
    Connection, // federation - metastore ownership
}
impl std::str::FromStr for SecurableType {
    type Err = ();

    fn from_str(input: &str) -> Result<SecurableType, Self::Err> {
        match input {
            "catalog" => Ok(SecurableType::Catalog),
            "schema" => Ok(SecurableType::Schema),
            "table" => Ok(SecurableType::Table),
            "storage_credential" => Ok(SecurableType::StorageCredential),
            "external_location" => Ok(SecurableType::ExternalLocation),
            "function" => Ok(SecurableType::Function),
            "share" => Ok(SecurableType::Share),
            "provider" => Ok(SecurableType::Provider),
            "recipient" => Ok(SecurableType::Recipient),
            "metastore" => Ok(SecurableType::Metastore),
            "volume" => Ok(SecurableType::Volume),
            "connection" => Ok(SecurableType::Connection),
            _ => Err(()),
        }
    }
}
impl ToString for SecurableType {
    fn to_string(&self) -> String {
        match self {
            SecurableType::Catalog => "catalog".to_string(),
            SecurableType::Schema => "schema".to_string(),
            SecurableType::Table => "table".to_string(),
            SecurableType::StorageCredential => "storage_credential".to_string(),
            SecurableType::ExternalLocation => "external_location".to_string(),
            SecurableType::Function => "function".to_string(),
            SecurableType::Share => "share".to_string(),
            SecurableType::Provider => "provider".to_string(),
            SecurableType::Recipient => "recipient".to_string(),
            SecurableType::Metastore => "metastore".to_string(),
            SecurableType::Volume => "volume".to_string(),
            SecurableType::Connection => "connection".to_string(),
        }
    }
}




// https://delta-io.github.io/delta-rs/usage/loading-table/
// https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants
#[derive(Debug, Clone, Deserialize)]
pub struct AzureDataLakeGen2Options {
    azure_storage_account_name: String, 
    azure_client_id: String,
    azure_client_secret: String,
    azure_tenant_id: String,
}
impl AzureDataLakeGen2Options {
    pub fn new(azure_storage_account_name: String, azure_client_id: String, azure_client_secret: String, azure_tenant_id: String ) -> Self {
        let options: AzureDataLakeGen2Options = AzureDataLakeGen2Options {
            azure_storage_account_name,
            azure_client_id,
            azure_client_secret,
            azure_tenant_id
        };
        options
    }

    pub fn to_hash_map(&self) -> HashMap<String, String> {
        let mut map = HashMap::new();
        map.insert("azure_storage_account_name".to_string(), self.azure_storage_account_name.clone());
        map.insert("azure_client_id".to_string(), self.azure_client_id.clone());
        map.insert("azure_client_secret".to_string(), self.azure_client_secret.clone());
        map.insert("azure_tenant_id".to_string(), self.azure_tenant_id.clone());
        map
    }
}