use reqwest::{header::HeaderMap, Response, Error};
use log;

#[derive(Clone)]
pub struct APIClient {
    pub db_token: String,
    pub workspace_name: String,
}

impl APIClient {

    pub async fn fetch(&self, url: &str) -> Result<Response, Error> {
        let client: reqwest::Client = reqwest::Client::new();
        let mut headers: HeaderMap = HeaderMap::new();
        headers.insert("content-type", "application/json".parse().unwrap());
        headers.insert("Authorization", format!("Bearer {}", &self.db_token).parse().unwrap());
        

        let response: Response = client.get(url)
        .headers(headers.clone())
        .send()
        .await?;
    


        // Check if the response status code is not 200
        if !response.status().is_success() {
            // Log an error message
            let error_response: Response = client.get(url)
            .headers(headers.clone())
            .send()
            .await?;

            let resp_text = error_response.text().await?;
            log::error!("Request to {} failed with status code: {} - {}", url, response.status(), resp_text);
        }
       // https://adb-984752964297111.11.azuredatabricks.net/api/2.1/unity-catalog/permissions/schema/rac_demo_catalog.productcopy_demo?principal=ryan.chynoweth@databricks.com
    //    https://adb-984752964297111.11.azuredatabricks.net/api/2.1/unity-catalog/permissions/schema/main.abs_dev?principal=ryan.chynoweth@databricks.com

        Ok(response)
    }
}