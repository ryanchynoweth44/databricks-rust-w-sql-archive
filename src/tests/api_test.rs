use crate::{fetch_catalogs, Catalog};
use serde_json::json;

#[tokio::test]
async fn test_fetch_catalogs() {

    // Assert that the result is as expected
    assert!(result.is_ok());
    let catalogs = result.unwrap();
    // assert_eq!(catalogs, expected_catalogs);
    assert_ne!(some_variable, std::ptr::null()); // This will assert that some_variable is not null

}
