# Databricks - Delta Lake - Rust

This repository is purposed for running rust workloads against Databricks Datasets. It will include the collection of data from the Databricks [REST API](https://docs.databricks.com/api/workspace/catalogs/list), reading data directly from storage using the Unity Catalog APIs for metadata, and a UI for exploring datasets. My hope is to integrate Rust and Polars/Spark with Databricks datasets. 


## Resources: 
- [Install and Learn Rust](https://www.rust-lang.org/learn)
- [Rust Book](https://doc.rust-lang.org/book/)
- VS Code Extension - [rust-analyzer](https://rust-analyzer.github.io/)


deltalake = { version = "0.17.3", features = ["azure"]}
https://github.com/delta-io/delta-rs/issues/392