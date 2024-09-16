# Databricks SQL Warehouse Unload Tool

The **Databricks SQL Warehouse Unload Tool** enables users to unload query results from Databricks SQL Warehouse to local files or AWS S3. It supports exporting query results in `parquet` or `csv` format, with options for parallel downloading to optimize performance when working with large datasets.

## Features

- **Unload Databricks SQL Data**: Execute SQL queries on Databricks SQL Warehouse and export the results.
- **Support for Multiple Formats**: Output data in `parquet` or `csv` format.
- **Local or AWS S3 Export**: Save query results to a local file or directly upload to an S3 bucket.
- **Parallel Downloading**: Enable parallel downloads to speed up the process of retrieving large datasets.
- **Resilient Downloads**: Automatically retries failed downloads due to timeouts or expired links.

## Requirements

- **Python 3.9+**
- **Databricks SDK**: Install the Databricks SDK for Python.
- **AWS CLI**: Required if uploading data to S3.

### Python Dependencies

You can install the required Python dependencies with:

```bash
cd src/databricks-sql-warehouse-unload/
pip install -r requirements.txt
```

## Prerequisites

1. **Databricks Host and Token**: You need to have a valid Databricks host URL and authentication token. These can either be provided through command-line arguments or set as environment variables (`DATABRICKS_HOST` and `DATABRICKS_TOKEN`).

2. **AWS Credentials** (Optional): If uploading data to AWS S3, ensure that you have AWS credentials configured for use with the AWS CLI. More information on how to configure the AWS CLI can be found [here](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

3. **Databricks SQL Warehouse**: A valid Databricks SQL Warehouse ID to execute the query.


## Usage

The tool provides a command-line interface to execute queries and unload results to either local storage or AWS S3. Below are the available options and how to use the tool.

### Command-Line Arguments

- **Required Arguments**:
  - `query`: The SQL query to be executed on the Databricks SQL Warehouse.
  - `output_path`: The local directory or S3 bucket where the results will be saved.
  - `-W, --warehouse_id`: The ID of the Databricks SQL Warehouse to run the query.

- **Optional Arguments**:
  - `-F, --result_format`: Specify the output format for query results, either `csv` or `parquet` (default: `parquet`).
  - `-C, --catalog`: Catalog for the Databricks SQL Warehouse.
  - `-S, --schema`: Schema for the Databricks SQL Warehouse.
  - `-P, --parallel_downloading`: Enable parallel downloading of result chunks for faster retrieval (default: disabled).
  - `--host`: The Databricks host URL (default: from environment variable `DATABRICKS_HOST`).
  - `--token`: The Databricks token (default: from environment variable `DATABRICKS_TOKEN`).

### Example

#### Unloading Data to Local Directory

To run a SQL query and save the results in `parquet` format to a local directory:

```bash
python databricks-sql-warehouse-unload.py \
  "SELECT * FROM my_table" \
  /path/to/output/directory \
  -W <warehouse_id> \
  -F parquet \
  --host <databricks_host> \
  --token <databricks_token>
```

#### Unloading Data to AWS S3

To unload query results to an S3 bucket:

```bash
python databricks-sql-warehouse-unload.py \
  "SELECT * FROM my_table" \
  s3://my-s3-bucket/path/to/save/ \
  -W <warehouse_id> \
  -F csv \
  --host <databricks_host> \
  --token <databricks_token>
```

This will save the query results directly to the specified S3 bucket.

#### Enabling Parallel Downloading

If the query results are large, you can enable parallel downloading for faster data retrieval by adding the `--parallel_downloading` option:

```bash
python databricks-sql-warehouse-unload.py \
  "SELECT * FROM large_table" \
  /path/to/output/directory \
  -W <warehouse_id> \
  -P \
  --host <databricks_host> \
  --token <databricks_token>
```

### AWS S3 Upload Process

If the `output_path` starts with `s3://`, the tool will first download the results to a local temporary directory and then upload them to the specified S3 bucket using the AWS CLI.

## Code Overview

### Main Functions

- **`unload()`**: Executes the SQL query, retrieves the results, and saves them locally or uploads them to S3.
- **`parallel_download()`**: Handles parallel downloading of query result chunks using multiple threads and processes.
- **`download_external_files()`**: Downloads individual result chunks from Databricks, either as `csv` or `parquet`.
- **`convert_to_parquet()`**: Converts Arrow format results to Parquet format for storage.

### Retry Mechanism

The tool employs a retry mechanism (`redo.retriable`) to handle temporary errors such as timeouts or expired file links during downloads. This ensures that large result sets are downloaded reliably even in cases of network instability.

## Logging

This tool uses `loguru` for logging. Logs are printed to the console to provide real-time updates on:
- Query execution progress
- File downloads
- Errors encountered during downloads or uploads

## Troubleshooting

### Missing Databricks Host or Token

Ensure you have provided the correct Databricks host and token via either:
- Command-line arguments (`--host` and `--token`), or
- Environment variables (`DATABRICKS_HOST` and `DATABRICKS_TOKEN`).

### Failed to Upload to S3

If the upload to S3 fails, check that your AWS CLI is properly configured and that your credentials have the necessary permissions to upload to the specified bucket.

### Download Timeouts

If you encounter frequent timeouts during downloads, consider:
- Increasing the `timeout` value in the code.
- Reducing the number of parallel downloads if network bandwidth is limited.
