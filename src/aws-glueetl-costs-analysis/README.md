# AWS Glue ETL Cost Analysis Tool

This tool allows you to **crawl AWS Glue ETL job cost data**, save job execution logs, and visualize cost metrics in scatter plots to help identify abnormal job costs. The tool fetches detailed information on Glue ETL job executions and calculates **Data Processing Unit (DPU)** costs over a user-defined period. It provides both log downloading and cost analysis functionalities, with optional command-line arguments for flexibility.

## Features

- **Job Run Data Crawling**: Download and store AWS Glue ETL job execution logs.
- **Cost Analysis**: Calculate DPU costs based on job execution time and plot them over time.
- **Data Visualization**: Generate scatter plots to visualize the DPU costs for each Glue job across different time intervals.
- **Command-line Interface**: Convenient options to download job logs and perform cost analysis using CLI.
- **Skip Option**: Users can skip downloading logs and analyze existing data files directly.

## Prerequisites

1. **AWS Credentials**: The tool uses `boto3` to interact with AWS Glue, so make sure your AWS credentials are properly configured. You can set them up by following the [Boto3 Configuration Guide](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/quickstart.html#configuration).
2. **AWS Permissions**: Ensure your AWS credentials have the necessary permissions to access Glue jobs and job runs.
3. **Python Dependencies**: Install the required Python libraries using `pip`:

    ```bash
    cd src/aws-glueetl-costs-analysis
    python3 -m pip install -r requirements.txt
    ```

## Usage

The tool can be used directly from the command line with several options:

```bash
python aws-glueetl-costs-analysis.py <start_date> <end_date> [OPTIONS]
```

#### Arguments

- **`<start_date>`**: The start date for the analysis period (format: `YYYY-MM-DD`).
- **`<end_date>`**: The end date for the analysis period (format: `YYYY-MM-DD`).
- **`-F` or `--costs-file`**: Specify the file to save or read costs data. Defaults to `./glueetl_costs.jsonl`.
- **`-K` or `--skip-download`**: Skip the download step if job logs are already available and only perform the cost analysis on the existing data.


### Examples

#### 1. Download Job Logs and Analyze Costs

To download job logs from AWS Glue and analyze the costs from `2023-11-01` to `2023-11-30`:

```bash
python aws-glueetl-costs-analysis.py 2023-11-01 2023-11-30
```

This command will:
- Fetch all Glue ETL job logs and store them in the default `glueetl_costs.jsonl` file.
- Perform cost analysis and display a scatter plot of DPU usage for each job run within the specified date range.

#### 2. Skip Downloading and Analyze Existing Logs

If you have already downloaded the logs and want to skip the download step, use the `--skip-download` option:

```bash
python aws-glueetl-costs-analysis.py 2023-11-01 2023-11-30 --skip-download -F ./my_glue_costs.jsonl
```

This will directly analyze the data in `my_glue_costs.jsonl` without downloading new logs.

## Output

The tool will generate an interactive scatter plot where:
- **X-axis**: Represents the end time of job executions (`EndTime`).
- **Y-axis**: Represents the DPU costs of each job run.
- **Color**: Different colors represent different job names, making it easier to identify high-cost jobs.

The scatter plot will help you visualize and identify jobs with abnormal DPU costs or unusually long execution times, facilitating cost optimization.

## How It Works

### Glue Job Log Download

The tool uses `boto3` to interact with AWS Glue and perform the following tasks:
1. **List all Glue jobs** using the `get_jobs()` API.
2. **Filter ETL jobs** (those using the `glueetl` command) from the list.
3. **Download job run data** for each ETL job using the `get_job_runs()` API.
4. **Save job run logs** to a file in JSON Lines format (`.jsonl`).

The `download_job_runs_log` function handles downloading and saving job logs.

### Cost Analysis

Once the logs are downloaded, the tool analyzes the job runs to calculate **DPU usage**:

```
DPUs = (ExecutionTime in hours) * MaxCapacity
```

Where:
- **ExecutionTime** is the job runtime in seconds.
- **MaxCapacity** is the maximum DPUs allocated to the job run.

The cost analysis function filters job runs by the specified date range, aggregates the data, and uses Plotly to generate an interactive scatter plot.
