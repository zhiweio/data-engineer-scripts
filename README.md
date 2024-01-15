# data-engineer-scripts

Simple scripts and tools for data engineer

| Script                                                                         | Description                                                                                                    | requirements                                                                                          |
|:-------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| [aws-glueetl-costs-analysis.py](./src/aws-glueetl-costs-analysis.py)           | Crawl the cost data of AWS Glue ETL jobs, create scatter plots, and help find jobs with abnormal costs.        | boto3==1.34.19<br/> pandas==2.1.4<br/> plotly==5.18.0                                                 |
| [databricks-sql-warehouse-unload.py](./src/databricks-sql-warehouse-unload.py) | Unload data from databricks sql warehouse, save to local file or AWS S3, results format support `parquet/csv`. | databricks-sdk==0.17.0<br/> loguru==0.7.2<br/> pyarrow==14.0.2<br/> requests==2.31.0<br/> redo==2.0.4 |
