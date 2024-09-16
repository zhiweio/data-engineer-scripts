import argparse
import json
import os
from datetime import datetime
from typing import Generator, List

import boto3
import pandas as pd
import plotly.express as px


class GlueClient:
    def __init__(self):
        self.client = boto3.client("glue")

    def get_jobs(self) -> Generator[dict, None, None]:
        """Generator to yield Glue jobs."""
        res = self.client.get_jobs(MaxResults=1000)
        jobs = res.get("Jobs", [])
        yield from jobs
        next_token = res.get("NextToken")
        while next_token:
            res = self.client.get_jobs(MaxResults=1000, NextToken=next_token)
            jobs = res.get("Jobs", [])
            yield from jobs
            next_token = res.get("NextToken")

    def get_all_job_runs(self, job_name: str) -> Generator[dict, None, None]:
        """Generator to yield all job runs for a given job name."""
        res = self.client.get_job_runs(JobName=job_name, MaxResults=200)
        job_runs = res.get("JobRuns", [])
        yield from job_runs
        next_token = res.get("NextToken")
        while next_token:
            res = self.client.get_job_runs(
                JobName=job_name, MaxResults=200, NextToken=next_token
            )
            job_runs = res.get("JobRuns", [])
            yield from job_runs
            next_token = res.get("NextToken")

    def filter_etl_jobs(self, jobs: Generator[dict, None, None]) -> List[str]:
        """Filter ETL jobs."""
        return [job["Name"] for job in jobs if job["Command"]["Name"] == "glueetl"]


def download_job_runs_log(glue_client: GlueClient, costs_file: str):
    """Download and save job runs log to a file."""
    etl_jobs = glue_client.filter_etl_jobs(glue_client.get_jobs())
    with open(costs_file, "w", encoding="utf8") as f:
        for job in etl_jobs:
            for run in glue_client.get_all_job_runs(job):
                stat = (
                    "Id: {Id}\tStartedOn: {StartedOn}\tCompletedOn: {CompletedOn}\t"
                    "ExecutionTime: {ExecutionTime}\tMaxCapacity: {MaxCapacity}\t"
                    "WorkerType: {WorkerType}\tNumberOfWorkers: {NumberOfWorkers}"
                ).format(
                    Id=run["Id"],
                    StartedOn=run["StartedOn"],
                    CompletedOn=run["CompletedOn"],
                    ExecutionTime=run["ExecutionTime"],
                    MaxCapacity=run.get("MaxCapacity"),
                    WorkerType=run.get("WorkerType"),
                    NumberOfWorkers=run.get("NumberOfWorkers"),
                )
                print(stat)
                f.write(json.dumps(run, ensure_ascii=False, default=str) + "\n")
    print(f"Data saved to {costs_file}")


def analysis_job_costs(costs_file, start_date: str, end_date: str):
    """Analyze Glue job costs based on DPUs usage."""
    if not os.path.exists(costs_file):
        raise FileNotFoundError(costs_file)

    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

    df = pd.read_json(costs_file, lines=True)
    df["StartedOn"] = pd.to_datetime(df["StartedOn"], format="mixed")
    mask = (df["StartedOn"].dt.date >= start_date) & (
        df["StartedOn"].dt.date <= end_date
    )
    df = df[mask]

    df["CompletedOn"] = pd.to_datetime(df["CompletedOn"], format="mixed")
    df["DPUs"] = df["ExecutionTime"] / 60 / 60 * df["MaxCapacity"]
    df["EndTime"] = df["CompletedOn"].dt.strftime("%Y-%m-%d %H")
    agg_data = df.groupby(["JobName", "EndTime"])["DPUs"].sum().reset_index()

    fig = px.scatter(
        agg_data,
        x="EndTime",
        y="DPUs",
        color="JobName",
        title="Daily Glue Job DPUs Costs",
        labels={"EndTime": "End time（Y-M-D Hour）", "DPUs": "DPU Costs"},
        hover_data=["JobName"],
        category_orders={"EndTime": sorted(agg_data["EndTime"].unique())},
    )
    fig.show()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    opt = parser.add_argument
    opt("start_date", type=str, help="Start date, e.g., 2023-11-01")
    opt("end_date", type=str, help="End date, e.g., 2023-11-30")
    opt(
        "-F",
        "--costs-file",
        type=str,
        default="./glueetl_costs.jsonl",
        help="File to save costs data",
    )
    opt(
        "-K",
        "--skip-download",
        action="store_true",
        help="Skip downloading job logs and only analyze",
    )

    args = parser.parse_args()

    glue_client = GlueClient()

    if not args.skip_download:
        download_job_runs_log(glue_client, args.costs_file)

    analysis_job_costs(args.costs_file, args.start_date, args.end_date)
