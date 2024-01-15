import argparse
import json
import os.path
from datetime import datetime
from typing import List

import boto3
import pandas as pd
import plotly.express as px

glue_client = boto3.client("glue")


def get_all_job_runs(job_name):
    res = glue_client.get_job_runs(JobName=job_name, MaxResults=200)
    job_runs: List = res["JobRuns"]
    next_token = res.get("NextToken")
    while next_token:
        res = glue_client.get_job_runs(
            JobName=job_name, MaxResults=200, NextToken=next_token
        )
        next_token = res.get("NextToken")
        job_runs.extend(res["JobRuns"])
    return job_runs


def filter_etl(jobs):
    etl = []
    for job in jobs:
        if job["Command"]["Name"] == "glueetl":
            etl.append(job["Name"])
    return etl


def get_etl_jobs():
    etl_jobs = []
    res = glue_client.get_jobs(MaxResults=1000)
    jobs = res["Jobs"]
    etl_jobs += filter_etl(jobs)
    next_token = res.get("NextToken")
    while next_token:
        res = glue_client.get_jobs(MaxResults=1000)
        next_token = res.get("NextToken")
        jobs = res["Jobs"]
        etl_jobs += filter_etl(jobs)
    return etl_jobs


def download_job_runs_log(costs_file):
    glue_jobs = get_etl_jobs()
    with open(costs_file, "w", encoding="utf8") as f:
        for job in glue_jobs:
            job_runs = get_all_job_runs(job)
            if not job_runs:
                continue
            print(f"Job Runs for {job}")
            for run in job_runs:
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
    print(f"data saved to {costs_file}")


def analysis_job_costs(costs_file, start_date: str, end_date: str):
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
        labels={"EndTime": "End time（Y-M-D Hour）", "MaxCapacity": "DPU Costs"},
        hover_data=["JobName"],
        category_orders={"EndTime": sorted(agg_data["EndTime"].unique())},
    )
    fig.show()


default_costs_file = "./glueetl_costs.jsonl"

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    opts = parser.add_argument
    opts("start_date", type=str, help="start date, eg: 2023-11-01")
    opts("end_date", type=str, help="end date, eg: 2023-11-30")
    opts(
        "--costs-file",
        type=str,
        default=default_costs_file,
        help="file to save costs data",
        metavar=default_costs_file,
    )
    args = parser.parse_args()

    download_job_runs_log(args.costs_file)
    analysis_job_costs(args.costs_file, args.start_date, args.end_date)
