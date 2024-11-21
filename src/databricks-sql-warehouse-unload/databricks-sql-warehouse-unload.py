import concurrent
import functools
import itertools
import os
import shlex
import subprocess
import tempfile
import time
from collections import deque
from concurrent.futures import (
    ThreadPoolExecutor,
    ProcessPoolExecutor,
    FIRST_COMPLETED,
    wait,
)
from datetime import datetime
from enum import Enum
from io import BytesIO
from multiprocessing import cpu_count
from pathlib import Path
from typing import List, Tuple, Union

import pyarrow as pa
import pyarrow.parquet as pq
import redo
import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.sql import (
    Disposition,
    Format,
    StatementState,
    ExternalLink,
)
from loguru import logger
from requests.adapters import HTTPAdapter
from requests.exceptions import RequestException, ReadTimeout, ConnectTimeout

LOG = logger

KB = 1024
MB = KB * KB

v_cores = cpu_count()


class ExpiredFileLinkError(RequestException):
    """"""


class Suffix(str, Enum):
    csv = ".csv"
    parquet = ".parquet"
    arrow_stream = ".arrow_stream"
    json = ".json"


@redo.retriable(
    attempts=5,
    sleeptime=60,
    max_sleeptime=300,
    sleepscale=1,
    retry_exceptions=(TimeoutError, ReadTimeout, ConnectTimeout),
)
def get_external_links(
    wc: WorkspaceClient, statement_id, chunk_id
) -> List[ExternalLink]:
    try:
        statement = wc.statement_execution.get_statement_result_chunk_n(
            statement_id, chunk_id
        )
        return statement.external_links or []
    except (TimeoutError, ReadTimeout, ConnectTimeout) as e:
        LOG.warning(
            f"Get external_links by chunk index {chunk_id} timeout, retrying..."
        )
        raise e


@redo.retriable(
    attempts=5,
    sleeptime=10,
    max_sleeptime=300,
    sleepscale=1,
    retry_exceptions=(ReadTimeout,),
)
def _download_as_csv(file_url, savefile, http_session, timeout=30):
    """Query task writes data in 20 MB chunks using uncompressed CSV format
    ref: https://www.databricks.com/blog/2021/08/11/how-we-achieved-high-bandwidth-connectivity-with-bi-tools.html
    """
    try:
        response = http_session.get(file_url, timeout=timeout)
        if response.status_code == 403:
            raise ExpiredFileLinkError(f"File link {file_url} expired")
        if response.status_code != 200:
            raise RequestException(
                f"File link download error, {response.status_code}\t{response.content}"
            )
        with open(savefile, "wb") as bos:
            bos.write(response.content)
    except ReadTimeout as e:
        LOG.warning(f"Download result {file_url} timeout, retrying...")
        raise ReadTimeout(e)
    except Exception as e:
        LOG.error(f"Download result {file_url} error, {e}")
        if "Read timed out" in str(e):
            raise ReadTimeout(e)
        raise e


def convert_to_parquet(arrow_stream: bytes, savefile: str):
    input_stream = BytesIO(arrow_stream)
    with pa.ipc.open_stream(input_stream) as reader:
        with pq.ParquetWriter(savefile, reader.schema) as writer:
            table = pa.Table.from_batches(reader, reader.schema)
            writer.write_table(table)


@redo.retriable(
    attempts=5,
    sleeptime=10,
    max_sleeptime=300,
    sleepscale=1,
    retry_exceptions=(ReadTimeout,),
)
def _download_as_parquet(file_url, savefile, http_session, timeout=15):
    """Query task writes data in 2 MB chunks using LZ4 compressed Arrow streaming format"""
    try:
        response = http_session.get(file_url, timeout=timeout)
        if response.status_code == 403:
            raise ExpiredFileLinkError(f"File link {file_url} expired")
        if response.status_code != 200:
            raise RequestException(
                f"File link download error, {response.status_code}\t{response.content}"
            )
    except ReadTimeout as e:
        LOG.warning(f"Download result {file_url} timeout, retrying...")
        raise ReadTimeout(e)
    except Exception as e:
        LOG.error(f"Download result {file_url} error, {e}")
        if "Read timed out" in str(e):
            raise ReadTimeout(e)
        raise e

    basename, _ = os.path.splitext(savefile)
    savefile = basename + Suffix.parquet.value
    convert_to_parquet(arrow_stream=response.content, savefile=savefile)


@redo.retriable(
    attempts=5,
    sleeptime=15,
    max_sleeptime=300,
    sleepscale=1,
    retry_exceptions=(ExpiredFileLinkError,),
)
def download_external_files(
    chunk_id: int,
    statement_id,
    output_path: str,
    suffix: str,
    chunk_size=100 * MB,
    wc: WorkspaceClient = None,
    databricks_host=None,
    databricks_token=None,
    http_session=None,
):
    if wc is None:
        if not databricks_token or not databricks_host:
            raise ValueError(f"Cannot create client for Workspace")
        wc = WorkspaceClient(host=databricks_host, token=databricks_token)

    if http_session is None:
        http_session = requests.session()

    LOG.info(f"Get result chunk by index {chunk_id}")
    external_links = get_external_links(wc, statement_id, chunk_id)
    for link in external_links:
        file_url = link.external_link
        LOG.info(
            f"Downloading chunk {chunk_id}, {file_url!r} expire at {link.expiration}"
        )
        filename = f"results_{statement_id}_{chunk_id}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}"
        savefile = os.path.join(output_path, filename)
        if suffix == Suffix.csv:
            savefile += Suffix.csv.value
            download = _download_as_csv
        elif suffix == Suffix.arrow_stream:
            savefile += Suffix.parquet.value
            download = _download_as_parquet
        # elif suffix == Suffix.json:
        #     savefile += Suffix.json.value
        else:
            raise ValueError(f"Do not support download as {suffix}")

        # failed to download external_link file, suppose it expired and refresh
        try:
            download(file_url, savefile, http_session)
        except ReadTimeout as e:
            raise ExpiredFileLinkError(str(e))

        LOG.info(f"Downloaded sql result data into {savefile} from link {file_url}")
    return True


def threading_download(
    chunk_ids: Union[List[int], Tuple[int]],
    statement_id,
    output_path: str,
    chunk_size=100 * MB,
    wc: WorkspaceClient = None,
    databricks_host=None,
    databricks_token=None,
    **kwargs,
):
    if wc is None:
        if not databricks_token or not databricks_host:
            raise ValueError(f"Cannot create client for Workspace")
        wc = WorkspaceClient(host=databricks_host, token=databricks_token)

    pid = os.getpid()
    LOG.info(f"Started download process {pid} for chunk {chunk_ids}")

    session = requests.session()
    adapter = HTTPAdapter(pool_connections=100, pool_maxsize=100)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    download = functools.partial(
        download_external_files,
        statement_id=statement_id,
        output_path=output_path,
        suffix=kwargs.get("suffix", ".csv"),
        chunk_size=chunk_size,
        wc=wc,
        http_session=session,
    )
    max_workers = len(chunk_ids)
    with ThreadPoolExecutor(max_workers) as t_executor:
        futures = {
            t_executor.submit(download, chunk_id): chunk_id for chunk_id in chunk_ids
        }
        LOG.info(f"Waiting for download process {pid} to finish...")
        for future in concurrent.futures.as_completed(futures):
            chunk_id = futures[future]
            try:
                ok = future.result()
                bool(ok)
            except Exception as e:
                LOG.error(f"Download chunk index {chunk_id} failed: {e}, pid: {pid}")
                raise e
            else:
                LOG.info(f"Download chunk index {chunk_id} ok: {ok}, pid: {pid}")
    LOG.info(f"Download process {pid} finished")
    return True


def chunked(iterable, chunk_size):
    it = iter(iterable)
    while True:
        chunk = list(itertools.islice(it, chunk_size))
        if not chunk:
            return
        yield chunk


def parallel_download(
    wc: WorkspaceClient,
    statement_id,
    chunk_ids: List[int],
    output_path: str,
    chunk_size=100 * MB,
    processes: int = v_cores,
    threads: int = 8,
    wait_time=60,
    **kwargs,
):
    chunks = [tuple(chunk_ids) for chunk_ids in chunked(chunk_ids, threads)]
    max_workers = min(processes, len(chunks))
    if max_workers <= 0:
        max_workers = v_cores

    rq = dict()  # non-process-safe queue
    tq = deque()
    tq.extend(chunks)
    download = functools.partial(
        threading_download,
        statement_id=statement_id,
        output_path=output_path,
        chunk_size=chunk_size,
        databricks_host=wc.config.host,
        databricks_token=wc.config.token,
        suffix=kwargs.get("suffix", ".csv"),
    )
    with ProcessPoolExecutor(max_workers=max_workers) as p_executor:
        while len(tq) > 0:
            if len(rq) < max_workers:
                chunk_ids = tq.popleft()
                future = p_executor.submit(download, chunk_ids=chunk_ids)
                rq[future] = chunk_ids
                LOG.info(f"Batch downloading task submitted for chunk {chunk_ids}")
            else:
                done, not_done = wait(
                    rq, timeout=wait_time, return_when=FIRST_COMPLETED
                )
                if not done:
                    LOG.info(
                        f"Waiting for {wait_time}s for batch downloading task to finish..."
                    )
                    continue
                for future in done:
                    chunk_ids = rq[future]
                    try:
                        ok = future.result()
                        bool(ok)
                    except Exception as e:
                        LOG.error(
                            f"Batch downloading task generated an exception {e}, chunk: {chunk_ids}"
                        )
                        raise e
                    else:
                        del rq[future]
                        LOG.info(
                            f"Batch downloading task completed and removed from queue, chunk: {chunk_ids}"
                        )


def unload(
    query: str,
    output_path: str,
    wc: WorkspaceClient,
    warehouse_id,
    catalog=None,
    schema=None,
    result_format: Format = Format.ARROW_STREAM,
    parallel_downloading: bool = True,
    parallel_processes: int = v_cores,
    parallel_threads: int = 16,
    parallel_wait_time: int = 60,
):
    if result_format not in (Format.ARROW_STREAM, Format.CSV):
        raise ValueError(
            f"{result_format} is not supported as the format of the databricks sql result data "
        )

    if parallel_threads == 1 and parallel_processes == 1:
        parallel_downloading = False

    if output_path.startswith("s3://"):
        local_path = tempfile.mkdtemp(prefix="databricks_sql_warehouse_results")
        is_upload_s3 = True
    else:
        local_path = output_path
        Path(local_path).mkdir(parents=True, exist_ok=True)
        is_upload_s3 = False

    LOG.info(
        f"Unloading {query!r} from sql warehouse {warehouse_id!r} and catalog {catalog}.{schema or ''} into {output_path}"
    )
    statement = wc.statement_execution.execute_statement(
        warehouse_id=warehouse_id,
        catalog=catalog,
        schema=schema,
        statement=query,
        disposition=Disposition.EXTERNAL_LINKS,
        format=result_format,
    )
    LOG.info(f"Statement execution: {statement.as_dict()}")
    while statement.status.state in (StatementState.RUNNING, StatementState.PENDING):
        LOG.info(f"Wait for statement to complete, 5s...")
        time.sleep(5)
        statement = wc.statement_execution.get_statement(statement.statement_id)
        LOG.info(f"Refreshing statement info: {statement.as_dict()}")

    if statement.status.state != StatementState.SUCCEEDED:
        raise DatabricksError(f"Execute statement error, {statement.as_dict()}")

    result_suffix = "." + result_format.value.lower()
    if not statement.manifest.chunks:
        LOG.info(f"No records found")
        return

    chunk_ids = [chunk.chunk_index for chunk in statement.manifest.chunks]
    if parallel_downloading:
        parallel_download(
            wc,
            statement.statement_id,
            chunk_ids,
            output_path=local_path,
            suffix=result_suffix,
            processes=parallel_processes,
            threads=parallel_threads,
            wait_time=parallel_wait_time,
        )
    else:
        for chunk_id in chunk_ids:
            download_external_files(
                chunk_id,
                statement.statement_id,
                output_path=local_path,
                suffix=result_suffix,
                wc=wc,
            )
    LOG.success(f"Databricks unload successfully")

    if is_upload_s3:
        LOG.info(f"Unloading databricks sql results data to {output_path}")
        cmd = [
            "aws",
            "s3",
            "cp",
            "--recursive",
            shlex.quote(local_path),
            shlex.quote(output_path),
        ]
        cmd = " ".join(cmd)
        ret = subprocess.run(cmd, shell=True, text=True)
        if ret.returncode != 0:
            raise RuntimeError(f"Upload s3 failed")
        LOG.success(f"S3 upload successfully")


DATABRICKS_HOST = os.environ.get("DATABRICKS_HOST", "")
DATABRICKS_TOKEN = os.environ.get("DATABRICKS_TOKEN", "")


def arg_parse():
    import argparse

    support_formats = ["csv", "parquet"]
    parser = argparse.ArgumentParser()
    opt = parser.add_argument
    opt("query")
    opt("output_path")
    opt("-W", "--warehouse_id", required=True, help="databricks sql warehouse id")
    opt(
        "-F",
        "--result_format",
        choices=support_formats,
        default="parquet",
        help=f"query results data format, support: {support_formats}, (default: %(default)s)",
    )
    opt("-C", "--catalog", help="catalog of databricks sql warehouse")
    opt("-S", "--schema", help="schema of databricks sql warehouse ")
    opt(
        "-P",
        "--parallel_downloading",
        action="store_true",
        default=False,
        help="enable parallel downloading",
    )
    opt(
        "--host",
        default=DATABRICKS_HOST,
        help="databricks host, default use environment variable 'DATABRICKS_HOST'",
    )
    opt(
        "--token",
        default=DATABRICKS_TOKEN,
        help="databricks token, default use environment variable 'DATABRICKS_TOKEN'",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    args = arg_parse()

    if args.result_format == "parquet":
        result_format = Format.ARROW_STREAM
    else:
        result_format = Format.CSV

    if not args.host or not args.token:
        raise NotFound("databricks host or token")

    wc = WorkspaceClient(host=args.host, token=args.token)
    LOG.info(f"Connected to databricks instance {args.host}")

    unload(
        query=args.query,
        output_path=args.output_path,
        wc=wc,
        warehouse_id=args.warehouse_id,
        catalog=args.catalog,
        schema=args.schema,
        result_format=result_format,
        parallel_downloading=args.parallel_downloading,
    )
