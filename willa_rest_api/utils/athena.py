import os
import time
from typing import Any, Dict, List, Optional

import boto3

# Defaults can be overridden via kwargs or environment variables
DEFAULT_REGION = os.getenv("AWS_REGION", "us-east-1")
DEFAULT_DATABASE = os.getenv("ATHENA_DATABASE", "willa_datalake")
DEFAULT_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "willa_datalake")


def get_athena_client(region: Optional[str] = None) -> Any:
    session = boto3.session.Session(region_name=region or DEFAULT_REGION)
    return session.client("athena", region_name=region or DEFAULT_REGION)


def run_athena_query(
    query: str,
    *,
    database: Optional[str] = None,
    workgroup: Optional[str] = None,
    region: Optional[str] = None,
    client: Optional[Any] = None,
    poll_interval_s: float = 0.5,
    max_wait_s: Optional[float] = None,
) -> List[Dict[str, Any]]:
    """
    Execute an Athena query and return results as a list of dicts.
    - database/workgroup/region override env defaults if provided.
    - client can be passed to reuse an existing boto3 athena client.
    - poll_interval_s controls query status polling cadence.
    - max_wait_s optionally caps total wait time before raising TimeoutError.
    """
    athena = client or get_athena_client(region)

    start_kwargs: Dict[str, Any] = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": database or DEFAULT_DATABASE},
        "WorkGroup": workgroup or DEFAULT_WORKGROUP,
    }
    start_resp = athena.start_query_execution(**start_kwargs)
    qid = start_resp["QueryExecutionId"]

    start_time = time.time()
    while True:
        info = athena.get_query_execution(QueryExecutionId=qid)
        state = info["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        if max_wait_s is not None and (time.time() - start_time) > max_wait_s:
            raise TimeoutError(f"Athena query timed out after {max_wait_s} seconds")
        time.sleep(poll_interval_s)
    if state != "SUCCEEDED":
        reason = info["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query failed: {state} {reason}")

    # Fetch paginated results (if any)
    items: List[Dict[str, Any]] = []
    next_token: Optional[str] = None
    headers: Optional[List[str]] = None
    while True:
        results_kwargs: Dict[str, Any] = {"QueryExecutionId": qid}
        if next_token:
            results_kwargs["NextToken"] = next_token
        results = athena.get_query_results(**results_kwargs)
        rows = results.get("ResultSet", {}).get("Rows", [])
        if not rows:
            break
        if headers is None:
            headers = [col.get("VarCharValue", f"col_{i}") for i, col in enumerate(rows[0].get("Data", []))]
            data_rows = rows[1:]
        else:
            data_rows = rows
        for row in data_rows:
            data_cells = row.get("Data", [])
            item = {headers[i]: cell.get("VarCharValue") for i, cell in enumerate(data_cells)}
            items.append(item)
        next_token = results.get("NextToken")
        if not next_token:
            break
    return items


