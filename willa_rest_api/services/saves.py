import os
import json
import base64
import boto3
import time
from typing import Any, Dict, List, Optional, Tuple

# Configuration (override via env if needed)
REGION = os.getenv("AWS_REGION", "us-east-1")
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "willa_datalake")
ATHENA_WORKGROUP = os.getenv("ATHENA_WORKGROUP", "willa_datalake")

_session = boto3.session.Session(region_name=REGION)
_athena = _session.client("athena", region_name=REGION)


def _encode_next_token(last_created_at: str, last_id: str) -> str:
    payload = {"createdat": last_created_at, "id": last_id}
    return base64.urlsafe_b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")


def _decode_next_token(token: str) -> Optional[Tuple[str, str]]:
    try:
        raw = base64.urlsafe_b64decode(token.encode("utf-8")).decode("utf-8")
        data = json.loads(raw)
        return data.get("createdat"), data.get("id")
    except Exception:
        return None


def _run_athena_query(query: str) -> List[Dict[str, Any]]:
    start_kwargs: Dict[str, Any] = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": ATHENA_DATABASE},
        "WorkGroup": ATHENA_WORKGROUP,
    }
    resp = _athena.start_query_execution(**start_kwargs)
    qid = resp["QueryExecutionId"]

    # Wait for completion (simple polling)
    while True:
        info = _athena.get_query_execution(QueryExecutionId=qid)
        state = info["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(0.5)
    if state != "SUCCEEDED":
        reason = info["QueryExecution"]["Status"].get("StateChangeReason", "")
        raise RuntimeError(f"Athena query failed: {state} {reason}")

    results = _athena.get_query_results(QueryExecutionId=qid)
    rows = results.get("ResultSet", {}).get("Rows", [])
    if not rows:
        return []
    headers = [col.get("VarCharValue", f"col_{i}") for i, col in enumerate(rows[0].get("Data", []))]
    items: List[Dict[str, Any]] = []
    for row in rows[1:]:
        data_cells = row.get("Data", [])
        item = {headers[i]: cell.get("VarCharValue") for i, cell in enumerate(data_cells)}
        items.append(item)
    return items


def list_saves_service(limit: int = 20, next_token: Optional[str] = None) -> Dict[str, Any]:
    """
    Return saves from 'latest_entity_save' in descending order by createdat with keyset pagination.
    Pagination token encodes the last (createdat, id) tuple from the previous page.
    """
    # Sanitize limit
    if not isinstance(limit, int):
        try:
            limit = int(limit)  # type: ignore[arg-type]
        except Exception:
            limit = 20
    limit = max(1, min(limit, 100))

    where_clause = ""
    if next_token:
        decoded = _decode_next_token(next_token)
        if decoded and all(decoded):
            last_created_at, last_id = decoded
            # Keyset pagination: strictly less than previous last tuple in DESC order
            # Note: createdat and id are modeled as strings in the datalake dictionary.
            where_clause = (
                f"WHERE (createdat < '{last_created_at}') "
                f"OR (createdat = '{last_created_at}' AND id < '{last_id}')"
            )

    # Explicitly list columns to keep payload tight and ordered
    columns = [
        "id",
        "url",
        "title",
        "description",
        "comments",
        "image",
        "imagekey",
        "publisher",
        "boardids",
        "createdat",
        "updatedat",
        "username",
        "isarchived",
    ]
    sql = (
        f"SELECT {', '.join(columns)} "
        f"FROM latest_entity_save "
        f"{where_clause} "
        f"ORDER BY createdat DESC, id DESC "
        f"LIMIT {limit}"
    )

    items = _run_athena_query(sql)

    # Compute new next token if we got a full page
    new_next_token: Optional[str] = None
    if items:
        last = items[-1]
        last_created_at = last.get("createdat")
        last_id = last.get("id")
        if last_created_at and last_id and len(items) == limit:
            new_next_token = _encode_next_token(last_created_at, last_id)

    return {
        "items": items,
        "nextToken": new_next_token,
        "count": len(items),
        "limit": limit,
    }


def get_saves_count() -> int:
    """
    Return the total count of rows in 'latest_entity_save'.
    """
    sql = "SELECT COUNT(1) AS total FROM latest_entity_save"
    rows = _run_athena_query(sql)
    if not rows:
        return 0
    # Athena returns strings; coerce safely
    total_str = rows[0].get("total") or rows[0].get("count") or "0"
    try:
        return int(total_str)
    except Exception:
        return 0