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


def list_saves_service(limit: int = 20, offset: Optional[int] = 0) -> Dict[str, Any]:
    """
    Return saves from 'latest_entity_save' in descending order by createdat using limit/offset pagination.
    """
    # Sanitize limit
    if not isinstance(limit, int):
        try:
            limit = int(limit)  # type: ignore[arg-type]
        except Exception:
            limit = 20
    limit = max(1, min(limit, 100))

    # Sanitize offset
    if offset is None or not isinstance(offset, int):
        try:
            offset = int(offset)  # type: ignore[arg-type]
        except Exception:
            offset = 0
    offset = max(0, offset)

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
    # Athena does not support OFFSET directly; emulate with row_number() window
    select_cols = ", ".join(columns)
    order_clause = "createdat DESC, id DESC"
    start_row = offset
    end_row = offset + limit
    sql = (
        "WITH ordered AS ("
        f"  SELECT {select_cols}, "
        f"         row_number() OVER (ORDER BY {order_clause}) AS rn "
        f"  FROM latest_entity_save"
        ") "
        f"SELECT {select_cols} "
        "FROM ordered "
        f"WHERE rn > {start_row} AND rn <= {end_row} "
        "ORDER BY rn"
    )

    items = _run_athena_query(sql)

    return {
        "items": items,
        "count": len(items),
        "limit": limit,
        "offset": offset,
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