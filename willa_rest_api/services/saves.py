import json
import base64
from typing import Any, Dict, List, Optional, Tuple
from willa_rest_api.utils.athena import run_athena_query

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

    items = run_athena_query(sql)

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
    rows = run_athena_query(sql)
    if not rows:
        return 0
    # Athena returns strings; coerce safely
    total_str = rows[0].get("total") or rows[0].get("count") or "0"
    try:
        return int(total_str)
    except Exception:
        return 0