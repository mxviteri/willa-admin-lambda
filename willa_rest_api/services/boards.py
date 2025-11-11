from willa_rest_api.utils.athena import run_athena_query

def list_boards_service(limit: int = 20, offset: int = 0):
    """
    Return boards from 'latest_entity_board' in descending order by createdat using limit/offset pagination.
    Uses row_number window to emulate OFFSET.
    """
    # Sanitize inputs
    try:
        limit = int(limit)
    except Exception:
        limit = 20
    limit = max(1, min(limit, 100))
    try:
        offset = int(offset)
    except Exception:
        offset = 0
    offset = max(0, offset)

    # Prefer explicit columns if known; safely default to all columns
    select_cols = "*"
    order_clause = "createdat DESC, id DESC"
    start_row = offset
    end_row = offset + limit
    sql = (
        "WITH ordered AS ("
        f"  SELECT {select_cols}, "
        f"         row_number() OVER (ORDER BY {order_clause}) AS rn "
        f"  FROM latest_entity_board"
        ") "
        "SELECT * "
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

def get_boards_count() -> int:
    """
    Return the total count of rows in 'latest_entity_board'.
    """
    sql = "SELECT COUNT(1) AS total FROM latest_entity_board"
    rows = run_athena_query(sql)
    if not rows:
        return 0
    # Athena returns strings; coerce safely
    total_str = rows[0].get("total") or rows[0].get("count") or "0"
    try:
        return int(total_str)
    except Exception:
        return 0