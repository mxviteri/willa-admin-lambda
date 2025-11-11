from willa_rest_api.utils.athena import run_athena_query

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