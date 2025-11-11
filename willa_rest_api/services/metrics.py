from typing import Dict, List
from datetime import datetime, timedelta, timezone

from willa_rest_api.utils.athena import run_athena_query


def get_general_metrics() -> Dict[str, int]:
    """
    Return total counts for latest_entity_save, latest_entity_board, latest_entity_edge
    in a single Athena query.
    """
    sql = (
        "SELECT s.total_saves, b.total_boards, e.total_edges "
        "FROM (SELECT COUNT(1) AS total_saves FROM latest_entity_save) s "
        "CROSS JOIN (SELECT COUNT(1) AS total_boards FROM latest_entity_board) b "
        "CROSS JOIN (SELECT COUNT(1) AS total_edges FROM latest_entity_edge) e"
    )
    rows = run_athena_query(sql)
    if not rows:
        return {"total_saves": 0, "total_boards": 0, "total_edges": 0}
    row = rows[0]
    def to_int(value: object) -> int:
        try:
            return int(value)  # type: ignore[arg-type]
        except Exception:
            return 0
    return {
        "total_saves": to_int(row.get("total_saves")),
        "total_boards": to_int(row.get("total_boards")),
        "total_edges": to_int(row.get("total_edges")),
    }


def get_time_series_metrics(days: int = 30) -> Dict[str, list]:
    """
    Return day-by-day counts for the last `days` days for saves, boards, and edges.
    Uses createdat timestamp, truncated to day.
    Output example:
      {
        "saves":  [ { "day": "2025-10-01", "total_saves": 23 }, ... ],
        "boards": [ { "day": "2025-10-01", "total_boards": 5 }, ... ],
        "edges":  [ { "day": "2025-10-01", "total_edges": 12 }, ... ]
      }
    """
    try:
        days = int(days)
    except Exception:
        days = 30
    days = max(1, min(days, 365))

    def normalize_day(v: object) -> str:
        s = str(v or "")
        # Athena returns 'YYYY-MM-DD HH:MM:SS.SSS' → slice date
        return s[:10] if len(s) >= 10 else s

    # Build complete day range to ensure zero-filled results
    now = datetime.now(timezone.utc)
    # Oldest first: [D-(days-1), ..., D]
    day_keys: List[str] = []
    for i in range(days):
      d = now - timedelta(days=(days - 1 - i))
      day_keys.append(d.strftime('%Y-%m-%d'))

    # Saves
    sql_saves = (
        "SELECT date_trunc('day', from_iso8601_timestamp(createdat)) AS day, "
        "       COUNT(1) AS total_saves "
        "FROM latest_entity_save "
        f"WHERE from_iso8601_timestamp(createdat) >= date_add('day', -{days}, current_timestamp) "
        "GROUP BY 1 "
        "ORDER BY 1"
    )
    rows_saves = run_athena_query(sql_saves) or []
    saves_map = {normalize_day(r.get("day")): int(r.get("total_saves") or 0) for r in rows_saves}
    saves_series = [{"day": dk, "total_saves": int(saves_map.get(dk, 0))} for dk in day_keys]

    # Boards
    sql_boards = (
        "SELECT date_trunc('day', from_iso8601_timestamp(createdat)) AS day, "
        "       COUNT(1) AS total_boards "
        "FROM latest_entity_board "
        f"WHERE from_iso8601_timestamp(createdat) >= date_add('day', -{days}, current_timestamp) "
        "GROUP BY 1 "
        "ORDER BY 1"
    )
    rows_boards = run_athena_query(sql_boards) or []
    boards_map = {normalize_day(r.get("day")): int(r.get("total_boards") or 0) for r in rows_boards}
    boards_series = [{"day": dk, "total_boards": int(boards_map.get(dk, 0))} for dk in day_keys]

    # Edges
    sql_edges = (
        "SELECT date_trunc('day', from_iso8601_timestamp(createdat)) AS day, "
        "       COUNT(1) AS total_edges "
        "FROM latest_entity_edge "
        f"WHERE from_iso8601_timestamp(createdat) >= date_add('day', -{days}, current_timestamp) "
        "GROUP BY 1 "
        "ORDER BY 1"
    )
    rows_edges = run_athena_query(sql_edges) or []
    edges_map = {normalize_day(r.get("day")): int(r.get("total_edges") or 0) for r in rows_edges}
    edges_series = [{"day": dk, "total_edges": int(edges_map.get(dk, 0))} for dk in day_keys]

    return {
        "saves": saves_series,
        "boards": boards_series,
        "edges": edges_series,
    }


