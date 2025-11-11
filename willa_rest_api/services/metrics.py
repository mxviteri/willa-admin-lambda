from typing import Dict

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


