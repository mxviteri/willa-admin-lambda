import json
from willa_rest_api.services.boards import list_boards_service, get_boards_count


def list_boards_controller(event: dict):
    """List boards controller with limit/offset pagination."""
    params = (event or {}).get("queryStringParameters") or {}
    limit_raw = params.get("limit")
    offset_raw = params.get("offset")
    try:
        limit = int(limit_raw) if limit_raw is not None else 20
    except Exception:
        limit = 20
    try:
        offset = int(offset_raw) if offset_raw is not None else 0
    except Exception:
        offset = 0
    if offset < 0:
        offset = 0

    result = list_boards_service(limit=limit, offset=offset)
    total_count = get_boards_count()
    result["totalCount"] = total_count
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "*",
            "Access-Control-Allow-Methods": "GET,OPTIONS",
        },
        "body": json.dumps(result),
    }


