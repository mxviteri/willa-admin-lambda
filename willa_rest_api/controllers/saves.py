import json
from willa_rest_api.services.saves import list_saves_service, get_saves_count


def list_saves_controller(event: dict):
    """List saves controller."""
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

    result = list_saves_service(limit=limit, offset=offset)
    # Augment with overall total count for numeric pagination
    total_count = get_saves_count()
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