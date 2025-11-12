import json
from willa_rest_api.services.users import list_users_service


def list_users_controller(event: dict):
    """List Cognito users with pagination token and limit."""
    params = (event or {}).get("queryStringParameters") or {}
    limit_raw = params.get("limit")
    next_token = params.get("nextToken") or params.get("paginationToken")
    try:
        limit = int(limit_raw) if limit_raw is not None else 20
    except Exception:
        limit = 20

    result = list_users_service(limit=limit, pagination_token=next_token)
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


