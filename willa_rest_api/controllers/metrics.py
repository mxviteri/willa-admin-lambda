import json
from willa_rest_api.services.metrics import get_general_metrics, get_time_series_metrics


def get_general_metrics_controller(event: dict):
    """Controller returning general metrics counts."""
    result = get_general_metrics()
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


def get_time_series_metrics_controller(event: dict):
    """Controller returning time series counts for saves, boards, edges."""
    params = (event or {}).get("queryStringParameters") or {}
    days_raw = params.get("days")
    try:
        days = int(days_raw) if days_raw is not None else 30
    except Exception:
        days = 30
    result = get_time_series_metrics(days=days)
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


