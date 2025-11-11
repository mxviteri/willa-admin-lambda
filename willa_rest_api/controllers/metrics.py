import json
from willa_rest_api.services.metrics import get_general_metrics


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


