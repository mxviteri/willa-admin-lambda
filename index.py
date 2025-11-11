import json
import os
import boto3
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from willa_rest_api.controllers.saves import list_saves_controller, get_save_by_id_controller
from willa_rest_api.controllers.metrics import get_general_metrics_controller
from willa_rest_api.controllers.boards import list_boards_controller
from willa_admin_agent.agent import call_agent

LAMBDA_CLIENT = boto3.client("lambda")
WS_MANAGEMENT_BASE = "https://eqqrx1ycgl.execute-api.us-east-1.amazonaws.com/prod"

def handler(event, context):
    try:
        # Async task handler (self-invoked)
        if isinstance(event, dict) and event.get("asyncTask") == "chat":
            return handle_async_chat(event)

        # Detect API Gateway WebSocket events
        request_context = (event or {}).get("requestContext") or {}
        route_key = request_context.get("routeKey")
        if route_key:
            # WebSocket routes
            if route_key == "$connect":
                return {"statusCode": 200}
            if route_key == "$disconnect":
                return {"statusCode": 200}
            # "$default" or "chat"
            body_str = (event or {}).get("body") or ""
            try:
                data = json.loads(body_str) if body_str else {}
            except Exception:
                data = {}
            message = data.get("message")
            # Kick off async processing and return immediately
            async_payload = {
                "asyncTask": "chat",
                "message": message,
                "connectionId": request_context.get("connectionId"),
                "domainName": request_context.get("domainName"),
                "stage": request_context.get("stage"),
            }
            # Fire-and-forget self invoke
            LAMBDA_CLIENT.invoke(
                FunctionName=context.invoked_function_arn,
                InvocationType="Event",
                Payload=json.dumps(async_payload).encode("utf-8"),
            )
            return {"statusCode": 200}

        # Fallback HTTP REST
        path = (event or {}).get("path", "")
        method = (event or {}).get("httpMethod", "")
        # New: GET /saves → list_saves_controller handles query params and response
        if method == "GET" and path.endswith("/saves"):
            return list_saves_controller(event)
        # GET /saves/{id} → get single save by id
        if method == "GET" and "/saves/" in path:
            return get_save_by_id_controller(event)
        # GET /metrics → consolidated counts
        if method == "GET" and path.endswith("/metrics"):
            return get_general_metrics_controller(event)
        # GET /boards → list boards
        if method == "GET" and path.endswith("/boards"):
            return list_boards_controller(event)
        # Fallback hello for other routes/tests
        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
            },
            "body": json.dumps({"message": "hello world"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Headers": "*",
                "Access-Control-Allow-Methods": "GET,OPTIONS",
            },
            "body": json.dumps({"error": str(e)})
        }

def handle_async_chat(event: dict):
    """
    Long-running chat processing with a 60-second timeout, then post back over WS.
    """
    connection_id = event.get("connectionId")
    message = event.get("message") or ""
    # Use hard-coded management API base URL
    apigw_mgmt = boto3.client("apigatewaymanagementapi", endpoint_url=WS_MANAGEMENT_BASE)

    def run_agent():
        return call_agent(message)

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(run_agent)
            result = future.result(timeout=60)
            response_text = result if isinstance(result, str) else str(result)
    except FuturesTimeout:
        response_text = "We encountered an issue processing your request. Please try again."
    except Exception as e:
        response_text = f"Error: {str(e)}"

    payload = {"type": "chat_response", "message": response_text}
    try:
        apigw_mgmt.post_to_connection(ConnectionId=connection_id, Data=json.dumps(payload).encode("utf-8"))
    except ClientError as ce:
        # If the client is gone, ignore
        status = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
        if status != 410:
            raise
    return {"statusCode": 200}

