import json
from agent import call_agent

def handler(event, context):
    try:
        path = (event or {}).get("path", "")
        method = (event or {}).get("httpMethod", "")
        if method == "POST" and path.endswith("/agent/chat"):
            body_str = (event or {}).get("body") or ""
            data = json.loads(body_str) if body_str else {}
            message = data.get("message")
            if not isinstance(message, str):
                return {
                    "statusCode": 400,
                    "headers": {"Content-Type": "application/json"},
                    "body": json.dumps({"error": "message must be a string"})
                }
            response = call_agent(message)
            return {
                "statusCode": 200,
                "headers": {"Content-Type": "application/json"},
                "body": json.dumps({"message": response})
            }
        # Fallback hello for other routes/tests
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": "hello world"})
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"error": str(e)})
        }

