import os
from typing import Any, Dict, List, Optional, Tuple
import boto3
from datetime import datetime


def list_users_service(limit: int = 20, pagination_token: Optional[str] = None) -> Dict[str, Any]:
    """
    List Cognito users from the configured User Pool.
    Returns { items: [...], nextToken?: str, count: int, limit: int }
    """
    try:
        limit = int(limit)
    except Exception:
        limit = 20
    limit = max(1, min(limit, 60))  # Cognito ListUsers max 60

    user_pool_id = os.environ.get("COGNITO_USER_POOL_ID") or os.environ.get("USER_POOL_ID")
    region = os.environ.get("AWS_REGION", "us-east-1")
    if not user_pool_id:
        raise ValueError("COGNITO_USER_POOL_ID not configured")

    client = boto3.client("cognito-idp", region_name=region)
    params: Dict[str, Any] = {
        "UserPoolId": user_pool_id,
        "Limit": limit,
    }
    if pagination_token:
        params["PaginationToken"] = pagination_token
    res = client.list_users(**params)
    users = res.get("Users", []) or []
    next_token = res.get("PaginationToken")

    # Shape a lean payload
    items: List[Dict[str, Any]] = []
    for u in users:
        attrs = {a.get("Name"): a.get("Value") for a in (u.get("Attributes") or [])}
        created_raw = u.get("UserCreateDate")
        updated_raw = u.get("UserLastModifiedDate")
        created_str = created_raw.isoformat() if isinstance(created_raw, datetime) else (str(created_raw) if created_raw else None)
        updated_str = updated_raw.isoformat() if isinstance(updated_raw, datetime) else (str(updated_raw) if updated_raw else None)
        items.append({
            "username": u.get("Username"),
            "status": u.get("UserStatus"),
            "enabled": u.get("Enabled"),
            "createdAt": created_str,
            "updatedAt": updated_str,
            "email": attrs.get("email"),
            "phone_number": attrs.get("phone_number"),
            "given_name": attrs.get("given_name"),
            "family_name": attrs.get("family_name"),
        })

    out: Dict[str, Any] = {
        "items": items,
        "count": len(items),
        "limit": limit,
    }
    if next_token:
        out["nextToken"] = next_token
    return out


