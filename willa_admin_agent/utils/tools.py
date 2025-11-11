from langchain_core.tools import tool
import os
import boto3
from dotenv import load_dotenv
from willa_admin_agent.utils.helpers import _run_athena_query, _get_data_dictionary

load_dotenv()

cognito = boto3.client("cognito-idp")

ATHENA_DATABASE = os.getenv("ATHENA_DATABASE", "willa_datalake")

# --- Define tools ---
@tool("get_cognito_user_id_by_email", return_direct=False)
def get_cognito_user_id_by_email(email: str, user_pool_id: str | None = None):
    """Return the Cognito userId (Username) for a given email address."""
    try:
        pool_id = user_pool_id or os.getenv("COGNITO_USER_POOL_ID")
        if not pool_id:
            return {"error": "Missing COGNITO_USER_POOL_ID. Set env var or pass user_pool_id."}
        resp = cognito.list_users(
            UserPoolId=pool_id,
            Filter=f'email = "{email}"',
            Limit=1,
        )
        users = resp.get("Users", [])
        if not users:
            return {"error": f"No user found for email {email}"}
        user = users[0]
        attrs = {a["Name"]: a["Value"] for a in user.get("Attributes", [])}
        return {
            "userId": user.get("Username"),
            "sub": attrs.get("sub"),
            "email_verified": attrs.get("email_verified"),
        }
    except Exception as e:
        print(f"[get_cognito_user_id_by_email:error] {e}")
        return {"error": str(e)}

@tool("get_cognito_user_id_by_sub", return_direct=False)
def get_cognito_user_info_by_sub(sub: str, user_pool_id: str | None = None):
    """Return user's firstName, lastName, and email for a given Cognito sub (userId)."""
    try:
        pool_id = user_pool_id or os.getenv("COGNITO_USER_POOL_ID")
        if not pool_id:
            return {"error": "Missing COGNITO_USER_POOL_ID. Set env var or pass user_pool_id."}
        resp = cognito.list_users(
            UserPoolId=pool_id,
            Filter=f'sub = "{sub}"',
            Limit=1,
        )
        users = resp.get("Users", [])
        if not users:
            return {"error": f"No user found for sub {sub}"}
        user = users[0]
        attrs = {a["Name"]: a["Value"] for a in user.get("Attributes", [])}
        return {
            "firstName": attrs.get("given_name"),
            "lastName": attrs.get("family_name"),
            "email": attrs.get("email"),
        }
    except Exception as e:
        print(f"[get_cognito_user_info_by_sub:error] {e}")
        return {"error": str(e)}

@tool("list_athena_tables")
def list_athena_tables():
    """List all available tables in the Athena database."""
    # Limit to tables prefixed with 'latest_' using information_schema for reliability
    try:
        res = _run_athena_query(
            f"SELECT table_name FROM information_schema.tables "
            f"WHERE table_schema = '{ATHENA_DATABASE}' AND table_name LIKE 'latest_%'"
        )
        if isinstance(res, str) and res.startswith("Error"):
            print(f"[list_athena_tables:error] {res}")
        return res
    except Exception as e:
        print(f"[list_athena_tables:error] {e}")
        return f"Error: {str(e)}"

@tool("describe_athena_table")
def describe_athena_table(table_name: str):
    """Describe the columns and types for a given Athena table."""
    try:
        # return _run_athena_query(f"DESCRIBE {table_name};")
        return _get_data_dictionary(table_name)
    except Exception as e:
        print(f"[describe_athena_table:error] {e}")
        return f"Error: {str(e)}"

@tool("query_athena_sql")
def query_athena_sql(query: str):
    """Run an arbitrary SQL query against Athena and return the results."""
    try:
        return _run_athena_query(query)
    except Exception as e:
        print(f"[query_athena_sql:error] {e}")
        return f"Error: {str(e)}"