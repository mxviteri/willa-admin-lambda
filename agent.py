from langchain_openai import ChatOpenAI
from langchain_core.tools import tool
from langchain.agents import create_agent
from dotenv import load_dotenv
import boto3
from boto3.dynamodb.conditions import Attr, Key
import os
import time

load_dotenv()

# --- AWS Setup ---
REGION = os.getenv("AWS_REGION", "us-east-1")
session = boto3.session.Session(region_name=REGION)
ATHENA_REGION = REGION
ATHENA_DATABASE = "willa_datalake"        # Glue database name
ATHENA_WORKGROUP = "willa_datalake"  
athena = session.client("athena", region_name=ATHENA_REGION)
cognito = session.client("cognito-idp")

# --- Helper Functions ---
def _run_athena_query(query: str):
    """Execute a SQL query in Athena and return results as a list of dicts."""
    start_kwargs = {
        "QueryString": query,
        "QueryExecutionContext": {"Database": ATHENA_DATABASE},
    }
    print(f"[athena] region={ATHENA_REGION} db={ATHENA_DATABASE} wg={ATHENA_WORKGROUP} sql={query[:120]}")
    # Prefer env-configured output/workgroup if provided
    athena_output = None
    if athena_output:
        start_kwargs["ResultConfiguration"] = {"OutputLocation": athena_output}
    athena_workgroup = ATHENA_WORKGROUP
    if athena_workgroup:
        start_kwargs["WorkGroup"] = athena_workgroup
        # If no explicit output is set and the selected workgroup lacks a result location,
        # fall back to 'primary' which commonly has a default output location configured.
        if not athena_output:
            try:
                wg = athena.get_work_group(WorkGroup=athena_workgroup)["WorkGroup"]
                has_output = bool(
                    wg.get("Configuration", {})
                    .get("ResultConfiguration", {})
                    .get("OutputLocation")
                )
                if not has_output:
                    start_kwargs["WorkGroup"] = "primary"
            except Exception:
                # If introspection fails, leave the chosen workgroup as-is
                pass
    try:
        response = athena.start_query_execution(**start_kwargs)
    except Exception as e:
        print(f"[athena:start:error] {e}")
        return f"Error: {str(e)}"
    qid = response["QueryExecutionId"]
    print(f"[athena] started qid={qid}")

    # Wait for query completion
    while True:
        try:
            result = athena.get_query_execution(QueryExecutionId=qid)
        except Exception as e:
            print(f"[athena:poll:error] qid={qid} err={e}")
            return f"Error: {str(e)}"
        state = result["QueryExecution"]["Status"]["State"]
        if state in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if state != "SUCCEEDED":
        print(f"[athena:error] {result['QueryExecution']['Status']}")
        return f"Error: Athena query failed with state '{state}'"
    else:
        print(f"[athena] succeeded qid={qid}")

    # Parse the result set
    data = athena.get_query_results(QueryExecutionId=qid)
    rows = data["ResultSet"]["Rows"]
    # Handle empty result sets gracefully
    if not rows:
        return []
    first_row = rows[0].get("Data", [])
    if not first_row:
        return []
    headers = [c.get("VarCharValue", f"col_{i}") for i, c in enumerate(first_row)]
    results: list[dict] = []
    for row in rows[1:]:
        data_cells = row.get("Data", [])
        item = {headers[i]: cell.get("VarCharValue", None) for i, cell in enumerate(data_cells)}
        results.append(item)
    return results

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
        return _run_athena_query(f"DESCRIBE {table_name};")
    except Exception as e:
        return f"Error: {str(e)}"

@tool("query_athena_sql")
def query_athena_sql(query: str):
    """Run an arbitrary SQL query against Athena and return the results."""
    try:
        return _run_athena_query(query)
    except Exception as e:
        return f"Error: {str(e)}"


# --- Build agent ---
SYSTEM_PROMPT = """
You are a careful AWS Athena analyst.

Rules:
- Think step-by-step.
- Before querying the database, call the tool `list_athena_tables` to get a list of available tables.
- When you need data, call the tool `query_athena_sql` with ONE SELECT query.
- Read-only only; no INSERT/UPDATE/DELETE/ALTER/DROP/CREATE/REPLACE/TRUNCATE.
- If asked for a user's information, call the tool `get_cognito_user_info_by_sub` with the user's sub (username).
- If the prompt includes a user's email address, call the tool `get_cognito_user_id_by_email` with the user's email address and then use the returned userId in the athena query.
- Limit to 5 rows of output unless the user explicitly asks otherwise.
- If the tool returns 'Error:', revise the SQL and try again.
- Prefer explicit column lists; avoid SELECT *.
"""

model = ChatOpenAI(model="gpt-4o-mini")

agent = create_agent(
    model,
    tools=[query_athena_sql, list_athena_tables, describe_athena_table, get_cognito_user_info_by_sub, get_cognito_user_id_by_email],
    system_prompt=SYSTEM_PROMPT,
)

# Call the agent
# result = agent.invoke({"messages": [{"role": "user", "content": "Which table has the most rows?"}]})
# print(result["messages"][-1].content)

# question = "How many saves encountered issues with authentication in their descriptions? For example, a web scaping issue. Return the first 5 saves in that state."

# for step in agent.stream(
#     {"messages": question},
#     # context=RuntimeContext(db=db),
#     stream_mode="values",
# ):
#     step["messages"][-1].pretty_print()

# Call the agent
# result = agent.invoke({"messages": [{"role": "user", "content": "Which table has the most rows?"}]})
# print(result["messages"][-1].content)

def call_agent(message: str):
    result = agent.invoke({"messages": [{"role": "user", "content": message}]})
    return result["messages"][-1].content