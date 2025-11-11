from dotenv import load_dotenv
import boto3
import os
import time

load_dotenv()

REGION = os.getenv("AWS_REGION", "us-east-1")
session = boto3.session.Session(region_name=REGION)
ATHENA_REGION = REGION
ATHENA_DATABASE = "willa_datalake"        # Glue database name
ATHENA_WORKGROUP = "willa_datalake"  
athena = session.client("athena", region_name=ATHENA_REGION)

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

def _get_data_dictionary(table_name: str):
    """Get the data dictionary for a given Athena table."""
    tables = {
        "latest_entity_save": {
            "table_name": "latest_entity_save",
            "table_description": "A table containing the latest save data. This includes things like the urls, titles, and descriptions of content saved by users.",
            "columns": [
                {
                    "name": "id",
                    "description": "The id of the save.",
                    "type": "string",
                },
                {
                    "name": "url",
                    "description": "The url of the save.",
                    "type": "string",
                },
                {
                    "name": "title",
                    "description": "The title of the save.",
                    "type": "string",
                },
                {
                    "name": "description",
                    "description": "The description of the save.",
                    "type": "string",
                },
                {
                    "name": "comments",
                    "description": "The comments on the save.",
                    "type": "string",
                },
                {
                    "name": "image",
                    "description": "The image of the save.",
                    "type": "string",
                },
                {
                    "name": "imagekey",
                    "description": "The key of the image of the save.",
                    "type": "string",
                },
                {
                    "name": "publisher",
                    "description": "The publishing website of the save (ex. Instagram, YouTube, etc.).",
                    "type": "string",
                },
                {
                    "name": "boardids",
                    "description": "The ids of the boards the save is associated with.",
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
                {
                    "name": "createdat",
                    "description": "The date and time the save was created.",
                    "type": "string",
                },
                {
                    "name": "updatedat",
                    "description": "The date and time the save was last updated.",
                    "type": "string",
                },
                {
                    "name": "username",
                    "description": "The cognito id of the user who saved the content.",
                    "type": "string",
                },
                {
                    "name": "isarchived",
                    "description": "Whether the save has been archived.",
                    "type": "boolean",
                }
            ]
        },
        "latest_entity_board": {
            "table_name": "latest_entity_board",
            "table_description": "A table containing the latest board data. This includes things like the name, description, and image of the board.",
            "columns": [
                {
                    "name": "id",
                    "description": "The id of the board.",
                    "type": "string",
                },
                {
                    "name": "name",
                    "description": "The name of the board.",
                    "type": "string",
                },
                {
                    "name": "boardimagesaveids",
                    "description": "The ids of the saves in the board.",
                    "type": "array",
                    "items": {
                        "type": "string",
                    },
                },
                {
                    "name": "username",
                    "description": "The cognito id of the user who created the board.",
                    "type": "string",
                },
                {
                    "name": "isarchived",
                    "description": "Whether the board has been archived.",
                    "type": "boolean",
                },
                {
                    "name": "createdat",
                    "description": "The date and time the board was created.",
                    "type": "string",
                },
                {
                    "name": "updatedat",
                    "description": "The date and time the board was last updated.",
                    "type": "string",
                }
            ]
        },
        "latest_entity_edge": {
            "table_name": "latest_entity_edge",
            "table_description": "A table containing the latest edge data. This includes things like the id, type, and timestamp of the edge (Save to Board).",
            "columns": [
                {
                    "name": "id",
                    "description": "The id of the edge.",
                    "type": "string",
                },
                {
                    "name": "saveid",
                    "description": "The id of the save that is associated with the edge.",
                    "type": "string",
                },
                {
                    "name": "boardid",
                    "description": "The id of the board that is associated with the edge.",
                    "type": "string",
                },
                {
                    "name": "createdat",
                    "description": "The date and time the edge was created.",
                    "type": "string",
                },
                {
                    "name": "updatedat",
                    "description": "The date and time the edge was last updated.",
                    "type": "string",
                },
                {
                    "name": "username",
                    "description": "The cognito id of the user who created the edge.",
                    "type": "string",
                },
                {
                    "name": "isarchived",
                    "description": "Whether the edge has been archived.",
                    "type": "boolean",
                }
            ]
        }
    }
    return tables[table_name]
