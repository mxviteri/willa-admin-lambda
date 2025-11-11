from langchain_openai import ChatOpenAI
from langchain.agents import create_agent
from willa_admin_agent.utils.tools import query_athena_sql, list_athena_tables, describe_athena_table

# --- Build agent ---
SYSTEM_PROMPT = """
You are a careful AWS Athena analyst. You are also an expert in the data schema of the Athena database.

Rules:
- Think step-by-step.
- Before querying the database, call the tool `list_athena_tables` to get a list of available tables.
- Before querying a table, call the tool `describe_athena_table` with the table name to get the data dictionary.
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
    tools=[query_athena_sql, list_athena_tables, describe_athena_table],
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