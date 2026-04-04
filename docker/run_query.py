import snowflake.connector
import sys

account = "ck51442.uae-north.azure"
user = "islambenchaiba"
password = "islamBENCHAIBA2002+"
database = "FOOD_DELIVERY_DW"
schema = "BRONZE"

query = sys.argv[1] if len(sys.argv) > 1 else f"SELECT DISTINCT churn_risk FROM {database}.{schema}.CUSTOMERS LIMIT 10"

try:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        database=database,
        schema=schema
    )
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    print(f"\nQUERY: {query}")
    print("RESULTS:")
    for row in results:
        print(row)
    conn.close()
except Exception as e:
    print(f"❌ Error: {e}")
