import snowflake.connector
import sys

account = "ck51442.uae-north.azure"
user = "islambenchaiba"
password = "islamBENCHAIBA2002+"

print(f"Testing connection with account: {account}...")
try:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    print("✅ Success with ck51442.uae-north.azure")
    conn.close()
except Exception as e:
    print(f"❌ Failed with ck51442.uae-north.azure: {e}")

account = "ck51442.uae-north.azure"
print(f"\nTesting connection with account: {account}...")
try:
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account
    )
    print("✅ Success with ck51442")
    conn.close()
except Exception as e:
    print(f"❌ Failed with ck51442: {e}")
