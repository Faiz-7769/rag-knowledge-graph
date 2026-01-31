import sqlite3

# Connect to NEW small database
target_conn = sqlite3.connect("small.db")
target_cursor = target_conn.cursor()

# Attach source database FIRST
target_cursor.execute("ATTACH DATABASE 'olist.sqlite' AS source")

# Create table from source DB
target_cursor.execute("""
CREATE TABLE orders AS
SELECT * FROM source.orders
LIMIT 200;
""")

target_conn.commit()
target_conn.close()

print("✅ small.db created successfully with orders table")
