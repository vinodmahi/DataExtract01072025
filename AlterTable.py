import pymysql
import pandas as pd

# Set pandas display options to show full column content when printing DataFrames
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# Dashboard database connection configuration
DASHBOARD_DB = {
    'host': '192.168.135.139',
    'user': 'root',
    'password': 'welcome!!',
    'database': 'Dashboard',
    'charset': 'utf8mb4'
}

# Step 1: Read the SQL query from an external .sql file
with open(r"C:\Users\charterda\Documents\BTT_carrier\1BTT_Carrier.sql", "r", encoding="utf-8") as file:
    sql_text = file.read()

# Step 2: Establish a connection to the Dashboard database
conn = pymysql.connect(**DASHBOARD_DB)
print("Connected to Dashboard")

# Step 3: Create a cursor object to execute SQL queries
cursor = conn.cursor()

# Step 4: Define the SQL update query using a parameterized statement (to prevent SQL injection)
update_query = """
UPDATE dashboard.ProjectsDataExtract
SET SrcDeSql = %s
WHERE PrjId = 1 
"""

# Step 5: Execute the update query with the SQL content from the file
cursor.execute(update_query, (sql_text,))

# Step 6: Commit the transaction to persist the changes
conn.commit()
print("SQL text successfully inserted into SrcDeSql")

# Optional verification: Uncomment to fetch and view the updated record
# df = pd.read_sql("SELECT * FROM dashboard.ProjectsDataExtract WHERE PrjId = 1", conn)
# print(df)

# Step 7: Close the cursor and connection to release resources
cursor.close()
conn.close()
