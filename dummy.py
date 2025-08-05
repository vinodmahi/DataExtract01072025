import pymysql
import pandas as pd

# Set pandas display options
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', None)

# DB config
DASHBOARD_DB = {
    'host': '192.168.135.139',
    'user': 'root',
    'password': 'welcome!!',
    'database': 'Dashboard',
    'charset': 'utf8mb4'
}

# Read SQL content from file
with open(r"C:\Users\charterda\Documents\BTT_carrier\Fiber_Disconnect.sql", "r", encoding="utf-8") as file:
    sql_text = file.read()

# Connect and update
conn = pymysql.connect(**DASHBOARD_DB)
print("Connected to Dashboard")


query = """ update dashboard.projectdataextract
set srcdbsql = %s
where prjid = 1"""