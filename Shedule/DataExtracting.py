import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.types import Text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

def log(msg):
    """Prints a message with a timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {msg}", flush=True)

# Step 1: Setup Dashboard connection using SQLAlchemy to avoid warnings
dashboard_db_config = {
    'host': '192.168.135.139',
    'user': 'root',
    'password': 'welcome!!',
    'database': 'Dashboard',
    'charset': 'utf8mb4'
}

dashboard_url = URL.create(
    drivername="mysql+pymysql",
    username=dashboard_db_config['user'],
    password=dashboard_db_config['password'],
    host=dashboard_db_config['host'],
    database=dashboard_db_config['database'],
    port=3306,
    query={"charset": "utf8mb4"}
)

dashboard_engine = create_engine(dashboard_url)
dashboard_conn = dashboard_engine.raw_connection()

metadata_query = """
SELECT 
    pe.SrcDB, pe.SrcPort, pe.SrcUserName, pe.SrcPassword, pe.SrcDbName, pe.SrcDeSql,
    pe.TgtDB, pe.TgtPort, pe.TgtUserName, pe.TgtPassword, pe.TgtDbName,
    p.PrjTbl, pe.DataLoadedUpto, pe.PrjId
FROM dashboard.projects p
LEFT JOIN dashboard.projectsdataextract pe ON p.PrjId = pe.PrjId
WHERE p.IsCurr = 1
"""

project_df = pd.read_sql(metadata_query, dashboard_engine)
log("Project Metadata Fetched")

# Step 2: Loop through each project to extract and load data
for index, row in project_df.iterrows():
    try:
        if not row['SrcDeSql']:
            log(f"[WARNING] Skipping row {index}: Empty source SQL.")
            continue

        try:
            src_port = int(float(row['SrcPort'])) if pd.notna(row['SrcPort']) else 3306
            tgt_port = int(float(row['TgtPort'])) if pd.notna(row['TgtPort']) else 3306
        except ValueError:
            log(f"[ERROR] Invalid port format in row {index}, skipping.")
            continue

        src_engine_url = (
            f"mysql+pymysql://{row['SrcUserName']}:{row['SrcPassword']}"
            f"@{row['SrcDB']}:{src_port}/{row['SrcDbName']}?charset=utf8"
        )
        src_engine = create_engine(src_engine_url)

        max_date_query = f"""
            SELECT MAX(
                CASE 
                    WHEN date_submitted > 1000000000 THEN FROM_UNIXTIME(date_submitted)
                    ELSE date_submitted
                END
            ) AS to_date 
            FROM {row['SrcDbName']}.mantis_bug_table
        """
        max_date_df = pd.read_sql(max_date_query, src_engine)
        to_date = max_date_df['to_date'][0]

        if pd.isna(to_date):
            log("[WARNING] to_date is NULL, running fallback query...")
            fallback_query = f"""
                SELECT MAX(date_submitted) AS to_date
                FROM {row['SrcDbName']}.mantis_bug_table
            """
            fallback_df = pd.read_sql(fallback_query, src_engine)
            to_date = fallback_df['to_date'][0]

        from_date = row['DataLoadedUpto'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(
            row['DataLoadedUpto']) else '2000-01-01 00:00:00'

        log(f"Extracting data from {from_date} to {to_date}")

        src_sql = row['SrcDeSql'].encode().decode('unicode_escape')
        src_sql = src_sql.replace("{from_date}", from_date).replace("{to_date}", str(to_date))

        src_df = pd.read_sql(src_sql, src_engine)
        src_engine.dispose()

        if src_df.empty:
            log("[WARNING] Source data is empty. Skipping insertion.")
            continue

        tgt_engine_url = (
            f"mysql+pymysql://{row['TgtUserName']}:{row['TgtPassword']}"
            f"@{row['TgtDB']}:{tgt_port}/{row['TgtDbName']}?charset=utf8"
        )
        tgt_engine = create_engine(tgt_engine_url)

        text_cols = [col for col in src_df.columns if src_df[col].dtype == 'object']
        src_df.to_sql(
            name=row['PrjTbl'],
            con=tgt_engine,
            if_exists='append',
            index=False,
            dtype={col: Text() for col in text_cols},
            chunksize=1000
        )
        tgt_engine.dispose()

        log(f"[SUCCESS] Inserted into {row['TgtDbName']}.{row['PrjTbl']}")

        currentdatetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        update_query = f"""
            UPDATE dashboard.projectsdataextract
            SET 
            DataLoadedUpto = '{to_date}',
            LoadProcessRunDttm = '{currentdatetime}'
            WHERE PrjId = {row['PrjId']};
        """
        with dashboard_conn.cursor() as cur:
            cur.execute(update_query)
            dashboard_conn.commit()

        log(f"[INFO] Updated DataLoadedUpto to {to_date} for PrjId {row['PrjId']}")

    except SQLAlchemyError as err:
        log(f"[ERROR] SQLAlchemy Error in row {index}: {err}")
    except Exception as e:
        log(f"[ERROR] General Error in row {index}: {e}")

dashboard_conn.close()
log("Dashboard connection closed. Script complete.")
