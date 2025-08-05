import pymysql
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Text
from sqlalchemy.exc import SQLAlchemyError
from datetime import datetime

# Step 1: Define connection parameters for the Dashboard database
dashboard_db_config = {
    'host': '192.168.135.139',
    'user': 'root',
    'password': 'welcome!!',
    'database': 'Dashboard',
    'charset': 'utf8mb4'
}

# Connect to Dashboard database
dashboard_conn = pymysql.connect(**dashboard_db_config)

# Query to fetch metadata for current projects
metadata_query = """
SELECT 
    pe.SrcDB, pe.SrcPort, pe.SrcUserName, pe.SrcPassword, pe.SrcDbName, pe.SrcDeSql,
    pe.TgtDB, pe.TgtPort, pe.TgtUserName, pe.TgtPassword, pe.TgtDbName,
    p.PrjTbl, pe.DataLoadedUpto, pe.PrjId
FROM dashboard.projects p
LEFT JOIN dashboard.projectsdataextract pe ON p.PrjId = pe.PrjId
WHERE p.IsCurr = 1
"""

# Load project metadata into a DataFrame
project_df = pd.read_sql(metadata_query, dashboard_conn)
print("\n✅ --- Project Metadata Fetched ---")

# Step 2: Iterate through each project row to process data extraction and loading
for index, row in project_df.iterrows():
    try:
        # Skip if no source SQL is provided
        if not row['SrcDeSql']:
            print(f"⚠️ Skipping row {index}: Empty source SQL.")
            continue

        # Normalize and validate port numbers
        try:
            src_port = int(float(row['SrcPort'])) if pd.notna(row['SrcPort']) else 3306
            tgt_port = int(float(row['TgtPort'])) if pd.notna(row['TgtPort']) else 3306
        except ValueError:
            print(f"❌ Invalid port format in row {index}, skipping.")
            continue

        # Create SQLAlchemy engine for source database
        src_engine_url = (
            f"mysql+pymysql://{row['SrcUserName']}:{row['SrcPassword']}"
            f"@{row['SrcDB']}:{src_port}/{row['SrcDbName']}?charset=utf8"
        )
        src_engine = create_engine(src_engine_url)

        # Query to determine the latest available date from the source table
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

        # Fallback query if the main one returns NULL
        if pd.isna(to_date):
            print("⚠️ to_date is NULL, running fallback query...")
            fallback_query = f"""
                SELECT MAX(date_submitted) AS to_date
                FROM {row['SrcDbName']}.mantis_bug_table
            """
            fallback_df = pd.read_sql(fallback_query, src_engine)
            to_date = fallback_df['to_date'][0]

        # Determine starting point for data extraction
        from_date = row['DataLoadedUpto'].strftime('%Y-%m-%d %H:%M:%S') if pd.notna(
            row['DataLoadedUpto']) else '2000-01-01 00:00:00'

        print()
        print(f"📅 Extracting data from {from_date} to {to_date}")

        # Prepare source SQL by injecting date parameters
        src_sql = row['SrcDeSql'].encode().decode('unicode_escape')
        src_sql = src_sql.replace("{from_date}", from_date).replace("{to_date}", str(to_date))

        # Execute the extraction query
        src_df = pd.read_sql(src_sql, src_engine)
        src_engine.dispose()  # Close connection to source DB

        # Skip if no data returned
        if src_df.empty:
            print("⚠️ Source data is empty. Skipping insertion.")
            continue

        # Create SQLAlchemy engine for target database
        tgt_engine_url = (
            f"mysql+pymysql://{row['TgtUserName']}:{row['TgtPassword']}"
            f"@{row['TgtDB']}:{tgt_port}/{row['TgtDbName']}?charset=utf8"
        )
        tgt_engine = create_engine(tgt_engine_url)

        # Determine which columns are textual
        text_cols = [col for col in src_df.columns if src_df[col].dtype == 'object']

        # Insert extracted data into target database
        src_df.to_sql(
            name=row['PrjTbl'],
            con=tgt_engine,
            if_exists='append',
            index=False,
            dtype={col: Text() for col in text_cols},
            chunksize=1000
        )
        tgt_engine.dispose()  # Close connection to target DB

        print(f"✅ Successfully inserted into {row['TgtDbName']}.{row['PrjTbl']}")

        # Update metadata to reflect load progress
        currentdatetime = datetime.now()
        currentdatetime_str = currentdatetime.strftime("%Y-%m-%d %H:%M:%S")

        update_query = f"""
            UPDATE dashboard.projectsdataextract
            SET 
            DataLoadedUpto = '{to_date}',
            LoadProcessRunDttm = '{currentdatetime_str}'
            WHERE PrjId = {row['PrjId']};
        """
        with dashboard_conn.cursor() as cur:
            cur.execute(update_query)
            dashboard_conn.commit()

        print(f"📌 Updated DataLoadedUpto to {to_date} for PrjId {row['PrjId']}")

    except SQLAlchemyError as err:
        # Catch and report SQLAlchemy-specific issues
        print(f"❌ SQLAlchemy Error in row {index}: {err}")
    except Exception as e:
        # General exception handling
        print(f"❌ General Error in row {index}: {e}")

# Step 3: Clean up and close the Dashboard DB connection
dashboard_conn.close()
