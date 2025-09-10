import smtplib, ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
currentdatetime = datetime.now()
currentdatetime_str = currentdatetime.strftime("%Y-%m-%d %H:%M:%S")

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

        log(f"Extracting {row['TgtDbName']}.{row['PrjTbl']} data from {from_date} to {to_date}")

        src_sql = row['SrcDeSql'].encode().decode('unicode_escape')
        src_sql = src_sql.replace("{from_date}", from_date).replace("{to_date}", str(to_date))

        src_df = pd.read_sql(src_sql, src_engine)
        src_engine.dispose()

        EndingDateTimes = datetime.now()
        EndingDateTimes_str = EndingDateTimes.strftime("%Y-%m-%d %H:%M:%S")

        record_counts = len(src_df)

        if src_df.empty:
            log(f"[WARNING] Source data is empty for project '{row['PrjTbl']}'. Skipping insertion into main table, logging in Data_log...")
            try:
                insert_sql = """
                    INSERT INTO dashboard.Data_log (
                        Process_startDT, Project_name, From_date, To_Date, Total_Record_Fetched, Process_EndDT
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """
                with dashboard_conn.cursor() as cur:
                    cur.execute(insert_sql, (
                        currentdatetime_str,
                        row['PrjTbl'],
                        from_date,
                        str(to_date),
                        record_counts,
                        EndingDateTimes_str
                    ))
                    dashboard_conn.commit()
                log("[INFO] Empty data logged successfully.")
            except Exception as e:
                log(f"[ERROR] Failed to insert empty-data log: {e}")
            continue

        tgt_engine_url = (
            f"mysql+pymysql://{row['TgtUserName']}:{row['TgtPassword']}"
            f"@{row['TgtDB']}:{tgt_port}/{row['TgtDbName']}?charset=utf8"
        )
        tgt_engine = create_engine(tgt_engine_url)

        text_cols = [col for col in src_df.columns if src_df[col].dtype == 'object']
        record_count = len(src_df)

        src_df.to_sql(
            name=row['PrjTbl'],
            con=tgt_engine,
            if_exists='append',
            index=False,
            dtype={col: Text() for col in text_cols},
            chunksize=1000
        )
        tgt_engine.dispose()

        log(f"[SUCCESS] Inserted into {record_count} {row['TgtDbName']}.{row['PrjTbl']}")

        EndingDateTime = datetime.now()
        EndingDateTime_str = EndingDateTime.strftime("%Y-%m-%d %H:%M:%S")

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

        Inserted_Table = f"""
                    INSERT INTO dashboard.Data_log (
                        Process_startDT, Project_name, From_date, To_Date, Total_Record_Fetched, Process_EndDT
                    ) VALUES (
                        '{currentdatetime_str}', '{row['PrjTbl']}', '{from_date}', '{to_date}', {record_count}, '{EndingDateTime_str}'
                    );
                """
        with dashboard_conn.cursor() as cur:
            cur.execute(Inserted_Table)
            dashboard_conn.commit()

        log(f"[INFO] Updated DataLoadedUpto to {to_date} for PrjId {row['PrjId']}")

    except SQLAlchemyError as err:
        log(f"[ERROR] SQLAlchemy Error in row {index}: {err}")
    except Exception as e:
        log(f"[ERROR] General Error in row {index}: {e}")

dashboard_conn.close()
log("Dashboard connection closed. Script complete.")

# --- DB Config ---
MYSQL_CONFIG = {
    'host': '192.168.135.139',
    'user': 'root',
    'password': 'welcome!!',
    'database': 'Dashboard',
    'charset': 'utf8mb4'
}

# --- Create Engine ---
engine = create_engine(
    f"mysql+pymysql://{MYSQL_CONFIG['user']}:{MYSQL_CONFIG['password']}@{MYSQL_CONFIG['host']}/{MYSQL_CONFIG['database']}?charset={MYSQL_CONFIG['charset']}"
)

# --- SQL Query ---
query = """
SELECT
    Project_name AS `Project Name`,
    Total_Record_Fetched AS `# of Records Loaded`,
    From_date AS `Load Start Datetime`,
    To_date AS `Load End Datetime`,
    Process_startDT AS `Process Start Date`,
    Process_EndDT AS `Process End Date`,
    TIME_FORMAT(TIMEDIFF(Process_EndDT, Process_startDT), '%%H:%%i:%%s') AS `Duration`
FROM dashboard.Data_log
WHERE DATE(Process_startDT) = CURRENT_DATE()
ORDER BY Process_startDT DESC;
"""

# --- Fetch into DataFrame ---
df = pd.read_sql(query, con=engine)

# --- Convert DataFrame to HTML table ---
html_table = df.to_html(index=False, border=1, justify="center")

# --- Build HTML content ---
html_content = f"""
<html>
<head>
<style>
    table {{
        border-collapse: collapse;
        width: 70%;
        font-family: Arial, sans-serif;
        font-size: 12px;  /* smaller text */
    }}
    th, td {{
        border: 1px solid #dddddd;
        text-align: center;
        padding: 5px;
        font-size: 12px;  /* smaller text */
    }}
    th {{
        background-color: #2F75B5;
        color: white;
        font-size: 12px;  /* smaller text */
    }}
    tr:nth-child(even) {{
        background-color: #f2f2f2;
    }}
</style>
</head>
<body>
<p>Hi,<br>
Below is the daily load status of BPO Projects Volume Analyzer.</p>
    {html_table}
<P>Load Start Datetime -> Timestamp of the latest data in dashboard database (Before Run)</p>
<p>Load End Datetime -> Timestamp of the latest data available in source</p>
</body>
</html>
"""

# --- Create Email Message ---
msg = MIMEMultipart("alternative")
msg['Subject'] = "BPO Projects Volume Analyzer - Daily load status"
msg['From'] = "vinod.mahajan@tpgsi.com"

# To recipients
to_recipients = ["vinod.mahajan@tpgsi.com", "prasanna.moorthi@tpgsi.com"]

# CC recipients
cc_recipients = ["vinod.mahajan@tpgsi.com", "prasanna.moorthi@tpgsi.com"]

# Add headers
msg['To'] = ", ".join(to_recipients)
msg['Cc'] = ", ".join(cc_recipients)

# Attach HTML content
msg.attach(MIMEText(html_content, "html"))

# --- SMTP Config ---
smtp_server = "mail10.tpgsi.com"
port = 465
sender_email = "vinod.mahajan@tpgsi.com"
password = "vinod!2#"

context = ssl.create_default_context()

# --- Send Email ---
all_recipients = to_recipients + cc_recipients  # include CC in send list

with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
    server.login(sender_email, password)
    server.sendmail(sender_email, all_recipients, msg.as_string())
    print("Email sent successfully")
