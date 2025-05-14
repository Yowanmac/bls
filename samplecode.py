import boto3
import pandas as pd
import pyodbc
import io

def lambda_handler(event, context):
    # S3 details
    bucket = 'bsl-sampledata1'            # ✅ Replace with your S3 bucket
    key = 'gyan_sample_data.csv'          # ✅ Replace with your CSV file path

    # SQL Server connection details
    server = 'bls-db1.c9wqy0iuir9n.ap-south-1.rds.amazonaws.com'   # ✅ Replace with your SQL Server endpoint
    database = 'bls_data_store'             # ✅ Replace with your DB name
    username = 'admin'                 # ✅ Replace with your DB username
    password = 'BlsAdmin123'             # ✅ Replace with your DB password
    table_name = 'Blssampledata'         # ✅ Desired SQL Server table name

    try:
        # Read CSV from S3 into Pandas DataFrame
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        df = pd.read_csv(io.StringIO(content))

        print("CSV loaded. Columns:", df.columns.tolist())

        # Connect to SQL Server
        conn_str = (
            "DRIVER={ODBC Driver 18 for SQL Server};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"UID={username};"
            f"PWD={password};"
            "Encrypt=yes;"
            "TrustServerCertificate=yes;"
        )
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Create table if not exists (columns as NVARCHAR(MAX))
        columns = ', '.join([f"[{col}] NVARCHAR(MAX)" for col in df.columns])
        create_sql = f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({columns})"
        cursor.execute(create_sql)
        conn.commit()

        # Insert DataFrame rows
        for _, row in df.iterrows():
            values = "', '".join(str(x).replace("'", "''") for x in row)
            insert_sql = f"INSERT INTO {table_name} VALUES ('{values}')"
            cursor.execute(insert_sql)

        conn.commit()
        cursor.close()
        conn.close()

        return {
            'statusCode': 200,
            'body': f'Successfully inserted {len(df)} rows into {table_name}.'
        }

    except Exception as e:
        print("Error:", e)
        return {
            'statusCode': 500,
            'body': str(e)
        }
