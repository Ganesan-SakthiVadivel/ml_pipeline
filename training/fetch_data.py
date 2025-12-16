import psycopg2
import pandas as pd

DB_CONFIG = {
    "dbname": "ml_data",
    "user": "postgres",
    "password": "ganesan",
    "host": "127.0.0.1",
    "port": 5432
}

def fetch_training_data():
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT question, answer
        FROM training_data
        ORDER BY created_at ASC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


if __name__ == "__main__":
    df = fetch_training_data()
    print(f"✅ Fetched {len(df)} rows from DB")
    print(df.head())
