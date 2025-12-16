import psycopg2
import pandas as pd
import os

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": int(os.getenv("DB_PORT", 5432))
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
