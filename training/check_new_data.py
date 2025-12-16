import psycopg2
from datetime import datetime

DB_CONFIG = {
    "dbname": "ml_data",
    "user": "postgres",
    "password": "ganesan",
    "host": "127.0.0.1",
    "port": 5432
}

def check_new_data():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # 1️⃣ Get last trained time
    cursor.execute("""
        SELECT last_trained_at
        FROM training_metadata
        ORDER BY id DESC
        LIMIT 1;
    """)
    last_trained_at = cursor.fetchone()[0]

    # 2️⃣ Get latest data timestamp
    cursor.execute("""
        SELECT MAX(created_at)
        FROM training_data;
    """)
    latest_data_time = cursor.fetchone()[0]

    cursor.close()
    conn.close()

    print(f"🕒 Last trained at : {last_trained_at}")
    print(f"📥 Latest data at : {latest_data_time}")

    # 3️⃣ Decision gate
    if latest_data_time is None:
        print("⚠️ No data in table")
        return False

    if latest_data_time > last_trained_at:
        print("✅ New data detected → trigger training")
        return True
    else:
        print("⏸️ No new data → skip training")
        return False


if __name__ == "__main__":
    should_train = check_new_data()
    exit(0 if should_train else 1)
