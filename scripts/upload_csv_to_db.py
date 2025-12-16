import pandas as pd
import psycopg2
import hashlib
import sys
from psycopg2 import sql

# ==============================
# CONFIG
# ==============================

CSV_PATH = "qa.csv"

DB_CONFIG = {
    "dbname": "ml_data",
    "user": "postgres",
    "password": "ganesan",
    "host": "127.0.0.1",   # safer than localhost on Windows
    "port": 5432
}

TABLE_NAME = "training_data"

# ==============================
# HELPERS
# ==============================

def generate_hash(text: str) -> str:
    """Generate SHA256 hash for duplicate detection"""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def create_table_if_not_exists(cursor):
    """Create table and unique constraint if not exists"""
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            id SERIAL PRIMARY KEY,
            question TEXT NOT NULL,
            answer TEXT NOT NULL,
            source_file TEXT,
            data_hash TEXT UNIQUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)


# ==============================
# MAIN
# ==============================

def main():
    print("🔹 Reading CSV...")
    try:
        df = pd.read_csv(CSV_PATH)
    except Exception as e:
        print("❌ Failed to read CSV:", e)
        sys.exit(1)

    required_cols = {"question", "answer"}
    if not required_cols.issubset(df.columns):
        print("❌ CSV must contain columns: question, answer")
        sys.exit(1)

    print(f"🔹 CSV loaded with {len(df)} rows")

    # ------------------------------
    # DB CONNECTION
    # ------------------------------
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print("❌ Database connection failed:", e)
        sys.exit(1)

    print("🔹 Connected to PostgreSQL")

    # ------------------------------
    # CREATE TABLE
    # ------------------------------
    try:
        create_table_if_not_exists(cursor)
        conn.commit()
    except Exception as e:
        print("❌ Failed to create table:", e)
        conn.rollback()
        sys.exit(1)

    print("🔹 Table ready")

    # ------------------------------
    # INSERT DATA
    # ------------------------------
    insert_query = sql.SQL("""
        INSERT INTO {} (question, answer, source_file, data_hash)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (data_hash) DO NOTHING;
    """).format(sql.Identifier(TABLE_NAME))

    inserted = 0
    skipped = 0

    for _, row in df.iterrows():
        combined_text = str(row["question"]) + str(row["answer"])
        data_hash = generate_hash(combined_text)

        try:
            cursor.execute(
                insert_query,
                (
                    row["question"],
                    row["answer"],
                    CSV_PATH,
                    data_hash
                )
            )
            if cursor.rowcount == 0:
                skipped += 1
            else:
                inserted += 1

        except Exception as e:
            print("⚠️ Insert failed for row:", e)

    conn.commit()
    cursor.close()
    conn.close()

    print("✅ Upload complete")
    print(f"   ➕ Inserted rows: {inserted}")
    print(f"   ➖ Skipped duplicates: {skipped}")


if __name__ == "__main__":
    main()
