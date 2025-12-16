import os
import shutil
import psycopg2
from datetime import datetime

MODEL_DIR = "models"
CANDIDATE = os.path.join(MODEL_DIR, "model_candidate.pkl")
LATEST = os.path.join(MODEL_DIR, "model_latest.pkl")

DB_CONFIG = {
    "dbname": "ml_data",
    "user": "postgres",
    "password": "ganesan",
    "host": "127.0.0.1",
    "port": 5432
}

def update_last_trained():
    conn = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        UPDATE training_metadata
        SET last_trained_at = %s
        WHERE id = (
            SELECT id FROM training_metadata
            ORDER BY id DESC
            LIMIT 1
        );
    """, (datetime.utcnow(),))

    conn.commit()
    cursor.close()
    conn.close()


def promote():
    if not os.path.exists(CANDIDATE):
        raise FileNotFoundError("Candidate model not found")

    os.makedirs(MODEL_DIR, exist_ok=True)

    # Backup old model
    if os.path.exists(LATEST):
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(
            MODEL_DIR, f"model_backup_{timestamp}.pkl"
        )
        shutil.copy2(LATEST, backup_path)
        print(f"📦 Backup created: {backup_path}")

    # Promote candidate → latest
    shutil.copy2(CANDIDATE, LATEST)
    print("🚀 Candidate model promoted to production")

    # Update training metadata
    update_last_trained()
    print("🕒 Training metadata updated")


if __name__ == "__main__":
    promote()
