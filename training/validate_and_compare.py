import joblib
import json
import os
import numpy as np
from sklearn.metrics import mean_absolute_error
from fetch_data import fetch_training_data

MODEL_DIR = "models"
CANDIDATE_MODEL = os.path.join(MODEL_DIR, "model_candidate.pkl")
LATEST_MODEL = os.path.join(MODEL_DIR, "model_latest.pkl")
METRICS_FILE = os.path.join(MODEL_DIR, "metrics.json")


def evaluate(model_bundle, df):
    model = model_bundle["model"]
    vectorizer = model_bundle["vectorizer"]

    X = vectorizer.transform(df["answer"].astype(str))
    y_true = df["answer"].astype(str).apply(len)

    y_pred = model.predict(X)
    mae = mean_absolute_error(y_true, y_pred)

    return mae


def main():
    print("🔹 Fetching validation data from DB...")
    df = fetch_training_data()

    if df.empty:
        raise ValueError("No data available for validation")

    print("🔹 Loading candidate model...")
    candidate = joblib.load(CANDIDATE_MODEL)
    mae_new = evaluate(candidate, df)

    print(f"📊 Candidate MAE: {mae_new:.4f}")

    # FIRST MODEL CASE
    if not os.path.exists(LATEST_MODEL):
        print("⚠️ No existing model found")
        approve = True
        mae_old = None
    else:
        print("🔹 Loading current production model...")
        latest = joblib.load(LATEST_MODEL)
        mae_old = evaluate(latest, df)
        print(f"📊 Current MAE: {mae_old:.4f}")

        approve = mae_new < mae_old

    # METRICS LOG
    metrics = {
        "candidate_mae": mae_new,
        "current_mae": mae_old,
        "approved": approve
    }

    os.makedirs(MODEL_DIR, exist_ok=True)
    with open(METRICS_FILE, "w") as f:
        json.dump(metrics, f, indent=4)

    # DECISION
    if approve:
        print("✅ Candidate model APPROVED")
        return 0
    else:
        print("❌ Candidate model REJECTED")
        return 1


if __name__ == "__main__":
    exit(main())
