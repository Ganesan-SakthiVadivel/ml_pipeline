import joblib
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LinearRegression
from fetch_data import fetch_training_data
import os

MODEL_DIR = "models"
CANDIDATE_MODEL_PATH = os.path.join(MODEL_DIR, "model_candidate.pkl")

def train():
    print("🔹 Fetching data from DB...")
    df = fetch_training_data()

    if df.empty:
        raise ValueError("No training data found")

    X_text = df["answer"].astype(str)
    y = df["answer"].astype(str).apply(len)

    print("🔹 Vectorizing text...")
    vectorizer = TfidfVectorizer(max_features=1000)
    X = vectorizer.fit_transform(X_text)

    print("🔹 Training model...")
    model = LinearRegression()
    model.fit(X, y)

    os.makedirs(MODEL_DIR, exist_ok=True)

    joblib.dump(
        {
            "model": model,
            "vectorizer": vectorizer
        },
        CANDIDATE_MODEL_PATH
    )

    print(f"✅ Candidate model saved at {CANDIDATE_MODEL_PATH}")


if __name__ == "__main__":
    train()
