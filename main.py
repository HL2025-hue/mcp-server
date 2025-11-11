from fastapi import FastAPI
from pydantic import BaseModel
from fastapi.responses import FileResponse
import pandas as pd
import io
import os
import tempfile
import traceback
import requests
import numpy as np
import uuid
import time
from datetime import datetime

app = FastAPI()

# ============================================================
# Configuration for temporary storage
# ============================================================
TEMP_DIR = "/tmp/diary_outputs"
os.makedirs(TEMP_DIR, exist_ok=True)
FILE_LIFETIME_SECONDS = 600  # 10 minutes


# ============================================================
# Health check
# ============================================================
@app.get("/")
def root():
    return {"message": "Preprocessing server is live ✅"}


# ============================================================
# Request schema
# ============================================================
class ToolInput(BaseModel):
    file_path: str


# ============================================================
# Utility functions
# ============================================================
def cleanup_old_files():
    """Delete files older than FILE_LIFETIME_SECONDS."""
    now = time.time()
    for fname in os.listdir(TEMP_DIR):
        fpath = os.path.join(TEMP_DIR, fname)
        if os.path.isfile(fpath) and now - os.path.getmtime(fpath) > FILE_LIFETIME_SECONDS:
            os.remove(fpath)


def save_temp_file(content: bytes, suffix: str) -> str:
    """Save bytes content to /tmp with random UUID filename."""
    fname = f"{uuid.uuid4().hex}_{suffix}.csv"
    fpath = os.path.join(TEMP_DIR, fname)
    with open(fpath, "wb") as f:
        f.write(content)
    return fname


def log_download(filename: str):
    """Simple download event logger."""
    print(f"[{datetime.utcnow().isoformat()}] 📥 File downloaded: {filename}")


# ============================================================
# File loader (CSV or Excel, multiple encodings)
# ============================================================
def load_file(file_url: str) -> pd.DataFrame:
    print(f"📥 Fetching file from URL: {file_url}")
    response = requests.get(file_url)
    response.raise_for_status()
    content = response.content
    print(f"📦 Fetched {len(content)} bytes")

    # Try Excel
    try:
        df = pd.read_excel(io.BytesIO(content), engine="openpyxl")
        print("✅ Loaded as Excel file")
        return df
    except Exception as e:
        print(f"❌ Failed to load as Excel: {e}")

    # Try UTF‑8 CSV
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8", on_bad_lines="skip")
        print("✅ Loaded as UTF‑8 CSV")
        return df
    except Exception as e:
        print(f"❌ Failed to load as UTF‑8 CSV: {e}")

    # Try ISO‑8859‑1 CSV
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="ISO-8859-1", on_bad_lines="skip")
        print("✅ Loaded as ISO‑8859‑1 CSV")
        return df
    except Exception as e:
        print(f"❌ Failed to load as ISO‑8859‑1 CSV: {e}")
        raise ValueError(f"Unsupported or unreadable file format from: {file_url}")


# ============================================================
# Core preprocessing logic
# ============================================================
def process_site_diary(file_url: str) -> dict:
    df = load_file(file_url)

    # Ensure required columns exist
    required_cols = [
        "Ignore Entry", "Internal Use Only", "Description",
        "Category", "From", "Until", "Ring", "Shift", "Duration"
    ]
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Convert flags to boolean
    df["Ignore Entry"] = df["Ignore Entry"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["Internal Use Only"] = df["Internal Use Only"].astype(str).str.lower().isin(["true", "1", "yes"])

    # Drop flagged or missing
    df = df[(df["Ignore Entry"] != True) & (df["Internal Use Only"] != True)]
    df = df.dropna(subset=["Description", "Category"])

    # Deduplication
    df_before_dedup = df.copy()
    df = df.drop_duplicates(subset=["From", "Until", "Ring", "Category", "Description"])
    filtered_out_df = (
        pd.merge(df_before_dedup, df, how="outer", indicator=True)
        .query('_merge == "left_only"')
        .drop(columns=["_merge"])
    )

    # Filter categories with <2 entries
    class_counts = df["Category"].value_counts()
    valid_classes = class_counts[class_counts >= 2].index.tolist()
    df = df[df["Category"].isin(valid_classes)]
    filtered_out_classes = class_counts[class_counts < 2].index.tolist()

    # Fix shift and duration
    df["Shift_Type"] = df["Shift"].astype(str).str.extract(r"^(Day|Night)", expand=False)
    df["Duration_min"] = pd.to_numeric(
        df["Duration"].astype(str).str.extract(r"(\d+)")[0], errors="coerce"
    )

    # Replace NaN / inf with None for JSON‑safe serialisation
    df = df.replace({np.nan: None, np.inf: None, -np.inf: None})
    filtered_out_df = filtered_out_df.replace({np.nan: None, np.inf: None, -np.inf: None})

    # Save to temporary download files
    cleaned_fname = save_temp_file(
        df.to_csv(index=False, encoding="utf-8-sig").encode(), "cleaned"
    )
    filtered_fname = save_temp_file(
        filtered_out_df.to_csv(index=False, encoding="utf-8-sig").encode(), "filtered"
    )

    cleaned_url = f"https://hl2025-hue-cem-diarypreprocessing.onrender.com/download/{cleaned_fname}"
    filtered_url = f"https://hl2025-hue-cem-diarypreprocessing.onrender.com/download/{filtered_fname}"

    print(f"✅ Processed successfully. Cleaned rows: {len(df)}, Filtered out: {len(filtered_out_df)}")

    return {
        "cleaned_download_url": cleaned_url,
        "filtered_download_url": filtered_url,
        "num_cleaned_rows": len(df),
        "num_filtered_rows": len(filtered_out_df),
        "categories_retained": valid_classes,
        "categories_removed": filtered_out_classes,
        "cleaned_df_dict": df.to_dict(orient="records"),
        "filtered_out_df_dict": filtered_out_df.to_dict(orient="records")
    }


# ============================================================
# API endpoint
# ============================================================
@app.post("/run")
def run_tool(request: ToolInput):
    file_path = request.file_path
    try:
        result = process_site_diary(file_path)

        # JSON‑safe cleaning for floats
        def clean_json(obj):
            if isinstance(obj, list):
                return [clean_json(x) for x in obj]
            elif isinstance(obj, dict):
                return {k: clean_json(v) for k, v in obj.items()}
            elif isinstance(obj, float):
                if np.isnan(obj) or np.isinf(obj):
                    return None
                return obj
            return obj

        return clean_json(result)

    except Exception as e:
        return {
            "error": "Failed to process file.",
            "file_path": file_path,
            "exception": str(e),
            "traceback": traceback.format_exc()
        }


# ============================================================
# Temporary download endpoint
# ============================================================
@app.get("/download/{filename}")
def download_file(filename: str):
    cleanup_old_files()
    path = os.path.join(TEMP_DIR, filename)
    if os.path.exists(path):
        log_download(filename)
        return FileResponse(path, media_type="text/csv", filename=filename)
    return {"error": "File not found"}


# ============================================================
# Local execution (Render ignores this)
# ============================================================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)