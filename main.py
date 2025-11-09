from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
import io
import os
import tempfile
import traceback
import requests

app = FastAPI()


# -----------------------------
# Root health check
# -----------------------------
@app.get("/")
def root():
    return {"message": "Preprocessing server is live ✅"}


# -----------------------------
# Request model
# -----------------------------
class ToolInput(BaseModel):
    file_path: str


# -----------------------------
# API endpoint
# -----------------------------
@app.post("/run")
def run_tool(request: ToolInput):
    file_path = request.file_path
    try:
        return process_site_diary(file_path)
    except Exception as e:
        return {
            "error": "Failed to process file.",
            "file_path": file_path,
            "exception": str(e),
            "traceback": traceback.format_exc()
        }


# -----------------------------
# File loader (supports CSV + Excel)
# -----------------------------
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

    # Try UTF-8 CSV
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="utf-8", on_bad_lines="skip")
        print("✅ Loaded as UTF-8 CSV")
        return df
    except Exception as e:
        print(f"❌ Failed to load as UTF-8 CSV: {e}")

    # Try ISO-8859-1 CSV
    try:
        df = pd.read_csv(io.BytesIO(content), encoding="ISO-8859-1", on_bad_lines="skip")
        print("✅ Loaded as ISO-8859-1 CSV")
        return df
    except Exception as e:
        print(f"❌ Failed to load as ISO-8859-1 CSV: {e}")
        raise ValueError(f"Unsupported or unreadable file format from: {file_url}")


# -----------------------------
# Preprocessing logic
# -----------------------------
def process_site_diary(file_url: str) -> dict:
    df = load_file(file_url)

    # Ensure all expected columns exist
    required_cols = ["Ignore Entry", "Internal Use Only", "Description", "Category", "From", "Until", "Ring", "Shift", "Duration"]
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
    filtered_out_df = pd.merge(df_before_dedup, df, how='outer', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])

    # Filter categories with <2 entries
    class_counts = df["Category"].value_counts()
    valid_classes = class_counts[class_counts >= 2].index.tolist()
    df = df[df["Category"].isin(valid_classes)]
    filtered_out_classes = class_counts[class_counts < 2].index.tolist()

    # Fix shift and duration
    df["Shift_Type"] = df["Shift"].astype(str).str.extract(r'^(Day|Night)', expand=False)
    df["Duration_min"] = pd.to_numeric(df["Duration"].astype(str).str.extract(r'(\d+)')[0], errors="coerce")

    # Replace NaN/NaT with None for JSON-safe serialization
    df_json = df.where(pd.notnull(df), None)
    filtered_json = filtered_out_df.where(pd.notnull(filtered_out_df), None)

    # Export cleaned + filtered CSVs
    output_dir = tempfile.gettempdir()
    cleaned_path = os.path.join(output_dir, "final_cleaned_site_diary.csv")
    filtered_path = os.path.join(output_dir, "filtered_out_site_diary.csv")
    df_json.to_csv(cleaned_path, index=False, encoding='utf-8-sig')
    filtered_json.to_csv(filtered_path, index=False, encoding='utf-8-sig')

    print(f"✅ Processed successfully. Cleaned rows: {len(df_json)}, Filtered out: {len(filtered_json)}")

    return {
        "cleaned_file_path": cleaned_path,
        "filtered_file_path": filtered_path,
        "num_cleaned_rows": len(df_json),
        "num_filtered_rows": len(filtered_json),
        "categories_retained": valid_classes,
        "categories_removed": filtered_out_classes,
        "cleaned_df_dict": df_json.to_dict(orient='records'),
        "filtered_out_df_dict": filtered_json.to_dict(orient='records')
    }


# -----------------------------
# Run locally (Render ignores this)
# -----------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)