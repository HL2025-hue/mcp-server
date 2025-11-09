from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
import pandas as pd
import io
import os
import tempfile
import mimetypes
import traceback
import requests

app = FastAPI()

@app.get("/")
def root():
    return {"message": "Preprocessing server is live ✅"}

class ToolInput(BaseModel):
    file_path: str

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
# Load file from public URL
# -----------------------------
def load_file(file_url: str) -> pd.DataFrame:
    response = requests.get(file_url)
    response.raise_for_status()
    content = response.content

    # Try Excel
    try:
        print("Trying Excel...")
        return pd.read_excel(io.BytesIO(content), engine="openpyxl")
    except Exception:
        pass

    # Try CSV UTF-8
    try:
        print("Trying CSV (UTF-8)...")
        return pd.read_csv(io.BytesIO(content), encoding="utf-8", on_bad_lines="skip")
    except Exception:
        pass

    # Try CSV ISO-8859-1
    try:
        print("Trying CSV (ISO-8859-1)...")
        return pd.read_csv(io.BytesIO(content), encoding="ISO-8859-1", on_bad_lines="skip")
    except Exception as e:
        raise ValueError(f"All file decoding attempts failed for: {file_url} — {str(e)}")

# -----------------------------
# Pre-process & clean logic
# -----------------------------
def process_site_diary(file_url: str) -> dict:
    df = load_file(file_url)

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
    df["Duration_min"] = df["Duration"].astype(str).str.extract(r'(\d+)').astype(float)

    # Export cleaned + filtered CSVs
    output_dir = tempfile.gettempdir()
    cleaned_path = os.path.join(output_dir, "final_cleaned_site_diary.csv")
    filtered_path = os.path.join(output_dir, "filtered_out_site_diary.csv")
    df.to_csv(cleaned_path, index=False, encoding='utf-8-sig')
    filtered_out_df.to_csv(filtered_path, index=False, encoding='utf-8-sig')

    return {
        "cleaned_file_path": cleaned_path,
        "filtered_file_path": filtered_path,
        "num_cleaned_rows": len(df),
        "num_filtered_rows": len(filtered_out_df),
        "categories_retained": valid_classes,
        "categories_removed": filtered_out_classes,
        "cleaned_df_dict": df.to_dict(orient='records'),
        "filtered_out_df_dict": filtered_out_df.to_dict(orient='records')
    }

# If run locally
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)