from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
import pandas as pd
import os
import tempfile
import traceback
import requests
from io import StringIO, BytesIO

app = FastAPI()

# Health check
@app.get("/")
def root():
    return {"message": "Preprocessing server is live ✅"}


# Input schema expected by Dify
class ToolInput(BaseModel):
    file_path: str


@app.post("/run")
def run_tool(request: ToolInput):
    file_path = request.file_path
    try:
        return process_site_diary(file_path)
    except Exception as e:
        return {
            "error": "Failed to load file.",
            "file_path": file_path,
            "exception": str(e),
            "traceback": traceback.format_exc()
        }


# ---------------------------------------------------------------------------
# Load file from URL or local path, automatically detecting format
# ---------------------------------------------------------------------------
def load_file(file_path: str) -> pd.DataFrame:
    # If it's a URL, download the file first
    if file_path.startswith("http"):
        response = requests.get(file_path)
        response.raise_for_status()

        # Try reading as CSV (text)
        try:
            return pd.read_csv(StringIO(response.text))
        except Exception:
            pass

        # Try reading as Excel (binary)
        try:
            return pd.read_excel(BytesIO(response.content), engine="openpyxl")
        except Exception:
            pass

        raise ValueError(f"Unsupported or unreadable file from URL: {file_path}")

    # Otherwise, assume local path
    if file_path.endswith(".csv"):
        return pd.read_csv(file_path)
    elif file_path.endswith(".xlsx"):
        return pd.read_excel(file_path, engine="openpyxl")
    else:
        raise ValueError(f"Unsupported file type or missing extension: {file_path}")


# ---------------------------------------------------------------------------
# Core preprocessing logic
# ---------------------------------------------------------------------------
def process_site_diary(file_path: str) -> dict:
    df = load_file(file_path)

    # --- Clean & Filter ---
    df["Ignore Entry"] = df["Ignore Entry"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["Internal Use Only"] = df["Internal Use Only"].astype(str).str.lower().isin(["true", "1", "yes"])

    df = df[(df["Ignore Entry"] != True) & (df["Internal Use Only"] != True)]
    df = df.dropna(subset=["Description", "Category"])

    df_before_dedup = df.copy()
    df = df.drop_duplicates(subset=["From", "Until", "Ring", "Category", "Description"])

    filtered_out_df = pd.merge(
        df_before_dedup,
        df,
        how='outer',
        indicator=True
    ).query('_merge == "left_only"').drop(columns=['_merge'])

    # --- Remove categories with fewer than 1 entries ---
    class_counts = df["Category"].value_counts()
    valid_classes = class_counts[class_counts >= 1].index.tolist()
    df = df[df["Category"].isin(valid_classes)]

    filtered_out_classes = class_counts[class_counts < 2].index.tolist()

    # --- Fix Shift & Duration ---
    df["Shift_Type"] = df["Shift"].astype(str).str.extract(r'^(Day|Night)', expand=False)
    df["Duration_min"] = df["Duration"].astype(str).str.extract(r'(\d+)').astype(float)

    # --- Export cleaned data ---
    output_dir = tempfile.gettempdir()
    cleaned_output_path = os.path.join(output_dir, "final_cleaned_site_diary.csv")
    filtered_output_path = os.path.join(output_dir, "filtered_out_site_diary.csv")

    df.to_csv(cleaned_output_path, index=False, encoding='utf-8-sig')
    filtered_out_df.to_csv(filtered_output_path, index=False, encoding='utf-8-sig')

    # --- Return results for Dify ---
    return {
        "cleaned_file_path": cleaned_output_path,
        "filtered_file_path": filtered_output_path,
        "num_cleaned_rows": len(df),
        "num_filtered_rows": len(filtered_out_df),
        "categories_retained": valid_classes,
        "categories_removed": filtered_out_classes,
        "cleaned_df_dict": df.to_dict(orient='records'),
        "filtered_out_df_dict": filtered_out_df.to_dict(orient='records')
    }


# ---------------------------------------------------------------------------
# Local run (ignored by Render)
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)