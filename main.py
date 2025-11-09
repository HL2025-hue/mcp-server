from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict
import pandas as pd
import os
import tempfile
import traceback
import requests
import io

app = FastAPI()

# Health check route
@app.get("/")
def root():
    return {"message": "Preprocessing server is live ✅"}

# Input schema for the API
class ToolInput(BaseModel):
    file_path: str

# Route to process the diary file
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

# Load file based on content type (not just file extension)
def load_file(file_path: str) -> pd.DataFrame:
    response = requests.get(file_path)
    response.raise_for_status()

    content_type = response.headers.get("Content-Type", "")

    if "text/csv" in content_type:
        return pd.read_csv(io.BytesIO(response.content))
    elif "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" in content_type:
        return pd.read_excel(io.BytesIO(response.content), engine="openpyxl")
    else:
        raise ValueError(f"Unsupported file type: {content_type}")

# Core processing function
def process_site_diary(file_path: str) -> dict:
    df = load_file(file_path)

    # Convert flags to booleans
    df["Ignore Entry"] = df["Ignore Entry"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["Internal Use Only"] = df["Internal Use Only"].astype(str).str.lower().isin(["true", "1", "yes"])

    # Remove flagged entries
    df = df[(df["Ignore Entry"] != True) & (df["Internal Use Only"] != True)]

    # Drop rows with missing description or category
    df = df.dropna(subset=["Description", "Category"])

    # Drop duplicate entries
    df_before_dedup = df.copy()
    df = df.drop_duplicates(subset=["From", "Until", "Ring", "Category", "Description"])

    # Get filtered-out duplicates
    filtered_out_df = pd.merge(
        df_before_dedup,
        df,
        how='outer',
        indicator=True
    ).query('_merge == "left_only"').drop(columns=['_merge'])

    # Remove classes with <2 examples
    class_counts = df["Category"].value_counts()
    valid_classes = class_counts[class_counts >= 2].index.tolist()
    df = df[df["Category"].isin(valid_classes)]

    # Store removed classes
    filtered_out_classes = class_counts[class_counts < 2].index.tolist()

    # Fix shift column
    df["Shift_Type"] = df["Shift"].astype(str).str.extract(r'^(Day|Night)', expand=False)

    # Convert duration to numeric
    df["Duration_min"] = df["Duration"].astype(str).str.extract(r'(\d+)').astype(float)

    # Export cleaned data to temp files
    output_dir = tempfile.gettempdir()
    cleaned_output_path = os.path.join(output_dir, "final_cleaned_site_diary.csv")
    filtered_output_path = os.path.join(output_dir, "filtered_out_site_diary.csv")

    df.to_csv(cleaned_output_path, index=False, encoding='utf-8-sig')
    filtered_out_df.to_csv(filtered_output_path, index=False, encoding='utf-8-sig')

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

# For local testing
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)