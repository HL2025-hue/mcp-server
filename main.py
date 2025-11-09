from fastapi import FastAPI
from pydantic import BaseModel
from typing import Dict, Any
import pandas as pd
import os
import tempfile

app = FastAPI()

# Add a simple healthcheck route
@app.get("/")
def root():
    return {"message": "Preprocessing server is live ✅"}

# Input schema expected by Dify
class ToolInput(BaseModel):
    inputs: Dict[str, Any]

@app.post("/run")
def run_tool(request: ToolInput):
    inputs = request.inputs
    return process_site_diary(inputs)

# Main processing function
def process_site_diary(inputs: dict) -> dict:
    file_path = inputs["file_path"]
    df = pd.read_excel(file_path)

    df["Ignore Entry"] = df["Ignore Entry"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["Internal Use Only"] = df["Internal Use Only"].astype(str).str.lower().isin(["true", "1", "yes"])
    df = df[(df["Ignore Entry"] != True) & (df["Internal Use Only"] != True)]
    df = df.dropna(subset=["Description", "Category"])

    df_before_dedup = df.copy()
    df = df.drop_duplicates(subset=["From", "Until", "Ring", "Category", "Description"])

    filtered_out_df = pd.merge(
        df_before_dedup, df, how='outer', indicator=True
    ).query('_merge == "left_only"').drop(columns=['_merge'])

    class_counts = df["Category"].value_counts()
    valid_classes = class_counts[class_counts >= 2].index.tolist()
    df = df[df["Category"].isin(valid_classes)]

    unique_labels = sorted(df["Category"].unique())
    label2id = {label: idx for idx, label in enumerate(unique_labels)}
    id2label = {idx: label for label, idx in label2id.items()}
    filtered_out_classes = class_counts[class_counts < 2].index.tolist()

    df["Shift_Type"] = df["Shift"].astype(str).str.extract(r'^(Day|Night)', expand=False)
    df["Duration_min"] = df["Duration"].astype(str).str.extract(r'(\d+)').astype(float)

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

# Run with Uvicorn
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=False)