# main.py
from fastapi import FastAPI, UploadFile
from fastapi.responses import JSONResponse
import pandas as pd
import tempfile
import os
import uvicorn

app = FastAPI()

@app.post("/process")
async def process_site_diary(file: UploadFile):
    # Save uploaded Excel to temp file
    contents = await file.read()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".xlsx") as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    # Load Excel
    df = pd.read_excel(tmp_path)

    # Convert flags
    df["Ignore Entry"] = df["Ignore Entry"].astype(str).str.lower().isin(["true", "1", "yes"])
    df["Internal Use Only"] = df["Internal Use Only"].astype(str).str.lower().isin(["true", "1", "yes"])
    df = df[(df["Ignore Entry"] != True) & (df["Internal Use Only"] != True)]

    # Drop missing or duplicate entries
    df = df.dropna(subset=["Description", "Category"])
    df_before_dedup = df.copy()
    df = df.drop_duplicates(subset=["From", "Until", "Ring", "Category", "Description"])
    filtered_out_df = pd.merge(df_before_dedup, df, how='outer', indicator=True).query('_merge == "left_only"').drop(columns=['_merge'])

    # Drop classes with <2 entries
    class_counts = df["Category"].value_counts()
    valid_classes = class_counts[class_counts >= 2].index.tolist()
    filtered_out_classes = class_counts[class_counts < 2].index.tolist()
    df = df[df["Category"].isin(valid_classes)]

    # Clean remaining columns
    df["Shift_Type"] = df["Shift"].astype(str).str.extract(r'^(Day|Night)', expand=False)
    df["Duration_min"] = df["Duration"].astype(str).str.extract(r'(\d+)').astype(float)

    return JSONResponse({
        "num_cleaned_rows": len(df),
        "num_filtered_rows": len(filtered_out_df),
        "categories_retained": valid_classes,
        "categories_removed": filtered_out_classes,
        "sample_cleaned": df.head(3).to_dict(orient='records'),
        "sample_filtered": filtered_out_df.head(3).to_dict(orient='records')
    })

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)