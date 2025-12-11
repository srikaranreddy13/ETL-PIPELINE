# run_pipeline.py
from extract import fetch_all_cities
from transform import transform_raw_to_df, RAW_DIR, OUTPUT_CSV
from load import load_to_supabase
from etl_analysis import run_analysis
import pandas as pd

def run_pipeline():

    print("\n===== STEP 1: EXTRACT =====")
    extract_results = fetch_all_cities()

    # Check if at least one city succeeded
    success = any(r.get("success") == "true" for r in extract_results)
    if not success:
        print("❌ No valid raw data fetched. Aborting pipeline.")
        return

    print("\n===== STEP 2: TRANSFORM =====")
    df = transform_raw_to_df(RAW_DIR)
    if df.empty:
        print("❌ No transformed data generated. Aborting pipeline.")
        return

    df.to_csv(OUTPUT_CSV, index=False)
    print(f"✅ Transformed CSV saved at: {OUTPUT_CSV}")
    print(df.head())

    print("\n===== STEP 3: LOAD =====")
    load_to_supabase()

    print("\n===== STEP 4: ANALYSIS =====")
    try:
        run_analysis()
    except Exception as e:
        print("⚠️ Analysis failed:", e)

    print("\n🎉 ETL Pipeline Completed Successfully!\n")

if __name__ == "__main__":
    run_pipeline()
