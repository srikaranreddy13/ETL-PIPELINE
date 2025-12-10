

import os
import pandas as pd
from supabase import create_client
from dotenv import load_dotenv


def get_supabase_client():
    load_dotenv()
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise ValueError("❌ Missing Supabase credentials")
    return create_client(url, key)


def validate_data():
    print("\n🔍 Starting Data Validation...\n")

    # -------------------------------
    # 1️⃣ Load processed (staged) CSV
    # -------------------------------
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    staged_file = os.path.join(base_dir, "data", "staged", "churn_staged.csv")

    if not os.path.exists(staged_file):
        raise FileNotFoundError("❌ Staged CSV not found")

    df = pd.read_csv(staged_file)
    original_row_count = len(df)

    # -------------------------------
    # 2️⃣ Check Missing Values
    # -------------------------------
    required_cols = ["tenure", "MonthlyCharges", "TotalCharges"]

    missing_check = {col: df[col].isna().sum() for col in required_cols}

    # -------------------------------
    # 3️⃣ Fetch Supabase row count
    # -------------------------------
    supabase = get_supabase_client()
    result = supabase.table("churn_data").select("*", count="exact").execute()

    supabase_row_count = result.count

    # -------------------------------
    # 4️⃣ Check segment categories
    # -------------------------------
    required_tenure_groups = {"New", "Regular", "Loyal", "Champion"}
    required_charge_segments = {"Low", "Medium", "High"}

    tenure_group_ok = required_tenure_groups.issubset(set(df["tenure_group"]))
    monthlycharge_group_ok = required_charge_segments.issubset(
        set(df["MonthlyCharges_group"])
    )

    # -------------------------------
    # 5️⃣ Contract type code check
    # -------------------------------
    valid_contract_codes = {0, 1, 2}
    invalid_contract_codes = set(df["contract_type_code"]) - valid_contract_codes

    # -------------------------------
    # 6️⃣ Print Summary
    # -------------------------------
    print("📌 VALIDATION SUMMARY")
    print("-------------------------------")

    # Missing values
    print("\n1️⃣ Missing Value Check:")
    for col, miss in missing_check.items():
        print(f"   • {col}: {miss} missing")

    # Row count
    print("\n2️⃣ Row Count Check:")
    print(f"   • Staged CSV rows  : {original_row_count}")
    print(f"   • Supabase rows    : {supabase_row_count}")
    if original_row_count == supabase_row_count:
        print("   ✔ Row counts match")
    else:
        print("   ❌ Row count mismatch")

    # Segment checks
    print("\n3️⃣ Segment Category Check:")
    print(f"   • tenure_group OK? → {tenure_group_ok}")
    print(f"   • MonthlyCharges_group OK? → {monthlycharge_group_ok}")

    # Contract codes
    print("\n4️⃣ Contract Code Check:")
    if len(invalid_contract_codes) == 0:
        print("   ✔ All contract_type_code values valid (0,1,2)")
    else:
        print(f"   ❌ Invalid contract_type_code values: {invalid_contract_codes}")

    print("\n🎉 Validation Completed!\n")


if __name__ == "__main__":
    validate_data()
