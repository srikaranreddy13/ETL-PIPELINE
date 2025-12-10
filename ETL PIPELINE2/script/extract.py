import os
import pandas as pd

def extract_data():
    # Set directory structure
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  
    data_dir = os.path.join(base_dir, "data", "raw")
    os.makedirs(data_dir, exist_ok=True)

    # Load the Telco dataset (local file placed inside data/raw or project folder)
    input_file = os.path.join(base_dir, "WA_Fn-UseC_-Telco-Customer-Churn.csv")
    df = pd.read_csv(input_file)

    # Save raw dataset
    raw_path = os.path.join(data_dir, "telco_raw.csv")
    df.to_csv(raw_path, index=False)

    print(f"✅ Telco raw data extracted and saved at: {raw_path}")
    return raw_path


if __name__ == "__main__":
    extract_data()
