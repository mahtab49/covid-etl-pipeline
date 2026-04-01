from extract import extract_data
from transform import transform_data
from load import load_data

def run_pipeline():
    print(" Starting COVID-19 ETL Pipeline...")

    df = extract_data()
    if df is None:
        print(" Pipeline stopped — extraction failed")
        return

    df = transform_data(df)
    load_data(df)

    print(" Pipeline finished successfully!")

if __name__ == "__main__":
    run_pipeline()