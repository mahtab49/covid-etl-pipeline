# extract.py

import pandas as pd

URL = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv"

def extract_data():
    try:
        df = pd.read_csv(URL)
        print(" Data extracted successfully")
        return df
    except Exception as e:
        print(" Extraction failed:", e)
        return None


if __name__ == "__main__":
    df = extract_data()
    print(df.head())