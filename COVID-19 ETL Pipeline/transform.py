import pandas as pd

def transform_data(df):
    print(" Transforming data...")

    # Keep only useful columns
    df = df[['location', 'date', 'total_cases', 'new_cases', 'total_deaths']]

    # Drop rows where location is missing
    df = df.dropna(subset=['location'])

    # Fill missing numbers with 0
    df['total_cases'] = df['total_cases'].fillna(0)
    df['new_cases'] = df['new_cases'].fillna(0)
    df['total_deaths'] = df['total_deaths'].fillna(0)

    # Convert date to proper format
    df['date'] = pd.to_datetime(df['date'])

    # Sort by location and date
    df = df.sort_values(['location', 'date'])

    # 7-day rolling average (feature engineering)
    df['rolling_avg_cases'] = (
        df.groupby('location')['new_cases']
        .transform(lambda x: x.rolling(7).mean())
    )

    df['rolling_avg_cases'] = df['rolling_avg_cases'].fillna(0)

    print(f"Transformation complete — {len(df)} rows ready")
    return df

if __name__ == "__main__":
    from extract import extract_data
    df = extract_data()
    df = transform_data(df)
    print(df.head(10))