from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    DB_URI = (
        f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
        f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
    )
    return create_engine(DB_URI)

def create_table(engine):
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS covid_data (
                location TEXT,
                date DATE,
                total_cases FLOAT,
                new_cases FLOAT,
                total_deaths FLOAT,
                rolling_avg_cases FLOAT,
                PRIMARY KEY (location, date)
            )
        """))
        conn.commit()
    print("Table created/verified")

def load_data(df):
    try:
        engine = get_engine()
        create_table(engine)

        chunk_size = 5000
        total = len(df)
        loaded = 0

        with engine.connect() as conn:
            for i in range(0, total, chunk_size):
                chunk = df.iloc[i:i + chunk_size]

                # Convert chunk to list of dicts
                rows = chunk.to_dict(orient='records')

                # Insert and skip duplicates automatically
                conn.execute(text("""
                    INSERT INTO covid_data 
                        (location, date, total_cases, new_cases, total_deaths, rolling_avg_cases)
                    VALUES 
                        (:location, :date, :total_cases, :new_cases, :total_deaths, :rolling_avg_cases)
                    ON CONFLICT (location, date) DO NOTHING
                """), rows)

                conn.commit()
                loaded += len(chunk)
                print(f" Loaded {loaded}/{total} rows...")

        print(" All data loaded into PostgreSQL!")

    except Exception as e:
        print(" Load failed:", e)