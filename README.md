# COVID-19 ETL Pipeline

A data engineering project that extracts real COVID-19 data,
transforms it, and loads it into a PostgreSQL database.

## Dataset
Source: Our World in Data (429,000+ rows)

## Tech Stack
- Python, pandas, SQLAlchemy, psycopg2, PostgreSQL

## How it works
1. **Extract** — Downloads CSV from Our World in Data
2. **Transform** — Cleans nulls, formats dates, adds 7-day rolling average
3. **Load** — Stores into PostgreSQL with duplicate handling

## How to run
1. Install dependencies: `pip install pandas sqlalchemy psycopg2-binary python-dotenv`
2. Create PostgreSQL database called `covid_db`
3. Add your credentials to `.env`
4. Run `python pipeline.py`

## Sample SQL Queries
```sql
-- Top 10 countries by total cases
SELECT location, MAX(total_cases) as peak_cases
FROM covid_data
GROUP BY location
ORDER BY peak_cases DESC
LIMIT 10;
```
