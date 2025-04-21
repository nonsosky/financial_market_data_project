import os
from dotenv import load_dotenv
import yfinance as yf
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, quarter, date_format
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockData") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", "postgresql-42.7.4.jar") \
    .getOrCreate()

# Define Tickers and Company Names
tickers = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA", "META", "NVDA", "NFLX", "BRK-B", "JPM", "V", "UNH", "PG", "JNJ", "MA", "DIS", "XOM", "BAC", "HD", "PFE"]

company_names = {
    'AAPL': 'Apple Inc.',
    'MSFT': 'Microsoft Corporation',
    'GOOGL': 'Alphabet Inc.',
    'AMZN': 'Amazon.com, Inc.',
    'TSLA': 'Tesla, Inc.',
    'META': 'Meta Platforms, Inc.',
    'NVDA': 'NVIDIA Corporation',
    'NFLX': 'Netflix, Inc.',
    'BRK-B': 'Berkshire Hathaway Inc.',
    'JPM': 'JPMorgan Chase & Co.',
    'V': 'Visa Inc.',
    'UNH': 'UnitedHealth Group Incorporated',
    'PG': 'Procter & Gamble Co.',
    'JNJ': 'Johnson & Johnson',
    'MA': 'Mastercard Incorporated',
    'DIS': 'The Walt Disney Company',
    'XOM': 'Exxon Mobil Corporation',
    'BAC': 'Bank of America Corporation',
    'HD': 'The Home Depot, Inc.',
    'PFE': 'Pfizer Inc.'
}

pull_history_data = False

# Define directory
data_dir = "dataset/merged_stock_data"

if pull_history_data:
    print("Pulling full historical data...")
    stock_data = yf.download(tickers, period="max", group_by="ticker", threads=True)
else:
    today = datetime.today().strftime('%Y-%m-%d')
    print(f"Pulling today's data: {today}")
    stock_data = yf.download(tickers, start=today, end=today, group_by="ticker", threads=True)

# Convert to PySpark DataFrames
dataframes = []
for ticker in tickers:
    if ticker in stock_data.columns.levels[0]:
        df = stock_data[ticker].reset_index()
        if df.empty:
            continue
        df['Ticker'] = ticker
        df['Company'] = company_names[ticker]
        spark_df = spark.createDataFrame(df)
        dataframes.append(spark_df)

# Exit early if no data was collected
if not dataframes:
    print("No new stock data found for today on Yahoo Finance. Terminating script.")
    exit(0)

# Merge and save
merged_df = dataframes[0]
for df in dataframes[1:]:
    merged_df = merged_df.unionByName(df)

merged_df.write.mode("overwrite").parquet(data_dir)
print("Stock data saved to:", data_dir)

# Data Transformation
merged_df = spark.read.parquet("dataset/merged_stock_data")
merged_df.show(20)
merged_df.printSchema()

# # Convert Date to Date datatype
# Clean and cast the Date field from ISO format to proper DateType
merged_df = merged_df.withColumn("Date", col("Date").cast("date"))
merged_df.show(50)
merged_df.printSchema()

# Check for null values in each column
print("Null values per column:")
for column in merged_df.columns:
    null_count = merged_df.filter(col(column).isNull()).count()
    print(f"{column}: {null_count} null(s)")

#Fill null values with appropriate defaults
for col_name, dtype in merged_df.dtypes:
    if dtype == "string":
        merged_df = merged_df.fillna({col_name: "Unknown"})
    elif dtype in ["double", "float"]:
        merged_df = merged_df.fillna({col_name: 0.0})
    elif dtype in ["int", "bigint"]:
        merged_df = merged_df.fillna({col_name: 0})
    elif dtype == "date":
        # Optional: Set a default date (e.g., 1900-01-01)
        merged_df = merged_df.fillna({col_name: "1900-01-01"})
print("Null handling completed.")

merged_df.show(50)

# # stock_data_df.printSchema()
# # stock_data_df.show(50)

# dim_company_df = stock_datax.select('Ticker', 'Company').dropDuplicates()
# # dim_company_df.show(50)

# final_df = final_df.orderBy(col("Date").desc())

dim_date_df = merged_df.select("Date").distinct() \
    .withColumn("Year", year("Date")) \
    .withColumn("Month", month("Date")) \
    .withColumn("Quarter", quarter("Date")) \
    .withColumn("DayOfWeek", date_format("Date", "EEEE")) \
    .withColumn("IsWeekend", date_format("Date", "u").cast("int") >= 6)

# Join with final_df to enrich with date attributes
finance_market_df = merged_df.join(dim_date_df, on="Date", how="left")
finance_market_df.show(50)

# Sort the entire DataFrame by the Date column in descending order
finance_market_df = finance_market_df.orderBy(col("Date").desc())

# finance_market_df = finance_market_df.withColumn('Id', monotonically_increasing_id()) \
#             .select('Id', 'Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume', 'Year', 'Month', 'Quarter', 'DayOfWeek', 'IsWeekend')

finance_market_df.printSchema()
finance_market_df.show(100)

finance_market_df.write.mode("overwrite").parquet('dataset/cleaned_data/finance_cleaned_data')


try:
     
    load_finance_df = spark.read.parquet("dataset/cleaned_data/finance_cleaned_data")

    # Sort so the most recent dates are at the top
    load_finance_df = load_finance_df.orderBy(col("Date").desc())
    load_finance_df.show(50)
    load_finance_df.printSchema()

    for col_name in load_finance_df.columns:
        load_finance_df = load_finance_df.withColumnRenamed(col_name, col_name.lower())

    # Develop a function to get the Database connection
    load_dotenv(override=True)

    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PORT = os.getenv("DB_PORT")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOSTS")

    # JDBC Config
    url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver"
    }

    def get_db_connection():
        connection = psycopg2.connect(
        host=DB_HOST,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS,
        port=DB_PORT,
        options="-c search_path=finance_market"
        )
        return connection

    #connect to our database
    conn = get_db_connection()

    # Create a function create tables
    def create_tables():
        conn = get_db_connection()
        cursor = conn.cursor()
        create_table_query = '''

                            CREATE SCHEMA IF NOT EXISTS finance_market;

                            CREATE TABLE IF NOT EXISTS finance_market.finance_market_tbl(
                                id BIGSERIAL PRIMARY KEY,
                                date DATE,
                                ticker VARCHAR(1000),
                                open DOUBLE PRECISION,
                                high DOUBLE PRECISION,
                                low DOUBLE PRECISION,
                                close DOUBLE PRECISION,
                                volume DOUBLE PRECISION,
                                year INT,
                                month INT,
                                quarter INT,
                                dayofweek VARCHAR(1000),
                                isweekend BOOLEAN,
                                UNIQUE (ticker, date)
                            );

                            DROP TABLE IF EXISTS finance_market.temp_finance_market_tbl;

                            CREATE TABLE finance_market.temp_finance_market_tbl AS
                            SELECT * FROM finance_market.finance_market_tbl WHERE 1=0;

                    '''

        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()

    # Create tables
    create_tables()

    # Drop duplicates from DataFrame
    load_finance_df = load_finance_df.dropDuplicates(["ticker", "date"])

    # Write to staging table
    load_finance_df.write.jdbc(
        url=url,
        table="finance_market.temp_finance_market_tbl",
        mode="overwrite",
        properties=properties
    )

    # Merge into main table, avoiding duplicates
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO finance_market.finance_market_tbl (
            date, ticker, open, high, low, close, volume, year, month, quarter, dayofweek, isweekend
        )
        SELECT 
            date, ticker, open, high, low, close, volume, year, month, quarter, dayofweek, isweekend
        FROM finance_market.temp_finance_market_tbl
        ON CONFLICT (ticker, date)
        DO NOTHING;
    ''')

    conn.commit()
    cursor.close()
    conn.close()

    print('Database, table, and data loaded successfully (deduplicated by ticker and date)')

except Exception as e:
    print("Data loading failed!", e)