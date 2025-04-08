import os
from dotenv import load_dotenv
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, quarter, date_format
import pyspark
import psycopg2

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockData") \
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
    .config("spark.jars", "postgresql-42.7.4.jar") \
    .getOrCreate()

#Data Ingestion for YahooFinance

#Define Tickers and Company Names
tickers = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA", "IBM", "KO", "PEP", "ASML", "NVDA", "F", "INTC"]

company_names = {
    'AAPL': 'Apple Inc.',
    'GOOGL': 'Alphabet Inc. (Class A)',
    'MSFT': 'Microsoft Corporation',
    'AMZN': 'Amazon.com, Inc.',
    'TSLA': 'Tesla, Inc.',
    'IBM': 'International Business Machines Corp.',
    'KO': 'The Coca-Cola Company',
    'PEP': 'PepsiCo, Inc.',
    'ASML': 'ASML Holding N.V.',
    'NVDA': 'NVIDIA Corporation',
    'F': 'Ford Motor Company',
    'INTC': 'Intel Corporation'
}

#stock_data = yf.download(tickers, period="1y", interval="1d", group_by='ticker', threads=True)
stock_data = yf.download(tickers, period="max", group_by='ticker', threads=True)

#Process and convert to PySpark DataFrames
# Prepare a list to store dataframes
dataframes = []

for ticker in tickers:
    if ticker in stock_data.columns.levels[0]:  # Check if data exists
        data = stock_data[ticker].reset_index()
        data['Ticker'] = ticker
        data['Company'] = company_names[ticker]
        # Convert pandas DataFrame to PySpark
        stock_df = spark.createDataFrame(data)
        dataframes.append(stock_df)

# Merge all the dataframes into one
stock_data_df = dataframes[0]
for df in dataframes[1:]:
    stock_data_df = stock_data_df.unionByName(df)


stock_data_df.write.mode("overwrite").option("header", True).csv("dataset/merged_stock_data")


# stock_data_df.printSchema()
# stock_data_df.show(50)


# Data Transformation

# Check for Null values
for column in stock_data_df.columns:
    print(column, 'Nulls: ', stock_data_df.filter(stock_data_df[column].isNull()).count())

# Fill null values with defaults
for col_name, dtype in stock_data_df.dtypes:
    if dtype == "string":
        stock_data_df = stock_data_df.fillna({col_name: "Unknown"})
    elif dtype in ["double", "float"]:
        stock_data_df = stock_data_df.fillna({col_name: 0.0})
    elif dtype in ["int", "bigint"]:
        stock_data_df = stock_data_df.fillna({col_name: 0})

# Convert Date to Date datatype
stock_data_df = stock_data_df.withColumn("Date", pyspark.sql.functions.to_date(stock_data_df["Date"], "yyyy/M/d"))

# stock_data_df.printSchema()
# stock_data_df.show(50)

# Sort the entire DataFrame by the Date column in descending order
#stock_data_df = stock_data_df.orderBy(col("Date").desc())

# stock_data_df.printSchema()
# stock_data_df.show(50)

dim_company_df = stock_data_df.select('Ticker', 'Company').dropDuplicates()
# dim_company_df.show(50)

dim_date_df = stock_data_df.select("Date").distinct() \
    .withColumn("Year", year("Date")) \
    .withColumn("Month", month("Date")) \
    .withColumn("Quarter", quarter("Date")) \
    .withColumn("DayOfWeek", date_format("Date", "E")) \
    .withColumn("IsWeekend", date_format("Date", "u").cast("int") >= 6)

dim_date_df = dim_date_df.orderBy(col("Date").desc())

dim_date_df.printSchema()

fact_table = stock_data_df.join(dim_company_df.alias('c'), ['Ticker'], 'inner') \
    .join(dim_date_df.alias('d'), ['Date'], 'inner') \
    .select('Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume')

fact_table = fact_table.orderBy(col("Date").desc())

fact_table = fact_table.withColumn('Id', monotonically_increasing_id()) \
            .select('Id', 'Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume')

fact_table.printSchema()

try:
     
    # Develop a function to get the Database connection
    load_dotenv(override=True)

    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PORT = os.getenv("DB_PORT")
    DB_PASS = os.getenv("DB_PASS")
    DB_HOST = os.getenv("DB_HOSTS")

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

                            DROP TABLE IF EXISTS finance_market.company CASCADE;
                            DROP TABLE IF EXISTS finance_market.date CASCADE;
                            DROP TABLE IF EXISTS finance_market.fact_table CASCADE;


                            CREATE TABLE IF NOT EXISTS finance_market.company(
                                ticker VARCHAR(1000) PRIMARY KEY,
                                company VARCHAR(1000)
                            );

                            CREATE TABLE IF NOT EXISTS finance_market.date(
                                date DATE PRIMARY KEY,
                                year INT,
                                month INT,
                                quarter INT,
                                dayofweek VARCHAR(1000),
                                isweekend BOOLEAN
                            );

                            CREATE TABLE IF NOT EXISTS finance_market.fact_table(
                                id BIGINT PRIMARY KEY,
                                date DATE,
                                ticker VARCHAR(1000),
                                open DOUBLE PRECISION,
                                high DOUBLE PRECISION,
                                low DOUBLE PRECISION,
                                close DOUBLE PRECISION,
                                volume DOUBLE PRECISION,
                                FOREIGN KEY (date) REFERENCES finance_market.date(date),
                                FOREIGN KEY (ticker) REFERENCES finance_market.company(ticker)
                            );

                    '''
        cursor.execute(create_table_query)
        conn.commit()
        cursor.close()
        conn.close()

    create_tables()

    url = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
    properties = {
        "user": DB_USER,
        "password": DB_PASS,
        "driver": "org.postgresql.Driver"
    }
            
    dim_company_df.write.jdbc(url=url, table="finance_market.company",  mode="append", properties=properties)
    dim_date_df.write.jdbc(url=url, table="finance_market.date",  mode="append", properties=properties)
    fact_table.write.jdbc(url=url, table="finance_market.fact_table",  mode="append", properties=properties)
    print('database, table and data loaded successfully')
except Exception as e:
    print("Data loading Failed!", e)