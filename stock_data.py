import os
import dotenv
import yfinance as yf
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql.functions import monotonically_increasing_id
import pandas as pd
import pyspark
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base

# Initialize Spark session
spark = SparkSession.builder.appName("StockData").getOrCreate()

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

# Step 6: Save as Parquet
stock_data_df.write.mode("overwrite").option("header", True).csv("dataset/merged_stock_data")

# Step 7: Inspect result
stock_data_df.printSchema()
stock_data_df.show(50)


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

stock_data_df.printSchema()
stock_data_df.show(50)

# Adding a unique ID column to the DF
# cleaned_data = stock_data_df.select('Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker') \
#                         .withColumn('Id', monotonically_increasing_id()) \
#                         .select('Id', 'Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Ticker')

# cleaned_data.show(20)

# Data Loading
#Loading Layer
# Base = declarative_base()

# class Stock_Price(Base):
#     __tablename__ = 'new_stock'

#     Id = Column(Integer, primary_key=True)
#     Date = Column(DateTime)
#     Open = Column(Float)
#     High = Column(Float)
#     Low = Column(Float)
#     Close = Column(Float)
#     Volume = Column(Integer)
#     Ticker = Column(String)

# DB_NAME = os.getenv("DB_NAME")
# DB_USER = os.getenv("DB_USER")
# DB_PORT = os.getenv("DB_PORT")
# DB_PASS = os.getenv("DB_PASS")
# DB_HOST = os.getenv("DB_HOSTS")

# dotenv.load_dotenv()
# engine = create_engine(f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}/{DB_NAME}')

# Session = sessionmaker(bind=engine)
# session = Session()
# Base.metadata.create_all(engine)

# data_to_insert = cleaned_data.to_dict(orient='records')
# session.bulk_insert_mappings(Stock_Price, data_to_insert)
# session.commit()
# session.close()