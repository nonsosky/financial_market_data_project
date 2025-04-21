from spark_utils import getSparkSession
import yfinance as yf
from airflow.exceptions import AirflowSkipException
from datetime import datetime

# Data Extraction
def run_extraction():
    try:

        # Initialize Spark session
        spark = getSparkSession()

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
        data_dir = "/home/kenneth/airflow/finance_market_dag/dataset/merged_stock_data"

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
            print("No new stock data found for today on Yahoo Finance. Skipping downstream tasks.")
            raise AirflowSkipException("No data available. Skipping pipeline.")

        # Merge and save
        merged_df = dataframes[0]
        for df in dataframes[1:]:
            merged_df = merged_df.unionByName(df)

        merged_df.write.mode("overwrite").parquet(data_dir)
        print("Stock data saved to:", data_dir)
        print("Extarction Layer Completed Successfully")
    except Exception as e:
        print("Data Extraction Failed: ", e)
        raise AirflowSkipException(f"Extraction failed: {e}")