from spark_utils import getSparkSession
import yfinance as yf

# Data Extraction
def run_extraction():
    try:

        # Initialize Spark session
        spark = getSparkSession()

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
        print("Data Extracted Successfully")
    except Exception as e:
        print("Data Extraction Failed: {e}")