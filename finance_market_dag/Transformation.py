from spark_utils import getSparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, quarter, date_format
import pyspark

# Data Transformation
def run_transformation():
    try:

        # Initialize Spark Session
        spark = getSparkSession()

        stock_data_df = spark.read.option("header", True).csv("finance_market_dag/dataset/merged_data")

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

        # Create company table
        dim_company_df = stock_data_df.select('Ticker', 'Company').dropDuplicates()

        #Create Date Dim Table
        dim_date_df = stock_data_df.select("Date").distinct() \
            .withColumn("Year", year("Date")) \
            .withColumn("Month", month("Date")) \
            .withColumn("Quarter", quarter("Date")) \
            .withColumn("DayOfWeek", date_format("Date", "E")) \
            .withColumn("IsWeekend", date_format("Date", "u").cast("int") >= 6)

        dim_date_df = dim_date_df.orderBy(col("Date").desc())

        # Fact table
        fact_table = stock_data_df.join(dim_company_df.alias('c'), ['Ticker'], 'inner') \
            .join(dim_date_df.alias('d'), ['Date'], 'inner') \
            .select('Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume')

        fact_table = fact_table.orderBy(col("Date").desc())

        fact_table = fact_table.withColumn('Id', monotonically_increasing_id()) \
                    .select('Id', 'Date', 'Ticker', 'Open', 'High', 'Low', 'Close', 'Volume')
        
        # Save Tables to Cleaned_data folder
        dim_date_df = spark.write.mode("overwrite").option("header", True).csv("finance_market_dag/dataset/cleaned_data/date")
        dim_company_df = spark.write.mode("overwrite").option("header", True).csv("finance_market_dag/dataset/cleaned_data/company")
        fact_table = spark.write.mode("overwrite").option("header", True).csv("finance_market_dag/dataset/cleaned_data/fact_table")
        print("Transformation Completed Successfully")
    except Exception as e:
        print("Transformation Failed: {e}")