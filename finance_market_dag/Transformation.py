from spark_utils import getSparkSession
from pyspark.sql.functions import col, year, month, quarter, date_format


# Data Transformation
def run_transformation():
    try:

        # Initialize Spark Session
        spark = getSparkSession()

        # Data Transformation
        merged_df = spark.read.parquet("/home/kenneth/airflow/finance_market_dag/dataset/merged_stock_data")
        merged_df.show(20)

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

        finance_market_df.printSchema()
        finance_market_df.show(100)

        finance_market_df.write.mode("overwrite").parquet('/home/kenneth/airflow/finance_market_dag/dataset/cleaned_data/finance_cleaned_data')

        print("Transformation Completed Successfully")
    except Exception as e:
        print("Transformation Failed: ", e)
