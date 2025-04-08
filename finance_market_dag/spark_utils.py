from pyspark.sql import SparkSession

#Initialize Spark Session
def getSparkSession():
    return SparkSession.builder \
        .appName('Finance_Market') \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .config("spark.jars", "finance_market_dag/postgresql-42.7.4.jar") \
        .getOrCreate()