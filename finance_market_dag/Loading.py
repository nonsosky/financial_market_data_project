from spark_utils import getSparkSession
from dotenv import load_dotenv
from pyspark.sql.functions import col
import psycopg2
import os

#Data Loading
def run_loading():
    try:

        #Initialize Spark Session
        spark = getSparkSession()
     
        load_finance_df = spark.read.parquet("/home/kenneth/airflow/finance_market_dag/dataset/cleaned_data/finance_cleaned_data")

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