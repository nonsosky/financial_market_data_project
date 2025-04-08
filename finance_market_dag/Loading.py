from spark_utils import getSparkSession
from dotenv import load_dotenv
import psycopg2
import os

#Data Loading
def run_loading():
    try:

        #Initialize Spark Session
        spark = getSparkSession()

        dim_date_df = spark.read.option("header", True).csv("finance_market_dag/dataset/cleaned_data/date")
        dim_company_df = spark.read.option("header", True).csv("finance_market_dag/dataset/cleaned_data/company")
        fact_table = spark.option("header", True).csv("finance_market_dag/dataset/cleaned_data/fact_table")

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