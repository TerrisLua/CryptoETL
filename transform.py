from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, stddev
import psycopg2
import pandas as pd
from pyspark.sql import functions as F
from dotenv import load_dotenv
import os



def transform_data():
    # Initialize Spark session
    spark = SparkSession.builder.appName("CryptoDataTransformation").getOrCreate()

    load_dotenv()

    db_user = os.getenv('DB_USER')
    db_password = os.getenv('DB_PASSWORD')
    
    # Load the JSON data
    df = spark.read.json("crypto_data.json")

    df.show()
    df = df.withColumn("percent_change_24h", col("percent_change_24h").cast("double"))  
    df = df.withColumn("percent_change_1h", col("percent_change_1h").cast("double"))
    df = df.withColumn("price", col("price").cast("double"))

    # Get the cryptocurrency with the highest percentage change
    trend_df = df.orderBy(col("percent_change_24h").desc())
    pandas_df = trend_df.toPandas()

    conn = psycopg2.connect(
    dbname="crypto_raw",
    user=db_user,
    password=db_password,
    host="localhost",
    port="5432"
    )
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS crypto_trends;")

    cursor.execute("""
            CREATE TABLE IF NOT EXISTS crypto_trends (
                name VARCHAR(50),
                symbol VARCHAR(10),
                price FLOAT,
                percent_change_1h FLOAT,
                percent_change_24h FLOAT,
                date TIMESTAMP
            )
        """)
    
    cursor.execute("DELETE FROM crypto_trends")

    # Insert data into PostgreSQL
    for index, row in pandas_df.iterrows():
        cursor.execute("""
            INSERT INTO crypto_trends (name, symbol, price, percent_change_1h, percent_change_24h, date)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (row['name'], row['symbol'], row['price'], row['percent_change_1h'], row['percent_change_24h'], row['date']))

  
    # Commit and close connection
    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == "__main__":
    transform_data()
