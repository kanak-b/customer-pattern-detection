import os
import time
import pandas as pd
from config import CHUNK_SIZE, TRANSACTIONS_PATH, S3_BUCKET, S3_PREFIX

import boto3
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Initialize S3 client
s3 = boto3.client(
    "s3",
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key
)

# Initialize Spark
spark = SparkSession.builder.appName("pattern-detection").getOrCreate()


def stream_upload():
    print(" Stream Uploader started...")

    # Read the entire transactions file
    transactions_df = spark.read.csv(TRANSACTIONS_PATH, header=True, inferSchema=True)
    transactions_pd = transactions_df.toPandas()

    # Stream in chunks of CHUNK_SIZE
    for i in range(0, len(transactions_pd), CHUNK_SIZE):
        chunk = transactions_pd.iloc[i:i + CHUNK_SIZE]

        # Save locally
        local_path = f"data/raw/chunk_{i}.csv"
        chunk.to_csv(local_path, index=False)

        # Upload to S3
        s3_key = f"{S3_PREFIX}/chunk_{i}.csv"
        s3.upload_file(local_path, S3_BUCKET, s3_key)

        print(f"✅ Uploaded: s3://{S3_BUCKET}/{s3_key}")

        time.sleep(1)
