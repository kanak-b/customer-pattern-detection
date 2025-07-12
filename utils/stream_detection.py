import os
import time
import io
import pandas as pd
import boto3
from dotenv import load_dotenv
from pyspark.sql import SparkSession

from src.pattern_runner import run_pattern_detections
from config import (
    CHUNK_SIZE, S3_BUCKET, S3_PREFIX, CUST_IMP_PATH
)

# Load AWS credentials
load_dotenv()
access_key = os.getenv("AWS_ACCESS_KEY_ID")
secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

# Constants
bucket = S3_BUCKET
prefix = S3_PREFIX


def consume_stream_detect_patterns():
    print("👂 Consumer started...")

    # Spark and S3 clients
    spark = SparkSession.builder.appName("pattern-detection").getOrCreate()
    s3 = boto3.client("s3", aws_access_key_id=access_key, aws_secret_access_key=secret_key)

    # Load customer importance dataset
    cust_imp = spark.read.csv(CUST_IMP_PATH, header=True, inferSchema=True)

    expected_chunk = 0
    total_rows = 0
    detection_file_index = 0
    detection_buffer_df = pd.DataFrame()

    while True:
        s3_key = f"{prefix}/chunk_{expected_chunk}.csv"

        try:
            # Step 1: Read chunk from S3
            response = s3.get_object(Bucket=bucket, Key=s3_key)
            content = response['Body'].read().decode("utf-8")
            chunk_df = pd.read_csv(io.StringIO(content))

            print(f"📥 Processed: {s3_key}, rows = {len(chunk_df)}")
            total_rows += len(chunk_df)

            # Step 2: Convert to Spark DataFrame
            spark_df = spark.createDataFrame(chunk_df)

            # Step 3: Run pattern detection
            detection_df = run_pattern_detections(spark_df, cust_imp)

            # Step 4: Append detections to buffer
            detection_pd = detection_df.toPandas()
            detection_buffer_df = pd.concat([detection_buffer_df, detection_pd], ignore_index=True)

            # Step 5: Write full batches of 50 rows
            while len(detection_buffer_df) >= 50:
                batch_df = detection_buffer_df.iloc[:50]
                detection_buffer_df = detection_buffer_df.iloc[50:]

                csv_buffer = io.StringIO()
                batch_df.to_csv(csv_buffer, index=False)

                detection_key = (
                    f"{prefix.replace('transaction_chunks', 'output_detections')}/"
                    f"detection_{detection_file_index}.csv"
                )
                s3.put_object(Bucket=bucket, Key=detection_key, Body=csv_buffer.getvalue())
                print(f"✅ Wrote detection batch: {detection_key}")
                detection_file_index += 1

            expected_chunk += CHUNK_SIZE  # If using i as 0, 100, 200, keep this as 100

        except s3.exceptions.NoSuchKey:
            # Optional: Check if this is likely the final chunk
            if expected_chunk >= 1000:  # Or some known upper limit
                break
            time.sleep(0.5)

        except Exception as e:
            print(f"❌ Error processing {s3_key}: {e}")

    # Step 6: Flush remaining <50 rows at the end
    if not detection_buffer_df.empty:
        csv_buffer = io.StringIO()
        detection_buffer_df.to_csv(csv_buffer, index=False)

        detection_key = (
            f"{prefix.replace('transaction_chunks', 'output_detections')}/"
            f"detection_{detection_file_index}.csv"
        )
        s3.put_object(Bucket=bucket, Key=detection_key, Body=csv_buffer.getvalue())
        print(f"🟡 Final flush: {detection_key} with {len(detection_buffer_df)} rows")

    print(f"✅ Consumer finished processing {total_rows} rows.")
