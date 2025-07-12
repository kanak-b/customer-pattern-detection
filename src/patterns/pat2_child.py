from pyspark.sql import functions as F

# Thresholds
MIN_TXN_COUNT = 80
MAX_AVG_AMOUNT = 23

def detect(transactions_df, y_start_time, detection_time):
    """
    Pattern 2:
    Detect customers whose average transaction value for a given merchant is < Rs 23
    and who made at least 80 transactions. These are marked as CHILD customers.
    """

    # Group by customer-merchant to compute average amount and total transactions
    stats_df = transactions_df.groupBy("customer", "merchant").agg(
        F.avg("amount").alias("avg_amount"),
        F.count("*").alias("txn_count")
    )

    # Filter for CHILD pattern condition
    pat2_df = stats_df.filter(
        (F.col("avg_amount") < MAX_AVG_AMOUNT) &
        (F.col("txn_count") >= MIN_TXN_COUNT)
    )

    # Format output to match the standard schema
    result_df = pat2_df.selectExpr(
        f"'{y_start_time}' as YStartTime",
        f"'{detection_time}' as detectionTime",
        "'PatId2' as patternId",
        "'CHILD' as actionType",
        "customer as customerName",
        "merchant as merchantId"
    )

    return result_df
