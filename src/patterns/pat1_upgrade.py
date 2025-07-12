from pyspark.sql import functions as F, Window

def detect(transactions, cust_imp, ist_time):
    transactions_clean = transactions.select(
        F.col("customer").alias("customerId"),
        F.col("merchant").alias("merchantId"),
        F.col("category").alias("transactionType")
    )

    cust_imp_clean = cust_imp.select(
        F.col("Source").alias("customerId"),
        F.col("Target").alias("merchantId"),
        F.col("Weight").cast("double").alias("weight"),
        F.col("typeTrans").alias("transactionType")
    )   
   
    # Step 1:
    # Total transactions per merchant
    merchant_txn_counts = transactions_clean.groupBy("merchantId").agg(
        F.count("*").alias("total_txns")
    )

    # filter merchants with ≥ 50k transactions
    eligible_merchants = merchant_txn_counts.filter("total_txns >= 50000")

    # Join to transactions to keep only eligible merchants
    filtered_txns = transactions_clean.join(
        eligible_merchants.select("merchantId"), on="merchantId", how="inner"
    )

    # Step 2:
    # Count of transactions per customer per merchant
    cust_txn_counts = filtered_txns.groupBy("merchantId", "customerId").agg(
        F.count("*").alias("txn_count")
    )

    # Window for percentile calculation per merchant
    txn_window = Window.partitionBy("merchantId").orderBy(F.desc("txn_count"))

    # Add raw percentile rank
    cust_txn_counts = cust_txn_counts.withColumn(
        "txn_percentile_rank_raw",
        F.percent_rank().over(txn_window)
    )

    # Round to 4 decimal places to remove scientific notation
    cust_txn_counts = cust_txn_counts.withColumn(
        "txn_percentile_rank", F.round("txn_percentile_rank_raw", 4)
    ).drop("txn_percentile_rank_raw")

    # Top 10% customers by txn count
    top_txn_customers = cust_txn_counts.filter(F.col("txn_percentile_rank") <= 0.1)
    # Step 3:
    # Join with cust_imp to get average weight
    joined = top_txn_customers.join(
        cust_imp_clean,
        on=["merchantId", "customerId"],
        how="inner"
    )

    avg_weight_df = joined.groupBy("merchantId", "customerId").agg(
        F.avg("weight").alias("avg_weight")
    )

    # Window to rank avg_weight per merchant
    weight_window = Window.partitionBy("merchantId").orderBy("avg_weight")

    avg_weight_df = avg_weight_df.withColumn(
        "weight_percentile_rank",
        F.percent_rank().over(weight_window)
    )

    # Bottom 10% by weight
    final_pat1 = avg_weight_df.filter("weight_percentile_rank <= 0.1")
    result_pat1 = final_pat1.withColumn("patternId", F.lit("PatId1")) \
        .withColumn("actionType", F.lit("UPGRADE")) \
        .withColumn("YStartTime", ist_time) \
        .withColumn("detectionTime", ist_time) \
        .withColumn("customerName", F.col("customerId")) \
        .select(
            "YStartTime", "detectionTime", "patternId", "actionType",
            "customerName", "merchantId"
        )

    return result_pat1
