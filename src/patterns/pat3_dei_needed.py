from pyspark.sql import functions as F

MIN_F_COUNT = 100

def detect(transactions, y_start_time, detection_time):
    gender_df = transactions.select(
        F.expr("substring(merchant, 2, length(merchant) - 2)").alias("merchantId"),
        F.expr("substring(customer, 2, length(customer) - 2)").alias("customerId"),
        F.expr("substring(gender, 2, 1)").alias("gender")
    )

    gender_clean_check = gender_df.groupBy("merchantId", "customerId") \
        .agg(F.countDistinct("gender").alias("gender_type_count"))

    clean_customers = gender_clean_check.filter("gender_type_count = 1") \
        .select("merchantId", "customerId")

    gender_cleaned = gender_df.join(clean_customers, on=["merchantId", "customerId"], how="inner")

    unique_pairs = gender_cleaned.select("merchantId", "customerId", "gender").distinct()

    gender_counts = unique_pairs.groupBy("merchantId") \
        .pivot("gender", ["F", "M"]).count().fillna(0)

    # Optional: cast for clarity
    gender_counts = gender_counts.withColumn("F", F.col("F").cast("int")) \
                                 .withColumn("M", F.col("M").cast("int"))

    dei_merchants = gender_counts.filter(
        (F.col("F") > MIN_F_COUNT) & (F.col("F") < F.col("M"))
    )

    pat3_df = dei_merchants \
        .withColumn("patternId", F.lit("PatId3")) \
        .withColumn("actionType", F.lit("DEI-NEEDED")) \
        .withColumn("YStartTime", F.lit(y_start_time)) \
        .withColumn("detectionTime", F.lit(detection_time)) \
        .withColumn("customerName", F.lit("")) \
        .select(
            "YStartTime", "detectionTime", "patternId", "actionType",
            "customerName", "merchantId"
        )

    return pat3_df
