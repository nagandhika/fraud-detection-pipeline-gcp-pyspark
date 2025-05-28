from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
# Step 1 = Start Spark Session
spark = SparkSession.builder.appName("FraudDetectionPipeline").getOrCreate()

# Step 2 = read csv from cloud storage
input_path = "gs://fraud-pipeline-staging-projectunique/onlinefraud.csv"
df = spark.read.option("header", True).csv(input_path).repartition(20)

# ✅ Step 3: Data Cleaning & Type Conversion
df = df.withColumn("amount", col("amount").cast("double")) \
       .withColumn("oldbalanceDest", col("oldbalanceDest").cast("double")) \
       .withColumn("newbalanceDest", col("newbalanceDest").cast("double"))

# ✅ Step 4: Fraud Detection Logic (simple rules)
df = df.withColumn("is_flagged_fraud", when(
    (col("amount") > 10000) &
    (col("oldbalanceDest") == 0) &
    (col("newbalanceDest") == 0), 1
).otherwise(0))

# ✅ Step 5: Write to BigQuery
df.write.format("bigquery") \
    .option("table", "vaulted-sector-461110-b7.fraud_detection.transactions_processed") \
    .option("temporaryGcsBucket", "fraud-pipeline-staging-projectunique") \
    .mode("overwrite") \
    .save()

print("✅ Data written to BigQuery with fraud flags.")
