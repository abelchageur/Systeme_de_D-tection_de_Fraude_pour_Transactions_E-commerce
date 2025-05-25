# #!/usr/bin/env python
# # coding: utf-8

# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
# from pyspark.sql.functions import col, to_timestamp, dayofmonth, dayofweek, month, year, monotonically_increasing_id, date_format, when
# from datetime import datetime

# # Initialize Spark Session
# spark = SparkSession.builder \
#     .appName("FraudDetection") \
#     .master("local[*]") \
#     .config("spark.sql.debug.maxToStringFields", "100") \
#     .config("spark.driver.memory", "4g") \
#     .config("spark.executor.memory", "4g") \
#     .config("spark.driver.maxResultSize", "2g") \
#     .getOrCreate()

# print("Spark session created successfully!")

# # Define schema for raw data
# schema = StructType([
#     StructField("Transaction_ID", StringType(), True),
#     StructField("Customer_ID", StringType(), True),
#     StructField("Transaction_Amount", FloatType(), True),
#     StructField("Transaction_Date", StringType(), True),
#     StructField("Payment_Method", StringType(), True),
#     StructField("Product_Category", StringType(), True),
#     StructField("Quantity", IntegerType(), True),
#     StructField("Customer_Age", IntegerType(), True),
#     StructField("Customer_Location", StringType(), True),
#     StructField("Device_Used", StringType(), True),
#     StructField("IP_Address", StringType(), True),
#     StructField("Shipping_Address", StringType(), True),
#     StructField("Billing_Address", StringType(), True),
#     StructField("Is_Fraudulent", IntegerType(), True),
#     StructField("Account_Age_Days", IntegerType(), True),
#     StructField("Transaction_Hour", IntegerType(), True)
# ])

# # Load data
# try:
#     file_path = "transaction_data.csv"
#     df = spark.read.option("header", "true") \
#                    .option("multiLine", "true") \
#                    .schema(schema) \
#                    .csv(file_path)
# except:
#     today = datetime.today()
#     hdfs_path = f"hdfs://namenode:9000/user/root/transactions/YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}/transaction_data.csv"
#     df = spark.read.option("header", "true") \
#                    .option("multiLine", "true") \
#                    .schema(schema) \
#                    .csv(hdfs_path)

# print("Data loaded successfully. Row count:", df.count())

# # Convert Transaction Date to timestamp and extract time dimensions
# df = df.withColumn("Transaction_Timestamp", to_timestamp(col("Transaction_Date"))) \
#        .withColumn("Transaction_DayOfWeek", dayofweek("Transaction_Timestamp")) \
#        .withColumn("Transaction_Month", month("Transaction_Timestamp")) \
#        .withColumn("Transaction_Year", year("Transaction_Timestamp"))

# # Define age group UDF
# def age_group(age):
#     if age is None:
#         return "Unknown"
#     elif age < 20:
#         return "Under 20"
#     elif age < 30:
#         return "20-29"
#     elif age < 40:
#         return "30-39"
#     elif age < 50:
#         return "40-49"
#     else:
#         return "50+"

# from pyspark.sql.functions import udf
# age_group_udf = udf(age_group, StringType())
# df = df.withColumn("Age_Group", age_group_udf(col("Customer_Age")))

# # Create dimension tables
# dim_customer = df.select(
#     col("Customer_ID").alias("Customer_ID"),
#     col("Customer_Age"),
#     col("Customer_Location"),
#     col("Account_Age_Days"),
#     col("Age_Group")
# ).dropDuplicates()

# dim_payment = df.select(
#     col("Payment_Method").alias("Payment_Method")
# ).dropDuplicates() \
#  .withColumn("Payment_Method_ID", monotonically_increasing_id())

# dim_product = df.select(
#     col("Product_Category").alias("Product_Category")
# ).dropDuplicates() \
#  .withColumn("Product_Category_ID", monotonically_increasing_id())

# dim_device = df.select(
#     col("Device_Used").alias("Device_Type")
# ).dropDuplicates() \
#  .withColumn("Device_ID", monotonically_increasing_id())

# dim_time = df.select(
#     col("Transaction_Timestamp").alias("Time_ID"),
#     col("Transaction_Hour"),
#     col("Transaction_DayOfWeek"),
#     col("Transaction_Month"),
#     col("Transaction_Year")
# ).dropDuplicates()

# # Create fact table
# fact_df = df.join(dim_payment, df["Payment_Method"] == dim_payment["Payment_Method"], "left") \
#             .join(dim_product, df["Product_Category"] == dim_product["Product_Category"], "left") \
#             .join(dim_device, df["Device_Used"] == dim_device["Device_Type"], "left")

# fact_transactions = fact_df.select(
#     col("Transaction_ID"),
#     col("Customer_ID"),
#     col("Transaction_Amount"),
#     col("Quantity"),
#     when(col("Is_Fraudulent") == 1, "true").when(col("Is_Fraudulent") == 0, "false").otherwise("false").alias("Is_Fraudulent"),
#     date_format(col("Transaction_Timestamp"), "yyyy-MM-dd HH:mm:ss").alias("Transaction_Timestamp"),
#     col("Payment_Method_ID").cast("string"),
#     col("Product_Category_ID").cast("string"),
#     col("Device_ID").cast("string")
# )

# # Save to HDFS
# today = datetime.today()
# base_path = "hdfs://namenode:9000/user/root/datalake"
# date_path = f"YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}"

# fact_path = f"{base_path}/fact_transactions/{date_path}"
# customers_path = f"{base_path}/dim_customers/{date_path}"
# payments_path = f"{base_path}/dim_payment_methods/{date_path}"
# products_path = f"{base_path}/dim_product_categories/{date_path}"
# devices_path = f"{base_path}/dim_devices/{date_path}"
# time_path = f"{base_path}/dim_time/{date_path}"

# fact_transactions.write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(fact_path)

# dim_customer.write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(customers_path)

# dim_payment.write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(payments_path)

# dim_product.write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(products_path)

# dim_device.write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(devices_path)

# dim_time.write.mode("overwrite") \
#     .option("header", "true") \
#     .csv(time_path)

# print("Data saved to HDFS successfully!")

# # Stop Spark session
# spark.stop()
# # python -m nbconvert --to script modelisation.ipynb
#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, to_timestamp, dayofmonth, dayofweek, month, year, monotonically_increasing_id, date_format, when
from datetime import datetime

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.driver.memory", "4g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.maxResultSize", "2g") \
    .getOrCreate()

print("Spark session created successfully!")

# Define schema for raw data
schema = StructType([
    StructField("Transaction_ID", StringType(), True),
    StructField("Customer_ID", StringType(), True),
    StructField("Transaction_Amount", FloatType(), True),
    StructField("Transaction_Date", StringType(), True),
    StructField("Payment_Method", StringType(), True),
    StructField("Product_Category", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Customer_Age", IntegerType(), True),
    StructField("Customer_Location", StringType(), True),
    StructField("Device_Used", StringType(), True),
    StructField("IP_Address", StringType(), True),
    StructField("Shipping_Address", StringType(), True),
    StructField("Billing_Address", StringType(), True),
    StructField("Is_Fraudulent", IntegerType(), True),
    StructField("Account_Age_Days", IntegerType(), True),
    StructField("Transaction_Hour", IntegerType(), True)
])

# Load data
try:
    file_path = "transaction_data.csv"
    df = spark.read.option("header", "true") \
                   .option("multiLine", "true") \
                   .schema(schema) \
                   .csv(file_path)
except:
    today = datetime.today()
    hdfs_path = f"hdfs://namenode:9000/user/root/transactions/YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}/transaction_data.csv"
    df = spark.read.option("header", "true") \
                   .option("multiLine", "true") \
                   .schema(schema) \
                   .csv(hdfs_path)

print("Data loaded successfully. Row count:", df.count())

# Convert Transaction Date to timestamp and extract time dimensions
df = df.withColumn("Transaction_Timestamp", to_timestamp(col("Transaction_Date"))) \
       .withColumn("Transaction_DayOfWeek", dayofweek("Transaction_Timestamp")) \
       .withColumn("Transaction_Month", month("Transaction_Timestamp")) \
       .withColumn("Transaction_Year", year("Transaction_Timestamp"))

# Define age group UDF
def age_group(age):
    if age is None:
        return "Unknown"
    elif age < 20:
        return "Under 20"
    elif age < 30:
        return "20-29"
    elif age < 40:
        return "30-39"
    elif age < 50:
        return "40-49"
    else:
        return "50+"

from pyspark.sql.functions import udf
age_group_udf = udf(age_group, StringType())
df = df.withColumn("Age_Group", age_group_udf(col("Customer_Age")))

# Create dimension tables
dim_customer = df.select(
    col("Customer_ID").alias("Customer_ID"),
    col("Customer_Age"),
    col("Customer_Location"),
    col("Account_Age_Days"),
    col("Age_Group")
).dropDuplicates()

dim_payment = df.select(
    col("Payment_Method").alias("Payment_Method")
).dropDuplicates() \
 .withColumn("Payment_Method_ID", monotonically_increasing_id())

dim_product = df.select(
    col("Product_Category").alias("Product_Category")
).dropDuplicates() \
 .withColumn("Product_Category_ID", monotonically_increasing_id())

dim_device = df.select(
    col("Device_Used").alias("Device_Type")
).dropDuplicates() \
 .withColumn("Device_ID", monotonically_increasing_id())

dim_time = df.select(
    date_format(col("Transaction_Timestamp"), "yyyy-MM-dd HH:mm:ss").alias("Time_ID"),
    col("Transaction_Hour"),
    col("Transaction_DayOfWeek"),
    col("Transaction_Month"),
    col("Transaction_Year")
).dropDuplicates()

# Create fact table
fact_df = df.join(dim_payment, df["Payment_Method"] == dim_payment["Payment_Method"], "left") \
            .join(dim_product, df["Product_Category"] == dim_product["Product_Category"], "left") \
            .join(dim_device, df["Device_Used"] == dim_device["Device_Type"], "left")

fact_transactions = fact_df.select(
    col("Transaction_ID"),
    col("Customer_ID"),
    col("Transaction_Amount"),
    col("Quantity"),
    when(col("Is_Fraudulent") == 1, "true").when(col("Is_Fraudulent") == 0, "false").otherwise("false").alias("Is_Fraudulent"),
    date_format(col("Transaction_Timestamp"), "yyyy-MM-dd HH:mm:ss").alias("Transaction_Timestamp"),
    col("Payment_Method_ID").cast("string"),
    col("Product_Category_ID").cast("string"),
    col("Device_ID").cast("string")
)

# Save to HDFS
today = datetime.today()
base_path = "hdfs://namenode:9000/user/root/datalake"
date_path = f"YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}"

fact_path = f"{base_path}/fact_transactions/{date_path}"
customers_path = f"{base_path}/dim_customers/{date_path}"
payments_path = f"{base_path}/dim_payment_methods/{date_path}"
products_path = f"{base_path}/dim_product_categories/{date_path}"
devices_path = f"{base_path}/dim_devices/{date_path}"
time_path = f"{base_path}/dim_time/{date_path}"

fact_transactions.write.mode("overwrite") \
    .option("header", "true") \
    .csv(fact_path)

dim_customer.write.mode("overwrite") \
    .option("header", "true") \
    .csv(customers_path)

dim_payment.write.mode("overwrite") \
    .option("header", "true") \
    .csv(payments_path)

dim_product.write.mode("overwrite") \
    .option("header", "true") \
    .csv(products_path)

dim_device.write.mode("overwrite") \
    .option("header", "true") \
    .csv(devices_path)

dim_time.write.mode("overwrite") \
    .option("header", "true") \
    .csv(time_path)

print("Data saved to HDFS successfully!")

# Stop Spark session
spark.stop()