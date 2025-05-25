from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, to_date, dayofmonth, dayofweek, month, when, lit, isnull, count
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline
from datetime import datetime
import time

# Initialize Spark Session with optimized configurations
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("spark://spark-master:7077") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.cores.max", "4") \
    .getOrCreate()

print("Spark session created successfully!")

# Define schema
schema = StructType([
    StructField("Transaction ID", StringType(), True),
    StructField("Customer ID", StringType(), True),
    StructField("Transaction Amount", FloatType(), True),
    StructField("Transaction Date", StringType(), True),
    StructField("Payment Method", StringType(), True),
    StructField("Product Category", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("Customer Age", IntegerType(), True),
    StructField("Customer Location", StringType(), True),
    StructField("Device Used", StringType(), True),
    StructField("IP Address", StringType(), True),
    StructField("Shipping Address", StringType(), True),
    StructField("Billing Address", StringType(), True),
    StructField("Is Fraudulent", IntegerType(), True),
    StructField("Account Age Days", IntegerType(), True),
    StructField("Transaction Hour", IntegerType(), True)
])

# Load data with error handling
try:
    today = datetime.today()
    hdfs_path = f"hdfs://namenode:9000/user/root/transactions/YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}/transaction_data.csv"
    
    df = spark.read.option("header", "true") \
                   .option("multiLine", "true") \
                   .schema(schema) \
                   .csv(hdfs_path)
    
    print("Data loaded successfully. Row count:", df.count())
except Exception as e:
    print(f"Error loading data: {str(e)}")
    spark.stop()
    exit()

##################################################################
# Data Preprocessing
print("\nStarting Data Preprocessing...")
start_time = time.time()
from pyspark.sql.functions import col, to_date, dayofmonth, dayofweek, month, when

# Convert Transaction Date and extract features
df = df.withColumn("Transaction Date", to_date(col("Transaction Date")))
df = df.withColumn("DayOfMonth", dayofmonth(col("Transaction Date")))
df = df.withColumn("DayOfWeek", dayofweek(col("Transaction Date")))
df = df.withColumn("Month", month(col("Transaction Date")))

# Create feature: is shipping different from billing
df = df.withColumn("AddressMismatch", 
                  when(col("Shipping Address") != col("Billing Address"), 1).otherwise(0))

# Create feature: transaction amount per quantity
df = df.withColumn("AmountPerQuantity", 
                  col("Transaction Amount") / col("Quantity"))

# Handle class imbalance
fraud_count = df.filter(col("Is Fraudulent") == 1).count()
non_fraud_count = df.filter(col("Is Fraudulent") == 0).count()
fraud_ratio = fraud_count / (fraud_count + non_fraud_count)

print(f"\nFraudulent transactions: {fraud_count} ({fraud_ratio:.2%})")
print(f"Non-fraudulent transactions: {non_fraud_count}")

################################################
# 1. Calculate class weights (to handle imbalance)
fraud_weight = non_fraud_count / (fraud_count + non_fraud_count)  # Weight for fraud class
non_fraud_weight = fraud_count / (fraud_count + non_fraud_count)  # Weight for non-fraud

df = df.withColumn("classWeight", 
                  when(col("Is Fraudulent") == 1, fraud_weight)
                  .otherwise(non_fraud_weight))

# 2. Fill nulls in categorical columns (defensive programming)
categorical_cols = ["Payment Method", "Product Category", "Customer Location", "Device Used"]
for col_name in categorical_cols:
    df = df.fillna("unknown", subset=[col_name])

print("\nClass weights applied and categorical nulls handled (if any existed).")
print("Sample of weights (fraud cases should have higher weight):")
df.select("Is Fraudulent", "classWeight").show(5)
##########################################################
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Imputer

# Define pipeline stages
stages = []

# 1. Categorical encoding
categorical_cols = ["Payment Method", "Product Category", "Customer Location", "Device Used"]
for col_name in categorical_cols:
    # Convert strings to numerical indices
    string_indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_Index", handleInvalid="keep")
    # One-hot encode indices
    encoder = OneHotEncoder(inputCols=[f"{col_name}_Index"], outputCols=[f"{col_name}_OHE"], handleInvalid="keep")
    stages += [string_indexer, encoder]

# 2. Numerical columns (impute missing values with median)
numerical_cols = ["Transaction Amount", "Quantity", "Customer Age", 
                 "Account Age Days", "Transaction Hour", "DayOfMonth",
                 "DayOfWeek", "Month", "AddressMismatch", "AmountPerQuantity"]

for num_col in numerical_cols:
    imputer = Imputer(inputCol=num_col, outputCol=f"{num_col}_imputed", strategy="median")
    stages.append(imputer)
    numerical_cols[numerical_cols.index(num_col)] = f"{num_col}_imputed"  # Update column name

# 3. Assemble all features into a vector
assembler_inputs = [f"{c}_OHE" for c in categorical_cols] + numerical_cols
assembler = VectorAssembler(inputCols=assembler_inputs, outputCol="rawFeatures", handleInvalid="keep")
stages.append(assembler)

# 4. Scale features (mean=0, std=1)
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
stages.append(scaler)

# 5. Random Forest with class weights
rf = RandomForestClassifier(
    featuresCol="features",
    labelCol="Is Fraudulent",
    weightCol="classWeight",  # Critical for imbalance!
    numTrees=50,
    maxDepth=5,
    seed=42
)
stages.append(rf)

# Create the pipeline
pipeline = Pipeline(stages=stages)
print("\nPipeline built successfully. Ready for training!")
#######################################################
# Split data (80% train, 20% test)
train, test = df.randomSplit([0.8, 0.2], seed=42)
print(f"\nTrain rows: {train.count()}, Test rows: {test.count()}")

# Train the model
print("\nTraining started...")
start_time = time.time()
model = pipeline.fit(train)
print(f"Training completed in {time.time() - start_time:.2f} seconds")

# Generate predictions on test set
predictions = model.transform(test)
print("\nPredictions ready for evaluation.")
#####################################################"""
from pyspark.ml.classification import (
    LogisticRegression, 
    RandomForestClassifier, 
    GBTClassifier, 
    DecisionTreeClassifier
)
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import time

# Define models to test
models = {
    "Logistic Regression": LogisticRegression(
        featuresCol="features", 
        labelCol="Is Fraudulent", 
        weightCol="classWeight"
    ),
    "Random Forest": RandomForestClassifier(
        featuresCol="features", 
        labelCol="Is Fraudulent", 
        weightCol="classWeight",
        numTrees=50
    ),
    "Gradient-Boosted Trees": GBTClassifier(
        featuresCol="features", 
        labelCol="Is Fraudulent", 
        weightCol="classWeight",
        maxIter=20
    ),
    "Decision Tree": DecisionTreeClassifier(
        featuresCol="features", 
        labelCol="Is Fraudulent", 
        weightCol="classWeight"
    )
}

# Metrics to evaluate
metrics = {
    "AUC": BinaryClassificationEvaluator(labelCol="Is Fraudulent"),
    "Recall": MulticlassClassificationEvaluator(
        labelCol="Is Fraudulent", 
        metricName="weightedRecall"
    )
}

results = []

# Train and evaluate each model
for model_name, model in models.items():
    print(f"\nTraining {model_name}...")
    start_time = time.time()
    
    # Create pipeline (reuse your existing stages, replace classifier)
    pipeline = Pipeline(stages=stages[:-1] + [model])  # Keep all but last stage
    
    # Fit model
    trained_model = pipeline.fit(train)
    predictions = trained_model.transform(test)
    
    # Evaluate
    auc = metrics["AUC"].evaluate(predictions)
    recall = metrics["Recall"].evaluate(predictions)
    training_time = time.time() - start_time
    
    results.append({
        "Model": model_name,
        "AUC": auc,
        "Recall": recall,
        "Time (s)": training_time
    })
    
    print(f"{model_name} - AUC: {auc:.4f}, Recall: {recall:.4f}, Time: {training_time:.1f}s")

# Show results
results_df = spark.createDataFrame(results).orderBy("Recall", ascending=False)
print("\nModel Performance Comparison:")
results_df.show(truncate=False)