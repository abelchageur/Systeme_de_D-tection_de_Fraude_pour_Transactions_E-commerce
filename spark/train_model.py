#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType
from pyspark.sql.functions import col, to_date, dayofmonth, dayofweek, month, when, lit, isnull, count
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler, StandardScaler, Imputer
from pyspark.ml.classification import RandomForestClassifier, LogisticRegression, GBTClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml import Pipeline, PipelineModel
from datetime import datetime
import time
import pandas as pd
import numpy as np

# Initialize Spark Session optimized for local execution
spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.maxResultSize", "4g") \
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
    # For local environment, use local file path
    try:
        file_path = "transaction_data.csv"  # Adjust to your file location
        df = spark.read.option("header", "true") \
                     .option("multiLine", "true") \
                     .schema(schema) \
                     .csv(file_path)
    except:
        # Try HDFS path
        today = datetime.today()
        hdfs_path = f"hdfs://namenode:9000/user/root/transactions/YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}/transaction_data.csv"
        df = spark.read.option("header", "true") \
                     .option("multiLine", "true") \
                     .schema(schema) \
                     .csv(hdfs_path)

    print("Data loaded successfully. Row count:", df.count())
except Exception as e:
    print(f"Error loading data: {str(e)}")
    print("Please update the file path to point to your transaction data.")

# Data Quality Check
print("\nData Quality Check:")
print("Null values per column:")
null_counts = df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])
null_counts.show(vertical=True)

# Show sample data
print("\nSample data:")
df.show(5)

# Check class distribution
fraud_count = df.filter(col("Is Fraudulent") == 1).count()
non_fraud_count = df.filter(col("Is Fraudulent") == 0).count()
total = fraud_count + non_fraud_count
fraud_ratio = fraud_count / total

print(f"\nFraudulent transactions: {fraud_count} ({fraud_ratio:.2%})")
print(f"Non-fraudulent transactions: {non_fraud_count} ({1-fraud_ratio:.2%})")
print(f"Imbalance ratio: 1:{non_fraud_count/fraud_count:.1f}")

# Data Preprocessing
print("\nStarting Data Preprocessing...")
start_time = time.time()

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
                 when(col("Quantity") > 0, col("Transaction Amount") / col("Quantity")).otherwise(col("Transaction Amount")))

# Create time-based features
df = df.withColumn("IsWeekend", 
                 when((col("DayOfWeek") == 1) | (col("DayOfWeek") == 7), 1).otherwise(0))

df = df.withColumn("IsNightTime", 
                 when((col("Transaction Hour") >= 22) | (col("Transaction Hour") <= 5), 1).otherwise(0))

# Calculate class weights
weight_multiplier = 5.0
fraud_weight = (non_fraud_count / total) * weight_multiplier
non_fraud_weight = (fraud_count / total)

df = df.withColumn("classWeight", 
                 when(col("Is Fraudulent") == 1, fraud_weight)
                 .otherwise(non_fraud_weight))

# Fill nulls in categorical columns
categorical_cols = ["Payment Method", "Product Category", "Customer Location", "Device Used"]
for col_name in categorical_cols:
    df = df.fillna("unknown", subset=[col_name])

# Define numerical columns for imputation
numerical_cols = ["Transaction Amount", "Quantity", "Customer Age", 
                 "Account Age Days", "Transaction Hour", "DayOfMonth", 
                 "DayOfWeek", "Month", "AddressMismatch", "AmountPerQuantity",
                 "IsWeekend", "IsNightTime"]

print(f"\nPreprocessing completed in {time.time() - start_time:.2f} seconds")
print("Sample of weights (fraud cases should have higher weight):")
df.select("Is Fraudulent", "classWeight").distinct().orderBy("Is Fraudulent").show(5)

# Show all features
print("\nAll features after preprocessing:")
df.printSchema()

# Define preprocessing pipeline stages
preprocessing_stages = []

# 1. Categorical encoding
categorical_features = []
for col_name in categorical_cols:
    string_indexer = StringIndexer(inputCol=col_name, outputCol=f"{col_name}_Index", handleInvalid="keep")
    preprocessing_stages.append(string_indexer)
    encoder = OneHotEncoder(inputCols=[f"{col_name}_Index"], outputCols=[f"{col_name}_OHE"], handleInvalid="keep")
    preprocessing_stages.append(encoder)
    categorical_features.append(f"{col_name}_OHE")

# 2. Numerical columns (impute missing values with median)
numerical_features = []
for num_col in numerical_cols:
    imputer = Imputer(inputCol=num_col, outputCol=f"{num_col}_imputed", strategy="median")
    preprocessing_stages.append(imputer)
    numerical_features.append(f"{num_col}_imputed")

# 3. Assemble all features into a vector
assembler = VectorAssembler(inputCols=categorical_features + numerical_features, 
                           outputCol="rawFeatures", 
                           handleInvalid="keep")
preprocessing_stages.append(assembler)

# 4. Scale features
scaler = StandardScaler(inputCol="rawFeatures", outputCol="features", withStd=True, withMean=True)
preprocessing_stages.append(scaler)

print("Feature pipeline built with stages:")
for i, stage in enumerate(preprocessing_stages):
    print(f"{i+1}. {type(stage).__name__}")

# Create preprocessing pipeline
preprocessing_pipeline = Pipeline(stages=preprocessing_stages)

# Split data
train, test = df.randomSplit([0.8, 0.2], seed=42)

# Check class distribution in splits
train_fraud_ratio = train.filter(col("Is Fraudulent") == 1).count() / train.count()
test_fraud_ratio = test.filter(col("Is Fraudulent") == 1).count() / test.count()
print(f"Train fraud ratio: {train_fraud_ratio:.2%}")
print(f"Test fraud ratio: {test_fraud_ratio:.2%}")

# Remove conflicting columns
columns_to_check = ["prediction", "rawFeatures", "features"] + [f"{c}_Index" for c in categorical_cols] + [f"{c}_OHE" for c in categorical_cols] + [f"{c}_imputed" for c in numerical_cols]
for column in columns_to_check:
    if column in train.columns:
        train = train.drop(column)
    if column in test.columns:
        test = test.drop(column)

# Apply preprocessing
print("\nApplying preprocessing pipeline...")
preprocessor_model = preprocessing_pipeline.fit(train)
train_preprocessed = preprocessor_model.transform(train)
test_preprocessed = preprocessor_model.transform(test)

# Save preprocessor to HDFS
try:
    base_path = "hdfs://namenode:9000/user/root/model"
    preprocessor_path = f"{base_path}/fraud_detection/preprocessor"
    print(f"\nSaving preprocessor to HDFS at: {preprocessor_path}")
    preprocessor_model.write().overwrite().save(preprocessor_path)
    print(f"Preprocessor successfully saved!")
except Exception as e:
    print(f"Error saving preprocessor to HDFS: {str(e)}")

# Define parameter combinations for Random Forest
rf_params = [
    {"numTrees": 50, "maxDepth": 5, "minInstancesPerNode": 1, "impurity": "gini"},
    {"numTrees": 100, "maxDepth": 10, "minInstancesPerNode": 2, "impurity": "entropy"},
    {"numTrees": 200, "maxDepth": 15, "minInstancesPerNode": 4, "impurity": "gini"}
]

rf_results = []
evaluator = BinaryClassificationEvaluator(labelCol="Is Fraudulent", metricName="areaUnderROC")
print("\nTraining Random Forest with multiple parameter combinations...")

for params in rf_params:
    start_time = time.time()
    rf = RandomForestClassifier(
        featuresCol="features", 
        labelCol="Is Fraudulent", 
        weightCol="classWeight",
        **params
    )
    try:
        rf_model = rf.fit(train_preprocessed)
        predictions = rf_model.transform(test_preprocessed)
        auc = evaluator.evaluate(predictions)
        recall_eval = MulticlassClassificationEvaluator(
            labelCol="Is Fraudulent", 
            metricName="weightedRecall"
        )
        recall = recall_eval.evaluate(predictions)
        training_time = time.time() - start_time
        rf_results.append({
            "numTrees": params["numTrees"],
            "maxDepth": params["maxDepth"],
            "minInstancesPerNode": params["minInstancesPerNode"],
            "impurity": params["impurity"],
            "AUC": auc,
            "Recall": recall,
            "Time": training_time,
            "Model": rf_model
        })
        print(f"RF with {params} - AUC: {auc:.4f}, Recall: {recall:.4f}, Time: {training_time:.1f}s")
    except Exception as e:
        print(f"Error during training with {params}: {str(e)}")

if not rf_results:
    print("\nNo models were successfully trained. Please check the pipeline and data.")
else:
    best_rf_params = max(rf_results, key=lambda x: x["AUC"])
    print("\nBest Random Forest parameters:")
    print(f"Number of trees: {best_rf_params['numTrees']}")
    print(f"Max depth: {best_rf_params['maxDepth']}")
    print(f"Min instances per node: {best_rf_params['minInstancesPerNode']}")
    print(f"Impurity: {best_rf_params['impurity']}")
    print(f"AUC: {best_rf_params['AUC']:.4f}")
    print(f"Recall: {best_rf_params['Recall']:.4f}")
    best_rf_model = best_rf_params["Model"]

# Define models to test
try:
    models = {
        "Logistic Regression": LogisticRegression(
            featuresCol="features", 
            labelCol="Is Fraudulent", 
            weightCol="classWeight",
            maxIter=20,
            regParam=0.01,
            elasticNetParam=0.5
        ),
        "Random Forest": RandomForestClassifier(
            featuresCol="features", 
            labelCol="Is Fraudulent", 
            weightCol="classWeight",
            numTrees=best_rf_params["numTrees"],
            maxDepth=best_rf_params["maxDepth"],
            minInstancesPerNode=best_rf_params["minInstancesPerNode"],
            impurity=best_rf_params["impurity"]
        ),
        "Gradient-Boosted Trees": GBTClassifier(
            featuresCol="features", 
            labelCol="Is Fraudulent", 
            weightCol="classWeight",
            maxIter=50,
            maxDepth=8,
            stepSize=0.1
        ),
        "Decision Tree": DecisionTreeClassifier(
            featuresCol="features", 
            labelCol="Is Fraudulent", 
            weightCol="classWeight",
            maxDepth=10,
            impurity="entropy"
        )
    }

    metrics = {
        "AUC": BinaryClassificationEvaluator(labelCol="Is Fraudulent", metricName="areaUnderROC"),
        "PR AUC": BinaryClassificationEvaluator(labelCol="Is Fraudulent", metricName="areaUnderPR"),
        "Recall": MulticlassClassificationEvaluator(labelCol="Is Fraudulent", metricName="weightedRecall"),
        "Precision": MulticlassClassificationEvaluator(labelCol="Is Fraudulent", metricName="weightedPrecision"),
        "F1": MulticlassClassificationEvaluator(labelCol="Is Fraudulent", metricName="f1")
    }

    results = []
    for model_name, model in models.items():
        print(f"\nTraining {model_name}...")
        start_time = time.time()
        try:
            trained_model = model.fit(train_preprocessed)
            predictions = trained_model.transform(test_preprocessed)
            model_results = {"Model": model_name}
            for metric_name, evaluator in metrics.items():
                score = evaluator.evaluate(predictions)
                model_results[metric_name] = score
            training_time = time.time() - start_time
            model_results["Time (s)"] = training_time
            results.append(model_results)
            print(f"{model_name} evaluation:")
            for metric, value in model_results.items():
                if metric != "Model" and metric != "Time (s)":
                    print(f"  - {metric}: {value:.4f}")
            print(f"  - Training time: {training_time:.1f}s")
        except Exception as e:
            print(f"Error training {model_name}: {str(e)}")

    if results:
        results_df = spark.createDataFrame(results).orderBy("F1", ascending=False)
        print("\nModel Performance Comparison:")
        results_df.show(truncate=False)
        results_pd = results_df.toPandas()
        print("Results converted to pandas DataFrame for visualization")
    else:
        print("No models were successfully trained for comparison")

except Exception as e:
    print(f"Error in model comparison step: {str(e)}")

# Determine the best model
try:
    if results and not results_df.isEmpty():
        best_model_row = results_df.first()
        best_model_name = best_model_row["Model"]
        print(f"\nThe best performing model is: {best_model_name}")
        print(f"F1 Score: {best_model_row['F1']:.4f}")
        print(f"AUC: {best_model_row['AUC']:.4f}")
        print(f"PR AUC: {best_model_row['PR AUC']:.4f}")
        print(f"Precision: {best_model_row['Precision']:.4f}")
        print(f"Recall: {best_model_row['Recall']:.4f}")

        best_model = None
        if best_model_name == "Gradient-Boosted Trees":
            best_model = models["Gradient-Boosted Trees"].fit(train_preprocessed)
        elif best_model_name == "Random Forest":
            best_model = models["Random Forest"].fit(train_preprocessed)
        elif best_model_name == "Logistic Regression":
            best_model = models["Logistic Regression"].fit(train_preprocessed)
        elif best_model_name == "Decision Tree":
            best_model = models["Decision Tree"].fit(train_preprocessed)

        if best_model:
            base_path = "hdfs://namenode:9000/user/root/model"
            model_path = f"{base_path}/fraud_detection/{best_model_name.replace(' ', '_').lower()}"
            try:
                print(f"\nSaving {best_model_name} model to HDFS at: {model_path}")
                best_model.write().overwrite().save(model_path)
                print(f"Model successfully saved!")
                metadata = spark.createDataFrame([{
                    "model_name": best_model_name,
                    "training_date": today.strftime("%Y-%m-%d"),
                    "metrics": {
                        "f1": float(best_model_row["F1"]),
                        "auc": float(best_model_row["AUC"]),
                        "pr_auc": float(best_model_row["PR AUC"]),
                        "precision": float(best_model_row["Precision"]),
                        "recall": float(best_model_row["Recall"]),
                        "training_time_seconds": float(best_model_row["Time (s)"])
                    }
                }])
                metadata_path = f"{model_path}_metadata"
                metadata.write.mode("overwrite").json(metadata_path)
                print(f"Model metadata saved to: {metadata_path}")
            except Exception as e:
                print(f"Error saving model to HDFS: {str(e)}")
        else:
            print(f"Could not retrieve the trained {best_model_name} model for saving.")
    else:
        print("No valid model results available to determine the best model.")

except Exception as e:
    print(f"Error in selecting and saving the best model: {str(e)}")

spark.stop()