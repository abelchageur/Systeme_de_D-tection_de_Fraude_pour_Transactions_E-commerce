from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
import subprocess
import psycopg2
import logging
import tempfile
import os
import re

def fetch_historical_data_from_api():
    """Fetch transaction data from the API at http://localhost:8000/csv-data/ and save it to a CSV."""
    # Define the API URL
    api_url = "http://fastapi:8000/csv-data/"

    # Make a request to fetch data
    response = requests.get(api_url)
    
    if response.status_code == 200:
        data = response.json()  # Assuming the response is in JSON format
    else:
        raise Exception(f"Failed to fetch data from API, status code {response.status_code}")

    # Convert the data to a pandas DataFrame
    df = pd.DataFrame(data)
    
    # Save to CSV
    file_path = '/tmp/transaction_data.csv'
    df.to_csv(file_path, index=False)
    return file_path

def upload_to_hdfs(**context):
    """Upload the local CSV file to HDFS with a partitioned path based on execution date."""
    local_file = '/tmp/transaction_data.csv'
    execution_date = context['ds']
    year, month, day = execution_date.split('-')
    hdfs_dir = f"/user/root/transactions/YYYY={year}/MM={month}/DD={day}"
    hdfs_file_path = f"{hdfs_dir}/transaction_data.csv"
    
    bash_command = (
        f'docker cp /tmp/transaction_data.csv namenode:/tmp/transaction_data.csv &&'
        f'docker exec namenode bash -c "hdfs dfs -mkdir -p {hdfs_dir} && '
        f'hdfs dfs -put -f {local_file} {hdfs_file_path}"'
    )

    result = subprocess.run(bash_command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise Exception(f"Command failed: {bash_command}\nError: {result.stderr}")

def update_hql_file(**kwargs):
    container_name = "hive"  # Make sure this matches your Hive container name
    file_path_in_container = "/home/query.hql"
    temp_local_file = "/tmp/query.hql"

    # Step 1: Delete existing file in the container
    print(f"Attempting to delete existing file: {file_path_in_container} inside container '{container_name}'")

    delete_result = subprocess.run(
        ["docker", "exec", container_name, "rm", "-f", file_path_in_container],
        capture_output=True,
        text=True
    )

    if delete_result.returncode != 0:
        raise Exception(f"Failed to delete old file in container: {delete_result.stderr}")

    print("Successfully deleted old HQL file.")


    # Step 2: Define the HQL content template
    hql_content = """
-- DIM: Customer
CREATE EXTERNAL TABLE IF NOT EXISTS dim_customers (
    Customer_ID STRING,
    Customer_Age INT,
    Customer_Location STRING,
    Account_Age_Days INT,
    Age_Group STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_customers/YYYY=****/MM=**/DD=**'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Payment
CREATE EXTERNAL TABLE IF NOT EXISTS dim_payment_methods (
    Payment_Method STRING,
    Payment_Method_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_payment_methods/YYYY=****/MM=**/DD=**'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Product Category
CREATE EXTERNAL TABLE IF NOT EXISTS dim_product_categories (
    Product_Category STRING,
    Product_Category_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_product_categories/YYYY=****/MM=**/DD=**'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Device
CREATE EXTERNAL TABLE IF NOT EXISTS dim_devices (
    Device_Type STRING,
    Device_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_devices/YYYY=****/MM=**/DD=**'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Time
CREATE EXTERNAL TABLE IF NOT EXISTS dim_time (
    Time_ID TIMESTAMP,
    Transaction_Hour INT,
    Transaction_DayOfWeek STRING,
    Transaction_Month STRING,
    Transaction_Year INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_time/YYYY=****/MM=**/DD=**'
TBLPROPERTIES ('skip.header.line.count'='1');

-- FACT: Transactions
CREATE EXTERNAL TABLE IF NOT EXISTS fact_transactions (
    Transaction_ID STRING,
    Customer_ID STRING,
    Transaction_Amount DECIMAL(10,2),
    Quantity INT,
    Is_Fraudulent BOOLEAN,
    Transaction_Timestamp TIMESTAMP,
    Payment_Method_ID INT,
    Product_Category_ID INT,
    Device_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/fact_transactions/YYYY=****/MM=**/DD=**'
TBLPROPERTIES ('skip.header.line.count'='1');
"""

    # Step 3: Replace placeholder date with today's date
    today = datetime.now()
    formatted_date = f"YYYY={today.year}/MM={today.month:02d}/DD={today.day:02d}"
    updated_content = hql_content.replace("YYYY=****/MM=**/DD=**", formatted_date)

    print(f"Date placeholder replaced with: {formatted_date}")


    # Step 4: Write updated HQL to local temporary file
    with open(temp_local_file, "w") as f:
        f.write(updated_content)

    print(f"New HQL file generated at: {temp_local_file}")


    # Step 5: Copy file into Docker container
    print(f"Copying file into container '{container_name}:{file_path_in_container}'")
    copy_result = subprocess.run(
        ["docker", "cp", temp_local_file, f"{container_name}:{file_path_in_container}"],
        capture_output=True,
        text=True
    )

    if copy_result.returncode != 0:
        raise Exception(f"Failed to copy file to container: {copy_result.stderr}")

    print("Successfully copied updated query.hql to container.")

    return "HQL file deleted and replaced successfully"

def check_model_and_train(**kwargs):
    hdfs_check_path = "hdfs://namenode:9000/user/root/model/fraud_detection"
    container_name_spark = "spark-master"
    container_name_namenode = "namenode"
    model_script_path = "train_model.py"  # Update this path as needed

    # Step 1: Check if model exists in HDFS
    hdfs_check_cmd = ["docker", "exec", container_name_namenode, "hdfs", "dfs", "-test", "-e", hdfs_check_path]
    
    result = subprocess.run(
        hdfs_check_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    if result.returncode == 0:
        print(f"✅ Model already exists at {hdfs_check_path}. Skipping training.")
        raise AirflowSkipException("Model already exists in HDFS.")

    print(f"⚠️ No existing model found at {hdfs_check_path}. Starting training...")

    # Step 2: Run model training inside Spark container
    train_cmd = ["docker", "exec", container_name_spark, "python3", model_script_path]

    train_result = subprocess.run(
        train_cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    if train_result.returncode != 0:
        raise Exception(f"Training failed with error: {train_result.stderr}")
    else:
        print("✅ Model training completed successfully.")
        print(train_result.stdout)

def run_hive_setup_or_query(**kwargs):
    container_name = "postgres"
    hive_container = "hive"

    def docker_exec(cmd):
        result = subprocess.run(
            ["docker", "exec", container_name] + cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        return result

    def docker_exec_hive(cmd):
        result = subprocess.run(
            ["docker", "exec", hive_container] + cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding='utf-8',
            errors='replace'
        )
        return result

    # Step 1: Check if metastore_db exists
    print("🔍 Checking if metastore_db exists...")
    result = docker_exec(["psql", "-U", "airflow", "-lqt", "-c", "SELECT 1 FROM pg_database WHERE datname = 'metastore_db';"])
    db_exists = '1 row' in result.stdout

    if not db_exists:
        print("❌ metastore_db does NOT exist. Creating it and initializing schema...")
        docker_exec(["psql", "-U", "airflow", "-c", "CREATE DATABASE metastore_db;"])

        print("⚙️ Initializing Hive metastore schema...")
        docker_exec_hive(["schematool", "-initSchema", "-dbType", "postgres"])

        print("🚀 Running Hive script...")
        docker_exec_hive(["hive", "-f", "/home/query.hql"])

    else:
        print("✅ metastore_db exists. Now checking if it's empty...")

        # Step 2: Check if metastore_db has any tables
        result = docker_exec(["psql", "-U", "airflow", "-d", "metastore_db", "-c", "SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public');"])
        has_tables = 't' in result.stdout

        if not has_tables:
            print("⚠️ metastore_db is empty. Initializing schema...")
            docker_exec_hive(["schematool", "-initSchema", "-dbType", "postgres"])

        print("🚀 Running Hive script...")
        docker_exec_hive(["hive", "-f", "/home/query.hql"])


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='fetch_transaction_data_and_upload_to_hdfs',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    start_task = DummyOperator(
        task_id='start'
    )

    # Fetch data from API
    fetch_data_task = PythonOperator(
        task_id='fetch_transaction_data',
        python_callable=fetch_historical_data_from_api
    )

    # Upload data to HDFS
    upload_to_hdfs_task = PythonOperator(
        task_id='upload_to_hdfs',
        python_callable=upload_to_hdfs,
        provide_context=True
    )

    check_and_train_task = PythonOperator(
        task_id="check_hdfs_and_train_model",
        python_callable=check_model_and_train,
        provide_context=True
    )

    run_spark_modelisation_task = BashOperator(
        task_id='run_spark_modelisation_command',
        bash_command='docker exec spark-master python3 modelisation.py'  
    )

    update_hql_task = PythonOperator(
        task_id='update_hql_file',
        python_callable=update_hql_file,
        provide_context=True
    )

    # Task to execute the Hive command
    run_hive_task = PythonOperator(
        task_id='run_hive_command',
        python_callable=run_hive_setup_or_query,
        provide_context=True
    )

    run_superset_dashboard = BashOperator(
        task_id='run_superset_dashboard',
        bash_command=(
            'docker exec superset python /app/dashboard/step1_connection_db.py && '
            'docker exec superset python /app/dashboard/step2_creation_of_datasets.py && '
            'docker exec superset python /app/dashboard/step3_create_charts.py && '
            'docker exec superset python /app/dashboard/step4_create_dashbord.py'
        )
    )


    end_task = DummyOperator(
        task_id='end'
    )

    # Define task dependencies
    start_task >> fetch_data_task >> upload_to_hdfs_task >> check_and_train_task 
    start_task >> fetch_data_task >> upload_to_hdfs_task >>  run_spark_modelisation_task >> update_hql_task >> run_hive_task >> run_superset_dashboard >> end_task
