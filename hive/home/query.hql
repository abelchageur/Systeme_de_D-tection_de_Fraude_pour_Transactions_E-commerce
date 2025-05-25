
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
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_customers/YYYY=2025/MM=05/DD=25'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Payment
CREATE EXTERNAL TABLE IF NOT EXISTS dim_payment_methods (
    Payment_Method STRING,
    Payment_Method_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_payment_methods/YYYY=2025/MM=05/DD=25'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Product Category
CREATE EXTERNAL TABLE IF NOT EXISTS dim_product_categories (
    Product_Category STRING,
    Product_Category_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_product_categories/YYYY=2025/MM=05/DD=25'
TBLPROPERTIES ('skip.header.line.count'='1');

-- DIM: Device
CREATE EXTERNAL TABLE IF NOT EXISTS dim_devices (
    Device_Type STRING,
    Device_ID INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_devices/YYYY=2025/MM=05/DD=25'
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
LOCATION 'hdfs://namenode:9000/user/root/datalake/dim_time/YYYY=2025/MM=05/DD=25'
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
LOCATION 'hdfs://namenode:9000/user/root/datalake/fact_transactions/YYYY=2025/MM=05/DD=25'
TBLPROPERTIES ('skip.header.line.count'='1');
