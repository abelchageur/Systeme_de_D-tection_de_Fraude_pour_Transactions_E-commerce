import requests

# Create a session to keep cookies
session = requests.Session()

# Step 1: Login to Superset
login_url = "http://localhost:8088/api/v1/security/login"
credentials = {
    "username": "admin",
    "password": "admin",
    "provider": "db"
}
response = session.post(login_url, json=credentials)
if response.status_code != 200:
    print(f"❌ Login failed: {response.text}")
    exit(1)
access_token = response.json()["access_token"]
print("✅ Logged in successfully")

# Step 2: Get CSRF Token
csrf_url = "http://localhost:8088/api/v1/security/csrf_token/"
headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
csrf_response = session.get(csrf_url, headers=headers)
if csrf_response.status_code != 200:
    print(f"❌ Failed to get CSRF token: {csrf_response.text}")
    exit(1)
csrf_token = csrf_response.json()["result"]
headers.update({"X-CSRFToken": csrf_token})
print("✅ CSRF token retrieved")

# Step 3: Get Hive Database ID
get_db_url = "http://localhost:8088/api/v1/database/"
params = {
    "q": "(filters:!((col:database_name,opr:eq,value:'Hive Warehouse')))"
}
db_response = session.get(get_db_url, headers=headers, params=params)
if db_response.status_code != 200 or not db_response.json()["result"]:
    print(f"❌ Failed to get Hive database ID: {db_response.text}")
    exit(1)
hive_db_id = db_response.json()["result"][0]["id"]
print(f"✅ Hive Warehouse database ID: {hive_db_id}")










# Step 4: Create and Save Test Query as a Dataset

# dataset_1 : Distribution of Payment Methods
dataset_payload = {
    "database": hive_db_id,
    "schema": "default",
    "table_name": "Distribution of Payment Methods",
    "sql": """
    SELECT 
        dpm.payment_method AS payment_method,
        COUNT(*) AS transaction_count
    FROM 
        fact_transactions ft
    JOIN 
        dim_payment_methods dpm ON ft.payment_method_id = dpm.payment_method_id
    GROUP BY 
        dpm.payment_method
    ORDER BY 
        transaction_count DESC
    """,
    "owners": [1]  # admin user ID
}
response = session.post(
    "http://localhost:8088/api/v1/dataset/",
    headers=headers,
    json=dataset_payload
)
if response.status_code == 201:
    print(f"✅ Dataset 'Distribution of Payment Methods' created successfully. ID: {response.json()['id']}")
else:
    print(f"❌ Failed to create dataset: {response.text}")
    print("Status:", response.status_code)



# dataset_2 : Transaction Volume by Device Type
dataset_payload = {
    "database": hive_db_id,
    "schema": "default",
    "table_name": "Transaction Volume by Device Type",
    "sql": """
    SELECT 
        dd.device_type AS device_type,
        COUNT(*) AS transaction_count
    FROM 
        fact_transactions ft
    JOIN 
        dim_devices dd ON ft.device_id = dd.device_id
    GROUP BY 
        dd.device_type
    ORDER BY 
        transaction_count DESC
    """,
    "owners": [1]  # admin user ID
}
response = session.post(
    "http://localhost:8088/api/v1/dataset/",
    headers=headers,
    json=dataset_payload
)
if response.status_code == 201:
    print(f"✅ Dataset 'Transaction Volume by Device Type' created successfully. ID: {response.json()['id']}")
else:
    print(f"❌ Failed to create dataset: {response.text}")
    print("Status:", response.status_code)


# dataset_3 : Average Transaction Amount by Customer Age Group
dataset_payload = {
    "database": hive_db_id,
    "schema": "default",
    "table_name": "Average Transaction Amount by Customer Age Group",
    "sql": """
    SELECT 
        dim_customers.age_group AS age_group, 
        AVG(fact_transactions.transaction_amount) AS average_amount
    FROM 
        fact_transactions
    JOIN 
        dim_customers 
        ON fact_transactions.customer_id = dim_customers.customer_id
    GROUP BY 
        dim_customers.age_group 
    """,
    "owners": [1]  # admin user ID
}
response = session.post(
    "http://localhost:8088/api/v1/dataset/",
    headers=headers,
    json=dataset_payload
)
if response.status_code == 201:
    print(f"✅ Dataset 'Average Transaction Amount by Customer Age Group' created successfully. ID: {response.json()['id']}")
else:
    print(f"❌ Failed to create dataset: {response.text}")
    print("Status:", response.status_code)


# dataset_4 : Number of Transactions by Month
dataset_payload = {
    "database": hive_db_id,
    "schema": "default",
    "table_name": "Number of Transactions by Month",
    "sql": """
    SELECT 
        date_format(transaction_timestamp, 'MMMM') AS transaction_month,
        MONTH(transaction_timestamp) AS month_number,
        COUNT(*) AS transaction_count
    FROM 
        fact_transactions
    GROUP BY 
        date_format(transaction_timestamp, 'MMMM'),
        MONTH(transaction_timestamp)
    ORDER BY 
        month_number 
    """,
    "owners": [1]  # admin user ID
}
response = session.post(
    "http://localhost:8088/api/v1/dataset/",
    headers=headers,
    json=dataset_payload
)
if response.status_code == 201:
    print(f"✅ Dataset 'Number of Transactions by Month' created successfully. ID: {response.json()['id']}")
else:
    print(f"❌ Failed to create dataset: {response.text}")
    print("Status:", response.status_code) 


# dataset_5 : Proportion of Fraudulent Transactions
dataset_payload = {
    "database": hive_db_id,
    "schema": "default",
    "table_name": "Proportion of Fraudulent Transactions",
    "sql": """
    SELECT 
        CASE 
            WHEN is_fraudulent THEN 'Fraudulent'
            ELSE 'Non-Fraudulent'
        END AS transaction_type,
        COUNT(*) AS transaction_count
    FROM 
        fact_transactions
    GROUP BY 
        CASE 
            WHEN is_fraudulent THEN 'Fraudulent'
            ELSE 'Non-Fraudulent'
        END 
    """,
    "owners": [1]  # admin user ID
}
response = session.post(
    "http://localhost:8088/api/v1/dataset/",
    headers=headers,
    json=dataset_payload
)
if response.status_code == 201:
    print(f"✅ Dataset 'Proportion of Fraudulent Transactions' created successfully. ID: {response.json()['id']}")
else:
    print(f"❌ Failed to create dataset: {response.text}")
    print("Status:", response.status_code)


# dataset_6 : Total Transaction Amount by Product Category
dataset_payload = {
    "database": hive_db_id,
    "schema": "default",
    "table_name": "Total Transaction Amount by Product Category",
    "sql": """
    SELECT 
        dpc.product_category AS product_category,
        SUM(ft.transaction_amount) AS total_transaction_amount
    FROM 
        fact_transactions ft
    JOIN 
        dim_product_categories dpc ON ft.product_category_id = dpc.product_category_id
    GROUP BY 
        dpc.product_category
    """,
    "owners": [1]  # admin user ID
}
response = session.post(
    "http://localhost:8088/api/v1/dataset/",
    headers=headers,
    json=dataset_payload
)
if response.status_code == 201:
    print(f"✅ Dataset 'Total Transaction Amount by Product Category' created successfully. ID: {response.json()['id']}")
else:
    print(f"❌ Failed to create dataset: {response.text}")
    print("Status:", response.status_code)