import requests

# Create a session to keep cookies
session = requests.Session()

# Step 1: Login
login_url = "http://localhost:8088/api/v1/security/login"
credentials = {
    "username": "admin",
    "password": "admin",
    "provider": "db"
}

response = session.post(login_url, json=credentials)

if response.status_code != 200:
    print("❌ Login failed.")
    print("Status:", response.status_code)
    print("Response:", response.text)
    exit()

access_token = response.json()["access_token"]
print("✅ Login successful.")
print("Access Token:", access_token)

# Step 2: Get CSRF Token
csrf_url = "http://localhost:8088/api/v1/security/csrf_token/"
headers = {
    "Authorization": f"Bearer {access_token}"
}
csrf_response = session.get(csrf_url, headers=headers)

if csrf_response.status_code != 200:
    print("❌ Failed to get CSRF token.")
    print("Status:", csrf_response.status_code)
    print("Response:", csrf_response.text)
    exit()

csrf_token = csrf_response.json()["result"]
print("✅ CSRF token received:")
print(csrf_token)

# Step 3: Create Hive Database
create_db_url = "http://localhost:8088/api/v1/database/"
headers.update({
    "X-CSRFToken": csrf_token,
    "Content-Type": "application/json"
})

db_payload = {
    "database_name": "Hive Warehouse",
    "sqlalchemy_uri": "hive://hive:10000/default?auth=NOSASL",
    "extra": "{\"metadata_params\": {}, \"engine_params\": {}}"
}
# ?auth=NOSASL
create_db_response = session.post(create_db_url, headers=headers, json=db_payload)

print("Status:", create_db_response.status_code)
print("Response:", create_db_response.text)
