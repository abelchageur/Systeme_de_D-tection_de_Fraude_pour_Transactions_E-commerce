import requests
import json

# Configuration
SUPERSET_URL = "http://localhost:8088"
ADMIN_CREDS = {"username": "admin", "password": "admin", "provider": "db"}

# 1. Authenticate
session = requests.Session()
print("🔐 Logging in...")
login_res = session.post(f"{SUPERSET_URL}/api/v1/security/login", json=ADMIN_CREDS)
access_token = login_res.json()["access_token"]

headers = {
    "Authorization": f"Bearer {access_token}",
    "Content-Type": "application/json"
}
csrf_res = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers)
headers["X-CSRFToken"] = csrf_res.json()["result"]

# 2. Delete All Dashboards
print("🗑️ Deleting dashboards...")
dashboards = session.get(f"{SUPERSET_URL}/api/v1/dashboard/", headers=headers).json()["result"]
for dashboard in dashboards:
    session.delete(f"{SUPERSET_URL}/api/v1/dashboard/{dashboard['id']}", headers=headers)

# 3. Delete All Charts
print("🗑️ Deleting charts...")
charts = session.get(f"{SUPERSET_URL}/api/v1/chart/", headers=headers).json()["result"]
for chart in charts:
    session.delete(f"{SUPERSET_URL}/api/v1/chart/{chart['id']}", headers=headers)

# 4. Delete All Datasets (except default)
print("🗑️ Deleting datasets...")
datasets = session.get(f"{SUPERSET_URL}/api/v1/dataset/", headers=headers).json()["result"]
for dataset in datasets:
    if dataset["table_name"] not in ["dashboards", "slices"]:  # Keep Superset system tables
        session.delete(f"{SUPERSET_URL}/api/v1/dataset/{dataset['id']}", headers=headers)

print("✅ Cleanup complete! Superset is now fresh for testing.")