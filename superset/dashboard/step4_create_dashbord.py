import requests
import json
import time

class SupersetDashboardCreator:
    def __init__(self, base_url="http://localhost:8088", username="admin", password="admin"):
        self.base_url = base_url
        self.session = requests.Session()
        self.headers = {}
        self.login(username, password)

    def login(self, username, password):
        """Authenticate with Superset"""
        print("🔐 Authenticating with Superset...")
        # Login
        login_url = f"{self.base_url}/api/v1/security/login"
        credentials = {"username": username, "password": password, "provider": "db"}
        response = self.session.post(login_url, json=credentials)
        if response.status_code != 200:
            print(f"❌ Login failed: {response.text}")
            exit(1)
        access_token = response.json()["access_token"]
        # Get CSRF Token
        csrf_url = f"{self.base_url}/api/v1/security/csrf_token/"
        self.headers = {
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json"
        }
        csrf_response = self.session.get(csrf_url, headers=self.headers)
        if csrf_response.status_code != 200:
            print(f"❌ Failed to get CSRF token: {csrf_response.text}")
            exit(1)
        csrf_token = csrf_response.json()["result"]
        self.headers.update({"X-CSRFToken": csrf_token})
        print("✅ Authentication successful")

    def find_chart_ids_by_names(self, chart_names):
        """Search for chart IDs by their names"""
        print("\n🔍 Searching for charts by name...")
        chart_ids = []
        for name in chart_names:
            url = f"{self.base_url}/api/v1/chart/"
            params = {"q": f"(filters:!((col:slice_name,opr:eq,value:'{name}')))"}
            response = self.session.get(url, headers=self.headers, params=params)
            if response.status_code == 200 and response.json()["result"]:
                chart_id = response.json()["result"][0]["id"]
                print(f"✅ Found chart '{name}' - ID: {chart_id}")
                chart_ids.append(chart_id)
            else:
                print(f"❌ Chart '{name}' not found")
        return chart_ids

    def create_dashboard(self, dashboard_name="📊 Fraud Analytics Dashboard", chart_names=None):
        """Create a new dashboard and assign charts by updating each chart's dashboards list"""
        if chart_names is None:
            chart_names = []

        # Step 1: Find chart IDs by name
        chart_ids = self.find_chart_ids_by_names(chart_names)
        if not chart_ids:
            print("❌ No valid chart IDs found. Make sure the chart names are correct.")
            return None

        # Step 2: Create empty dashboard
        print(f"\n🚀 Creating dashboard '{dashboard_name}'...")
        dashboard_payload = {
            "dashboard_title": dashboard_name,
            "json_metadata": json.dumps({
                "default_filters": "{}",
                "timed_refresh_immune_slices": [],
                "expanded_slices": {},
                "refresh_frequency": 0,
                "color_scheme": "supersetColors"
            })
        }

        create_url = f"{self.base_url}/api/v1/dashboard/"
        response = self.session.post(create_url, headers=self.headers, json=dashboard_payload)

        if response.status_code != 201:
            print(f"❌ Failed to create dashboard: {response.text}")
            return None

        dashboard_id = response.json()['id']
        print(f"✅ Dashboard '{dashboard_name}' created successfully! ID: {dashboard_id}")

        # Step 3: Add charts to the dashboard (by updating each chart's dashboards field)
        print(f"🔗 Adding {len(chart_ids)} charts to the dashboard...")

        for idx, chart_id in enumerate(chart_ids, 1):
            print(f"[{idx}/{len(chart_ids)}] Updating chart {chart_id} to add to dashboard {dashboard_id}...")

            # Get current chart details to retrieve existing dashboards
            chart_url = f"{self.base_url}/api/v1/chart/{chart_id}"
            chart_response = self.session.get(chart_url, headers=self.headers)
            if chart_response.status_code != 200:
                print(f"❌ Could not fetch chart {chart_id}: {chart_response.text}")
                continue

            current_dashboards = chart_response.json()["result"].get("dashboards", [])
            dashboard_list = [d["id"] for d in current_dashboards]
            if dashboard_id not in dashboard_list:
                dashboard_list.append(dashboard_id)

            # Prepare update payload
            update_payload = {
                "dashboards": dashboard_list
            }

            # Update chart
            update_response = self.session.put(
                f"{self.base_url}/api/v1/chart/{chart_id}",
                headers=self.headers,
                json=update_payload
            )

            if update_response.status_code == 200:
                print(f"✅ Chart {chart_id} added to dashboard {dashboard_id}")
            else:
                print(f"❌ Failed to update chart {chart_id}: {update_response.text}")

        print(f"✅ All charts added to dashboard {dashboard_id}")
        print(f"🔗 View dashboard at: {self.base_url}/superset/dashboard/{dashboard_id}/")
        return dashboard_id


def main():
    # Initialize Superset connection
    creator = SupersetDashboardCreator()

    # List of chart names you created in your step3_create_charts.py
    chart_names = [
        "Fraud Detection Overview",
        "Number of Transactions by Month",
        "Average Transaction Amount by Age Group",
        "Distribution of Payment Methods",
        "Transaction Volume by Device Type",
        "Revenue by Product Category"
    ]

    # Create dashboard and add charts
    dashboard_id = creator.create_dashboard(
        dashboard_name="📊 Fraud Detection Dashboard",
        chart_names=chart_names
    )

    if dashboard_id:
        print(f"\n🎉 Dashboard created successfully!")
        print(f"   🔗 Dashboard URL: {creator.base_url}/superset/dashboard/{dashboard_id}/")
    else:
        print("\n❌ Failed to fully configure dashboard.")

if __name__ == "__main__":
    main()