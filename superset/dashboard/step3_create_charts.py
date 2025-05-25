import requests
import json
from datetime import datetime
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
    
    def get_dataset_id_by_name(self, dataset_name):
        """Get dataset ID by name"""
        url = f"{self.base_url}/api/v1/dataset/"
        params = {"q": f"(filters:!((col:table_name,opr:eq,value:'{dataset_name}')))"}
        response = self.session.get(url, headers=self.headers, params=params)
        
        if response.status_code == 200 and response.json()["result"]:
            return response.json()["result"][0]["id"]
        return None
    
    def create_chart(self, chart_config):
        """Create a single chart"""
        print(f"📊 Creating chart: {chart_config['name']}")
        
        # Get dataset ID
        dataset_id = self.get_dataset_id_by_name(chart_config["dataset_name"])
        if not dataset_id:
            print(f"❌ Dataset '{chart_config['dataset_name']}' not found")
            return None
        
        # Prepare chart payload
        chart_payload = {
            "datasource_id": dataset_id,
            "datasource_type": "table",
            "slice_name": chart_config["name"],
            "viz_type": chart_config["chart_type"],
            "params": json.dumps({
                "datasource": f"{dataset_id}__table",
                "viz_type": chart_config["chart_type"],
                "groupby": chart_config["dimensions"],
                "metrics": chart_config["metrics"],
                "adhoc_filters": [],
                "row_limit": 10000,
                **chart_config.get("options", {})
            }),
            "query_context": json.dumps({
                "datasource": {"id": dataset_id, "type": "table"},
                "queries": [{
                    "columns": chart_config["dimensions"],
                    "metrics": chart_config["metrics"],
                    "orderby": [],
                    "row_limit": 10000
                }]
            })
        }
        
        # Create chart
        response = self.session.post(
            f"{self.base_url}/api/v1/chart/", 
            headers=self.headers, 
            json=chart_payload
        )
        
        if response.status_code == 201:
            chart_id = response.json()['id']
            print(f"✅ Chart '{chart_config['name']}' created (ID: {chart_id})")
            return chart_id
        else:
            print(f"❌ Failed to create chart '{chart_config['name']}': {response.text}")
            return None
    

def main():
    # Initialize Superset connection
    creator = SupersetDashboardCreator()
    
    # =============================================================================
    # CHART CONFIGURATIONS - MODIFY THIS SECTION
    # =============================================================================
    
    charts_config = [
        {
            "name": "Fraud Detection Overview",
            "dataset_name": "Proportion of Fraudulent Transactions",
            "chart_type": "dist_bar",
            "dimensions": ["transaction_type"],
            "metrics": [{
                "label": "transaction_count",
                "expressionType": "SQL",
                "sqlExpression": "SUM(transaction_count)"
            }],
            "options": {
                "donut": False,
                "show_labels": True,
                "pie_label_type": "key_percent",
                "color_scheme": "supersetColors"
            }
        },
        {
            "name": "Number of Transactions by Month",
            "dataset_name": "Number of Transactions by Month",
            "chart_type": "dist_bar",
            "dimensions": ["transaction_month"],
            "metrics": [{
                "label": "transaction_count",
                "expressionType": "SQL",
                "sqlExpression": "SUM(transaction_count)"
            }],
            "options": {
                "bar_stacked": False,
                "show_legend": True,
                "x_axis_label": "Month",
                "y_axis_label": "Transaction Count",
                "color_scheme": "supersetColors"
            }
        },
        {
            "name": "Average Transaction Amount by Age Group",
            "dataset_name": "Average Transaction Amount by Customer Age Group",
            "chart_type": "dist_bar",
            "dimensions": ["age_group"],
            "metrics": [{
                "label": "average_amount",
                "expressionType": "SQL",
                "sqlExpression": "SUM(average_amount)"
            }],
            "options": {
                "bar_stacked": False,
                "show_legend": True,
                "x_axis_label": "Age Group",
                "y_axis_label": "Average Amount",
                "color_scheme": "supersetColors"
            }
        },
        {
            "name": "Distribution of Payment Methods",
            "dataset_name": "Distribution of Payment Methods",
            "chart_type": "dist_bar",
            "dimensions": ["payment_method"],
            "metrics": [{
                "label": "transaction_count",
                "expressionType": "SQL",
                "sqlExpression": "SUM(transaction_count)"
            }],
            "options": {
                "bar_stacked": False,
                "show_legend": True,
                "x_axis_label": "Payment Method",
                "y_axis_label": "Transaction Count",
                "color_scheme": "supersetColors"
            }
        },
        {
            "name": "Transaction Volume by Device Type",
            "dataset_name": "Transaction Volume by Device Type",
            "chart_type": "dist_bar",
            "dimensions": ["device_type"],
            "metrics": [{
                "label": "transaction_count",
                "expressionType": "SQL",
                "sqlExpression": "SUM(transaction_count)"
            }],
            "options": {
                "donut": False,
                "show_labels": True,
                "pie_label_type": "key_percent",
                "color_scheme": "supersetColors"
            }
        },
        {
            "name": "Revenue by Product Category",
            "dataset_name": "Total Transaction Amount by Product Category",
            "chart_type": "dist_bar",
            "dimensions": ["product_category"],
            "metrics": [{
                "label": "Total Transaction Amount",
                "expressionType": "SQL",
                "sqlExpression": "SUM(total_transaction_amount)"
            }],
            "options": {
                "bar_stacked": False,
                "show_legend": True,
                "x_axis_label": "Product Category",
                "y_axis_label": "Total Transaction Amount",
                "color_scheme": "supersetColors"
            }
        }
    ]
    
    # =============================================================================
    # DASHBOARD CONFIGURATION
    # =============================================================================
    
    dashboard_name = "Transaction Analytics Dashboard"
    
    # =============================================================================
    # EXECUTION - DON'T MODIFY BELOW THIS LINE
    # =============================================================================
    
    print(f"\n🚀 Starting dashboard creation process...")
    print(f"📊 Charts to create: {len(charts_config)}")
    print(f"🏗️ Dashboard name: {dashboard_name}")
    print("=" * 50)
    
    # Create all charts
    chart_ids = []
    for chart_config in charts_config:
        chart_id = creator.create_chart(chart_config)
        if chart_id:
            chart_ids.append(chart_id)
        time.sleep(1)  # Small delay between chart creations
    
    print(f"\n📈 Successfully created {len(chart_ids)} out of {len(charts_config)} charts")
    


if __name__ == "__main__":
    main()