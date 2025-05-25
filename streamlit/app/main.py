import streamlit as st
import json
import pandas as pd
import plotly.express as px
from kafka import KafkaConsumer
from datetime import datetime

# Configuration
KAFKA_BOOTSTRAP = "kafka:9092"
FRAUD_TOPIC = "fraud_topic"

# Set page config
st.set_page_config(page_title="Real-time Fraud Detection", layout="wide")

# Title
st.title("🚨 Real-time Fraud Detection Dashboard")

# Create columns for counters
col1, col2 = st.columns(2)

# Counters
with col1:
    fraud_counter = st.empty()
    fraud_metric = st.empty()
    
with col2:
    normal_counter = st.empty()
    normal_metric = st.empty()

# Create tabs for different visualizations
tab1, tab2, tab3, tab4 = st.tabs([
    "Transaction Trends", 
    "Fraud Analysis", 
    "Payment Methods", 
    "Raw Data"
])

# Initialize chart placeholders outside the loop
with tab1:
    trend_chart = st.line_chart([])
    
with tab2:
    fraud_pie_placeholder = st.empty()
    amount_chart_placeholder = st.empty()
    
with tab3:
    payment_method_placeholder = st.empty()
    fraud_rate_payment_placeholder = st.empty()
    amount_by_payment_placeholder = st.empty()
    payment_over_time_placeholder = st.empty()
    
with tab4:
    raw_data_placeholder = st.empty()

# Kafka Consumer
consumer = KafkaConsumer(
    FRAUD_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize data storage
fraud_count = 0
normal_count = 0
transaction_data = []

# Standardize payment method names
PAYMENT_METHOD_MAPPING = {
    "credit card": "Credit Card",
    "debit card": "Debit Card",
    "bank transfer": "Bank Transfer",
    "paypal": "PayPal",
    "PayPal": "PayPal"
}

def standardize_payment_method(method):
    """Standardize payment method names."""
    return PAYMENT_METHOD_MAPPING.get(method.lower() if method else "unknown", "unknown")

# Main loop
for message in consumer:
    transaction = message.value
    
    # Standardize payment method
    transaction["payment_method"] = standardize_payment_method(transaction.get("payment_method", "unknown"))
    
    # Update counters
    if transaction["is_fraud"]:
        fraud_count += 1
    else:
        normal_count += 1
    
    # Store transaction data
    transaction["timestamp"] = datetime.strptime(transaction["timestamp"], "%Y-%m-%d %H:%M:%S")
    transaction_data.append(transaction)
    df = pd.DataFrame(transaction_data)
    
    # Update counters display
    fraud_counter.text(f"Fraudulent Transactions Detected")
    fraud_metric.metric(label="", value=fraud_count, delta=f"Total fraud: {fraud_count}")
    
    normal_counter.text(f"Normal Transactions Processed")
    normal_metric.metric(label="", value=normal_count, delta=f"Total normal: {normal_count}")
    
    # Update trend chart
    trend_data = pd.DataFrame({
        "Fraud": [fraud_count],
        "Normal": [normal_count]
    })
    trend_chart.add_rows(trend_data)
    
    # Update fraud analysis tab
    if not df.empty:
        # Persistent pie chart
        pie_fig = px.pie(
            df,
            names=df["is_fraud"].map({True: "Fraud", False: "Normal"}),
            title="Fraud vs Normal Transactions"
        )
        fraud_pie_placeholder.plotly_chart(pie_fig, use_container_width=True)
        
        # Persistent amount chart
        amount_fig = px.box(
            df,
            x="is_fraud",
            y="amount",
            color="is_fraud",
            title="Transaction Amount Distribution"
        )
        amount_chart_placeholder.plotly_chart(amount_fig, use_container_width=True)
    
    # Update payment method tab
    if not df.empty:
        # Histogram of payment method distribution
        payment_fig = px.histogram(
            df,
            x="payment_method",
            color="is_fraud",
            barmode="group",
            title="Payment Method Distribution"
        )
        payment_method_placeholder.plotly_chart(payment_fig, use_container_width=True)
        
        # Fraud rate by payment method
        fraud_rate_df = df.groupby("payment_method").agg({
            "is_fraud": "mean",
            "transaction_id": "count"
        }).reset_index()
        fraud_rate_df["is_fraud"] = fraud_rate_df["is_fraud"] * 100  # Convert to percentage
        fraud_rate_fig = px.bar(
            fraud_rate_df,
            x="payment_method",
            y="is_fraud",
            text="transaction_id",
            title="Fraud Rate by Payment Method (%)",
            labels={"is_fraud": "Fraud Rate (%)", "transaction_id": "Transaction Count"}
        )
        fraud_rate_fig.update_traces(texttemplate='%{text} txns', textposition='auto')
        fraud_rate_payment_placeholder.plotly_chart(fraud_rate_fig, use_container_width=True)
        
        # Transaction amount by payment method and fraud status
        amount_by_payment_fig = px.box(
            df,
            x="payment_method",
            y="amount",
            color="is_fraud",
            title="Transaction Amount by Payment Method and Fraud Status"
        )
        amount_by_payment_placeholder.plotly_chart(amount_by_payment_fig, use_container_width=True)
        
        # Payment method frequency over time
        df["date"] = df["timestamp"].dt.date
        time_series_df = df.groupby(["date", "payment_method", "is_fraud"]).size().reset_index(name="count")
        time_fig = px.line(
            time_series_df,
            x="date",
            y="count",
            color="payment_method",
            line_dash="is_fraud",
            title="Payment Method Frequency Over Time (Solid: Normal, Dashed: Fraud)"
        )
        payment_over_time_placeholder.plotly_chart(time_fig, use_container_width=True)
    
    # Update raw data tab
    raw_data_placeholder.dataframe(df.sort_values("timestamp", ascending=False))
