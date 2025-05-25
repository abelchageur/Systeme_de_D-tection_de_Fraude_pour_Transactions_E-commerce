-- Creating keyspace for fraud detection
CREATE KEYSPACE IF NOT EXISTS fraud_detection
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

-- Creating table for raw transaction data
CREATE TABLE IF NOT EXISTS fraud_detection.transactions (
    transaction_id text,
    customer_id text,
    transaction_amount float,
    transaction_date timestamp,
    payment_method text,
    product_category text,
    quantity int,
    customer_age int,
    customer_location text,
    device_used text,
    ip_address text,
    shipping_address text,
    billing_address text,
    account_age_days int,
    transaction_hour int,
    is_fraudulent int,
    PRIMARY KEY (transaction_id, transaction_date)
) WITH CLUSTERING ORDER BY (transaction_date DESC);

-- Creating table for fraud detection results
CREATE TABLE IF NOT EXISTS fraud_detection.fraud_results (
    transaction_id text,
    is_fraud boolean,
    timestamp timestamp,
    amount float,
    payment_method text,
    product_category text,
    error text,
    PRIMARY KEY (transaction_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC);