CREATE SCHEMA IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.daily_sales (
    event_date date PRIMARY KEY,
    order_count bigint NOT NULL CHECK (order_count > 0),
    units_sold bigint NOT NULL CHECK (units_sold >= order_count),
    unique_customers bigint NOT NULL CHECK (unique_customers BETWEEN 1 AND order_count),
    revenue_eur numeric(38,2) NOT NULL CHECK (revenue_eur > 0),
    average_order_value_eur numeric(28,2) NOT NULL CHECK (average_order_value_eur >= 0)
);

CREATE TABLE IF NOT EXISTS analytics.country_sales (
    event_date date NOT NULL,
    country text NOT NULL CHECK (length(trim(country)) > 0),
    order_count bigint NOT NULL CHECK (order_count > 0),
    units_sold bigint NOT NULL CHECK (units_sold >= order_count),
    unique_customers bigint NOT NULL CHECK (unique_customers BETWEEN 1 AND order_count),
    revenue_eur numeric(38,2) NOT NULL CHECK (revenue_eur > 0),
    average_order_value_eur numeric(28,2) NOT NULL CHECK (average_order_value_eur >= 0),
    PRIMARY KEY (event_date, country)
);

CREATE TABLE IF NOT EXISTS analytics.product_sales (
    event_date date NOT NULL,
    product_id text NOT NULL CHECK (length(trim(product_id)) > 0),
    product_name text NOT NULL CHECK (length(trim(product_name)) > 0),
    order_count bigint NOT NULL CHECK (order_count > 0),
    units_sold bigint NOT NULL CHECK (units_sold >= order_count),
    unique_customers bigint NOT NULL CHECK (unique_customers BETWEEN 1 AND order_count),
    revenue_eur numeric(38,2) NOT NULL CHECK (revenue_eur > 0),
    average_order_value_eur numeric(28,2) NOT NULL CHECK (average_order_value_eur >= 0),
    PRIMARY KEY (event_date, product_id, product_name)
);

CREATE TABLE IF NOT EXISTS analytics.pipeline_state (
    singleton_id integer PRIMARY KEY CHECK (singleton_id = 1),
    gold_run_id text NOT NULL,
    source_silver_run_id text NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    gold_manifest jsonb NOT NULL
);
