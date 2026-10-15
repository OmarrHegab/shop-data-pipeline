-- Every table represents the same orders: use one table for each calculation.
SELECT SUM(order_count) AS orders,
       SUM(units_sold) AS units,
       SUM(revenue_eur) AS revenue_eur,
       ROUND(SUM(revenue_eur) / NULLIF(SUM(order_count), 0), 2) AS average_order_value_eur
FROM analytics.daily_sales;

SELECT country, SUM(order_count) AS orders, SUM(revenue_eur) AS revenue_eur
FROM analytics.country_sales
GROUP BY country
ORDER BY revenue_eur DESC;

SELECT product_id, product_name, SUM(units_sold) AS units, SUM(revenue_eur) AS revenue_eur
FROM analytics.product_sales
GROUP BY product_id, product_name
ORDER BY revenue_eur DESC;

SELECT gold_run_id, source_silver_run_id, loaded_at
FROM analytics.pipeline_state;
