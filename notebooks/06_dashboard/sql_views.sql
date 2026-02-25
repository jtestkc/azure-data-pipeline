-- Total Revenue Query
CREATE OR REPLACE VIEW sales_total_revenue AS
SELECT SUM(total_revenue) as total_revenue, SUM(total_orders) as total_orders, SUM(unique_customers) as unique_customers 
FROM delta.`abfss://rawdata@adlsrtsae23474.dfs.core.windows.net/gold/regional_performance`;

-- Regional Performance Query
CREATE OR REPLACE VIEW sales_regional_performance AS
SELECT region, total_revenue, total_orders, unique_customers, total_units_sold, avg_order_value
FROM delta.`abfss://rawdata@adlsrtsae23474.dfs.core.windows.net/gold/regional_performance`
ORDER BY total_revenue DESC;

-- Daily Revenue Query
CREATE OR REPLACE VIEW sales_daily_revenue AS
SELECT event_date, region, revenue, order_count, total_units_sold, avg_order_value
FROM delta.`abfss://rawdata@adlsrtsae23474.dfs.core.windows.net/gold/daily_revenue`
ORDER BY event_date DESC, region;

-- Top Products Query
CREATE OR REPLACE VIEW sales_top_products AS
SELECT product_id, product_name, revenue, units_sold, order_count
FROM delta.`abfss://rawdata@adlsrtsae23474.dfs.core.windows.net/gold/top_products`
ORDER BY revenue DESC
LIMIT 20;

-- Customer LTV Query
CREATE OR REPLACE VIEW sales_customer_ltv AS
SELECT customer_id, lifetime_value, total_orders, total_units_purchased, avg_order_value
FROM delta.`abfss://rawdata@adlsrtsae23474.dfs.core.windows.net/gold/customer_ltv`
ORDER BY lifetime_value DESC
LIMIT 20;
