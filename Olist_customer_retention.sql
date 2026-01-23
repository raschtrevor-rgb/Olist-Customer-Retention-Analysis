-- If you are interested in reading the executive summary please look in this repository:) 

SELECT * FROM brazilian_e_commerce.order_items;
-- Segmenting the data by Recency, frequency, and monetary value is my gameplan for this project
-- Find most recent order for each customer
SELECT *
FROM(SELECT *, MAX(order_purchase_timestamp) OVER(PARTITION BY customer_id) AS 'recent_order'
FROM brazilian_e_commerce.orders
ORDER BY recent_order) AS recent_order_table
-- now add in the number of orders per customer
;
SELECT 
    c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp,
    MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id) AS recent_order,
    COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id) AS number_of_orders
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
ORDER BY COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id);

-- checking the above query, seemed wrong but is correct
SELECT customer_unique_id, COUNT(customer_unique_id)
FROM customers 
GROUP BY customer_unique_id 
HAVING COUNT(customer_unique_id) > 1;
-- adding in the total spent into segmentation
SELECT 
    c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp,
    MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id) AS recent_order,
    COUNT(DISTINCT c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id) AS number_of_orders,
    ROUND(SUM(p.payment_value) OVER(PARTITION BY c.customer_unique_id), 2) AS total_spent
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_payments p ON o.order_id = p.order_id
ORDER BY COUNT(DISTINCT c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id);
-- realized that the total spent is not correct for customers who used more than one payment method
-- a with statement should fix this to aggregate the payment value before aggregating the customers total spent
WITH order_totals AS( SELECT 
order_id,
SUM(payment_value) AS total_order_price
FROM order_payments
GROUP BY order_id
)
SELECT
 c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp,
    MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id) AS recent_order,
    COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id) AS number_of_orders,
    ROUND(SUM(ot.total_order_price) OVER(PARTITION BY c.customer_unique_id), 2) AS total_spent
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_totals ot ON o.order_id = ot.order_id
ORDER BY COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id);
-- to make segmentation easier in tableau, add in a recency days column that has how many days since most recent order
WITH order_totals AS( SELECT 
order_id,
SUM(payment_value) AS total_order_price
FROM order_payments
GROUP BY order_id
)
SELECT
	c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp,
    MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id) AS recent_order,
    COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id) AS number_of_orders,
    ROUND(SUM(ot.total_order_price) OVER(PARTITION BY c.customer_unique_id), 2) AS total_spent,
	DATEDIFF(('2018-09-03'), (MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id))) AS recency_days
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_totals ot ON o.order_id = ot.order_id
ORDER BY COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id)
LIMIT 50000;
-- we have to split the file in half to export it and then we can UNION the two tables in tableau
WITH order_totals AS( SELECT 
order_id,
SUM(payment_value) AS total_order_price
FROM order_payments
GROUP BY order_id
)
SELECT
	c.customer_unique_id,
    o.order_id,
    o.order_purchase_timestamp,
    MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id) AS recent_order,
    COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id) AS number_of_orders,
    ROUND(SUM(ot.total_order_price) OVER(PARTITION BY c.customer_unique_id), 2) AS total_spent,
	DATEDIFF(('2018-09-03'), (MAX(o.order_purchase_timestamp) OVER(PARTITION BY c.customer_unique_id))) AS recency_days
FROM orders o
JOIN customers c ON c.customer_id = o.customer_id
JOIN order_totals ot ON o.order_id = ot.order_id
ORDER BY COUNT(c.customer_unique_id) OVER(PARTITION BY c.customer_unique_id) DESC LIMIT 49440
;
-- Checking the repeat rate of any purchased item category
SELECT 
    p.product_category_name,
    COUNT(DISTINCT o.customer_id) AS total_customers,
    -- Calculate what % of customers who bought this item ordered again
    ROUND(SUM(CASE WHEN c_counts.order_count > 1 THEN 1 ELSE 0 END) * 100.0 / COUNT(DISTINCT o.customer_id), 4) AS repeat_rate
FROM order_items i
JOIN orders o ON i.order_id = o.order_id
JOIN products p ON i.product_id = p.product_id
JOIN (
    -- Subquery to get total orders per customer
    SELECT customer_id, COUNT(order_id) as order_count 
    FROM orders 
    GROUP BY customer_id
) c_counts ON o.customer_id = c_counts.customer_id
GROUP BY p.product_category_name
HAVING total_customers > 50 -- Filter out tiny categories
ORDER BY repeat_rate DESC;

