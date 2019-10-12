-- Get the top 3 product types that have proven most profitable
SELECT DISTINCT(product_code),
    profits
FROM  fact_order_items
ORDER BY profits DESC
LIMIT 3;

-- Get the top 3 products by most items sold
SELECT product_code,
      SUM(quantity_ordered) AS total_quantity
FROM fact_order_items
GROUP BY product_code
ORDER BY total_quantity DESC
LIMIT 3;

-- Get the top 3 products by items sold per country of customer for: USA,
-- Spain, Belgium
SELECT f.product_code,
  SUM(f.quantity_ordered) AS total_quantity
FROM fact_order_items AS f
INNER JOIN dim_customers AS c
  USING(customer_number)
WHERE c.country IN ('USA', 'Spain', 'Belgium')
GROUP BY f.product_code
ORDER BY total_quantity DESC
LIMIT 3;

-- Get the most profitable day of the week
SELECT d.day_of_week,
  SUM(f.profits) AS total_per_day
FROM fact_order_items AS f
INNER JOIN dim_dates AS d
  ON order_date = iso_date
GROUP BY d.day_of_week
ORDER BY total_per_day DESC
LIMIT 1;

-- Get the top 3 city-quarters with the highest average profit margin in their
-- sales
SELECT o.city,
  AVG(f.profit_margin) AS avg_profit_margin,
  d.quarter
FROM fact_order_items AS f
INNER JOIN dim_dates AS d
  ON order_date = iso_date
INNER JOIN dim_offices AS o
    USING(office_code)
GROUP BY (o.city, d.quarter)
ORDER BY avg_profit_margin DESC
LIMIT 3;

-- List the employees who have sold more goods (in $ amount) than the average
-- employee.
-- SELECT e.first_name,
--   e.last_name,
--   SUM(f.revenue) AS total_revenue,
--   AVG(f.revenue) AS avg_revenue
-- FROM fact_order_items AS f
-- INNER JOIN dim_employees AS e
--   USING(employee_number);
-- GROUP BY employee_number;

-- List all the orders where the sales amount in the order is in the top 10%
-- of all order sales amounts (BONUS: Add the employee number)
