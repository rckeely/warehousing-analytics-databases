-- Connect to the star_db, needed for executing queries
\c star_db

-- Get the top 3 product types that have proven most profitable
SELECT p.product_name,
  p.product_code,
  r.profits
FROM dim_products AS p
INNER JOIN (
  SELECT DISTINCT(product_code),
      profits
  FROM  fact_order_items
  ORDER BY profits DESC
  LIMIT 3
) AS r
USING (product_code);

-- Get the top 3 products by most items sold
SELECT p.product_name,
  p.product_code,
  t.total_quantity
FROM dim_products AS p
INNER JOIN (
  SELECT product_code,
    SUM(quantity_ordered) AS total_quantity
    FROM fact_order_items
    GROUP BY product_code
    ORDER BY total_quantity DESC
    LIMIT 3) AS t
  USING(product_code);

-- Get the top 3 products by items sold per country of customer for: USA,
-- Spain, Belgium

-- NOTE:
-- I wasn't sure exactly what you meant by this, so I got the top 3 by quantity
-- for each of the 3 countries in a single table. To do this I needed to use two
-- nested subqueries, which I don't think is the best way to do it, but it
-- seemed to be the only way to get the rank column in a way that was usable
-- for a WHERE clause.
SELECT s.product_code,
      p.product_name,
      s.country,
      s.total_quantity,
      s.rank
FROM (
  SELECT s.product_code,
    s.country,
    s.total_quantity,
    RANK() OVER (PARTITION BY s.country ORDER BY s.total_quantity DESC)
  FROM (
    SELECT f.product_code,
      SUM(f.quantity_ordered) AS total_quantity,
      c.country
    FROM fact_order_items AS f
    INNER JOIN dim_customers AS c
      USING(customer_number)
    WHERE c.country IN ('USA', 'Spain', 'Belgium')
    GROUP BY (f.product_code, c.country)
    ORDER BY total_quantity DESC) AS s) AS s
INNER JOIN dim_products AS p
  USING(product_code)
WHERE s.rank <= 3;

-- This treats the three countries as a set. This may have been what you wanted
-- SELECT DISTINCT(p.product_name),
--   p.product_code,
--   s.total_quantity,
--   s.country
-- FROM fact_order_items AS f
-- INNER JOIN dim_products AS p
--   USING(product_code)
-- INNER JOIN (
--     SELECT f.product_code,
--       SUM(f.quantity_ordered) AS total_quantity,
--       c.country
--     FROM fact_order_items AS f
--     INNER JOIN dim_customers AS c
--       USING(customer_number)
--     GROUP BY (f.product_code, c.country)
--     ORDER BY total_quantity DESC) AS s
--   USING(product_code)
-- WHERE s.country IN ('USA', 'Spain', 'Belgium')
-- ORDER BY total_quantity DESC
-- LIMIT 3;

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
  CAST(AVG(f.profit_margin) AS NUMERIC(10,2)) AS avg_profit_margin,
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

-- NOTE:
-- For the next query, I do not think this is necessarily the simplest way to
-- perform the query, but I can't seem to find a simpler way to do them. I think
-- that maybe a different schema based on employee sales would improve things,
-- but then it would be awkward to do the previous four queries.
SELECT e.last_name,
  e.first_name,
  s.total_revenue
FROM dim_employees AS e
INNER JOIN (
    SELECT employee_number,
      SUM(revenue) AS total_revenue
    FROM fact_order_items
    GROUP BY employee_number
    HAVING SUM(revenue::NUMERIC) > (
      SELECT AVG(s.total_revenue::NUMERIC)
      FROM (
          SELECT employee_number,
            SUM(revenue) AS total_revenue
          FROM fact_order_items
          GROUP BY employee_number) AS s)
    ORDER BY SUM(revenue) DESC) AS s
  USING(employee_number)
ORDER BY s.total_revenue DESC;

-- List all the orders where the sales amount in the order is in the top 10%
-- of all order sales amounts (BONUS: Add the employee number)
SELECT DISTINCT(s.order_total),
  f.employee_number,
  s.order_number
FROM fact_order_items AS f
INNER JOIN(
    SELECT f.order_number,
      SUM(f.revenue) AS order_total
    FROM fact_order_items AS f
    GROUP BY f.order_number) AS s
  USING(order_number)
ORDER BY order_total DESC
LIMIT (SELECT ROUND(0.1 * COUNT(DISTINCT(order_number)), 0)
        FROM fact_order_items);
