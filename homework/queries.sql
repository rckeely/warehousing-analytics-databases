--- Get the top 3 product types that have proven most profitable
select product_code, product_name, (_m_s_r_p - buy_price) as profit
  from products
  order by profit desc
  limit 3;

--- Get the top 3 products by most items sold


--- Get the top 3 products by items sold per country of customer for: USA, Spain, Belgium


--- Get the most profitable day of the week


--- Get the top 3 city-quarters with the highest average profit margin in their sales


-- List the employees who have sold more goods (in $ amount) than the average employee.


-- List all the orders where the sales amount in the order is in the top 10% of all order sales amounts (BONUS: Add the employee number)
