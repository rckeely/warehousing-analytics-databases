DROP DATABASE star_db;
CREATE DATABASE star_db;
\c star_db;

CREATE TABLE dim_dates(
  iso_date VARCHAR PRIMARY KEY,
  year INTEGER,
  quarter INTEGER,
  month INTEGER,
  week INTEGER,
  day_of_year INTEGER,
  -- The others are built in, this could be added
  -- day_of_quarter INTEGER,
  day_of_month INTEGER,
  day_of_week VARCHAR
);

CREATE TABLE dim_employees(
  employee_number INTEGER PRIMARY KEY,
  last_name VARCHAR,
  first_name VARCHAR,
  job_title VARCHAR
);

CREATE TABLE dim_offices(
  office_code INTEGER PRIMARY KEY,
  city VARCHAR,
  state VARCHAR,
  country VARCHAR,
  office_location BYTEA
);

CREATE TABLE dim_products(
  product_code VARCHAR PRIMARY KEY,
  product_line VARCHAR,
  product_name VARCHAR,
  product_scale VARCHAR,
  product_vendor VARCHAR
  -- These variables do not seem useful for categorising data
  -- product_description VARCHAR,
  -- html_description VARCHAR
);

CREATE TABLE dim_orders(
  order_number INTEGER PRIMARY KEY,
  status VARCHAR
  -- Again, comments does not seem useful for categorising data
  -- comments VARCHAR
);

CREATE TABLE dim_customers(
  customer_number INTEGER PRIMARY KEY,
  customer_name VARCHAR,
  contact_first_name VARCHAR,
  contact_last_name VARCHAR,
  city VARCHAR,
  state VARCHAR,
  country VARCHAR,
  customer_location BYTEA
);

CREATE TABLE fact_order_items(
  order_item_number VARCHAR PRIMARY KEY,
  order_line_number INTEGER,
  -- Foreign keys, to hook into dim tables
  order_number INTEGER REFERENCES dim_orders(order_number),
  customer_number INTEGER REFERENCES dim_customers(customer_number),
  product_code VARCHAR REFERENCES dim_products(product_code),
  office_code INTEGER REFERENCES dim_offices(office_code),
  employee_number INTEGER REFERENCES dim_employees(employee_number),
  employee_boss INTEGER REFERENCES dim_employees(employee_number),
  order_date VARCHAR REFERENCES dim_dates(iso_date),
  required_date VARCHAR REFERENCES dim_dates(iso_date),
  shipped_date VARCHAR REFERENCES dim_dates(iso_date),
  -- Measures
  quantity_ordered INTEGER,
  price_each MONEY,
  customer_credit_limit MONEY,
  quantity_in_stock INTEGER,
  buy_price MONEY,
  _m_s_r_p MONEY,
  -- Derived measures
  revenue MONEY,
  costs MONEY,
  profits MONEY,
  profit_per_item MONEY,
  profit_margin DECIMAL
);
