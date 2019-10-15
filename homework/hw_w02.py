import DBConnection
import pandas as pd
from collections import OrderedDict

###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-
# SQL Queries
###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-

join_query = """
    SELECT -- Keys --
           i.order_item_number,
           i.order_line_number,
           o.order_number,
           c.customer_number,
           p.product_code,
           e.office_code,
           e.employee_number,
           -- Name change because of ambiguity of reports_to
           -- in context of order_items table
           CAST(e.reports_to AS NUMERIC(10,2)) AS employee_boss,
           o.order_date,
           o.required_date,
           o.shipped_date,
           -- Measures --
           i.quantity_ordered,
           i.price_each,
           -- Name change because of ambiguity of credit_limit
           -- in context of order_items table
           c.credit_limit AS customer_credit_limit,
           p.quantity_in_stock,
           -- Forcing money type to numeric for convenience in creating
           -- derived measures
           CAST(p.buy_price AS NUMERIC(10,2)),
           p._m_s_r_p
    FROM orders AS o
    INNER JOIN order_items AS i
        USING(order_number)
    INNER JOIN customers AS c
        USING(customer_number)
    INNER JOIN employees AS e
        ON sales_rep_employee_number = employee_number
    -- This table has no measures, so the join is
    -- unnecessary
    -- INNER JOIN offices AS f
    --    USING(office_code)
    INNER JOIN products as p
        USING(product_code);
"""

select_employees_query = """
    SELECT employee_number,
            last_name,
            first_name,
            job_title
    FROM employees;
    """

select_offices_query = """
    SELECT office_code,
            city,
            state,
            country,
            office_location
    FROM offices;
    """

select_products_query = """
    SELECT product_code,
          product_line,
          product_name,
          product_scale,
          product_vendor
    FROM products;
    """

select_orders_query = """
    SELECT order_number,
          status
    FROM orders;
    """

select_customers_query = """
    SELECT customer_number,
        customer_name,
        contact_first_name,
        contact_last_name,
        city,
        state,
        country,
        customer_location
    FROM customers;
    """

select_dates_query = """
    SELECT order_date,
        required_date,
        shipped_date
    FROM orders;
    """

###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-
# Module functions
###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-
def create_df(query, conn):
    column_names, returned_rows = conn.select_query(query)
    df = pd.DataFrame(returned_rows)
    df.columns = column_names
    return df

def fix_measures_dt(measures_df):
    for key in ["order_date",
                "required_date",
                "shipped_date",]:
        # Similar to the correction done on the dates, convert the dates
        # to strings for use as keys, setting NaT to 0000-00-00
        measures_df[key] = (pd.to_datetime(measures_df[key])
                           .apply(lambda x: x.strftime('%Y-%m-%d')
                                  if not pd.isnull(x) else '0000-00-00'))

def create_derived_measures(measures_df):
    # Transforms
    measures_df['revenue'] = (measures_df['quantity_ordered'] *
            measures_df['price_each'])
    measures_df['costs'] = (measures_df['quantity_ordered'] *
            measures_df['buy_price'])
    measures_df['profits'] = (measures_df['revenue'] -
            measures_df['costs'])
    measures_df['profit_per_item'] = (measures_df['profits'] /
            measures_df['quantity_ordered'])
    measures_df['profit_margin'] = (measures_df['profits'] /
            measures_df['revenue'])

def load_table(conn, table_name, frame):
    """Load a dataframe into a specified table"""
    # Build table
    for index, row in frame.iterrows():
        query_str, var_tuple = build_query(row, table_name, "INSERT")
        #print(query_str)
        conn.insert_query(query_str, var_tuple)

def build_query(row, table, query_type):
    """Construct query from row for table"""
    # Abstracted to allow for additon of UPDATE and SELECT later on
    # if required
    if query_type == "INSERT":
        return build_insert_query(row, table)
    else:
        return ""

def build_insert_query(row, table):
    """Construct insert query from row for table"""
    query_columns = ""
    variable_list = list()
    for key in row.keys():
        # Insert is columns then values, so construct as two strings
        query_columns += f"{key}, "
        # Use format value to handle value types in SQL
        variable_list.append(row[key])
    values = "%s, " * len(row.keys())
    # Index to -2 to remove ", " from last value
    query = f"INSERT INTO {table} ({query_columns[:-2]}) VALUES ({values[:-2]})"
    return query, variable_list

def create_date_df(select_dates_query, conn_op_db):
    raw_date_df = create_df(select_dates_query, conn_op_db)
    # Include all possible dates initially
    date_series = pd.concat([raw_date_df['order_date'],
                        raw_date_df['required_date'],
                        raw_date_df['shipped_date']],axis=0)
    # Drop the duplicate dates
    date_df = pd.DataFrame(date_series.drop_duplicates())
    # Rename the sole column to the key name
    date_df.columns = ['iso_date']
    return date_df

def transform_date_df(date_df):
    # Use inbuilt functions to create a number of potentially
    # useful date filters
    date_df['year'] = pd.to_datetime(date_df.iso_date).dt.year
    date_df['quarter'] = pd.to_datetime(date_df.iso_date).dt.quarter
    date_df['month'] = pd.to_datetime(date_df.iso_date).dt.month
    date_df['week'] = pd.to_datetime(date_df.iso_date).dt.week
    date_df['day_of_year'] = pd.to_datetime(date_df.iso_date).dt.dayofyear
    date_df['day_of_month'] = pd.to_datetime(date_df.iso_date).dt.day
    dayOfWeek={0:'Monday', 1:'Tuesday', 2:'Wednesday', 3:'Thursday',
        4:'Friday', 5:'Saturday', 6:'Sunday'}
    date_df['day_of_week'] = (pd.to_datetime(date_df.iso_date)
        .dt.dayofweek.map(dayOfWeek))
    # This converts the date to a string in ISO format, YYYY-MM-DD
    # and converting the NaT datetime to 0000-00-00 to avoid having
    # to drop those columns
    date_df['iso_date'] = (pd.to_datetime(date_df['iso_date'])
                               .apply(lambda x: x.strftime('%Y-%m-%d')
                                      if not pd.isnull(x) else '0000-00-00'))
    # Fix all of the other NaTs to None
    date_df = date_df.where(pd.notnull(date_df), None)
    # Reset the index, dropping the old index to fix effects of concatenation
    date_df.reset_index(inplace=True, drop=True)
    return date_df

def etl_date_dim(select_dates_query, conn_op_db,
                   conn_star_db, table_name = 'dim_dates'):
    date_df = create_date_df(select_dates_query, conn_op_db)
    date_df = transform_date_df(date_df)
    load_table(conn_star_db, table_name, date_df)

def etl_table(query, conn_op_db, conn_star_db, table):
    # The date dim has more specific changes, so it is built with a
    # separate function
    if table == 'dim_dates':
        etl_date_dim(query, conn_op_db, conn_star_db)
    # Similarly, the measures table has specific effects
    elif table == 'fact_order_items':
        etl_measures_table(query, conn_op_db, conn_star_db)
    else:
        df = create_df(query, conn_op_db)
        load_table(conn_star_db, table, df)

def etl_measures_table(join_query, conn_op_db, conn_star_db,
        table='fact_order_items'):
    measures_df = create_df(join_query, conn_op_db)
    # Convert the datetimes to strings for easier use as keys
    fix_measures_dt(measures_df)
    # Create derived measures for use in summaries
    create_derived_measures(measures_df)
    load_table(conn_star_db, table, measures_df)

###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-
# Run script
###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-###-
if __name__ == "__main__":
    conn_op_db = DBConnection.DBConnection(dbname="company_db")
    conn_star_db = DBConnection.DBConnection(dbname="star_db")

    # Using an ordered dictionary as the measures table must be loaded last
    query_dict = OrderedDict([('dim_employees', select_employees_query),
                    ('dim_offices', select_offices_query),
                    ('dim_products', select_products_query),
                    ('dim_orders', select_orders_query),
                    ('dim_customers', select_customers_query),
                    ('dim_dates', select_dates_query),
                    ('fact_order_items', join_query)])
    for key in query_dict.keys():
        print(f"Starting ETL of {key}... ", end="")
        etl_table(query_dict[key], conn_op_db, conn_star_db, key)
        print(f"complete")
    print("ETL complete.")

    conn_op_db.disconnect(True)
    conn_star_db.disconnect(True)
