#!/bin/sh

# Load database using SQL file
cat create_star_db.sql | docker run --net host -i postgres psql --host 0.0.0.0 --user postgres
# Perform ETL step
python hw_w02.py
# Execute the requested queries
cat queries.sql | docker run --net host -i postgres psql --host 0.0.0.0 --user postgres > results.txt
