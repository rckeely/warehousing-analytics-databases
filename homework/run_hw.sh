#!/bin/sh

# Load database using SQL file
cat create_company_db.sql | docker run --net host -i postgres psql --host 0.0.0.0 --user postgres
# Fill database from Python
python hw_w01.py
