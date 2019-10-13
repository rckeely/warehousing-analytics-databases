## Homework - ETL and Dimensional Model

Please make a pull request with the following files:

1. A .sql file that defines a new database schema, in Postgres, according to the dimensional modeling ideas discussed in class (star schema!) for the classic models dataset you used last week.
2. A .py file that performs the ETL process. It should load data from the database you created last week into the new database (defined in step 1) for analytics (remember, in one Postgres server you can create multiple databases).
3. The queries.sql file should be filled in with queries to your analytics database, answering the questions posed.

To start the server, run start_server.sh:
```sh
./start_server.sh
```
(This file has been updated to use a named volume as persistence now seemed valuable)

To run postgres interactively, run postgres_interactive.sh
```sh
./postgres_interactive.sh
```

To run the star database creation SQL file and the Python ETL file, and then the query file run run_hw.sh
```sh
./run_hw.sh
```
This outputs the results into a file called [ results.txt ], also committed for your convenience.
