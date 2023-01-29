# dbproject

To import db2218.sql file, you have to use psql command line. Before import the database, create a database in your pgAdmin. Lets assume you created a db with name exampledb.
Execute this command on your terminal:
    psql -U username exampledb < pathofsqlfile/db2218.sql
username will be overriden acccording to your user name. 