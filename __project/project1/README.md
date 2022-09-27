1. Project Summary:
#-----------------
Base on Data Modeling on Postgres, build an ETL pipeline using Python. To complete the project:
    1. Define a fact and dimensions tables for a star schema for a particular analystic focus
    2. Write an ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQLs

##############################################################
2. How to run Python Scripts:
    1. To run py files: just put to Jupiter Note book as syntax "%run <file_name>.py"
    2. Order to run:
        -> run the create_tables.py firstly
        -> run etl.ipynb  for check a sigle log file or run whole data-set by running elt.py
        -> run test.ipynb for check result
    Note: each time finished test or debug, It should be reset kernal, or shutdown the files for next turn test.
##############################################################
3. Referring lists:
#---------------
for convert numpy.dt64:
https://stackoverflow.com/questions/13703720/converting-between-datetime-timestamp-and-datetime64

to fix issue "violates foreign key constraint" , and could not insert songid, arstistid = None value to table songplay_table
https://knowledge.udacity.com/questions/651840


issue when run Sanity test:
%load_ext sql

"Environment variable $DATABASE_URL not set, and no connect string given.
Connection info needed in SQLAlchemy format, example:
               postgresql://username:password@hostname/dbname
               or an existing connection: dict_keys([])"
