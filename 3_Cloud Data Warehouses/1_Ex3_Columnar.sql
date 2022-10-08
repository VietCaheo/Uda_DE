/* Exercise 3:
Exercise demo for Columnar vs Row Storage
 */

-- The Columnar storage extension used here: cstore_fdw by citus_data https://github.com/citusdata/cstore_fdw

%load_ext sql

-- Creat the database
!sudo -u postgres psql -c 'CREATE DATABASE reviews;'

!wget http://examples.citusdata.com/customer_reviews_1998.csv.gz
!wget http://examples.citusdata.com/customer_reviews_1999.csv.gz

!gzip -d customer_reviews_1998.csv.gz 
!gzip -d customer_reviews_1999.csv.gz 

!mv customer_reviews_1998.csv /tmp/customer_reviews_1998.csv
!mv customer_reviews_1999.csv /tmp/customer_reviews_1999.csv

-- Connect to the database
DB_ENDPOINT = "127.0.0.1"
DB = 'reviews'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)

-- connecting to host sever for postgreSQL ...
%sql $conn_string

-- Creat a table with a normal (Row) storage & load data
%%sql
DROP TABLE IF EXISTS customer_reviews_row;
CREATE TABLE customer_reviews_row
(
    customer_id TEXT,
    review_date DATE,
    review_rating INTEGER,
    review_votes INTEGER,
    review_helpful_votes INTEGER,
    product_id CHAR(10),
    product_title TEXT,
    product_sales_rank BIGINT,
    product_group TEXT,
    product_category TEXT,
    product_subcategory TEXT,
    similar_product_ids CHAR(10)[]
)

%%sql 
COPY customer_reviews_row FROM '/tmp/customer_reviews_1998.csv' WITH CSV;
COPY customer_reviews_row FROM '/tmp/customer_reviews_1999.csv' WITH CSV;

--  Creat a table with columnar storage & load data
%%sql

-- load extension first time after install
CREATE EXTENSION cstore_fdw;

-- create server object
CREATE SERVER cstore_server FOREIGN DATA WRAPPER cstore_fdw;

-- Creat FOREIGN TABLE called `customer_reviews_col` with columns name contained in the two csv files:
%%sql
-- create foreign table
DROP FOREIGN TABLE IF EXISTS customer_reviews_col;

-------------
CREATE FOREIGN TABLE customer_reviews_col
(
    customer_id TEXT,
    review_date DATE,
    review_rating INTEGER,
    review_votes INTEGER,
    review_helpful_votes INTEGER,
    product_id CHAR(10),
    product_title TEXT,
    product_sales_rank BIGINT,
    product_group TEXT,
    product_category TEXT,
    product_subcategory TEXT,
    similar_product_ids CHAR(10)[]
)

-------------
-- leave code below as is
SERVER cstore_server
OPTIONS(compression 'pglz');

--  Implementing popuate the data into two tables
%%sql 
COPY customer_reviews_col FROM '/tmp/customer_reviews_1998.csv' WITH CSV;
COPY customer_reviews_col FROM '/tmp/customer_reviews_1999.csv' WITH CSV;

--  Using queue to comaparing performace between 2 kinds of table that just created
-- Verify the _row table
%%time
%%sql

SELECT 
    customer_id, review_date, review_rating, product_id, product_title
FROM 
    customer_reviews_row
WHERE
    customer_id = 'A27T7HVDXA3K2A' AND
    product_title LIKE '%Dune%' AND
    review_date >='1998-01-01' AND
    review_date <='1998-12-31';

-- Verify the col table
%%time
%%sql

SELECT 
    customer_id, review_date, review_rating, product_id, product_title
FROM 
    customer_reviews_col
WHERE
    customer_id = 'A27T7HVDXA3K2A' AND
    product_title LIKE '%Dune%' AND
    review_date >='1998-01-01' AND
    review_date <='1998-12-31';

