!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-star.sql

-- Connect to the local database where Pagila is loaded
import sql
%load_ext sql

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila_star'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
%sql $conn_string

-- Roll-up
--   Stepping up the level of aggregation to a large grouping
--TODO: Write a query that calculates revenue (sales_amount) by day, rating, and country. Sort the data by revenue in descending order, and limit the data to the top 20 results. The first few rows of your output should match the table below.
%%time
%%sql

SELECT dimDate.day, dimMovie.rating, dimCustomer.country, sum(sales_amount) as revenue
FROM factSales
JOIN dimMovie ON (dimMovie.movie_key = factSales.movie_key)
JOIN dimDate ON (dimDate.date_key = factSales.date_key)
JOIN dimCustomer ON (dimCustomer.customer_key =  factSales.customer_key)
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.country)
ORDER BY revenue DESC
LIMIT 20

-- Drill-down
--  Breaking up one of the dimensions to a lower level
--  city is broken up into districts
-- TODO: Write a query that calculates revenue (sales_amount) by day, rating, and district. Sort the data by revenue in descending order, and limit the data to the top 20 results. The first few rows of your output should match the table below.
%%time
%%sql

SELECT dimDate.day, dimMovie.rating, dimCustomer.district, sum(sales_amount) as revenue
FROM factSales
JOIN dimMovie ON (dimMovie.movie_key = factSales.movie_key)
JOIN dimDate ON (dimDate.date_key = factSales.date_key)
JOIN dimCustomer ON (dimCustomer.customer_key =  factSales.customer_key)
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.district)
ORDER BY revenue DESC
LIMIT 10
