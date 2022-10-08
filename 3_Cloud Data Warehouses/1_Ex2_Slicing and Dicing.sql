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

-- Start with a simple cube
--TODO: Write a query that calculates the revenue (sales_amount) by day, rating, and city. Remember to join with the appropriate dimension tables to replace the keys with the dimension labels. Sort by revenue in descending order and limit to the first 20 rows. The first few rows of your output should match the table below.
%%time
%%sql

SELECT dimDate.day, dimMovie.rating, dimCustomer.city, sum(sales_amount) as revenue
FROM factSales
JOIN dimMovie ON (dimMovie.movie_key = factSales.movie_key)
JOIN dimDate ON (dimDate.date_key = factSales.date_key)
JOIN dimCustomer ON (dimCustomer.customer_key =  factSales.customer_key)
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.city)
ORDER BY revenue DESC
LIMIT 20

--SLICING
--Slicing is the  recuction of the dimensionalty of a cube by 1 (eg. from 3 to 2), fixing one of the dimensions to a single value. In the example above, we have a 3-dimensional cube on day, rating and country.

--TODO: Write a query that reduces the dimensionality of the above example by limiting the results to only include movies with a rating of "PG-13". Again, sort by revenue in descending order and limit to the first 20 rows. The first few rows of your output should match the table below.
%%time
%%sql

SELECT dimDate.day, dimMovie.rating, dimCustomer.city, sum(sales_amount) as revenue
FROM factSales
JOIN dimMovie ON (dimMovie.movie_key = factSales.movie_key)
JOIN dimDate ON (dimDate.date_key = factSales.date_key)
JOIN dimCustomer ON (dimCustomer.customer_key =  factSales.customer_key)
WHERE dimMovie.rating = 'PG-13'
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.city)
ORDER BY revenue DESC
LIMIT 20

-- DICING
-- Dicing is ceating a subcube with the same dimensionality but fewer values for two or more dimensions.
-- TODO Write a query to create a subcube of the initial cube that includes moves with:

-- ratings of PG or PG-13
-- in the city of Bellevue or Lancaster
-- day equal to 1, 15, or 30
%%time
%%sql

SELECT dimDate.day, dimMovie.rating, dimCustomer.city, sum(sales_amount) as revenue
FROM factSales
JOIN dimMovie ON (dimMovie.movie_key = factSales.movie_key)
JOIN dimDate ON (dimDate.date_key = factSales.date_key)
JOIN dimCustomer ON (dimCustomer.customer_key =  factSales.customer_key)
WHERE (dimMovie.rating IN ('PG-13', 'PG') )
    AND (dimCustomer.city IN ('Bellevue', 'Lancaster'))
    AND (dimDate.day IN (1, 15, 30))
GROUP BY (dimDate.day, dimMovie.rating, dimCustomer.city)
ORDER BY revenue DESC
LIMIT 20


