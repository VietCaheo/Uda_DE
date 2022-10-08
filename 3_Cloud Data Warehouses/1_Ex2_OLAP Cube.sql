/* Exercise 02 - OLAP Cubes - CUBE */

!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-star.sql

-- Connection the local database where Pagila is loaded
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

-- CUBE
-- Group by CUBE (dim1, dim2), produces all combinations of different lengths in one go.
-- TODO: Write a query that calculates the various levels of aggregation done in the grouping sets exercise (total, by month, by country, by month & country) using the CUBE function.
%%time
%%sql

SELECT  dimDate.month mo, dimStore.country as co, sum(sales_amount) as revenue
FROM factSales
JOIN dimStore ON (dimStore.store_key =  factSales.store_key)
JOIN dimDate ON (dimDate.date_key =  factSales.date_key)
GROUP BY CUBE (mo, co)
ORDER BY mo
LIMIT 20