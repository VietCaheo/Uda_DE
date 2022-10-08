
-- OLAP Cubes - Grouping Sets


!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila_star
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila_star -f Data/pagila-star.sql

--  Connect to the local database where Pagila is loaded
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

-- Total Revenue
--  TODO: Write a query that calculates total revenue (sales_amount)
%%sql
SELECT sum(sales_amount) as revenue
FROM factSales

-- Revenue by country
%%time
%%sql

SELECT  dimCustomer.country, sum(sales_amount) as revenue
FROM factSales
JOIN dimCustomer ON (dimCustomer.customer_key =  factSales.customer_key)
GROUP BY ( dimCustomer.country)
ORDER BY revenue DESC
LIMIT 20

-- Revenue by month
%%time
%%sql

SELECT  dimDate.month, sum(sales_amount) as revenue
FROM factSales
JOIN dimDate ON (dimDate.date_key =  factSales.date_key)
GROUP BY (dimDate.month)
ORDER BY revenue DESC
LIMIT 20

-- Revenue by month and country
%%time
%%sql

SELECT  dimDate.month mo, dimStore.country as co, sum(sales_amount) as revenue
FROM factSales
JOIN dimStore ON (dimStore.store_key =  factSales.store_key)
JOIN dimDate ON (dimDate.date_key =  factSales.date_key)
GROUP BY grouping sets (( dimDate.month, dimStore.country))
ORDER BY mo
LIMIT 20

-- Revenue by grouping sets with ordered followed by:  month, country, and month & country
%%time
%%sql

SELECT  dimDate.month mo, dimStore.country as co, sum(sales_amount) as revenue
FROM factSales
JOIN dimStore ON (dimStore.store_key =  factSales.store_key)
JOIN dimDate ON (dimDate.date_key =  factSales.date_key)
GROUP BY grouping sets ( (), dimDate.month, dimStore.country, ( dimDate.month, dimStore.country))
ORDER BY mo
LIMIT 20

