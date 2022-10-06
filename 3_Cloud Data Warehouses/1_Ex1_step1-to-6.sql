"""
in Lession 1:
 -> What is a Data Warehouse (in a business perspective)
 -> What is a Data Warehouse (in a technical perspective)
 -> Dimension modeling
 -> DWH architecture
 -> OLAP Cubes
 -> DWH storgae technology

 """

-- Recap about Rational Database using SQL

""" Ex1 _ Step 1&2
Sakila Star Schema & ETL
"""

-- STEP0: Using ipython-sql
-- %sql: for one-line SQL query , can access a python var using $

----  %%sql: for multi-line query ;  can not access a python var using $

""" Step1: Connect to the local database where Pagila is loaded """
-- Creat the pagila db and fill it with data
-- using "!" for run db and postgre SQL only in Jupiter shell

!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql

-- Connect to the newly created db
%load_ext sql

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

-- postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)

%sql $conn_string

""" Step2: Explore the 3NF Schema """
nStores = %sql select count(*) from store;
nFilms = %sql select count(*) from film;
nCustomers = %sql select count(*) from customer;
nRentals = %sql select count(*) from rental;
nPayment = %sql select count(*) from payment;
nStaff = %sql select count(*) from staff;
nCity = %sql select count(*) from city;
nCountry = %sql select count(*) from country;

print("nFilms\t\t=", nFilms[0][0])
print("nCustomers\t=", nCustomers[0][0])
print("nRentals\t=", nRentals[0][0])
print("nPayment\t=", nPayment[0][0])
print("nStaff\t\t=", nStaff[0][0])
print("nStores\t\t=", nStores[0][0])
print("nCities\t\t=", nCity[0][0])
print("nCountry\t\t=", nCountry[0][0])

-- What time period are we we talking about
%%sql
select min(payment_date) as start, max(payment_date) as end from payment;


-- Where do events in this database occur?
%%sql
select district, count(city_id) as n
from address
group by district
order by n desc
limit 10;

""" STEP3 : Perform some simple data analysis"""
--  NO NEED RUN BELOW, IN CASE JUPITER STILL KEEP ON AFTER RUN TWO ABOVE STEPS
!PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
!PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql

%load_ext sql

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

-- postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
%sql $conn_string

-- display Films table
%%sql
select film_id, title, release_year, rental_rate, rating  from film limit 5;

-- Payment
%%sql
select * from payment limit 5;

--  Inventory
%%sql
select * from inventory limit 5;

--  Get the movie on every payment
%%sql
SELECT f.title, p.amount, p.payment_date, p.customer_id
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
limit 5;

-- Sum of movie rental revenue
%%sql
SELECT f.title, sum(p.amount) as revenue
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
GROUP BY f.title
ORDER BY revenue DESC
limit 10;

-- Top Grossing cities
-- Get the city of each payment
%%sql
SELECT p.customer_id, p.rental_id, p.amount, ci.city
FROM payment p
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
order by p.payment_date
limit 10;

--  Top Grossing cities
%%sql
SELECT ci.city as city, sum(p.amount) as revenue
FROM payment p
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
GROUP BY city
ORDER BY revenue DESC
limit 10;

-- Total revenue by month
%%sql
SELECT sum(p.amount) as revenue, EXTRACT(month FROM p.payment_date) as month
from payment p
group by month
order by revenue desc
limit 10;

-- Each movie by customer city and by month (data cube)
%%sql
SELECT f.title, p.amount, p.customer_id, ci.city, p.payment_date,EXTRACT(month FROM p.payment_date) as month
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
order by p.payment_date
limit 10;

-- Sum of revenue of each movie by customer city and by month
%%sql
SELECT f.title as title, SUM(p.amount) as revenue, ci.city as city, EXTRACT(month FROM p.payment_date) as month
FROM payment p
JOIN rental r    ON ( p.rental_id = r.rental_id )
JOIN inventory i ON ( r.inventory_id = i.inventory_id )
JOIN film f ON ( i.film_id = f.film_id)
JOIN customer c  ON ( p.customer_id = c.customer_id )
JOIN address a ON ( c.address_id = a.address_id )
JOIN city ci ON ( a.city_id = ci.city_id )
GROUP BY title, city, month
ORDER BY month, revenue DESC
limit 10;

-- STEP4: To transform from 3NF to Star Schema, creating Facts and Dimensions table
-- To creat Facts and Dimensions
# !PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
# !PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
# !PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql


%load_ext sql

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
%sql $conn_string

-- Create the first dimension table
-- Creat table dimDate
%%sql
CREATE TABLE dimDate
(
    date_key integer NOT NULL PRIMARY KEY,
    date date NOT NULL,
    year smallint NOT NULL,
    quarter smallint NOT NULL,
    month smallint NOT NULL,
    day smallint NOT NULL,
    week smallint NOT NULL,
    is_weekend boolean
);

-- Just verify column name and data-type
%%sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name   = 'dimdate'

-- Creat another dimension dimension tables
%%sql
CREATE TABLE dimCustomer
(
  customer_key SERIAL PRIMARY KEY,
  customer_id  smallint NOT NULL,
  first_name   varchar(45) NOT NULL,
  last_name    varchar(45) NOT NULL,
  email        varchar(50),
  address      varchar(50) NOT NULL,
  address2     varchar(50),
  district     varchar(20) NOT NULL,
  city         varchar(50) NOT NULL,
  country      varchar(50) NOT NULL,
  postal_code  varchar(10),
  phone        varchar(20) NOT NULL,
  active       smallint NOT NULL,
  create_date  timestamp NOT NULL,
  start_date   date NOT NULL,
  end_date     date NOT NULL
);

CREATE TABLE dimMovie
(
  movie_key          SERIAL PRIMARY KEY,
  film_id            smallint NOT NULL,
  title              varchar(255) NOT NULL,
  description        text,
  release_year       year,
  language           varchar(20) NOT NULL,
  original_language  varchar(20),
  rental_duration    smallint NOT NULL,
  length             smallint NOT NULL,
  rating             varchar(5) NOT NULL,
  special_features   varchar(60) NOT NULL
);
CREATE TABLE dimStore
(
  store_key           SERIAL PRIMARY KEY,
  store_id            smallint NOT NULL,
  address             varchar(50) NOT NULL,
  address2            varchar(50),
  district            varchar(20) NOT NULL,
  city                varchar(50) NOT NULL,
  country             varchar(50) NOT NULL,
  postal_code         varchar(10),
  manager_first_name  varchar(45) NOT NULL,
  manager_last_name   varchar(45) NOT NULL,
  start_date          date NOT NULL,
  end_date            date NOT NULL
);


-- Creat fact table:
-- sales_key integer NOT NULL PRIMARY KEY,
%%sql
CREATE TABLE factSales
(
    sales_key SERIAL PRIMARY KEY,
    date_key integer REFERENCES dimDate(date_key),
    customer_key integer REFERENCES dimCustomer(customer_key),
    movie_key integer REFERENCES dimMovie(movie_key),
    store_key integer REFERENCES dimStore(store_key),
    sales_amount integer
);


%%sql
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name   = 'factsales'

/* STEP 5: ETL the data from 3NF tables to Facts & Dimensions Tables */

/* Run 3  lines below only if needd */
# !PGPASSWORD=student createdb -h 127.0.0.1 -U student pagila
# !PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-schema.sql
# !PGPASSWORD=student psql -q -h 127.0.0.1 -U student -d pagila -f Data/pagila-data.sql

%load_ext sql

DB_ENDPOINT = "127.0.0.1"
DB = 'pagila'
DB_USER = 'student'
DB_PASSWORD = 'student'
DB_PORT = '5432'

# postgresql://username:password@host:port/database
conn_string = "postgresql://{}:{}@{}:{}/{}" \
                        .format(DB_USER, DB_PASSWORD, DB_ENDPOINT, DB_PORT, DB)

print(conn_string)
%sql $conn_string


-- To ETL from 3NF to Star Schema by using INSERT
-- Syntax below todo: EXTRACT directly data from payment TABLE, then INSERT onto dimDate table
%%sql
INSERT INTO dimDate (date_key, date, year, quarter, month, day, week, is_weekend)
SELECT DISTINCT(TO_CHAR(payment_date :: DATE, 'yyyyMMDD')::integer) AS date_key,
       date(payment_date)                                           AS date,
       EXTRACT(year FROM payment_date)                              AS year,
       EXTRACT(quarter FROM payment_date)                           AS quarter,
       EXTRACT(month FROM payment_date)                             AS month,
       EXTRACT(day FROM payment_date)                               AS day,
       EXTRACT(week FROM payment_date)                              AS week,
       CASE WHEN EXTRACT(ISODOW FROM payment_date) IN (6, 7) THEN true ELSE false END AS is_weekend
FROM payment;


-- TODO: populate the dimCustomer table with data from tables: customer; address; city and country
%%sql
INSERT INTO dimCustomer (customer_key, customer_id, first_name, last_name, email, address,
                         address2, district, city, country, postal_code, phone, active,
                         create_date, start_date, end_date)
SELECT  c.customer_id    AS customer_key,
        c.customer_id,
        c.first_name,
        c.last_name,
        c.email,
        a.address,
        a.address2,
        a.district,
        ci.city,
        co.country,
        a.postal_code,
        a.phone,
        c.active,
        now()           AS create_date,
        now()           AS start_date,
        now()           AS end_date
FROM customer c
JOIN address a  ON (c.address_id = a.address_id)
JOIN city ci    ON (a.city_id = ci.city_id)
JOIN country co ON (ci.country_id = co.country_id);


-- TODO: polulate the dimMovie table with data from film, and language tables
%%sql
INSERT INTO dimMovie (movie_key, film_id, title, description, release_year,
                        language ,original_language, rental_duration, length,
                        rating, special_features )
SELECT  f.film_id           AS movie_key,
        f.film_id,
        f.title,
        f.description,
        f.release_year,
        l.name              AS language,
        orig_lang.name      AS original_language,
        f.rental_duration,
        f.length,
        f.rating,
        f.special_features
FROM film f
JOIN language l              ON (f.language_id=l.language_id)
LEFT JOIN language orig_lang ON (f.original_language_id = orig_lang.language_id);

-- TODO populate dimStore table with data from the tables: store, staff, address, city, country
%%sql
INSERT INTO dimStore    (store_key, store_id, address, address2,
                        district, city, country, postal_code,
                        manager_first_name, manager_last_name,
                        start_date, end_date )
SELECT  sto.store_id          AS store_key,
        sto.store_id,
        a.address,
        a.address2,
        a.district,
        ci.city,
        co.country,
        a.postal_code,
        sta.first_name      AS  manager_first_name,
        sta.first_name      AS  manager_last_name,
        now()               AS start_date,
        now()               AS end_date
FROM store sto
JOIN staff sta              ON(sto.store_id = sta.store_id)
JOIN address a             ON(a.address_id = sta.address_id)
JOIN city ci                ON(ci.city_id=a.city_id)
JOIN country co             ON(co.country_id=ci.country_id)


-- TODO populate for factSales from the tables: payment/ rental/ inventory
-- Note: igonore Primary key `sales_key` due to it automatically generated, do INSERT from date_key to remaining
%%sql
INSERT INTO factSales (sales_key, date_key, customer_key,
                         movie_key, store_key, sales_amount)
SELECT  r.rental_id                                             AS sales_key,
        TO_CHAR(p.payment_date :: DATE, 'yyyyMMDD')::integer    AS date_key,
        p.customer_id                                           AS customer_key,
        i.film_id                                               AS movie_key,
        i.store_id                                              AS store_key,
        p.amount                                                AS sales_amount
FROM payment p
JOIN rental r           ON(r.rental_id = p.rental_id)
JOIN inventory i        ON(i.inventory_id = r.inventory_id)


--Tempo to drop table
-- %sql drop table factSales