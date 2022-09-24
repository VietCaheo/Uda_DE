# Import the Library
import psycopg2

# Create a connection to the database
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)

try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get curser to the Database")
    print(e)


conn.set_session(autocommit=True)

# Creat 4 tables for music store, there are 4 tables focus on customer purchases: customer_transactions, customer, store, items_purchased

# TODO Creat the Fact table
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS customer_transactions (customer_id int, store_id int, spent numeric);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("INSERT INTO customer_transactions (customer_id, store_id, spent) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, 20.50))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
try: 
    cur.execute("INSERT INTO customer_transactions (customer_id, store_id, spent) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, 35.21))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Creat dimensions tables and insert data
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS items_purchased (customer_id int, item_number int, item_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO items_purchased (customer_id, item_number, item_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO items_purchased (customer_id, item_number, item_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 3, "Let It Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS store (store_id int, state varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO store (store_id, state) \
                 VALUES (%s, %s)", \
                 (1, "CA"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
try: 
    cur.execute("INSERT INTO store (store_id, state) \
                 VALUES (%s, %s)", \
                 (2, "WA"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS customer (customer_id int, name varchar, rewards boolean);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)
    
try: 
    cur.execute("INSERT INTO customer (customer_id, name, rewards) \
                 VALUES (%s, %s, %s)", \
                 (1, "Amanda", True))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO customer (customer_id, name, rewards) \
                 VALUES (%s, %s, %s)", \
                 (2, "Toby", False))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Query 1:  Find all the customers that spent more than 30 dollars, who are they, which store they bought it from, location of the store, what they bought and if they are a rewards member

try: 
    cur.execute("SELECT name, store.store_id, state FROM ((customer_transactions JOIN customer ON \
                                                    customer_transactions.customer_id = customer.customer_id) JOIN store\
                                                    ON customer_transactions.store_id = store.store_id) \
                                                    WHERE customer_transactions.spent > 30;")
    
    
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# Query 2: How much did Customer2 spent
try: 
    cur.execute("SELECT name, spent FROM (customer_transactions JOIN customer ON \
                                          customer_transactions.customer_id = customer.customer_id) \
                                          WHERE customer.customer_id = 2;")
    
    
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# Drop the tables that created
try: 
    cur.execute("DROP table customer_transactions")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table items_purchased")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table customer")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table store")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)

# And finally close your cursor and connection
cur.close()
conn.close()



















