import psycopg2

# Create a connection to the database, get a cursor, and set autocommit to true
try: 
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
except psycopg2.Error as e: 
    print("Error: Could not make connection to the Postgres database")
    print(e)
try: 
    cur = conn.cursor()
except psycopg2.Error as e: 
    print("Error: Could not get cursor to the Database")
    print(e)
conn.set_session(autocommit=True)

# From 04 tables 3NF ; add new table `sales`
# Tables list include: transactions2/ albums_sold/ employees/ sales

# TODO: Add all Create statements for all tables
try: 
    cur.execute("CREATE TABLE IF NOT EXISTS transactions2 (transaction_id int, customer_name varchar, cashier_id int, year int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS albums_sold (album_id int, transaction_id int, album_name varchar);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS employees (employee_id int, employee_name varchar)")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS sales (transaction_id int, amount_spent int)")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

      
# TODO: Insert data into the tables    

try: 
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions2 (transaction_id, customer_name, cashier_id, year) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, "Max", 2, 2018))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (1, 1, "Rubber Soul"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (2, 1, "Let It Be"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (3, 2, "My Generation"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (4, 3, "Meet the Beatles"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO albums_sold (album_id, transaction_id, album_name) \
                 VALUES (%s, %s, %s)", \
                 (5, 3, "Help!"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (1, "Sam"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO employees (employee_id, employee_name) \
                 VALUES (%s, %s)", \
                 (2, "Bob"))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)    
    
try: 
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (1, 40))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)    
    
try: 
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (2, 19))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e) 

try: 
    cur.execute("INSERT INTO sales (transaction_id, amount_spent) \
                 VALUES (%s, %s)", \
                 (3, 45))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e) 

#TODO Confirm data were added correctly by using SELECT statement
print("Table: transactions2 \n")
try: 
    cur.execute("SELECT * FROM transactions2;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: albums_sold \n")
try: 
    cur.execute("SELECT * FROM albums_sold;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

print("\nTable: employees \n")
try: 
    cur.execute("SELECT * FROM employees;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()
    
print("\nTable: sales \n")
try: 
    cur.execute("SELECT * FROM sales;")
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# TODO Need to query that gives: transaction_id/ customer_name/ cashier_name/ year/ albums_sold/ amount_sold
# Complete Statement below to perform a 3 way JOIN on the 4 tables that have created
try: 
    cur.execute("SELECT * FROM ((transactions2 JOIN albums_sold ON \
                          transactions2.transaction_id = albums_sold.transaction_id) JOIN employees ON \
                          employees.employee_id = transactions2.cashier_id) JOIN sales ON \
                          sales.transaction_id=transactions2.transaction_id")
    
    
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# DO JOINS with heavy data will make very slow process,with denormalization -> need to reduce JOIN to look way do 2 query
# 1st: to generate the amount spent on each transaction
# Query1: select transaction_id, customer_name, amount_spent FROM <min number of tables>

# TODO: Create all tables
try:
    cur.execute("CREATE TABLE IF NOT EXISTS transactions (transaction_id int, \
                                           customer_name varchar, \
                                           cashier_id int, year int , \
                                           amount_spend int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)

#Insert data into all tables 
    
try: 
    cur.execute("INSERT INTO transactions (transaction_id, \
                                           customer_name, \
                                           cashier_id, year , \
                                           amount_spend)\
                 VALUES (%s, %s, %s, %s, %s)", \
                 (1, "Amanda", 1, 2000, 40))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions (transaction_id, \
                                           customer_name, \
                                           cashier_id, year , \
                                           amount_spend)\
                 VALUES (%s, %s, %s, %s, %s)", \
                 (2, "Toby", 1, 2000, 19))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)
    
try: 
    cur.execute("INSERT INTO transactions (transaction_id, \
                                           customer_name, \
                                           cashier_id, year , \
                                           amount_spend)\
                 VALUES (%s, %s, %s, %s, %s)", \
                 (3, "Max", 2, 2018, 45))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Simmplifier query as needed
try: 
    cur.execute("select transaction_id, customer_name, amount_spend FROM transactions")
        
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()

# 2nd: generate the total sales by cashier
# Query 2: select cashier_name, SUM(amount_spent) FROM (min number of tables) GROUP_BY cashier_name
# Creat new table for which column we need  <- to avoid any JOIN command later
# Table name: cashier_sales with columns: transaction_id, cashier_id, cashier_name (employee_name),  amount_spent

try: 
    cur.execute("CREATE TABLE IF NOT EXISTS cashier_sales (transaction_id int, \
                                                           cashier_id int, \
                                                           cashier_name varchar, amount_spent int);")
except psycopg2.Error as e: 
    print("Error: Issue creating table")
    print (e)


#Insert into all tables 
    
try: 
    cur.execute("INSERT INTO cashier_sales (transaction_id, \
                                    cashier_id, \
                                    cashier_name, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (1, 1,"Sam", 40))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO cashier_sales (transaction_id, \
                                    cashier_id, \
                                    cashier_name, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (2, 1,"Sam", 19))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

try: 
    cur.execute("INSERT INTO cashier_sales (transaction_id, \
                                    cashier_id, \
                                    cashier_name, amount_spent) \
                 VALUES (%s, %s, %s, %s)", \
                 (3, 2,"Bob", 45))
except psycopg2.Error as e: 
    print("Error: Inserting Rows")
    print (e)

# Run the query to get expected info
try: 
    cur.execute("SELECT cashier_name, SUM(amount_spent) \
                 FROM cashier_sales \
                 GROUP BY cashier_name")
        
except psycopg2.Error as e: 
    print("Error: select *")
    print (e)

row = cur.fetchone()
while row:
   print(row)
   row = cur.fetchone()


# Drop the table and quit 
try: 
    cur.execute("DROP table transactions2")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table albums_sold")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table employees")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table sales")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table transactions")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)
try: 
    cur.execute("DROP table cashier_sales")
except psycopg2.Error as e: 
    print("Error: Dropping table")
    print (e)

# Close cursor and conn
cur.close()
conn.close()


