Apache Cassandra:
# scalability and high availability

NoSQL = Not only SQL ; NoSQL <=> NonRelational
various types of NoSQL databases
---
# COmmon Types of NoSQL Databases
#  Apache Cassandra (Partition Row Store)
#  MongoDB (Document store)
#  DynamoDB (Key-Value store)
#  Apache HBase (Wide Column store)
#  Neo4J (Graph Database)

 # Important terms in Apache Cassandra
 keyspace: is similar to a schema in PostgreSQL
 Partition:
 PrimaryKey:
 Columns:

 # Which companies will use Apache Cassandra: transaction logging (retail/heath care); IoT; Time series data; Any workload that is heavy on writes to the db (since Apache Cassandra is optimized for writes).

 """ When to use NoSQL Database:
 -------------------------------
 Need to be able to store different data type formats
 Large amounts of data
 Need horizontal scalability
 Need high throughput: very fast read and write
 Need a flexible schema
 Need high availability 
   """

When not to use NoSQL Database:
"""
Need ACID transactions
Need ability to do JOINS
Ability to do aggregations and analytics
Have changing business requirements
Query are not available and need to have flexibility
Have a smal dataset
 """
[!] There are some NoSQL DB that offer some form of ACID transaction


