#2. Introduce about Non-relational Databases

#3. Distributed Databases:
"""
3.
`Eventual Consistency`:

In Apache Cassandra every node connected to every node: it's peer to peer  database architecture
`Deployment strategies`:
    refer link: https://docs.datastax.com/en/dseplanning/docs/capacityPlanning.html

`Apache Cassandra Architecture`: "peer-to-peer distributed system across its nodes, and data is distributed among all the nodes in a cluster"
    refer:  https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/architecture/archTOC.html
            https://www.tutorialspoint.com/cassandra/cassandra_architecture.htm

Data Replication in Cassandra:
Components of Cassandra:
    Node / Data Center/ Cluster/ Commit log/ Mem-table/ SS table/ Bloom filter

Cassandra Query Language (CQL - compare to SQL: Structured Query Language)

How Cassandra reads, writes, updates and deletes the data:
        https://docs.datastax.com/en/archived/cassandra/3.0/cassandra/dml/dmlIntro.html

-> Google key: "Characteristics of relational vs. non-relational databases"

4. CAP Theorem: Consistency - Available - Partition

5. Denormalization in Apache Cassandra (A.C):
 -> The first thing MUST to do in NoSQL data model
 -> There are no JOINS in Cassandra
 -> ALWAYS think Queries first
 -> Denormalization must be done for fast reads, A.C has been optimized for fast writes
 -> Due to no more JOIN query: NoSQL one query only for one table.
 -> "to model your table to that query":
    When doing data modeling in Apache Cassandra knowing your queries first and modeling to those queries is essential.

6. CQL (Cassandra Query Language)
 -> very similar to SQL except: no JOINs, no GROUP BY and not support subqueries


Python driver for Cassandra:
    https://docs.datastax.com/en/developer/python-driver/3.25/


7. PRIMARY KEY:
    -> is how each row can be uniquely identified and how the data is distributed across the nodes (or severs) in our system
    -> the first element of the PRIMARY KEY is the PARTITION KEY (which will determine the distribution)
    -> made up of either just the PARTITION KEY or wit the addtion of CLUSTERING COLUMNS
    -> PARTITION KEY row value will be hashed (turn into a number) and store in the node in the system.
    https://docs.datastax.com/en/archived/cql/3.3/cql/cql_using/useSimplePrimaryKeyConcept.html#useSimplePrimaryKeyConcept

"""









