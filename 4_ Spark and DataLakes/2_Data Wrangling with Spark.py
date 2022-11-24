""" 
17. Data wrangling with DataFrames Extra Tips
    General functions: select()/ filter()/ where()/ groupBy()
    Aggregate Functions: count()/ countDistinct()/ avg)()/ max()/ ...
    User define function
    Window Functions: 

 """

""" Spark SQL:
Spark provide SQL library that allow query DataFrames using the same SQL syntax
 
 
 """

#  Spark SQL build-in functions
 https://spark.apache.org/docs/latest/api/sql/index.html

#  Spark SQL guide
https://spark.apache.org/docs/latest/sql-getting-started.html

""" 
27. Spark's Core
RDD API
 -> Resilient Distributed Datasets : are low-level abstraction of the data

 Difference between RDDs and DataFrames 
 https://databricks.com/blog/2016/07/14/a-tale-of-three-apache-spark-apis-rdds-dataframes-and-datasets.html
-> APIs- RDDs:
    + is an immutable distributed collection of elements of data, partition across nodes in cluster 
    + offer `transformations` and `actions`
    + use case: data is unstructed, want low-level transformation and actions and control dataset; dont care about imposing the schema
-> DataFrames:
    + is an immutable distributed collection of data.
    + data is organized (like in Relational Database )
    + design to make large data sets processing even easier
    + allow impose a structure onto distributed collection of data, allowing higher-level abstraction
-> Datasets:
    +takes on two distinct APIs characteristics: a strongly-typed and untyped APIs
    + consider DataFrame as an alias for a collection of generic objects Dataset[Row]
    + for Python: have only untyped-APIs names: DataFrames

 RDD programming Guide:
 https://spark.apache.org/docs/latest/rdd-programming-guide.html


 
 """