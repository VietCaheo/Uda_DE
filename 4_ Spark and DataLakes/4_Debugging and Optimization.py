4. Debugging and Optimization:
-----------------------------

#Accumulator: https://spark.apache.org/docs/2.2.0/rdd-programming-guide.html#accumulators

# Sample accumulator
SparkContext.accumulator(0,0)

# Spark Broadcast: (need to import SparkContext)
    Broadcast join is a way of joining a large table and small table in Spark
    Broadcast joins is like map-side join in MapReduce
    
# Transformations and Actions

-> Here are two type pf functions in Spark: transformations and Actions
-> Spark uses lazy evaluation to evaluate RDD and dataframe. Lazy evaluation means the code is not executed until it is needed. The action functions trigger the lazily evaluated functions.

# Spark WebUI: build-in user interface that you can access from web browser.

# Connecting to the Spark WebUI: when use SSH (private secure shell) insead of public protocol (HTTP), it 's needed for some specific set-up

# Monitoring and Instrumentation
https://spark.apache.org/docs/latest/monitoring.html

# Review the Log Data
Configuring Logging: https://spark.apache.org/docs/latest/configuration.html

# Data Skewness; means due to non-optimal partitioning, the data is heavy o few partitioning
    -> Skewed data indicators: if look ta that data, partitioned by month, we would have a large volume during November and December. It would to process this dataset through Spark using different partitions.
    -> How to solve skewed data problems:
        + use alternate columns that are more normally distributed (or called: temporary partition key)
        + make composite key (combining two columns) (or called: repartition)
        + partition by number of Spark workers:
        
Spark: https://spark.apache.org/docs/latest/tuning.html
spark SQL: https://spark.apache.org/docs/latest/sql-performance-tuning.html
    