# Data Quality:
#-------------
# Data Lineage: the data lineage of datasets describes the discrete steps involved in the creation, movement, and calculation of that dataset.

""" 
Why data lineage:
    + Instilling confidence
    + Defining Metrics
    + Debugging

Visualizing Data Lineage:
-> data lineage of dataset: descriptopn of the discrete steps involved in the creation, movement, and calculation of the dataset.
-> Airflow components use to track data lineage:
    + rendered code tab for a task
    + Graph view for a DAG
    + Historical runs under the tree view
"""

# Data Pipeline Schedules
""" 
Shedules which determine what data should be analyzed and when

-> How to use schudules:
    Determine what data should be analyzed and when
-> to determine schedule:
    start_date/ end_date/ schedule_interval (only start_date is requirement, another two ones have default values)
->
 """

#  Types of Partioning: logical/ size/ around events/ ...

# Data Partitioning:
""" 
Logical Partitioning:
    -> breaking conceptually related data into discrete groups for processing.
    -> can be partitioned into discrete segments and processed separately. With logical partitioning, un related things belong in separate steps.
Size Partitioning:
    -> separate data for processing based on desired storage limits.
Time Partitioning:
    -> Processing data based on a schedule or when it was created.
Size Partitioning:
    -> Separating data for processing base on desired or requuired storage limits.

 """

# Data Quality : how to measure
""" 
Example of Data Quality Requirements:
    -> must be certain size
    -> accurate to some margin of error
    -> must arrive within a given timeframe from the start of execution 
    -> pipeline must run on a particular schedule
    -> must not contain any sensitive information
 
 [!note] SLAs: Service Level Agreement
 """
