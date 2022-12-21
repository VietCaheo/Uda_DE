#  Overview
""" 
How to build maintainable and reusable pipelines in Airflow.

    Focus on elevating your DAGs to reliable, production-quality data pipelines.
    Extending airflow with custom plug-ins to create your own hooks and operators.
    How to design task boundaries so that we maximize visibility into our tasks.
    subDAGs and how we can actually reuse DAGs themselves.
    Monitoring in Airflow
 """

#  Extending AirFlow with Plugins:
""" 
-> Airflow allows users to extend and customize functionality through plugins
-> Common types of user createed plugins for Airflow are `Operators` and `Hooks`

 """

#  Airflow contribution
""" 
There is an open community, allow to adding new functionality and extending the capabilities of Airflow
https://github.com/apache/airflow/tree/main/airflow/contrib 

[key-words]: 
    Operators
    Hooks
"""

# Task Boundaries:
""" 
-> DAG Tasks should be designed such that they are:
    + Atomic and single purpose
    + Maximum parallelism
    + Make failure states obvious
-> Every task should be perform only one job

 """

#  Monitoring:
""" 
SLAs: Service Level Agreement : 
    -> define as a time by which a DAG must complete.
    -> apply for time-sensitive application
Emails and Alerts:
    -> Either Tasks or DAGs change can Airflow send notifications
    -> 
Metrics:
    ->send system mmetrics using `statsd`, can be coupled with tool `Grafana`

 """

# Addition links for data pipeline
#  Example: https://github.com/pditommaso/awesome-pipeline


