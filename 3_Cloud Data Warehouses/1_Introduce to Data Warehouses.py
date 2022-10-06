"""
in Lession 1:
 -> What is a Data Warehouse (in a business perspective)
 -> What is a Data Warehouse (in a technical perspective)
 -> Dimension modeling
 -> DWH architecture
 -> OLAP Cubes
 -> DWH storgae technology

 """

# Recap about Rational Database using SQL


""" 
DWH Architecture:
    + Kimball's Bus
    + Independent Data Mars
    + Inmon's Corporate Information Factory (CIF)
    + Hybrid Bus & CIF
""" 

""" 
-> Kimball's Bus Architecture:

Review about ETL:
    Extract: 
        Get the data from source
        Possibly delete old state
    Transforming:
        Integrates many sources together
        Possibly cleansing: inconsistencies , duplicated, missing values, ..
        Possibly produing diagnostic metadata
    Loading:
        Structuring and loading the data into the dimensional data model
"""

""" 
-> Independent Data Marts:
    + Departments have separate ETL processes and dimensional models
    + Separting dimensional models: called `Data Marts`
    + Diffferent Fact Tables for same events, no conformed dimensions
    + Uncoordinated efforts can lead to inconsistent views
    + This architecture from department autonomy -> generally discourage
 """

 """ 
 -> (Inmon's Corporate Information Factory) CIF
    - 2 ETL processes:
        + Source Systems -> 3NF DBs
        + 3NF DB -> Departmental Data Marts
    - The 3NF DB acts an enterprise wide data store.
        + Single integrated source of truth for data-marts
        + Could be accessed by end-user if needed
    - Data marts: they are mostly aggregated (unlike Kimball's dimensional models)
  """

  """ 
  -> Hybrid Bus & CIF:
    The most interested in real world
   """













