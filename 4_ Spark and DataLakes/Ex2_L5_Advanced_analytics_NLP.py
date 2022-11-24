""" Advanced Analytics NLP """
!pip install spark-nlp==1.7.3

import pandas as pd
pd.set_option('max_colwidth', 800)

# Creat a spark content that includes a 3rd party jar for NLP
#jarPath = "spark-nlp-assembly-1.7.3.jar"

from pyspark.sql import SparkSession
spark = SparkSession.builder \
        .config("spark.jars.packages", "JohnSnowLabs:spark-nlp:1.8.2") \
        .getOrCreate()
spark

# Read multiple files in a dir as one DataFrame
dataPath = "./data/reddit/*.json"
df = spark.read.json(dataPath)
print(df.count())
df.printSchema()

# Deal with Struct type to query subfields
title = "data.title"
author = "data.author"
# Todo
dfAuthorTitle = df.select(title, author)
dfAuthorTitle.limit(5),toPandas()


# Try to implement the equivalent of flatMap in dataframes
import pyspark.sql.functions as F
dfWordCount = df.select(F.explode(F.split(title, "\\s+")).alias("word")).groupBy("word").count().orderBy(F.desc("count"))
dfWordCount.limit(10).toPandas()

# Use an NLP library to do Part-of-Speech Tagging
from com.johnsnowlabs.nlp.pretrained.pipeline.en import BasicPipeline as bp
dfAnnotated = bp.annotate(dfAuthorTitle, "title")
dfAnnotated.printSchema()

# Deal with Map type to query subfields
dfPos = dfAnnotated.select("text", "pos.metadata", "pos.result")
dfPos.limit(5).toPandas()

dfPos = dfAnnotated.select(F.explode("pos").alias("pos"))
dfPos.printSchema()
dfPos.toPandas()

# Keep only proper nouns NNP or NNPS
nnpFilter = "pos.result = 'NNP' or pos.result = 'NNPS' "
dfNNP = dfPos.where(nnpFilter)
dfNNP.limit(10).toPandas()

# Extract columns form a map in a column
dfWordTag = dfNNP.selectExpr("pos.metadata['word'] as word", "pos.result as tag")
dfWordTag.limit(10).toPandas()

from pyspark.sql.functions import desc
dfWordTag.groupBy("word").count().orderBy(desc("count")).show()

