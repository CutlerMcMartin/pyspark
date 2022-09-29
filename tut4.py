#%%
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer
# %%
spark = SparkSession.builder.appName("Practice").getOrCreate()
# %%
## Reading the dataset
df_pyspark = spark.read.csv("test3.csv", header=True, inferSchema=True)
df_pyspark.show()
# %%
df_pyspark.printSchema()
# %%
df_pyspark.groupBy('Name').sum().show()
# %%
## Grouby departments, which gives max salary
df_pyspark.groupBy('Departments').max().show()
# %%
df_pyspark.groupBy('Departments').count().show()
# %%
df_pyspark.agg({'Salary':'sum'}).show()
# %%
