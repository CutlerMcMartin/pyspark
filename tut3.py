#%%
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import Imputer
# %%
spark = SparkSession.builder.appName("Practice").getOrCreate()
# %%
## Reading the dataset
df_pyspark = spark.read.csv("test1.csv", header=True, inferSchema=True)
df_pyspark.show()
# %%
## Salary of people less than or equal to 20000
df_pyspark.filter(df_pyspark["Salary"] <= 20000).show()
# %%
## Only geetting "Name" and "age" columns for people with salary less than or equal to 20000
df_pyspark.filter(df_pyspark["Salary"] <= 20000).select("Name","age").show()
# %%
## Salary of people less than or equal to 20000
df_pyspark.filter((df_pyspark["Salary"] <= 20000) & (df_pyspark["Salary"] >= 15000)).show()

#%%