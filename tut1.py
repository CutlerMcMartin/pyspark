#%%
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
# %%
## Making a spark
spark = SparkSession.builder.appName("Practice").getOrCreate()
# %%
## Reading the dataset
df_pyspark = spark.read.csv("test1.csv", header=True, inferSchema=True)
# %%
## Check the schema
df_pyspark.printSchema()
# %%
## Selecting a column with pyspark
doink = df_pyspark.select(["Name","Experience"])
#%%
## Incorrect way to get a column
doink = df_pyspark["Name"]
# %%
## Giving the statistics fo the columns
df_pyspark.describe().show()
# %%
## Adding Columns into pysparks dataframe
df_pyspark = df_pyspark.withColumn("Twice_The_Experiencne", df_pyspark["Experience"]*2)
df_pyspark.show()
# %%
## Drop the column
df_pyspark = df_pyspark.drop("Twice_The_Experiencne")
df_pyspark.show()
# %%
## Renaming the column
df_pyspark = df_pyspark.withColumnRenamed("Experience", "Exp")
df_pyspark.show()
# %%
