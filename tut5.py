#%%
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
# %%
spark = SparkSession.builder.appName("Practice").getOrCreate()
# %%
training = spark.read.csv('test1.csv', header=True, inferSchema=True)
training.show()
#%%
training.columns
# %%
featureassembler = VectorAssembler(inputCols=['age','Experience'], outputCol='Independent Features')
#%%
output = featureassembler.transform(training)
output.show()
# %%
finalized_data=output.select("Independent Features","Salary")
finalized_data.show()
# %%
from pyspark.ml.regression import LinearRegression
# %%
train_data,test_data = finalized_data.randomSplit([0.75,0.25])
regressor = LinearRegression(featuresCol='Independent Features', labelCol='Salary')
regressor = regressor.fit(train_data)
# %%
regressor.coefficients
# %%
regressor.intercept
# %%
## Predictionn
pred_results = regressor.evaluate(test_data)
# %%
pred_results.predictions.show()
# %%
## Determine the Mean Squared Error
pred_results.meanSquaredError

# %%
