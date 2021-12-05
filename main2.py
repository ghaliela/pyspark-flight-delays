import pyspark
from pyspark.sql.functions import date_format, col
from pyspark.sql.session import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import seaborn as sn
import matplotlib.pyplot as plt
import numpy as np

sp = SparkSession.builder.appName("flight delays prediction").getOrCreate()
df = sp.read.csv("c:/users/ghali/desktop/2008.csv", header=True, inferSchema=True)
# df = sp.read.option('header', 'true').csv("c:/users/ghali/desktop/2008.csv")

print(df.printSchema())

print("head of df")
print(df.head())

dropedcol = ["ArrTime", "ActualElapsedTime", "AirTime", "TaxiIn", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"]

df = df.drop(*dropedcol)

print("head of truncated df")
print(df.head())

targetvar = df.select("ArrDelay")

print(targetvar.count())

# ##################
from pyspark.ml.stat import Correlation
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import IntegerType

# convert to vector column first
vector_col = "corr_features"
# for col in df.columns:
#     df = df.withColumn(col, df[col].cast(IntegerType()))

# assembler = VectorAssembler(inputCols=df.columns, outputCol=vector_col)
# df_vector = assembler.transform(df).select(vector_col)

# # get correlation matrix
# matrix = Correlation.corr(df_vector, vector_col)

# print(matrix.collect()[0]["pearson({})".format(vector_col)].values)

# ######################""


# corrMatrix = df.corr()
# sn.heatmap(matrix, annot=True)
# plt.show()

# print(matrix.show())

# print(Correlation.corr(df,['DepDelay','ArrDelay']))

# print(df['DepDelay'].corr(df['ArrDelay']))

# Correlation end

# flightsData['UniqueCarrier'].unique()
print(df.select('UniqueCarrier').distinct().collect())

#Indexing the UniqueCarrier variable
# df['IndexUniqueCarrier'] = df.groupby('UniqueCarrier').ngroup()

from pyspark.sql import Window
w = Window.partitionBy('UniqueCarrier')
print(df.select('UniqueCarrier', pyspark.sql.functions.count('UniqueCarrier').over(w).alias('IndexUniqueCarrier')).sort('UniqueCarrier').distinct().collect())

# print(len(df.index))

#Dropping the rows where the flights were cancelled or diverted
# df = df.loc[df['Cancelled'] == 0]
df = df.filter(df.Cancelled == 0)

print(df.count())

# len(df.index)

#Now, we need to index the departure and arrival variables
#We'll first add them to create a tuple Departure-Arrival, and then index this tuple
# df['OriginDest'] = df['Origin'] + df['Dest']
df = df.withColumn("OriginDest", col("Origin")+col("Dest"))

#Now we index it
# df['IndexOriginDest'] = df.groupby('OriginDest').ngroup()
w = Window.partitionBy('OriginDest')
print(df.select('OriginDest', pyspark.sql.functions.count('OriginDest').over(w).alias('IndexOriginDest')).sort('OriginDest').distinct().collect())


print(df.head())

numberDelayedFlights = df.filter(df.ArrDelay == 0).count()
numberNotDelayedFlights = df.filter(df.ArrDelay != 0).count()

# plotdata = pd.DataFrame({"Number of flights": [numberDelayedFlights, numberNotDelayedFlights]}, index=["Delayed", "Not Delayed"])
# plotdata.plot(kind="bar")

#We check for unlabaled data
# df['ArrDelay'].isnull().sum()
print("null arr del", df.filter(df.ArrDelay.isNull()).count())

#Getting rid of the rows with missing values on the output variable
# df = df[df['ArrDelay'].notna()]
df = df.filter(df.ArrDelay.isNotNull())

# print(df['ArrDelay'].isnull().sum())

print(df.filter(df.ArrDelay.isNull()).count())