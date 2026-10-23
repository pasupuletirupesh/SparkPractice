import os
import sys

# Tell Spark to use this virtual environment's Python
python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("RDDPractice")
    .master("local[1]")
    .config("spark.pyspark.python", python_exe)
    .config("spark.pyspark.driver.python", python_exe)
    .getOrCreate()
)

sc = spark.sparkContext

rdd = sc.textFile("C:\\Users\\pasup\\Downloads\\test.txt")

print(rdd.collect())

rdd2 = rdd.flatMap(lambda x: x.split(' '))
print(rdd2.collect())

# COMMAND ----------

rdd3 = rdd2.map(lambda x: (x,len(x)))
print(rdd3.collect())

# COMMAND ----------

print(rdd3.groupByKey().mapValues(list).collect())

rdd4 = rdd2.map(lambda x:x )
print(rdd4.collect())

# rdd2 = rdd.map(lambda x : x.split(' '))

# print(rdd2)

# rdd3 = rdd2.flatMap(lambda x: x).collect()

# print(rdd3)

# rdd2 = rdd.flatMap(lambda x: x.split(' '))

# print(rdd2.collect())

# rdd3 = rdd2.map(lambda x: (x,len(x)))

# print(rdd3.collect())


# print(rdd3.groupByKey().mapValues(list).collect())




