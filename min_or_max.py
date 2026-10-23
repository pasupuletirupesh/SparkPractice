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

rdd = sc.textFile("C:\\Users\\pasup\\Downloads\\average_quiz_sample.csv")

print(rdd.collect())

rdd2_max = rdd.map(lambda x:x.split(",")) .map(lambda x:(x[0],x[2])) .reduceByKey(lambda x,y: x if int(float(x))> int(float(y)) else y)


rdd3_min = rdd.map(lambda x:x.split(",")) .map(lambda x:(x[0],x[2])) .reduceByKey(lambda x,y: x if int(float(x))< int(float(y)) else y)


print(rdd2_max.collect())

print(rdd3_min.collect())