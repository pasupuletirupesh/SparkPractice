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

# rdd2 = rdd.flatMap(lambda x :x.split(" "))

# print(rdd2.getNumPartitions())

# print(rdd2.count())

# print(rdd2.countByValue().items())

# rdd3 = rdd2.map(lambda x: (x,len(x)))

# rdd4 = rdd3.groupByKey().mapValues(list)

# print(rdd4.collect())

# rdd5 = rdd3.reduceByKey(lambda x,y: x+y).collect()

# print(rdd5)
