
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
sc.setLogLevel("ERROR")

numbers = sc.parallelize([1, 2, 3, 4, 5])
print("MAP:", numbers.map(lambda x: x * 2).collect())

sentences = sc.parallelize(["apple mango", "apple banana"])
print("FLATMAP:", sentences.flatMap(lambda x: x.split()).collect())

pairs = sc.parallelize([
    ("Ravi", 80),
    ("Kumar", 90),
    ("Ravi", 85)
])
print("GROUPBYKEY:", pairs.groupByKey().mapValues(list).collect())

spark.stop()
