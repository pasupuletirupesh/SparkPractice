import os
import sys

# Tell Spark to use this virtual environment's Python
python_exe = sys.executable
os.environ["PYSPARK_PYTHON"] = python_exe
os.environ["PYSPARK_DRIVER_PYTHON"] = python_exe

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,round,filter

spark = (
    SparkSession.builder
    .appName("RDDPractice")
    .master("local[1]")
    .config("spark.pyspark.python", python_exe)
    .config("spark.pyspark.driver.python", python_exe)
    .getOrCreate()
)

sc = spark.sparkContext

df = spark.read.options(header=True, inferSchema=True).csv("C:\\Users\\pasup\\Downloads\\StudentData.csv")

# df.show()
df2  = df.withColumn("total",lit(120))

df3 = df2.withColumn("Average",round((col("marks")/col("total"))*100,2))

# df3.show()

df4 = df3.filter((col("Average")>=80) & (col("course")=="OOP"))

# df4.show()

df5 = df3.filter((col("Average")>=60)&(col("course")=="Cloud"))

df5.show()

