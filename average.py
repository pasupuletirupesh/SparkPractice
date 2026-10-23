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

#  ['JAN,NY,3.0']

rdd2 = rdd.map(lambda x : x.split(","))
# [['JAN', 'NY', '3.0'], ['JAN', 'PA', '1.0']]

('JAN', (10, 4))

rdd3 = rdd2.map(lambda x : (x[0],(int(float(x[2])),1))).reduceByKey(lambda x,y : (x[0]+y[0],x[1]+y[1])).map(lambda x : (x[0], x[1][0]/x[1][1]))
print(rdd3.collect())   # [('JAN', (3.0, 1)), ('JAN', (1.0, 1)), ('JAN', (2.0, 1))]


# # print(rdd.collect())


# #  ====> [['JAN', 'NY', '3.0'], ['JAN', 'PA', '1.0']]

# rdd2 = rdd.map(lambda x : x.split(","))

# rdd3 = rdd2.map(lambda x :(
#     x[0],
#     (x[1],int(float(x[2])))
#     )
# )

# # print("this is rdd3", rdd3.collect())   # [('JAN', ('NY', 3)), ('JAN', ('PA', 1)), ('JAN', ('NJ', 2))]

# rdd4 = rdd3.groupByKey()

# # print("this is rdd4", rdd4.collect()) #[('JAN', <pyspark.resultiterable.ResultIterable object at 0x000002085B7BB140>), ('FEB', <pyspark.resultiterable.ResultIterable object at 0x000002085B7E5C40>), ('MAR', <pyspark.resultiterable.ResultIterable object at 0x000002085B7E6B10>)]

# # [('JAN', ['NY', 'PA', 'NJ', 'CT'], [3, 1, 2, 4]), ('FEB', ['PA', 'NJ', 'NY', 'VT'],[1, 1, 2, 1])]
# rdd5 = rdd4.map(lambda x: (x[0],[y[0] for y in x[1]],[y[1] for y in x[1]])) 

# rdd6 = rdd5.map(lambda x : (x[0],x[1],(sum(x[2])/len(x[2]))))

# print(rdd6.collect())
