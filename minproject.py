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

rdd = sc.textFile("C:\\Users\\pasup\\Downloads\\StudentData.csv")

# print(rdd.collect())

header = rdd.first()

# print("Header:", header)

data = rdd.collect()

# print("Data:", data[1:])

# print("Data count:",rdd.count()-1)

data_without_header = rdd.filter(lambda x: x != header)

rdd = data_without_header.map(lambda x : x.split(","))

# print(rdd.collect()[0])  # ['28', 'Female', 'Hubert Oliveras', 'DB', '02984', '59', 'Annika Hoffman_Naoma Fritts@OOP.com']

# rdd2 = rdd.map(lambda x :(x[1], 1) if x[1] == "Female" else (x[1], 1)).reduceByKey(lambda x,y : x+y)

# print("Total number of females and males:-",rdd2.collect())  [('Female', 501), ('Male', 499)] 

# age,gender,name,course,roll,marks,email 

# rdd2 = rdd.map(lambda x : (x[1],int(x[5]))).reduceByKey(lambda x,y : x+y)
# # print(rdd2.collect())  # [('Female', 29636), ('Male', 30461)] 

# print("total_marks_female:", rdd2.collect()[0])
# print("total_marks_male:", rdd2.collect()[1])

# rdd3 = rdd.map(lambda x :((1) if int(x[5])>50 else (0)))

# print("Count of passed students:", rdd3.collect().count(1))
# print("Count of failed students:", rdd3.collect().count(0))

# passed_students = rdd.filter(lambda x : int(x[5])>50).count()

# failed_students = rdd.filter(lambda x : int(x[5])<=50).count()

# print("Count of passed students:", passed_students)

# print("Count of failed students:", failed_students)

# rdd4 = rdd.map(lambda x : (x[3],1)).reduceByKey(lambda x,y : x+y).collect()

# print("Count of students in each course:", rdd4)  # [('DB', 200), ('OOP', 200), ('ML', 200), ('AI', 200), ('DS', 200)]

# rdd5 = rdd.map(lambda x : (x[3],int(x[5]))).reduceByKey(lambda x,y : x+y).collect()

# print("Total marks of students in each course:", rdd5)  # [('DB', 29636), ('OOP', 30461), ('ML', 29636), ('AI', 30461), ('DS', 29636)]


# total_number_of_students_per_course = rdd.map(lambda x : (x[3],1)).reduceByKey(lambda x,y : x+y).collect()

# total_marks_per_course = rdd.map(lambda x : (x[3],int(x[5]))).reduceByKey(lambda x,y : x+y).collect()

# print("Total number of students per course:", total_number_of_students_per_course)
# print("Total marks per course:", total_marks_per_course)

# average_marks_per_course = [(course, total_marks / total_students) for (course, total_students), (course, total_marks) in zip(total_number_of_students_per_course, total_marks_per_course)]

# print("Average marks per course:", average_marks_per_course)



find_max_marks_per_course = rdd.map(lambda x : (x[3],int(x[5]))).reduceByKey(lambda x,y : x if x > y else y).collect()

# print("Maximum marks per course:", find_max_marks_per_course)  # [('DB', 100), ('OOP', 100), ('ML', 100), ('AI', 100), ('DS', 100)]

find_min_marks_per_course = rdd.map(lambda x : (x[3],int(x[5]))).reduceByKey(lambda x,y : x if x < y else y).collect()

# print("Minimum marks per course:", find_min_marks_per_course)  # [('DB', 1), ('OOP', 1), ('ML', 1), ('AI', 1), ('DS', 1)]

find_max_min_marks_per_course = [(course,"Max:" + str(max) + ", Min:" + str(min)) for (course,max),(course,min) in zip(find_max_marks_per_course, find_min_marks_per_course)]

print("Maximum and Minimum marks per course:", find_max_min_marks_per_course)


rdd8 = rdd.map(lambda x : (x[1],int(x[0]))).reduceByKey(lambda x,y:x+y).collect()

rdd9 = rdd.map(lambda x : (x[1],1)).reduceByKey(lambda x,y:x+y).collect()

print(rdd8)
print(rdd9)

average_age = [(gender,(count/student)) for (gender,count),(gender,student) in zip(rdd8,rdd9)]

print("Average age of students per gender:", average_age)  # [('Female', 28.0), ('Male', 28.0)]

df = spark.createDataFrame(average_age, ["gender", "average_age"])

df.show()