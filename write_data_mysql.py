from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.types import *

spark = SparkSession.builder.config("spark.jars", "/home/ubuntu/mysql-connector-j-8.0.31.jar") \
    .master("local") \
    .appName("PySpark_MySQL_test4") \
    .getOrCreate()


contact_df = spark.read.csv("/home/ubuntu/part-00000-b41af4ed-da27-4dc3-8ef4-ca9fa57b61d1-c000.csv")

contact_df.write.format('jdbc').options(
      url='jdbc:mysql://localhost/address_book',
      driver='com.mysql.cj.jdbc.Driver',
      dbtable='contacts',
      user='root',
      password='Vaibhav@123').mode('append').save()