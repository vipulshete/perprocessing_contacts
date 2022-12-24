"""
    @Author: Vipul Shete
    @Date: 2022-12-22
    @Last Modified by: Vipul Shete
    @Last Modified Date: 2022-12-22
    @Title : Cleaning of contacts and store in csv or mysql (UC-4) .
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat, lit, col
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import col
import uuid

spark = SparkSession.builder \
    .master("local") \
    .appName("PySpark_MySQL_test3") \
    .getOrCreate()

sc=spark.sparkContext


data = spark.read.csv('contacts.csv', header=True)


## fillter first name only alphabte other wise null
first_name = data.withColumn("new_first_name", f.when(col("first_name").rlike("^[a-zA-Z]*$"), lit(data.first_name)).otherwise(lit("null")))

## fillter last name only alphabte other wise null
last_name = first_name.withColumn("new_last_name", f.when(col("last_name").rlike("^[a-zA-Z]*$"), lit(data.last_name)).otherwise(lit("null")))

## fillter last name only alphabte other wise null
email_filter = last_name.withColumn("new_email", f.when(col("email").rlike("^([a-zA-Z0-9\-\.]+)@([a-zA-Z0-9_\-\.]+)\.([a-zA-Z]{2,5})$"), lit(data.email)).otherwise(lit("null")))

## select columns 
new_data = email_filter.select("new_first_name", "new_last_name", "city", "mobile_no", "phone_no", "new_email", "Is_email_s", "Stream")

data_1 = new_data.withColumnRenamed("new_first_name", "first_name")
data_2 = data_1.withColumnRenamed("new_last_name", "last_name")
data_3 = data_2.withColumnRenamed("new_email", "email")

is_mail = spark.read.json("email_code.json")
mail = is_mail.collect()[0]

## replace value in IS_mail column
send_mail_send = data_3.withColumn("Is_mail_send", 
    f.when(data_3.Is_email_s=="yes", is_mail.collect()[0][1]) \
   .when(data_3.Is_email_s=="no", is_mail.collect()[0][0]) \
   .otherwise(data_3.Is_email_s)) 

stream = spark.read.json("stream_code.json")
stream_encoding = stream.collect()[0]

## replace value in Branch column
stream_type = send_mail_send.withColumn("Type_of_strem", 
    f.when(send_mail_send.Stream=="B.com", stream_encoding[0]) \
   .when(send_mail_send.Stream=="CE", stream_encoding[1]) \
    .when(send_mail_send.Stream=="CS", stream_encoding[2]) \
    .when(send_mail_send.Stream=="EEE", stream_encoding[3]) \
    .when(send_mail_send.Stream=="IT", stream_encoding[4]) \
    .when(send_mail_send.Stream=="Law", stream_encoding[5]) \
    .when(send_mail_send.Stream=="Mech", stream_encoding[6]) \
    .when(send_mail_send.Stream=="Teacher", stream_encoding[7]) \
    .otherwise(send_mail_send.Stream)) 


# select columns
map_columns = stream_type.select("first_name", "last_name", "city", "mobile_no", "phone_no", "email", "Is_mail_send", "Stream", "Type_of_strem") 

## mobile_no pre-procesiing
mobile_no = map_columns.select("*", f.regexp_replace(f.col("mobile_no"), "[0/91]?[6789]{1}[0-9]{9}$", "True").alias("correct_mobile"))  
mobile_no_1 = mobile_no.filter(mobile_no.correct_mobile=='True')
validate_mobile = mobile_no_1.select('first_name', 'last_name', 'city', 'mobile_no', 'email', "Is_mail_send", "Stream", "Type_of_strem")
phone_no = map_columns.select('first_name', 'last_name', 'city', 'phone_no', 'email', "Is_mail_send", "Stream", "Type_of_strem")
phone_no_1=phone_no.withColumnRenamed("phone_no","mobile_no")
phone_no_2 = phone_no_1.select("*", f.regexp_replace(f.col("mobile_no"), "[0/91]?[6789]{1}[0-9]{9}$", "True").alias("correct_mobile"))  
phone_no_3 = phone_no_2.filter(phone_no_2.correct_mobile=='True')
validate_phone = phone_no_3.select('first_name', 'last_name', 'city', 'mobile_no', 'email', "Is_mail_send", "Stream", "Type_of_strem")
validate_number = validate_mobile.union(validate_phone)


## unique mobile number
drop_duplicate_number = validate_number.drop_duplicates(["mobile_no"])

country_code=drop_duplicate_number.withColumn("country_code", f.lit('+91'))
final_contacts=country_code.select("*", concat(col("country_code"),lit(" "),col("mobile_no")).alias("countery_code_with_mobile_no"))
contacts = final_contacts.select('first_name', 'last_name', 'city', 'countery_code_with_mobile_no', 'email', "Is_mail_send", "Stream", "Type_of_strem")

try:
    id = []
    for i in range(contacts.count()):
        id.append(str(uuid.uuid4()))
    contact_df = contacts.repartition(1).withColumn('id',f.udf(lambda x: id[x])(f.monotonically_increasing_id()))
    contact_df = contact_df.select('id','first_name','last_name','city','countery_code_with_mobile_no','email', "Is_mail_send", "Stream", "Type_of_strem")
    contact_df.show(truncate=False)
except Exception as e:
    print(e)

## load dataframe to csv file
contact_df.write.option("header",True).csv("/home/ubuntu/validated_contacts")