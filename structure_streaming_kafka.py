from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import col,from_json
from pyspark.sql.types import StructType, StringType, IntegerType
import os


# явным образом задаем структуру json-контента
schema = StructType().add("id",IntegerType()).add("action", StringType())

users_schema = StructType().add("id",IntegerType()).add("user_name", StringType()).add("user_age", IntegerType())

spark = SparkSession.builder.appName("SparkStreamingKafka").getOrCreate()


input_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "broker:29092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe", "netology-spark") \
  .option("failOnDataLoss", False) \
  .load()

#разберем входящий контент из json
json_stream = input_stream.select(col("timestamp").cast("string"), from_json(col("value").cast("string"), schema).alias("parsed_value"))

#выделем интересующие элементы
clean_data = json_stream.select(col("timestamp"), col("parsed_value.id").alias("id"), col("parsed_value.action").alias("action"))

# #добавим join с статическим dataset - создаем данные
users_data = [(1,"Jimmy",18),(2,"Hank",48),(3,"Johnny",9),(4,"Erle",40)]
users = spark.createDataFrame(data=users_data,schema=users_schema)

# #делаем join
join_stream = clean_data.join(users, clean_data.id == users.id, "left_outer").select(users.user_name, users.user_age, clean_data.id, col("timestamp"), col("action"))
join_stream.writeStream.format("console").outputMode("append").option("truncate", False).start().awaitTermination()

#добавим агрегат - отображать число уникальных записей по каждому id
stat_stream = clean_data.groupBy("id").count()
stat_stream.writeStream.format("console").outputMode("complete").option("truncate", False).start().awaitTermination()
