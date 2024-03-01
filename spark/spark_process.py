from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
import os


MongoURI = "mongodb://admin:password@mongo:27017/stackoverflow?authSource=admin"
Databases = "stackoverflow" 
DataDir = "/usr/local/share/data"
OutputDir = "output"

if __name__ == "__main__":
    # Tạo một Spark Session
    spark = SparkSession.builder \
        .appName("StackOverflow") \
        .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
        .config("spark.mongodb.read.connection.uri", MongoURI) \
        .config("spark.mongodb.write.connection.uri", MongoURI) \
        .enableHiveSupport() \
        .getOrCreate()
    sc = spark.sparkContext
    
    # Định nghĩa các trường của structType question
    question_schema = StructType([
        StructField("Id", IntegerType(), nullable=True),
        StructField("OwnerUserId", IntegerType(), nullable=True),
        StructField("CreationDate", DateType(), nullable=True),
        StructField("ClosedDate", DateType(), nullable=True),
        StructField("Score", IntegerType(), nullable=True),
        StructField("Title", StringType(), nullable=True),
        StructField("Body", StringType(), nullable=True)
    ])

    # Đọc collection "question" từ MongoDB
    questions_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", MongoURI) \
        .option("collection", "questions") \
        .load()
    questions_df = questions_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
    questions_df = questions_df.withColumn("CreationDate", F.to_date("CreationDate"))
    questions_df = questions_df.withColumn("ClosedDate", F.to_date("ClosedDate"))

    # Định nghĩa các trường của structType question
    answers_schema = StructType([
        StructField("Id", IntegerType(), nullable=True),
        StructField("OwnerUserId", IntegerType(), nullable=True),
        StructField("CreationDate", DateType(), nullable=True),
        StructField("ParentId", IntegerType(), nullable=True),
        StructField("Score", IntegerType(), nullable=True),
        StructField("Body", StringType(), nullable=True)
    ])
    answers_df = spark.read.format("com.mongodb.spark.sql.DefaultSource") \
        .option("uri", MongoURI) \
        .option("collection", "answers") \
        .load()
    # Chuyển đổi dữ liệu cho trường CreationDate và ClosedDate và xóa cột Oid
    answers_df = answers_df.withColumn("OwnerUserId", F.col("OwnerUserId").cast("integer"))
    answers_df = answers_df.withColumn("ParentId", F.col("ParentId").cast("integer"))
    answers_df = answers_df.withColumn("CreationDate", F.to_date("CreationDate")) 
    
    # Thực hiện Join 
    """ spark.sql("CREATE DATABASE IF NOT EXISTS MY_DB")
    spark.sql("USE MY_DB")
    
    # Chia Bucket để Join 
    questions_df.coalesce(1).write \
        .bucketBy(10, "Id") \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "{}/data_questions".format(DataDir)) \
        .saveAsTable("MY_DB.questions")
    
    answers_df.coalesce(1).write \
        .bucketBy(10, "ParentId") \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "{}/data_answers".format(DataDir)) \
        .saveAsTable("MY_DB.answers") 
    spark.sql("USE MY_DB")
    questions_df = spark.read.table("MY_DB.questions")
    
    answers_df = spark.read.table("MY_DB.answers") """
    
    
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
     
    questions_df = questions_df.withColumnRenamed("Id", "QuestionId")
    questions_df = questions_df.withColumnRenamed("OwnerUserId", "QuestionerID")
    questions_df = questions_df.withColumnRenamed("Score", "QuestionScore")
    questions_df = questions_df.withColumnRenamed("CreationDate", "QuestionCreationDate")

    questions_df_show = questions_df.show()
    answers_df_show = answers_df.show()
    
    # Điều kiện join
    join_expr = questions_df.QuestionId == answers_df.ParentId

    # Thực hiện join
    join_df = questions_df.join(answers_df, join_expr, "inner")
    join_df_show = join_df.show()
    
    # Thực hiện tính toán và lọc
    join_df_count = join_df.select("QuestionId", "Id") \
                        .groupBy("QuestionId") \
                        .agg(F.count("Id").alias("Number of answers")) \
                        .sort(F.asc("QuestionId"))
                            
    # Lưu kết quả vào file csv 
    join_df_count_show = join_df_count.show()
    
    join_df_count.coalesce(1).write \
        .format("csv") \
        .option("header", "true") \
        .mode("overwrite") \
        .csv(os.path.join(DataDir, OutputDir))

