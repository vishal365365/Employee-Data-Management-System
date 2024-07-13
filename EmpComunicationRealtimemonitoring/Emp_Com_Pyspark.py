from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType,LongType
import sys
import json

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .config("spark.streaming.stopGracefullyOnShutdown", True) \
    .config("spark.sql.shuffle.partitions", 8) \
    .config("spark.sql.warehouse.dir",sys.argv[sys.argv.index('--warehouse')+1]) \
    .enableHiveSupport()\
    .getOrCreate()

bootstrap_servers = "ec2-34-237-234-93.compute-1.amazonaws.com:9092"

spark.sql("CREATE DATABASE IF NOT EXISTS Emp_Mangement")
spark.sql("use Emp_Mangement")


# Create the streaming_df to read from kafka
streaming_df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", bootstrap_servers) \
    .option("subscribe", "emp_com") \
    .option("failOnDataLoss", "false")\
    .option("startingOffsets", "earliest") \
    .load()

# we have one broker so we got only one partition so repartitioning it to 8
# for parllelism
streaming_df = streaming_df.repartition(8)


vocab_path = "s3://bootcamp-emp-workflow-bronze/emp-raw-data-processed/vocab_marked/vocab.json"
marked_path = "s3://bootcamp-emp-workflow-bronze/emp-raw-data-processed/vocab_marked/marked_word.json"


# Read the file vocab and extract to the list of words
vocab = spark.read.text(vocab_path).rdd.map(lambda row: json.loads(row.value))\
        .flatMap(lambda x: x).collect()
# Read the file  and extract to the list of words
marked = spark.read.text(marked_path).rdd.map(lambda row: json.loads(row.value))\
        .flatMap(lambda x: x).collect()

host  = sys.argv[sys.argv.index('--host')+1]
jdbc_url = f"jdbc:mysql://{host}/Emp_Stream"
table_name = "chat_records"
connection_properties = {
    "user": "vishal",
    "password": "Vishal1997",
    "driver": "com.mysql.jdbc.Driver"  # Adjust the driver based on your database type
}

# Decoding to encode data to string
json_df = streaming_df.selectExpr("cast(value as string) as value")

# defining the schema
json_schema = StructType([
    StructField("sender", LongType(), True),
    StructField("receiver", LongType(), True),
    StructField("message", StringType(), True),
])

# expanding the JSON data within the "value" column of the DataFrame json_df 
# using the specified json_schema, creating a new DataFrame json_expanded_df 
# with value coloum of struct type.
json_expanded_df = json_df.withColumn("value", from_json(json_df["value"], json_schema))

# selecting all the columns from the "value" struct type column in the DataFrame 
# flattening the JSON structure into individual columns and updating the 
# DataFrame with only these columns.
json_expanded_df = json_expanded_df.select("value.*")

# procsesing the data batch
def process(df,batch_id):
    
    #adding current timestamp
    df = df.withColumn("timestamp",current_timestamp())
    # removing such rows have same sender and receiver
    df = df.filter(df.sender != df.receiver)
    # Split message string into words and convert list of words to array
    df = df.withColumn("message", split(col("message"), " ")) \
       .withColumn("vocab_array",array([lit(i) for i in vocab]))
    
    # Find intersection between message words and word list
    df = df.withColumn("message", array_intersect(col("message"), 
                                                col("vocab_array")))
    
    # creating array column with marked words
    df = df.withColumn("marked", array([lit(j) for j in marked]))
    
    # getting sender sender who used marked words
    df = df.withColumn("is_flagged", when(size(array_intersect(col("message"), 
                                                col("marked"))) > 0,"Yes")\
                                                .otherwise("No"))
    #making record of messages send by sender
    df.select("sender","message","is_flagged","receiver","timestamp")\
        .write.format("parquet").partitionBy("sender","is_flagged").mode('append')\
        .saveAsTable("sender_chat_history")
    
    #making record of messages received by receiver
    df.select("receiver","message","is_flagged","sender","timestamp")\
        .write.format("parquet").partitionBy("receiver","is_flagged").mode('append')\
        .saveAsTable("receiver_chat_history")

    #sending only records which are flagged to DB
    df = df.withColumn("batch_id",lit(batch_id)).filter("is_flagged = 'Yes'")\
        .select("sender","is_flagged","timestamp","batch_id")
    
    df.write \
    .jdbc(url=jdbc_url, table=table_name, mode="append", properties=connection_properties)



    


writing_df = json_expanded_df.writeStream \
    .foreachBatch(process) \
    .outputMode("append")\
    .option("checkpointLocation","/home/hadoop/com_checkpoints") \
    .trigger(processingTime = '20 seconds')\
    .start()

writing_df.awaitTermination()