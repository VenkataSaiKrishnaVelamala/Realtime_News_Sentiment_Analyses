# %%
from pyspark.sql import SparkSession

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StringType, StructField, StructType


# %%
spark=SparkSession.builder.appName("test").getOrCreate()

# %%
# Define the schema according to the JSON data you expect
schema = StructType([
    StructField("Headline", StringType(), True),
    StructField("URL", StringType(), True),
    StructField("Full Content", StringType(), True),
])


# %%
# Kafka configuration parameters
topic = "newsData"
bootstrap_servers = "pkc-56d1g.eastus.azure.confluent.cloud:9092"

# Read data from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092") \
    .option("subscribe", "newsData") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='PE2K3XKMKQMBKPUS' password='7IiXWeafLDGoUaUVsrGxjYJR49f1VxDqMmTVV2wNWcu/55dM6Pv/PYZUfxViWEPe';") \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING) as json_data")

# %%
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()


# %%
import random

# Global variables for reservoir sampling
reservoir_size = 100
reservoir = []

def update_reservoir(new_item):
    global reservoir, reservoir_size
    if len(reservoir) < reservoir_size:
        reservoir.append(new_item)
    else:
        # Replace elements in the reservoir with a decreasing probability
        replace_index = random.randint(0, len(reservoir) - 1)
        reservoir[replace_index] = new_item


# %%
def process_batch(df, epoch_id):
    global reservoir
    # Convert DataFrame to Pandas for easier manipulation in this example
    pandas_df = df.toPandas()
    for index, row in pandas_df.iterrows():
        article = {
            "headline": row['headline'],
            "url": row['url'],
            "content": row['content']
        }
        update_reservoir(article)

    # Optionally, you can print the current state of the reservoir
    print("Current Reservoir Samples:")
    for sample in reservoir:
        print(sample)


# %%
import random

# Reservoir size
reservoir_size = 100
reservoir = []

def update_reservoir(item):
    global reservoir, reservoir_size
    if len(reservoir) < reservoir_size:
        reservoir.append(item)
    else:
        # Randomly replace elements in the reservoir with a decreasing probability
        pos = random.randint(0, len(reservoir) - 1)
        reservoir[pos] = item

def process_row(row):
    # Convert row to dictionary to handle data easily
    article = row.asDict()
    update_reservoir(article)

def foreach_batch_function(df, epoch_id):
    # Apply the Reservoir Sampling algorithm to each row in the batch
    df.foreach(procesqs_row)
    # Print or log the reservoir samples for verification or further processing
    print("Current Reservoir Samples:")
    for sample in reservoir:
        print(sample['headline'])


# %%
query = df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .outputMode("update") \
    .start()

query.awaitTermination()

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, StructField

# Assuming spark session is already created and configured as shown previously

# Schema based on your JSON structure
schema = StructType([
    StructField("id", StringType()),
    StructField("headline", StringType()),
    StructField("url", StringType()),
    StructField("content", StringType()),
])

# Streaming DataFrame setup
query = df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()


# %%
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Define the schema corresponding to the JSON data
schema = StructType([
    StructField("id", StringType()),
    StructField("headline", StringType()),
    StructField("url", StringType()),
    StructField("content", StringType())
])

# Parse the JSON data
parsed_df = df.withColumn("data", from_json(col("json_data"), schema)) \
              .select("data.*")


# %%
from pyspark.sql.functions import udf
from textblob import TextBlob

# Define a UDF to compute sentiment polarity
def sentiment(text):
    return TextBlob(text).sentiment.polarity

sentiment_udf = udf(sentiment, StringType())

# Add a sentiment column to the DataFrame
sentiment_df = parsed_df.withColumn("sentiment", sentiment_udf(col("content")))


# %%
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from textblob import TextBlob

# Define a UDF that computes sentiment polarity and categorizes it
def classify_sentiment(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        return 'Positive'
    elif polarity < 0:
        return 'Negative'
    else:
        return 'Neutral'

sentiment_classify_udf = udf(classify_sentiment, StringType())

# Apply the UDF to the DataFrame
classified_df = parsed_df.withColumn("sentiment_category", sentiment_classify_udf(col("content")))


# %%
# Write the results to the console
query = sentiment_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


# %%
# Write the results to the console
query = classified_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


# %%
from pyspark.sql.functions import udf, col
from pyspark.sql.types import FloatType, StringType, StructType, StructField
from textblob import TextBlob

# Define the schema for the UDF return type
sentiment_schema = StructType([
    StructField("score", FloatType(), False),
    StructField("category", StringType(), False)
])

# Define a UDF that computes sentiment polarity and categorizes it
def sentiment_analysis(text):
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0:
        category = 'Positive'
    elif polarity < 0:
        category = 'Negative'
    else:
        category = 'Neutral'
    return (polarity, category)

# Register the UDF
sentiment_udf = udf(sentiment_analysis, sentiment_schema)

# Apply the UDF to the DataFrame
sentiment_df = parsed_df.withColumn("sentiment", sentiment_udf(col("content")))


# %%
# Expand the sentiment column into separate score and category columns
expanded_df = sentiment_df.select(
    "*",
    col("sentiment.score").alias("sentiment_score"),
    col("sentiment.category").alias("sentiment_category")
)


# %%

output_directory = "/Users/krishna/Semester2/BigData/project/output"
checkpoint_directory = "/Users/krishna/Semester2/BigData/project/output"

json_query = expanded_df.writeStream \
    .outputMode("append") \
    .format("json") \
    .option("path", output_directory) \
    .option("checkpointLocation", checkpoint_directory) \
    .start()
json_query.awaitTermination()


# %%
# Write the results to the console
query = expanded_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()

# %%
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("MongoDBIntegration") \
    .config("spark.mongodb.input.uri", "mongodb+srv://vvenkatasaik:aOqYyq9t3FL7GCD5@cluster0.cmd6ekl.mongodb.net/") \
    .config("spark.mongodb.output.uri", "mongodb+srv://vvenkatasaik:aOqYyq9t3FL7GCD5@cluster0.cmd6ekl.mongodb.net/") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()


# %%
query=expanded_df.writeStream \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option('uri', "mongodb+srv://vvenkatasaik:aOqYyq9t3FL7GCD5@cluster0.cmd6ekl.mongodb.net/") \
    .option('database', 'news') \
    .option('collection', 'newsdata') \



# %%
# Write the results to the console
query = df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()


# %%
from pyspark.sql.functions import to_json, struct, col

# Assuming 'expanded_df' is your DataFrame with all the required fields
kafka_output_df = expanded_df.selectExpr("id AS key", "to_json(struct(*)) AS value")


# %%
kafka_query = kafka_output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='PE2K3XKMKQMBKPUS' password='7IiXWeafLDGoUaUVsrGxjYJR49f1VxDqMmTVV2wNWcu/55dM6Pv/PYZUfxViWEPe';") \
    .option("topic", "your-output-topic") \
    .option("checkpointLocation", "/path/to/kafka/checkpoint") \
    .outputMode("update") \
    .start()


# %%
# Parse the JSON data
parsed_df = df


# %%
! pip install textblob

# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, struct
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from textblob import TextBlob

# Define the schema of your incoming Kafka data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaNews") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()


# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json, struct
from pyspark.sql.types import StringType, StructType, StructField, FloatType
from textblob import TextBlob

# Define the schema of your incoming Kafka data
schema = StructType([
    StructField("id", StringType(), True),
    StructField("headline", StringType(), True),
    StructField("url", StringType(), True),
    StructField("content", StringType(), True),
])

spark = SparkSession.builder \
    .appName("KafkaNewsConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.1") \
    .getOrCreate()

# Kafka source
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092") \
    .option("subscribe", "newsData") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='PE2K3XKMKQMBKPUS' password='7IiXWeafLDGoUaUVsrGxjYJR49f1VxDqMmTVV2wNWcu/55dM6Pv/PYZUfxViWEPe';") \
    .load() \
    .select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*")


# %%
from pyspark.ml.feature import HashingTF, IDF, BucketedRandomProjectionLSH
from pyspark.ml import Pipeline

# Text processing for LSH
hashingTF = HashingTF(inputCol="content", outputCol="rawFeatures", numFeatures=1024)
idf = IDF(inputCol="rawFeatures", outputCol="features")
brp = BucketedRandomProjectionLSH(inputCol="features", outputCol="hashes", bucketLength=2.0, numHashTables=3)

pipeline = Pipeline(stages=[hashingTF, idf, brp])
model = pipeline.fit(df)  # Fit model to the static part of the data if possible or use approximations
hashed_df = model.transform(df)




# %%


# %%
spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "pkc-56d1g.eastus.azure.confluent.cloud:9092") \
    .option("subscribe", "newsData") \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "PLAIN") \
    .option("kafka.sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username='PE2K3XKMKQMBKPUS' password='7IiXWeafLDGoUaUVsrGxjYJR49f1VxDqMmTVV2wNWcu/55dM6Pv/PYZUfxViWEPe';") \
    .option("kafka.ssl.endpoint.identification.algorithm", "https") \
    .option("startingOffsets", "earliest") \


