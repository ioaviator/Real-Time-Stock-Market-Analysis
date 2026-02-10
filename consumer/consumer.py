from pyspark.sql import SparkSession
from pyspark.sql.types import TimestampType
from pyspark.sql.functions import from_json, col
from config import postgres_config, checkpoint_dir, kafka_data_schema

spark = (SparkSession.builder
         .appName('KafkaSparkStreaming')
         .getOrCreate() 
)

df = ( spark.readStream.format('kafka')
  .option('kafka.bootstrap.servers', 'kafka:9092')
  .option('subscribe', 'stock_analysis') # topic subscription
  .option('startingOffsets', 'latest') # Read only new incoming messages (ignore old messages in the topic)
  .option('failOnDataLoss', 'false') # If Kafka deletes old messages (retention), Spark won't crash.
  .load() # start reading the Kafka topic as a stream
)

# Convert the 'value' column (which is a JSON string) into structured columns
parsed_df = df.selectExpr( 'CAST(value AS STRING)') \
              .select(from_json(col("value"), kafka_data_schema).alias("data")) \
              .select("data.*")

processed_df = parsed_df.select(
    col("date").cast(TimestampType()).alias("date"),
    col("high").alias("high"),
    col("low").alias("low"),
    col("open").alias("open"),
    col("close").alias("close"),
    col("symbol").alias("symbol")
)

def write_to_postgres(batch_df, batch_id):
    """
    Writes a microbatch DataFrame to PostgreSQL using JDBC in 'append' mode.
    """
    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .options(**postgres_config) \
        .save()

# --- Stream to PostgreSQL using foreachBatch ---
query = (
  processed_df.writeStream
  .foreachBatch(write_to_postgres) # Use foreachBatch for JDBC sinks
  .option('checkpointLocation', checkpoint_dir)  # directory where Spark will store its checkpoint data. crucial in streaming to enable fault tolerance
  .outputMode('append') # Or 'append', depending on your use case and table schema
  .start()
)

# Wait for the termination of the query
query.awaitTermination()
