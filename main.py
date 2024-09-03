from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, avg, max, min, stddev, to_timestamp, date_format
)
from datetime import datetime
from cryptography.fernet import Fernet
import os

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Data Processing Pipeline") \
    .getOrCreate()

# Correct file path using raw string
file_path = r"C:\Users\RITIK\OneDrive\Desktop\IBM\assignment_dataset.csv"

# Load the data from a CSV file
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Display the schema to verify correct data types
df.printSchema()

# Show a few rows to understand the data
df.show(5)

# Data Transformation

# Convert the 'timestamp' column to a timestamp object
df = df.withColumn("timestamp", to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))

# Data Cleanup

# Drop duplicate rows
df = df.dropDuplicates()

# Handle missing or null values by filling with mean values
# Calculate mean values for relevant columns
mean_values = df.select(
    avg(col("temperature")).alias("mean_temperature"),
    avg(col("humidity")).alias("mean_humidity"),
    avg(col("air_quality")).alias("mean_air_quality"),
    avg(col("altitude")).alias("mean_altitude")
).collect()[0]

# Fill missing values with the calculated mean
df = df.na.fill({
    "temperature": mean_values["mean_temperature"],
    "humidity": mean_values["mean_humidity"],
    "air_quality": mean_values["mean_air_quality"],
    "altitude": mean_values["mean_altitude"]
})

# Data Aggregation

# Group by hourly timestamp and calculate required metrics
df_aggregated = df.groupBy(
    date_format(col("timestamp"), "yyyy-MM-dd HH:00:00").alias("timestamp")
).agg(
    avg("value").alias("average_value"),
    max("value").alias("max_value"),
    min("value").alias("min_value"),
    stddev("value").alias("std_dev"),
    avg("temperature").alias("average_temperature"),
    avg("humidity").alias("average_humidity"),
    avg("air_quality").alias("average_air_quality"),
    avg("altitude").alias("average_altitude")
)

# Show aggregated data
df_aggregated.show(5)

# Generate dynamic output filename using Python's datetime
run_date = datetime.now().strftime("%Y%m%d")
output_filename = f"processed_data_{run_date}.csv"
output_path = f"C:/Users/RITIK/OneDrive/Desktop/IBM/{output_filename}"

# Save the processed data to a CSV file
df_aggregated.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

# Encrypt the file
# Generate a key for encryption
key = Fernet.generate_key()
cipher_suite = Fernet(key)

# Encrypt the CSV file
with open(output_path, 'rb') as file:
    file_data = file.read()

encrypted_data = cipher_suite.encrypt(file_data)

# Write the encrypted file back
encrypted_output_path = output_path + '.enc'
with open(encrypted_output_path, 'wb') as file:
    file.write(encrypted_data)

# Optionally, remove the unencrypted file
os.remove(output_path)

# Set file permissions to read/write for the owner only
os.chmod(encrypted_output_path, 0o600)

# Print the encryption key (you should store this securely)
print(f"Encryption key (store securely): {key.decode()}")
print(f"Processed data saved and encrypted to {encrypted_output_path}")

# Stop the Spark session
spark.stop()
