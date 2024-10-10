# Databricks notebook source
import requests
from pyspark.sql import DataFrame

# COMMAND ----------

# Define the file name in DBFS
file_name = "/FileStore/tables/glaciers.csv"

# Read the CSV file into a DataFrame
df = spark.read.format("csv").option("header", "true").load(file_name)
 
# Extract the file format from the file name
file_format = file_name.split(".")[-1]
display(df) 

# COMMAND ----------


def read_data(file_name):
    if file_format == 'csv':
        df = spark.read.format(file_format).option("header", "true").load(file_name)
    elif file_format == 'json':
        try:
            df = spark.read.format(file_format).load(file_name)
        except:
            df = spark.read.format(file_format).option("multiline", "true").load(file_name)
    elif file_format == 'parquet':
        df = spark.read.format(file_format).load(file_name)
    elif file_format == 'txt':
        df = spark.read.text(file_name)
    return df

# Load the DataFrame using the read_data function
df = read_data(file_name)
display(df)
 


# COMMAND ----------

# Transform data
def transform_data(df):
    nintys_df = df.filter(df.Year.like('19%')).orderBy('Year')
    modern_df = df.filter(df.Year.like('20%')).orderBy('Year')
    return nintys_df, modern_df

nintys_df, modern_df = transform_data(df)


# COMMAND ----------

# Display the transformed DataFrames
display(nintys_df) 
display(modern_df)


# COMMAND ----------

 
# # Create file names for output
# def create_file_names():
#     nintys_file_namez = nintys_df.agg({"Year": "min"}).collect()[0][0] + "-" + nintys_df.agg({"Year": "max"}).collect()[0][0]
#     modern_file_name = modern_df.agg({"Year": "min"}).collect()[0][0] + "-" + modern_df.agg({"Year": "max"}).collect()[0][0]
#     return nintys_file_namez, modern_file_name

# m, n = create_file_names()
# print(m, n)


# COMMAND ----------

from pyspark.sql import SparkSession

# Create file names for output
def create_file_names():
    nintys_file_name = str(nintys_df.agg({"Year": "min"}).collect()[0][0]) + "-" + str(nintys_df.agg({"Year": "max"}).collect()[0][0])
    modern_file_name = str(modern_df.agg({"Year": "min"}).collect()[0][0]) + "-" + str(modern_df.agg({"Year": "max"}).collect()[0][0])
    return nintys_file_name, modern_file_name

# Create a Spark session with S3 credentials
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("fs.s3a.access.key", "AKIAXIMMIOQIWNWWI77V") \
    .config("fs.s3a.secret.key", "yvZ4/xAdr3HueZtmxEJKar9bJCgG6bHO7DHPtFNg") \
    .getOrCreate()

def write_df(file_type: str, dfs, file_names, s3_bucket: str):
    for df, name in zip(dfs, file_names):
        # Define the S3 path
        path = f"s3a://{s3_bucket}/{name}.{file_type}"

        # Remove existing files if they exist
        try:
            # Adjust this if you're not on Databricks
            dbutils.fs.rm(path, recurse=True)  
        except NameError:
            print(f"dbutils is not available, skipping file removal for {name}")
        except Exception as e:
            print(f"Error removing existing file: {e}")

        # Write the DataFrame to the specified S3 path
        try:
            df.write.format(file_type).save(path)
            print(f"Successfully saved {name} to {path}")
        except Exception as e:
            print(f"Error writing DataFrame {name}: {e}")

# Sample DataFrames (Replace with your actual DataFrames)
# nintys_df = spark.createDataFrame([...])
# modern_df = spark.createDataFrame([...])

# Generate the file names
nintys_file_name, modern_file_name = create_file_names()

# Specify your S3 bucket name
s3_bucket = "snowflake-demobkt"

# Call the function to write the DataFrames
write_df("parquet", [nintys_df, modern_df], [nintys_file_name, modern_file_name], s3_bucket)

# Optionally, check the saved files in S3 using AWS CLI or S3 console


# COMMAND ----------

# from pyspark.sql import SparkSession

# # Create a Spark session with S3 credentials
# spark = SparkSession.builder \
#     .appName("YourAppName") \
#     .config("fs.s3a.access.key", "AKIAXIMMIOQIWNWWI77V") \
#     .config("fs.s3a.secret.key", "yvZ4/xAdr3HueZtmxEJKar9bJCgG6bHO7DHPtFNg") \
#     .getOrCreate()

# def write_df(file_type: str, dfs, file_names, s3_bucket: str):
#     for df, name in zip(dfs, file_names):
#         # Define the S3 path
#         path = f"s3a://{s3_bucket}/{name}.{file_type}"

#         # Remove existing files if they exist
#         try:
#             dbutils.fs.rm(path, recurse=True)  # Remove existing files
#         except Exception as e:
#             print(f"Error removing existing file: {e}")  # Ignore if the file doesn't exist

#         # Write the DataFrame to the specified S3 path
#         try:
#             df.write.format(file_type).save(path)
#             print(f"Successfully saved {name} to {path}")
#         except Exception as e:
#             print(f"Error writing DataFrame {name}: {e}")

# # Sample DataFrames (Replace with your actual DataFrames)
# # nintys_df = spark.createDataFrame([...])
# # modern_df = spark.createDataFrame([...])

# # Specify your S3 bucket name
# s3_bucket = "snowflake-demobkt"

# # Call the function to write the DataFrames
# write_df("parquet", [nintys_df, modern_df], ["nintys", "modern"], s3_bucket)

# # Optionally, check the saved files in S3 using AWS CLI or S3 console


# COMMAND ----------

# # # Function to write DataFrames to DBFS
# def write_df(file_type: str, dfs, file_names):
#     for x, y in zip(dfs, file_names):
#         path = f"/dbfs/{y}.{file_type}"
#         # Check if the file exists and remove it
#         if len(dbutils.fs.ls('/dbfs/')) > 0:  # Check if the directory exists
#             try:
#                 dbutils.fs.rm(path, recurse=True)  # Remove existing files
#             except:
#                 pass  # Ignore if the file doesn't exist
#         x.write.format(file_type).save(path)


# Write the transformed DataFrames
def write_df(file_type: str, dfs, file_names):
    for x, y in zip(dfs, file_names):
        path = f"/FileStore/{y}.{file_type}"  # Use FileStore directly
        # Check if the output directory exists and create it if not
        try:
            dbutils.fs.ls('/FileStore/')  # List to check if directory exists
        except:
            dbutils.fs.mkdirs('/FileStore/')  # Create the directory if it doesn't exist

        # Remove existing files if they exist
        try:
            dbutils.fs.rm(path, recurse=True)  # Remove existing files
        except:
            pass  # Ignore if the file doesn't exist

        # Write the DataFrame to the specified path
        x.write.format(file_type).save(path)
 
 

# COMMAND ----------

# Write the transformed DataFrames
write_df("parquet", [nintys_df, modern_df], [m, n])
 

# COMMAND ----------


# Optionally, check the saved files
dbutils.fs.ls('/dbfs/')
