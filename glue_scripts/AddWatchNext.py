###### TEDx-Load-Aggregate-Model
######

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, collect_set, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

try:
    ##### FROM FILES
    watch_next_dataset_path = "s3://fsangregorio-tcm-data-1/watch_next_dataset.csv"
    
    ###### READ PARAMETERS
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    ##### START JOB CONTEXT AND JOB
    sc = SparkContext()
    
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
        
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    #### READ INPUT FILES TO CREATE AN INPUT DATASET
    watch_next_dataset = spark.read \
        .option("header","true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv(watch_next_dataset_path)
        
    watch_next_dataset.printSchema()
    
    #### FILTER ITEMS WITH NULL POSTING KEY
    count_items = watch_next_dataset.count()
    count_items_null = watch_next_dataset.filter("idx is not null").count()
    
    print(f"Number of items from RAW DATA {count_items}")
    print(f"Number of items from RAW DATA with NOT NULL KEY {count_items_null}")
    
    #### DROP URL COLUMN
    watch_next_dataset = watch_next_dataset.drop("url")
    
    #### AGGREGATE ITEMS UNDER SAME INDEX
    watch_next_dataset_agg = watch_next_dataset.groupBy(col("idx").alias("idx_ref")).agg(collect_set("watch_next_idx").alias("watch_next"))
    watch_next_dataset_agg.printSchema()
    
    ### READ TEDx DATASET FROM MONGODB
    mongo_uri = "mongodb://cluster0-shard-00-00.ih73c.mongodb.net:27017,cluster0-shard-00-01.ih73c.mongodb.net:27017,cluster0-shard-00-02.ih73c.mongodb.net:27017"
    
    read_mongo_options = {
        "uri": mongo_uri,
        "database": "unibg_tedx_2021",
        "collection": "tedx_data",
        "username": "admin123",
        "password": "admin123",
        "partitioner": "MongoSamplePartitioner",
        "partitionerOptions.partitionSizeMB": "10",
        "partitionerOptions.partitionKey": "_id",
        "ssl": "true",
        "ssl.domain_match": "false"
    }
    
    tedx_dataset = glueContext.create_dynamic_frame.from_options(
        connection_type="mongodb",
        connection_options=read_mongo_options).toDF()
    
    tedx_dataset.printSchema()
    
    
    # CREATE THE AGGREGATE MODEL, ADD WATCHNEXT TO TEDX_DATASET
    tedx_dataset_agg = tedx_dataset.join(watch_next_dataset_agg, tedx_dataset._id == watch_next_dataset_agg.idx_ref, "left") \
        .drop("idx_ref") \
    
    tedx_dataset_agg.printSchema()
    
    # WRITE RESULT TO MONGODB
    write_mongo_options = {
        "uri": mongo_uri,
        "database": "unibg_tedx_2021",
        "collection": "tedx_data",
        "username": "admin123",
        "password": "admin123",
        "ssl": "true",
        "ssl.domain_match": "false"}
        
    tedx_dataset_dynamic_frame = DynamicFrame.fromDF(
        tedx_dataset_agg, 
        glueContext, 
        "nested")
    
    glueContext.write_dynamic_frame.from_options(
        tedx_dataset_dynamic_frame, 
        connection_type="mongodb", 
        connection_options=write_mongo_options)
    
except Exception as e:
    print(e)
    raise e
        