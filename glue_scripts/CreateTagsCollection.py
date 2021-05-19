import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_set, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


###### READ PARAMETERS
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

##### START JOB CONTEXT AND JOB
sc = SparkContext()

glueContext = GlueContext(sc)
spark = glueContext.spark_session
    
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## READ TAGS DATASET
tags_dataset_path = "s3://fsangregorio-tcm-data-1/tags_dataset.csv"
tags_dataset = spark.read.option("header","true").csv(tags_dataset_path)

# CREATE THE AGGREGATE MODEL
tags_dataset_agg = tags_dataset.groupBy(col("tag")).agg(collect_set("idx").alias("talk_id"))
tags_dataset_agg.printSchema()

mongo_uri = "mongodb://cluster0-shard-00-00.ih73c.mongodb.net:27017,cluster0-shard-00-01.ih73c.mongodb.net:27017,cluster0-shard-00-02.ih73c.mongodb.net:27017"

write_mongo_options = {
    "uri": mongo_uri,
    "database": "unibg_tedx_2021",
    "collection": "tedx_tags",
    "username": "admin123",
    "password": "admin123",
    "ssl": "true",
    "ssl.domain_match": "false"}
from awsglue.dynamicframe import DynamicFrame
tedx_dataset_dynamic_frame = DynamicFrame.fromDF(tags_dataset_agg, glueContext, "nested")

glueContext.write_dynamic_frame.from_options(tedx_dataset_dynamic_frame, connection_type="mongodb", connection_options=write_mongo_options)