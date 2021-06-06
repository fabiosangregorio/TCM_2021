import nltk
from sklearn.feature_extraction.text import TfidfVectorizer, TfidfTransformer
from sklearn.neighbors import NearestNeighbors

import sys
import json
import pyspark
from pyspark.sql.functions import col, collect_list, array_join

from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import traceback
import tempfile
import boto3
import joblib

try:
    s3 = boto3.resource('s3')
    nltk.data.path.append("/tmp")
    nltk.download("punkt", download_dir = "/tmp")
    
    ##### FROM FILES
    tedx_dataset_path = "s3://fsangregorio-tcm-data-1/tedx_dataset.csv"
    
    ###### READ PARAMETERS
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    
    ##### START JOB CONTEXT AND JOB
    sc = SparkContext()
    
    
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    
        
    job = Job(glueContext)
    job.init(args['JOB_NAME'], args)
    
    
    #### READ INPUT FILES TO CREATE AN INPUT DATASET
    tedx_dataset = spark.read \
        .option("header","true") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiline","true") \
        .csv(tedx_dataset_path)
    
    # merge title main speaker and detail into one column
    df = tedx_dataset.select("*").toPandas()
    
    count_vectorizer = TfidfVectorizer(
        analyzer="word", preprocessor=None, stop_words='english', max_features=None)
    
    tfidf = count_vectorizer.fit(df['details'])
    tfidf_matrix = count_vectorizer.transform(df['details'])
    
    # tfidf_transformer = TfidfTransformer()
    # tfidf_df = tfidf_transformer.fit_transform(tfidf)
    nn = NearestNeighbors(n_neighbors=5, n_jobs=-1)
    nn.fit(tfidf_matrix)
    
    # Save NN model
    with tempfile.TemporaryFile() as fp:
        joblib.dump(nn, fp)
        fp.seek(0)
        # use bucket_name and OutputFile - s3 location path in string format.
        s3.Bucket('fsangregorio-tcm-model-1').put_object(Key="model.pkl", Body=fp.read())
        
    # Save vectorizer
    with tempfile.TemporaryFile() as fp:
        joblib.dump(tfidf, fp)
        fp.seek(0)
        # use bucket_name and OutputFile - s3 location path in string format.
        s3.Bucket('fsangregorio-tcm-model-1').put_object(Key="vectorizer.pkl", Body=fp.read())
        
    idxs = list(df["idx"].values)
    mapped_list = { i: idxs[i] for i in range(len(idxs))}
    
    # Save id_map
    with tempfile.TemporaryFile() as fp:
        joblib.dump(mapped_list, fp)
        fp.seek(0)
        # use bucket_name and OutputFile - s3 location path in string format.
        s3.Bucket('fsangregorio-tcm-model-1').put_object(Key="id_map.json", Body=fp.read())
        
        
        
except Exception as e:
    traceback.print_exc()
    raise e