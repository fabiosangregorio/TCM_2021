import logging
import json
import tempfile
import os
import boto3
import joblib
from sklearn.feature_extraction.text import TfidfTransformer
from dotenv import load_dotenv

load_dotenv("./variables.env")

def lambda_handler(event, context):
    
    s3 = boto3.client('s3')
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    text_query = event["query"]
    
    # load model
    with tempfile.TemporaryFile() as fp:
        s3.download_fileobj(Fileobj=fp, Bucket='fsangregorio-tcm-model-1', Key="model.pkl")
        fp.seek(0)
        nn = joblib.load(fp)
    
    # load vectorizer
    with tempfile.TemporaryFile() as fp:
        s3.download_fileobj(Fileobj=fp, Bucket='fsangregorio-tcm-model-1', Key="vectorizer.pkl")
        fp.seek(0)
        count_vectorizer = joblib.load(fp)
    
    # load id_map
    with tempfile.TemporaryFile() as fp:
        s3.download_fileobj(Fileobj=fp, Bucket='fsangregorio-tcm-model-1', Key="id_map.json")
        fp.seek(0)
        id_map = joblib.load(fp)
    
    
    # get predictions
    logger.info(count_vectorizer)
    vectorized = count_vectorizer.transform([text_query])
    reccomendations = nn.kneighbors(vectorized, return_distance=False).reshape(-1)
    
    df_rec = [id_map[r] for r in reccomendations]
    
    # return predicitons
    return df_rec
    