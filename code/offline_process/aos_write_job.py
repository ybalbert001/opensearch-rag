#!/usr/bin/env python
# coding: utf-8

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers
import boto3
import random
import json
from awsglue.utils import getResolvedOptions
import sys
import hashlib
import datetime
import re
import os
import itertools
from bs4 import BeautifulSoup
from langchain.document_loaders import PDFMinerPDFasHTMLLoader
from langchain.docstore.document import Document
from langchain.text_splitter import RecursiveCharacterTextSplitter,CharacterTextSplitter, MarkdownTextSplitter
import logging
import urllib.parse
import numpy as np
from urllib.parse import unquote
from datetime import datetime

args = getResolvedOptions(sys.argv, ['bucket', 'object_key','AOS_ENDPOINT','REGION','AOS_INDEX'])
s3 = boto3.resource('s3')
bucket = args['bucket']
object_key = args['object_key']

AOS_ENDPOINT = args['AOS_ENDPOINT']
AOS_INDEX = args['AOS_INDEX']
REGION = args['REGION']

bedrock = boto3.client(service_name='bedrock-runtime',
                       region_name=REGION)

def iterate_items(file_content, object_key,doc_classify,smr_client, index_name, endpoint_name):
    json_content = json.loads(file_content)

    for idx, item in enumerate(json_content):
        try:
            document = { "publish_date": publish_date, "doc" : questions[i], "idx": idx, "doc_type" : "Question", "content" : contents[i], "doc_title": doc_title,"doc_author":authors[i] if authors[i] else doc_author, "doc_category": doc_category, "doc_meta": json.dumps(meta[i], ensure_ascii=False), "doc_classify":doc_classify,"embedding" : embeddings_q[i]}
            yield {"_index": index_name, "_source": document, "_id": hashlib.md5(str(document).encode('utf-8')).hexdigest()}
        except Exception as e:
            print(f"failed to process, {str(e)}")

def load_content_json_from_s3(bucket, object_key, content_type, credentials):
    return {}

def WriteVecIndexToAOS(bucket, object_key, content_type, doc_classify, smr_client, aos_endpoint=AOS_ENDPOINT, region=REGION, index_name=INDEX_NAME):
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region)

    try:
        file_content = load_content_json_from_s3(bucket, object_key, content_type, credentials)

        client = OpenSearch(
            hosts = [{'host': aos_endpoint, 'port': 443}],
            http_auth = auth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection,
            timeout = 60, # 默认超时时间是10 秒，
            max_retries=5, # 重试次数
            retry_on_timeout=True
        )

        gen_aos_record_func = iterate_items(file_content, object_key, doc_classify, smr_client, index_name, EMB_MODEL_ENDPOINT)
        
        response = helpers.bulk(client, gen_aos_record_func, max_retries=3, initial_backoff=200, max_backoff=801, max_chunk_bytes=10 * 1024 * 1024)#, chunk_size=10000, request_timeout=60000) 
        return response
    except Exception as e:
        print(f"There was an error when ingest:{object_key} to aos cluster, Exception: {str(e)}")
        return None   

def process_s3_uploaded_file(bucket, object_key):
    print("********** object_key : " + object_key)
    #if want to use different aos index, the object_key format should be: ai-content/company/username/filename

    response = WriteVecIndexToAOS(bucket, object_key, content_type, doc_classify, smr_client, index_name=index_name)
    print("response:")
    print(response)
    print("ingest {} chunk to AOS".format(response[0]))


##如果是从chatbot上传，则是ai-content/username/filename
def get_filename_from_obj_key(object_key):
    paths = object_key.split('/')
    return paths[1] if len(paths) > 2 else 's3_upload'

for s3_key in object_key.split(','):
    s3_key = urllib.parse.unquote(s3_key) ##In case Chinese filename
    s3_key = s3_key.replace('+',' ') ##replace the '+' with space. ps:if the original file name contains space, then s3 notification will replace it with '+'.
    print("processing {}".format(s3_key))
    process_s3_uploaded_file(bucket, s3_key)