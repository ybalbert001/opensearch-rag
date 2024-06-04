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

publish_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def iterate_items(file_content, object_key):
    json_obj = json.loads(file_content)
    file_name = object_key.split('/')[-1].replace('.json', '')

    if json_obj["type"] == "multilingual_terminology":
        arr = json_obj["data"]
        doc_type = json_obj["type"]
        author = json_obj.get("author","")
        print(f"doc_type:{doc_type}, author:{author}")

        for idx, item in enumerate(arr):
            content = json.dumps(item["mapping"])
            doc_category = item["entity_type"]
            try:
                document = { "publish_date": publish_date, "doc" : '', "idx": idx, "doc_type" : doc_type, "content" : content, "doc_title": file_name, "doc_author": author, "doc_category": doc_category}
                yield {"_index": AOS_INDEX, "_source": document, "_id": hashlib.md5(str(document).encode('utf-8')).hexdigest()}
            except Exception as e:
                print(f"failed to process, {str(e)}")

    elif json_obj["type"] == "crosslingual_terminology":
        arr = json_obj["data"]
        doc_type = json_obj["type"]
        author = json_obj.get("author", "")
        print(f"doc_type:{doc_type}, author:{author}")

        for idx, item in enumerate(arr):
            doc_category = item["entity_type"]

            for term in item["terms"]:
                try:
                    document = { "publish_date": publish_date, "doc" : '', "idx": idx, "doc_type" : doc_type, "content" : term, "doc_title": file_name, "doc_author": author, "doc_category": doc_category}
                    yield {"_index": AOS_INDEX, "_source": document, "_id": hashlib.md5(str(document).encode('utf-8')).hexdigest()}
                except Exception as e:
                    print(f"failed to process, {str(e)}")
    elif json_obj["type"] == "moderation":
        arr = json_obj["data"]
        doc_type = json_obj["type"]
        author = json_obj.get("author", "")
        print(f"doc_type:{doc_type}, author:{author}")

        for idx, item in enumerate(arr):
            doc = item.get('nick_name', item.get('motto'))   #aos doc字段
            content_type = item["content_type"]              #aos doc_category字段
            reason = item["reason"]                          #aos content字段
            lang = item["lang"]                              #aos lang字段

            try:
                document = { "publish_date": publish_date, "doc" : doc, "idx": idx, "doc_type" : doc_type, "content" : reason, "doc_title": file_name, "doc_author": author, "doc_category": content_type}
                yield {"_index": AOS_INDEX, "_source": document, "_id": hashlib.md5(str(document).encode('utf-8')).hexdigest()}
            except Exception as e:
                print(f"failed to process, {str(e)}")

def load_content_json_from_s3(bucket, object_key):
    if object_key.endswith('.json'):
        obj = s3.Object(bucket, object_key)
        file_content = obj.get()['Body'].read().decode('utf-8', errors='ignore').strip()
    else:
        raise RuntimeError("Invalid S3 File Format")
        
    return file_content

def WriteVecIndexToAOS(bucket, object_key):
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, REGION)

    try:
        file_content = load_content_json_from_s3(bucket, object_key)

        client = OpenSearch(
            hosts = [{'host': AOS_ENDPOINT, 'port': 443}],
            http_auth = auth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection,
            timeout = 60, # 默认超时时间是10 秒，
            max_retries=5, # 重试次数
            retry_on_timeout=True
        )

        gen_aos_record_func = iterate_items(file_content, object_key)
        
        response = helpers.bulk(client, gen_aos_record_func, max_retries=3, initial_backoff=200, max_backoff=801, max_chunk_bytes=10 * 1024 * 1024)#, chunk_size=10000, request_timeout=60000) 
        return response
    except Exception as e:
        print(f"There was an error when ingest:{object_key} to aos cluster, Exception: {str(e)}")
        return None   

def process_s3_uploaded_file(bucket, object_key):
    print("********** object_key : " + object_key)
    #if want to use different aos index, the object_key format should be: ai-content/company/username/filename

    response = WriteVecIndexToAOS(bucket, object_key)
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