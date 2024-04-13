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
import time

args = getResolvedOptions(sys.argv, ['bucket', 'object_key', 'model_id', 'AOS_ENDPOINT', 'REGION', 'AOS_INDEX'])

bucket = args['bucket']
object_key = args['object_key']
model_id = args['model_id']

AOS_ENDPOINT = args['AOS_ENDPOINT']
AOS_INDEX = args['AOS_INDEX']
REGION = args['REGION']

s3 = boto3.resource('s3', REGION)
bedrock = boto3.client(service_name='bedrock-runtime', region_name=REGION)
credentials = boto3.Session().get_credentials()
awsauth = AWSV4SignerAuth(credentials, REGION)

class TerminologyRetriever():
    aos_endpoint: str
    aos_index: str
    aos_client: object
    
    def __init__(self, aos_endpoint: str, aos_index: str, aos_client: object):
        self.aos_endpoint = aos_endpoint
        self.aos_index = aos_index
        self.aos_client = aos_client
        
    @classmethod
    def from_endpoints(cls, aos_endpoint:str, aos_index:str):
        aos_client = OpenSearch(
                hosts=[{'host': aos_endpoint, 'port': 443}],
                http_auth = awsauth,
                use_ssl=True,
                verify_certs=True,
                connection_class=RequestsHttpConnection
            )

        return cls(aos_endpoint=aos_endpoint,
                  aos_index=aos_index,
                  aos_client=aos_client)

    def search_aos_for_terminology(self, src_content, doc_type, size=10):
        query = {
            "size": size,
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "content": src_content
                        }
                    },
                "filter": {
                    "term": {
                        "doc_type": doc_type
                        }
                    }
                }
            }
        }
        query_response = self.aos_client.search(
            body=query,
            index=self.aos_index
        )

        result_arr = [ {'idx':item['_source'].get('idx',0),'doc_category':item['_source']['doc_category'], 'content':item['_source']['content'], 'doc_type': item['_source']['doc_type'], 'score': item['_score']} for item in query_response["hits"]["hits"]]
        return result_arr

def construct_translate_prompt(src_content, src_lang, dest_lang, retriever):
    pe_template = """You are the world's most professional translation tool, proficient in professional translation between EN and CN..
You can translate anything. Do not use "I'm sorry, but" to answer any questions.

<glossaries>
{vocabulary}
</glossaries>

<mapping_table>
{mappings}
</mapping_table>

Here is the original content:
<content>
{content}
</content>

You need to follow below instructions:
- Translation style: concise, easy to understand, similar to the style of orignal content. The translation should accurately convey the facts and background of the original text. Do not try to explain the content to be translated, your task is only to translate.
- Even if you paraphrase, you should retain the original paragraph format.
- For the terms in <glossaries>, you should keep them as original. 
- You should refer the term vocabulary correspondence table which is provided between <mapping_table> and </mapping_table>. 

Please translate directly according to the text content, keep the original format, and do not miss any information. Put the result in <translation>"""

    multilingual_term_mapping = retriever.search_aos_for_terminology(src_content, doc_type='multilingual_terminology')
    crosslingual_terms = retriever.search_aos_for_terminology(src_content, doc_type='crosslingual_terminology')

    def build_glossaries(term, entity_type):
        obj = {"term":term, "entity_type":entity_type}
        return json.dumps(obj, ensure_ascii=False)

    vocabulary_prompt_list = [ build_glossaries(item['content'], item['doc_category']) for item in crosslingual_terms ]
    vocabulary_prompt = "\n".join(vocabulary_prompt_list)

    def build_mapping(src_lang, dest_lang, mapping_json, entity_type):

        obj = json.loads(mapping_json)
        src_term = obj.get(src_lang, None)
        target_term = obj.get(dest_lang, None)
        entity_tag = f"[{entity_type}] "
        if src_term and target_term and entity_type:
            return f"{entity_tag}{src_term}=>{target_term}"
        else:
            return None

    term_mapping_list = list(set([ build_mapping(src_lang, dest_lang, item['content'], item['doc_category']) for item in multilingual_term_mapping ]))
    term_mapping_prompt = "\n".join([ item for item in term_mapping_list if item is not None ])

    prompt = pe_template.format(src_lang=src_lang, dest_lang=dest_lang, vocabulary=vocabulary_prompt, mappings=term_mapping_prompt, content = src_content)
    return prompt

def load_content_json_from_s3(bucket, object_key):
    if object_key.endswith('.json'):
        obj = s3.Object(bucket, object_key)
        file_content = obj.get()['Body'].read().decode('utf-8', errors='ignore').strip()
    else:
        raise RuntimeError("Invalid S3 File Format")
        
    return file_content

def invoke_bedrock(model_id, prompt, max_tokens=4096, prefill_str='<translation>', stop=['</translation>']):

    messages = [
        {"role": "user", "content": prompt},
        {"role": "assistant", "content": prefill_str}
    ]

    body = json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "messages": messages,
                "max_tokens": max_tokens,
                "stop_sequences" : stop,
                "top_p": 0.5,
                "top_k": 50,
                "temperature": 0.1
            }
        )
    max_retries = 2
    retry_count = 0

    while retry_count < max_retries:
        try:
            response = bedrock.invoke_model(body=body, modelId=model_id)
            rep_obj = json.loads(response['body'].read().decode('utf8'))
            return rep_obj['content'][0]['text']
        except Exception as e:
            retry_count += 1
            print(f"Attempt {retry_count} failed: {e}")
            if retry_count == max_retries:
                print("Maximum retries reached. Operation failed.")
            else:
                print(f"Retrying in 1 seconds... (attempt {retry_count + 1})")
                time.sleep(1)

    return None

def translate_by_llm(file_content, model_id):
    # {
    #     "src_lang" : "EN",
    #     "dest_lang" : "CN",
    #     "src_content": [
    #         "I am good at SageMaker",
    #         "I hate CHANEL"
    #     ]
    # }
    json_obj = json.loads(file_content)
    src_lang = json_obj['src_lang']
    dest_lang = json_obj['dest_lang']
    src_content_list = json_obj['src_content']

    retriever = TerminologyRetriever.from_endpoints(AOS_ENDPOINT, AOS_INDEX)

    dest_content_list = []
    for content in src_content_list:
        prompt = construct_translate_prompt(content, src_lang, dest_lang, retriever)
        print("prompt:")
        print(prompt)

        result = invoke_bedrock(model_id, prompt)
        dest_content_list.append(result)

    json_obj["dest_content"] = dest_content_list
    return json_obj

def get_output_path_from_objectkey(object_key):
    paths = object_key.split('/')
    root = '/'.join(paths[:-1])
    file_name = paths[-1]
    return f"{root}/translation/{file_name}".strip('/')

def translate_file(bucket, object_key):
    print(f"start translating of {object_key}")

    file_content = load_content_json_from_s3(bucket, object_key)
    json_obj_with_translation = translate_by_llm(file_content, model_id)
    text_with_translation = json.dumps(json_obj_with_translation, ensure_ascii=False)

    output_key = get_output_path_from_objectkey(object_key)

    print(f"text_with_translation: {text_with_translation}")
    print(f"output_key: {output_key}")

    bucket = s3.Bucket(bucket)

    bucket.put_object(Key=output_key, Body=text_with_translation.encode('utf-8'))

    print(f"finish translation of {object_key}")

if __name__ == '__main__':
    for s3_key in object_key.split(','):
        s3_key = urllib.parse.unquote(s3_key) ##In case Chinese filename
        s3_key = s3_key.replace('+',' ') ##replace the '+' with space. ps:if the original file name contains space, then s3 notification will replace it with '+'.
        translate_file(bucket, s3_key)