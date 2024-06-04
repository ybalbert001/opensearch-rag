import boto3
import os
import logging
import json
import re

from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, helpers

logger = logging.getLogger()
logger.setLevel(logging.INFO)

aos_endpoint = os.environ.get("aos_endpoint")
aos_index = os.environ.get("aos_index")
region = os.environ.get("region", 'us-west-2')

boto3_bedrock = boto3.client(
    service_name="bedrock-runtime",
    region_name=region
)
credentials = boto3.Session().get_credentials()
auth = AWSV4SignerAuth(credentials, region)
aos_client = None

class APIException(Exception):
    def __init__(self, message, code: str = None):
        if code:
            super().__init__("[{}] {}".format(code, message))
        else:
            super().__init__(message)

def handle_error(func):
    """Decorator for exception handling"""

    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except APIException as e:
            logger.exception(e)
            raise e
        except Exception as e:
            logger.exception(e)
            raise RuntimeError(
                "Unknown exception, please check Lambda log for more details"
            )

    return wrapper

def get_aos_client(aos_endpoint):
    global aos_client, auth
    if not aos_client:
        assert aos_endpoint
        aos_client = OpenSearch(
            hosts = [{'host': aos_endpoint, 'port': 443}],
            http_auth = auth,
            use_ssl = True,
            verify_certs = True,
            connection_class = RequestsHttpConnection,
            timeout = 60, # 默认超时时间是10 秒，
            max_retries=5, # 重试次数
            retry_on_timeout=True
        )

    return aos_client

def moderate_by_llm(model_id, system_prompt, instruct_prompt, content):
    messages = [ 
        {"role":"user", "content" : instruct_prompt },
        {"role":"assistant", "content": f"<moderation><content>{content}</content><result>"}
    ]

    input_body = {}
    input_body["anthropic_version"] = "bedrock-2023-05-31"
    input_body["messages"] = messages
    input_body["system"] = system_prompt
    input_body["max_tokens"] = 4096
    input_body["stop_sequences"] = ['</moderation>']

    body = json.dumps(input_body)

    request_options = {
        "body": body,
        "modelId": model_id,
        "accept": "application/json",
        "contentType": "application/json",
    }

    response = boto3_bedrock.invoke_model(**request_options)

    body = response.get('body').read().decode('utf-8')

    body_dict = json.loads(body)

    output = body_dict['content'][0].get("text")

    return f"<content>{content}</content><result>" + output

def retrieve_from_aos(aos_index, aos_endpoint, text, text_type):
    retrieve_client = get_aos_client(aos_endpoint)

    white_query = {
        "query" : {
            "bool": {
                "must": [
                    {
                      "match": { "doc": text }
                    }
                ],
                "filter": [
                    {
                      "term": {
                        "doc_title": f"moderation_{text_type}_white"
                      }
                    },
                    {
                      "match": {
                        "doc_category": "Whitelist"
                      }
                    }
                ]
            }
        },
        "size" : 5,
        "_source": [
            "doc",
            "content",
            "doc_title",
            "doc_category"
        ]
    }

    {'took': 2, 'timed_out': False, '_shards': {'total': 5, 'successful': 5, 'skipped': 0, 'failed': 0}, 'hits': {'total': {'value': 0, 'relation': 'eq'}, 'max_score': None, 'hits': []}}

    white_response = retrieve_client.search(
        body=white_query,
        index=aos_index
    )

    # 如果没有命中
    if not white_response['hits']['hits']:
        white_query = {
            "query" : {
                "bool": {
                    "must": [
                        {
                          "match_all": {}
                        }
                    ],
                    "filter": [
                        {
                          "term": {
                            "doc_title": f"moderation_{text_type}_white"
                          }
                        },
                        {
                          "match": {
                            "doc_category": "Whitelist"
                          }
                        }
                    ]
                }
            },
            "size" : 5,
            "_source": [
                "doc",
                "content",
                "doc_title",
                "doc_category"
            ]
        }
        white_response = retrieve_client.search(
            body=white_query,
            index=aos_index
        )


    black_query = {
        "query" : {
            "bool": {
                "must": [
                    {
                      "match": { "doc": text }
                    }
                ],
                "filter": [
                    {
                      "term": {
                        "doc_title": f"moderation_{text_type}_black"
                      }
                    },
                    {
                      "match": {
                        "doc_category": "Blacklist"
                      }
                    }
                ]
            }
        },
        "size" : 5,
        "_source": [
            "doc",
            "content",
            "doc_title",
            "doc_category"
        ]
    }

    black_response = retrieve_client.search(
        body=black_query,
        index=aos_index
    )

    if not black_response['hits']['hits']:
        black_query = {
            "query" : {
                "bool": {
                    "must": [
                        {
                          "match_all": {}
                        }
                    ],
                    "filter": [
                        {
                          "term": {
                            "doc_title": f"moderation_{text_type}_black"
                          }
                        },
                        {
                          "match": {
                            "doc_category": "Blacklist"
                          }
                        }
                    ]
                }
            },
            "size" : 5,
            "_source": [
                "doc",
                "content",
                "doc_title",
                "doc_category"
            ]
        }

        black_response = retrieve_client.search(
            body=black_query,
            index=aos_index
        )

    return white_response, black_response

def build_moderate_prompt(white_examples, black_examples, content):

    def reformat(aos_result, result):
        hits = aos_result['hits']['hits']
        exmples = []
        for hit in hits:
            content = hit['_source']['doc']
            explanation = hit['_source']['content']
            exmples.append(f"<moderation><content>{content}</content><result>{result}</result><explanation>{explanation}</explanation></moderation>")

        return "\n".join(exmples)

    white_examples_part = reformat(white_examples, "pass")
    black_examples_part = reformat(black_examples, "reject")

    system_prompt = """You are a content moderation assistant trained to detect inappropriate content from user. """
        
    instruct_prompt = f"""Your task is to Identify and classify any inappropriate content in the given nick_names according to below policies

    <policies>
    # Account Trade:
    Low: Asking for account services, mentioning account trade experiences, promoting account giveaways/exchanges.
    Middle: Providing account trading or account services for currency.

    # Scams & Advertisements:
    Middle: Promoting phishing websites, free primogems, hack services, refund services, or advertisements with purchase links/contact info.

    # Information Leakage:
    Low: Mentioning information leakage events, blacklisted websites.
    Middle: Discussing or sharing leaked information.

    # Verbal Abuse & Threats:
    Low: Offensive speech, personal attacks, hate speech towards subjects.
    Middle: Personal attacks, hate speech, threats towards individuals/groups.

    # Sensitivity & Nudity:
    Low: Mentioning private parts, sexual activities, asking for sexual services, topics related to LGBT(gay, lesbian, etc)
    Low example:
    1. I am Gay
    2. So Gay
    3. Dick
    Middle: Providing or promoting sexual services and websites
    High: Any Discrimination against LGBT

    # Minor Safety:
    Low: Grooming behavior, normalizing pedophilia, admitting romantic relationships with minors.
    Middle: Sexual content involving minors.
    High: Providing sexual services involving minors, revealing account owner is under 13.

    # Illegal Activities:
    Low: Simply mentioning illegal goods/activities, temptation of committing crimes, fake news, conspiracy theories.
    Low example:
    1. yeah i know weed is better than tobacco
    2. have you ever try coccaine?
    High: Actually promoting illegal goods/activities, glorifying illegal activities.
    High example:
    1. Weeds on sale
    2. Guns on sale, visit this page

    # Religious and Political Content:
    High: Discussing REAL WORLD (NOT IN GAME) war conflicts, territorial disputes, military installations, diplomatic relationships, political events, entities, desecrating symbols, persuading beliefs, political statements, support/opposition, controversial flags/slogans, Chinese politics.

    # Personal Identifiable Information:
    Low: Depicting personal information like phone numbers, addresses, IDs, bank accounts.

    # Violent Extremism:
    Low: Mentioning extremisms, hate ideologies like terrorism, white supremacy, Nazism.
    High: Promoting extremisms, hate ideologies.

    # Suicide & Self-Harm:
    Low: Texts depicting trustworthy suicidal tendency. 
    Low example:
    1. I'm so tired, I want to kill myself
    High: Texts mentioning or promoting suicide challenges, such as momo challenge and blue whale challenge
    </policies>

    You can also refer below precedents in Whitelist and Blacklist:
    <Whitelist>
    {white_examples_part}
    </Whitelist>

    <Blacklist>
    {black_examples_part}
    </Blacklist>

    The content within the <content> tag below is pending review. 
    <content>{content}<content>

    Please provide the result of content moderation, and output between <moderation> tag."""  

    return system_prompt, instruct_prompt

def extract_tag_content(xml_string):
    tag_contents = {}
    pattern = r'<(\w+)>(.*?)</\1>'
    matches = re.findall(pattern, xml_string, re.DOTALL)
    for match in matches:
        tag_name, content = match
        tag_contents[tag_name] = content
    return tag_contents

# 对文本进行分词
@handle_error
def lambda_handler(event, context):

    model_id = event.get('model_id')
    text = event.get('text')
    text_type = event.get('type') # it could be 'nickname' and 'motto'

    white_response, black_response = retrieve_from_aos(aos_index, aos_endpoint, text, text_type)
    system_prompt, instruct_prompt = build_moderate_prompt(white_response, black_response, text)

    xml_output = moderate_by_llm(model_id, system_prompt, instruct_prompt, text)

    result = extract_tag_content(xml_output)

    if not result:
        print("xml_output:")
        print(xml_output)

    return result

