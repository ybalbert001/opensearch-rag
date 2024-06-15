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

    print("instruct_prompt:")
    print(instruct_prompt)

    messages = [ 
        {"role":"user", "content" : instruct_prompt },
        {"role":"assistant", "content": f"<moderation><content>{content}</content><explanation>"}
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

    return f"<content>{content}</content><explanation>" + output

def retrieve_from_aos(aos_index, aos_endpoint, text, text_type):
    retrieve_client = get_aos_client(aos_endpoint)

    # { "content": content, "category" : category, "reason": reason, "assessment" : content_type, "lang" : lang, "doc_title": file_name}
    white_query = {
        "query" : {
            "bool": {
                "must": [
                    {
                      "match": { "content": text }
                    }
                ],
                "filter": [
                    {
                      "term": {
                        "content_type": text_type
                      }
                    },
                    {
                      "match": {
                        "assessment": "Whitelist"
                      }
                    }
                ]
            }
        },
        "size" : 5,
        "_source": [
            "content",
            "category",
            "reason"
        ]
    }

    white_response = retrieve_client.search(
        body=white_query,
        index=aos_index
    )

    default_white_response = {}
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
                            "content_type": text_type
                          }
                        },
                        {
                          "match": {
                            "assessment": "Whitelist"
                          }
                        }
                    ]
                }
            },
            "size" : 5,
            "_source": [
                "content",
                "category",
                "reason"
            ]
        }
        default_white_response = retrieve_client.search(
            body=white_query,
            index=aos_index
        )

    black_query = {
        "query" : {
            "bool": {
                "must": [
                    {
                      "match": { "content": text }
                    }
                ],
                "filter": [
                    {
                      "term": {
                        "content_type": text_type
                      }
                    },
                    {
                      "match": {
                        "assessment": "Blacklist"
                      }
                    }
                ]
            }
        },
        "size" : 5,
        "_source": [
            "content",
            "category",
            "reason"
        ]
    }

    black_response = retrieve_client.search(
        body=black_query,
        index=aos_index
    )

    default_black_response = {}
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
                            "content_type": text_type
                          }
                        },
                        {
                          "match": {
                            "assessment": "Blacklist"
                          }
                        }
                    ]
                }
            },
            "size" : 5,
            "_source": [
                "content",
                "category",
                "reason"
            ]
        }

        default_black_response = retrieve_client.search(
            body=black_query,
            index=aos_index
        )

    return white_response, black_response, default_white_response, default_black_response

def build_moderate_prompt(white_examples, black_examples, content):

    def reformat(aos_result, result):
        hits = aos_result['hits']['hits']
        exmples = []
        for hit in hits:
            content = hit['_source']['content']
            explanation = hit['_source']['reason']
            category = hit['_source']['category']
            exmples.append(f"<moderation><content>{content}</content><explanation>{explanation}</explanation><result>{result}</result><category>{category}</category><confidence>...</confidence></moderation>")

        return "\n".join(exmples)

    white_examples_part = reformat(white_examples, "pass")
    black_examples_part = reformat(black_examples, "reject")

    system_prompt = """You are a content moderation assistant of Mihoyo, you are trained to detect inappropriate content from user. """
        
    instruct_prompt = f"""Your task is to Identify and classify any inappropriate content in the given motto according to below policies and precedents.

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

    <Precedents>
    <Whitelist>
    {white_examples_part}
    </Whitelist>

    <Blacklist>
    {black_examples_part}
    </Blacklist>
    </Precedents>

    Please remember below requirements:
    1. You are facing the Mihoyo game scenario, so some combat and weapon-related terms are normal. 
    2. Please follow the output format in <Precedents>, give the explanation at first and then output the result, category and confidence.
    3. The <confidence> value is with a score range of 1-5. The less confident you are about judging the content, the lower the score you should give.
    4. There are only two options(reject, pass) for your result.
    5. If the content doesn't directly 

    Below is the content which is pending review. 
    <content>{content}<content>"""  


    system_prompt_v2 = """You are a content moderation assistant responsible for reviewing text content to ensure they comply with community guidelines."""
    instruct_prompt_v2 = f"""<guidelines>
1. Hate speech, discrimination, or bias against protected groups (e.g., race, religion, gender, sexual orientation) is not allowed.
2. Explicit or graphic violence, gore, or illegal activities should not be allowed. To express negative emotions like dissatisfaction, anger, and frustration, the use of violent and insult words and expressions is tolerated.
3. Sexually explicit or pornographic content is not permitted while implicit, suggestive sexual content is tolerated
4. Some national swear words, such as '他妈的', 'fucking', 'damn', etc., should not be considered profanity, vulgarity, or obscene language.
5. Harassment, bullying, or personal attacks towards individuals or groups are not allowed. 
6. Misinformation, conspiracy theories, or harmful pseudoscience should be flagged.
7. Content that promotes self-harm, eating disorders, or dangerous behaviors is not allowed.
8. Spam, phishing attempts, or unauthorized commercial content should be removed.
</guidelines>

Here are some examples to help you understand the guidelines:

<examples>
<Whitelist>
{white_examples_part}
</Whitelist>

<Blacklist>
{black_examples_part}
</Blacklist>
</examples>

To complete your task, please follow these steps:

1. Review the given text input carefully.
2. Identify any content that violates the guidelines mentioned above.
3. Please follow the output format in <examples>, give the explanation at first and then output the result, category and confidence. 
4. If the content is appropriate and complies with all guidelines, mark the Result as "pass".
5. If the content is risky, potentially violates guidelines, or you are unsure, mark the Result as "review".
6. If the content explicitly violates any guidelines, mark the Result as "reject" and specify the violated Category.

Here is the content:
<content>{content}</content>
"""

    instruct_prompt_v3 = f"""<guidelines>
1. Hate speech, discrimination, or bias against protected groups (e.g., race, religion, gender, sexual orientation) is not allowed.
2. For expressing negative strong negative sentiment, like dissatisfaction, anger, and frustration, the use of violent/insult/vulgar words is tolerated. But cruel, illegal criminal vocabulary is not be allowed.
3. Sexually explicit or pornographic content is not permitted while implicit, suggestive sexual content is tolerated
4. Some national swear words, such as '他妈的', 'fucking', 'damn', etc., should not be considered profanity, vulgarity, or obscene language.
5. Harassment, bullying, or personal attacks towards individuals or groups are not allowed. Due to the gaming context at mihoyo, words related to fighting, killing, or weapons are considered normal/acceptable.
6. Misinformation, conspiracy theories, or harmful pseudoscience should be flagged.
7. Content that promotes self-harm, eating disorders, or dangerous behaviors is not allowed.
8. Spam, phishing attempts, or unauthorized commercial content should be removed.
</guidelines>

Here are some examples to help you understand the guidelines:

<examples>
<Whitelist>
{white_examples_part}
</Whitelist>

<Blacklist>
{black_examples_part}
</Blacklist>
</examples>

To complete your task, please follow these steps:

1. Review the given text input carefully.
2. Identify any content that violates the guidelines mentioned above.
3. Please follow the output format in <examples>, give the explanation at first and then output the result, category and confidence. 
4. If the content is appropriate and complies with all guidelines, mark the Result as "pass".
5. If the content is ambiguous, potentially violates guidelines, or you are unsure, mark the Result as "review".
6. If the content explicitly violates any guidelines, mark the Result as "reject" and specify the violated Category.

Here is the content:
<content>{content}</content>
"""

#2. For expressing negative emotions like dissatisfaction, anger, and frustration, the use of violent and insult words and expressions is tolerated. But cruel, illegal criminal vocabulary is not be allowed.
#2. For expressing negative strong negative sentiment, like dissatisfaction, anger, and frustration, the use of violent/insult/vulgar words is tolerated. But cruel, illegal criminal vocabulary is not be allowed.

#2. Due to the gaming context at mihoyo, words related to fighting, killing, or weapons are considered normal/acceptable.


#1. Review the given text input carefully. 如果内容意思不明确，请采取保守的态度
    system_prompt_v4 = """You are a content moderation assistant responsible for reviewing text content to ensure they are not against community policies."""
    instruct_prompt_v4 = f"""Here is the policies of moderation:

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
Note that additionally: 
1. You are facing the Mihoyo game scenario, so some combat, weapon and game-related terms should be tolerated.
2. For expressing negative strong negative sentiment to game/game experience, like dissatisfaction, anger, and frustration, the use of violent/insult/vulgar words should be tolerated.
3. Some national swear words, such as '他妈的', 'fucking', 'damn', etc., should be tolerated.

# Sensitivity & Nudity:
Low: Mentioning private parts, sexual activities, asking for sexual services, insult words or discrimination to LGBT(gay, lesbian, etc)
Middle: Providing or promoting sexual services and websites
High: Any Discrimination against LGBT
Note that additionally: 
1. Implicit, suggestive or ambiguous sexual contents should be tolerated.

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

Here are some examples to help you understand the policies:

<examples>
<Whitelist>
{white_examples_part}
</Whitelist>

<Blacklist>
{black_examples_part}
</Blacklist>
</examples>

To complete your task, please follow these steps:

1. Review the given text input carefully. 
2. Categorize and rate the text content(low/medium/high) based on the provided policy rules.
3. If the content is appropriate, mark the Result as "pass".
4. If the content is ambiguous, or you are unsure, mark the Result as "review".
5. If the risk level of the content is low, and recongized as mild which could be tolerated in the policies, mark the Result as "review".
6. If the content is explicitly high risky, mark the Result as "reject" and specify the violated Category.
7. Please follow the output format in <examples>, give the explanation at first and then output the result, category and confidence(score range:1-5). 

Below is the content which is pending review. 
<content>{content}<content>"""

    return system_prompt_v4, instruct_prompt_v4

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

    query_white_response, query_black_response, default_white_response, default_black_response = retrieve_from_aos(aos_index, aos_endpoint, text, text_type)
    white_response_cnt = len(query_white_response['hits']['hits'])
    black_response_cnt = len(query_black_response['hits']['hits'])

    white_response = query_white_response if white_response_cnt else default_white_response
    black_response = query_black_response if black_response_cnt else default_black_response

    system_prompt, instruct_prompt = build_moderate_prompt(white_response, black_response, text)

    xml_output = moderate_by_llm(model_id, system_prompt, instruct_prompt, text)
    result = extract_tag_content(xml_output)

    print("xml_output:")
    print(xml_output)

    result["white_response_cnt"] = white_response_cnt
    result["black_response_cnt"] = black_response_cnt

    return result

