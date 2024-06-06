# opensearch-rag

### 适用场景
内容审核，除了审核要求的文字描述以外，还存在一些人审的判例数据，通过搜索召回这些判例，可以供LLM来把握内容审核的尺度。

### 实现方式
整个方案分离线和在线两部分，离线部分主要是人审判例的摄入，通过Glue Job进行调度。在线部分为一个lambda，构建OpenSearch Client对判例进行查询，构建Prompt后通过bedrock调用LLM进行内容审核。

### 在线调用方法参考
```python
import boto3

lambda_client = boto3.client('lambda', region)
function_name = 'rag_moderator'
payload = {'model_id': model_id, 'text': content, 'type':'motto'}

# 调用Lambda函数
response = lambda_client.invoke(
    FunctionName=function_name,
    InvocationType='RequestResponse',  # 同步调用
    Payload=bytes(json.dumps(payload), encoding='utf-8')
)

payload = response['Payload'].read()
result = json.loads(payload)
print(result)
```

### 输出参考
```json
{
  "content": ".....",
  "result": "reject",
  "category": "...",
  "explanation": "The nickname \"...\" contains a reference to private body parts, which falls under the \"Low\" level policy for .....",
  "confidence": "5"
}
```

### 部署文档
[飞书文档](https://amzn-chn.feishu.cn/docx/LJ93deoBNoeZ4bxvjwGcz2lpn9c?from=from_copylink)


### 其他
1. 判例数据格式可以参考 ./docs/samples/moderation_examples.json