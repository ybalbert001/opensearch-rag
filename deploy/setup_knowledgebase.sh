#!/bin/bash

OPENSEARCH_ENDPOINT="$1"
DIMENSION="${2:-1024}"

payload1="{
    \"settings\" : {
        \"index\":{
            \"number_of_shards\" : 1,
            \"number_of_replicas\" : 0,
            \"knn\": \"true\",
            \"knn.algo_param.ef_search\": 32
        }
    },
    \"mappings\": {
        \"properties\": {
            \"content\": {
                \"type\": \"text\",
                \"analyzer\": \"standard\",
                \"search_analyzer\": \"ik_smart\"
            },
            \"content_type\": {
                \"type\": \"keyword\"
            },
            \"category\": {
                \"type\": \"keyword\"
            },
            \"reason\": {
                \"type\": \"keyword\"
            },
            \"assessment\": {
                \"type\": \"keyword\"
            },
            \"lang\": {
                \"type\": \"keyword\"
            },
            \"doc_title\": {
                \"type\": \"keyword\"
            },
            \"embedding\": {
                \"type\": \"knn_vector\",
                \"dimension\": ${DIMENSION},
                \"method\": {
                    \"name\": \"hnsw\",
                    \"space_type\": \"cosinesimil\",
                    \"engine\": \"nmslib\",
                    \"parameters\": {
                        \"ef_construction\": 128,
                        \"m\": 16
                    }
                }            
            }
        }
    }
}"

# 创建chatbot-index索引
#echo $payload1 
echo "delete existed index[rag-data-index] of opensearch."
curl -XDELETE "$OPENSEARCH_ENDPOINT/rag-data-index" -H "Content-Type: application/json"
echo 
echo "create new index[rag-data-index] of opensearch"
curl -XPUT "$OPENSEARCH_ENDPOINT/rag-data-index" -H "Content-Type: application/json" -d "$payload1"