import json
import os
import sys
from neo4j import GraphDatabase, exceptions
from decimal import Decimal

# --- 환경 변수 설정 ---
URI = os.environ.get('NEO4J_URI')
USER = os.environ.get('NEO4J_USER')
PASSWORD = os.environ.get('NEO4J_PASSWORD')

# Global Driver (Lambda Warm Start 시 재사용)
driver = None

# Neo4j 연결 초기화 함수
def init_driver():
    global driver
    if not driver:
        if not all([URI, USER, PASSWORD]):
            raise EnvironmentError("Neo4j connection environment variables are missing.")
        try:
            driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD), max_connection_lifetime=300)
            driver.verify_connectivity()
            print("Neo4j driver initialized successfully.")
        except Exception as e:
            print(f"Failed to create Neo4j driver or verify connectivity: {e}")
            raise

# --- 최종 Cypher 통합 쿼리 ---
CYPHER_QUERY = """
    MERGE (lang:Language {code: $targetLanguage})
    MERGE (u:User {id: $userId})
    MERGE (u)-[:STUDYING]->(lang)

    MERGE (s:Scenario {id: $scenarioId, language: $targetLanguage})
    ON CREATE SET s.createdAtTs = $createdAtTs, s.createdAtIso = $createdAtIso

    MERGE (w_ko_main:Word {name: $originalWord, lang: 'ko'})
    MERGE (w_ko_main)-[:BELONGS_TO_LANGUAGE]->(lang)

    MERGE (u)-[r_main:STUDIED]->(w_ko_main)
    ON CREATE SET r_main.count = 1, r_main.last_studied = $createdAtTs
    ON MATCH SET r_main.count = r_main.count + 1, r_main.last_studied = $createdAtTs

    MERGE (u)-[:PERFORMED]->(s)
    MERGE (s)-[:FOCUS_ON]->(w_ko_main)

    WITH w_ko_main, lang, u, s, $relatedWords AS relatedWordsList, $createdAtTs AS ts

    UNWIND relatedWordsList AS related_word
    MERGE (w_rel:Word {name: related_word, lang: 'ko'})
    MERGE (w_rel)-[:BELONGS_TO_LANGUAGE]->(lang)

    MERGE (u)-[r_rel:STUDIED]->(w_rel)
    ON CREATE SET r_rel.count = 1, r_rel.last_studied = ts
    ON MATCH SET r_rel.count = r_rel.count + 1, r_rel.last_studied = ts

    MERGE (w_ko_main)-[:RELATED_TO {targetLang: lang.code}]->(w_rel)
    RETURN 'Data Ingestion Complete (Optimized).' AS Status
"""

def execute_cypher_transaction(tx, params):
    tx.run(CYPHER_QUERY, params)

def lambda_handler(event, context):
    try:
        init_driver()
    except Exception as e:
        print(f"Driver initialization failed: {e}")
        raise

    for record in event.get('Records', []):
        try:
            # 1. SQS 메시지 바디 추출 및 JSON 디코딩
            message_body_json_string = record.get('body')
            data = json.loads(message_body_json_string)

            # 💡 2. Cypher 쿼리에 필요한 매개변수 준비 (수정된 부분)
            related_words_dict = data.get('relatedWords_KR', {}) # 
            related_words_list = list(related_words_dict.values()) # 

            params = {
                'userId': data['userId'],
                'scenarioId': data['scenarioId'],
                'createdAtTs': int(data['createdAtTs']),
                'createdAtIso': data['createdAtIso'],
                'targetLanguage': data['targetLanguage'],
                'originalWord': data['originalWord'],
                'relatedWords': related_words_list, 
            }

            # 3. Neo4j 트랜잭션 실행
            with driver.session() as session:
                session.execute_write(execute_cypher_transaction, params)

            print(f"Successfully processed message {record.get('messageId')} for user {params['userId']}")

        except exceptions.ServiceUnavailable as e:
            print(f"Neo4j Service Unavailable: {e}. Message will be retried.")
            raise e
        except Exception as e:
            print(f"Failed to process message {record.get('messageId')}. Error: {e}")
            # 개발 중에는 상세 오류 확인을 위해 raise e 유지, 운영 시에는 필요에 따라 조절
            raise e

    return {'statusCode': 200}