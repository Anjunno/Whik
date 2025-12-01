import json
import os
from neo4j import GraphDatabase, exceptions

# --- 환경 변수 설정 ---
URI = os.environ.get('NEO4J_URI') 
USER = os.environ.get('NEO4J_USER')
PASSWORD = os.environ.get('NEO4J_PASSWORD')

# 공통 CORS 헤더 정의
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "OPTIONS,GET,POST",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,uuid",
    "Content-Type": "application/json"
}


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
            print(f"Failed to create Neo4j driver: {e}")
            raise

# 그래프 조회 쿼리 (Contextual Count 및 Graph Data 획득)
def get_user_learning_graph(tx, user_id, target_lang):
    """
    특정 사용자와 언어에 대한 학습망을 조회하고, Scenario를 기반으로 횟수를 계산합니다.
    """
    
    word_metrics_query = f"""
        MATCH (u:User {{id: $userId}})
              -[:STUDYING]-> (l:Language {{code: $targetLang}}) 
              <-[:BELONGS_TO_LANGUAGE]- (w_main:Word)
              
        // w_main이 해당 언어 컨텍스트에서 연결된 모든 시나리오(s_all)를 찾습니다.
        MATCH (s_all:Scenario {{language: $targetLang}})
              WHERE (s_all)-[:FOCUS_ON]->(w_main) OR 
                    (s_all)-[:FOCUS_ON]->()-[:RELATED_TO]->(w_main)
        
        MATCH (u)-[:PERFORMED]->(s_all) 
        
        // u와 l 변수를 Contextual 계산 후에도 유지하도록 WITH 절에 명시
        WITH u, l, w_main, 
             COUNT(s_all) AS ContextualStudyCount, 
             MAX(s_all.createdAtTs) AS ContextualLastStudied

        // Optional: Get related words for the graph visualization
        // (r_related 관계의 존재 여부만 확인)
        OPTIONAL MATCH (w_main)-[r_related:RELATED_TO {{targetLang: $targetLang}}]-(w_rel:Word)

        // 최종 RETURN: 모든 노드와 계산된 메트릭을 반환합니다.
        RETURN u, l, w_main, w_rel, ContextualStudyCount, ContextualLastStudied, r_related
    """
    
    result_stream = tx.run(word_metrics_query, userId=user_id, targetLang=target_lang)

    # --- 결과 처리 및 JSON 변환 로직 ---
    nodes_map = {}
    edges_set = set() # 엣지 중복 방지용: (source, destination, text) 튜플 저장
    
    # 1. User, Language 노드 추가
    user_id_norm = f"u_{user_id}"
    lang_id_norm = f"lang_{target_lang}"
    
    nodes_map[user_id_norm] = {"id": user_id_norm, "label": "User", "text": user_id, "size": 25, "color": "0xFF00BFFF"}
    nodes_map[lang_id_norm] = {"id": lang_id_norm, "label": "Language", "text": target_lang.upper(), "size": 20, "color": "0xFF00FF00"}
    
    # STUDYING 관계 추가
    edges_set.add((user_id_norm, lang_id_norm, "STUDYING"))

    # 2. 결과 레코드 순회하며 Word 노드 및 관계 추가
    for record in result_stream:
        w_main = record['w_main']
        w_rel = record.get('w_rel')
        study_count = record['ContextualStudyCount']
        last_ts = record['ContextualLastStudied']
        
        w_main_id_norm = f"w_{w_main['name']}"

        # Word 노드 추가/업데이트 (w_main: 숙련도 반영)
        if w_main_id_norm not in nodes_map or nodes_map[w_main_id_norm].get('count', 0) < study_count:
            nodes_map[w_main_id_norm] = {
                "id": w_main_id_norm,
                "label": "Word",
                "text": w_main['name'],
                "size": 20 + min(study_count * 5, 50), 
                "color": "0xFFF0E68C",
                "count": study_count,
                "lastTs": last_ts
            }
        
        # BELONGS_TO_LANGUAGE 엣지 추가 (w_main -> lang)
        edges_set.add((w_main_id_norm, lang_id_norm, "BELONGS_TO_LANGUAGE"))

        # RELATED_TO 엣지 및 w_rel 노드 추가 (w_rel이 존재할 경우)
        if w_rel:
            w_rel_id_norm = f"w_{w_rel['name']}"
            
            # w_rel 노드가 nodes_map에 없으면 기본값으로 추가
            if w_rel_id_norm not in nodes_map:
                # Note: w_rel은 현재 레코드에서는 숙련도가 계산되지 않았으므로 기본 크기 사용
                nodes_map[w_rel_id_norm] = {"id": w_rel_id_norm, "label": "Word", "text": w_rel['name'], "size": 20, "color": "0xFFF0E68C"}
            
            # RELATED_TO 엣지 추가 (단방향 중복 방지)
            related_edge_key = (w_main_id_norm, w_rel_id_norm, "RELATED_TO")

            # 엣지 중복 방지: RELATED_TO 엣지 추가
            # 단방향으로 저장되었으므로, 이 순서대로만 set에 추가하여 중복을 막음.
            edges_set.add(related_edge_key)
                 
            # BELONGS_TO_LANGUAGE 엣지 추가 (w_rel -> lang)
            edges_set.add((w_rel_id_norm, lang_id_norm, "BELONGS_TO_LANGUAGE"))


    # 3. 최종 JSON 구조로 변환
    # Set에 저장된 엣지 튜플을 JSON 형식으로 변환 (중복 없음 보장)
    final_edges = [{"source": s, "destination": d, "text": t} for s, d, t in edges_set]

    return {"nodes": list(nodes_map.values()), "edges": final_edges}

# --- Lambda 핸들러 ---
def lambda_handler(event, context):
    
    # 1️. 입력 값 검증 및 정규화
    user_id = (event.get("requestContext", {}).get("authorizer") or {}).get("user_uuid")
    if not user_id:
        return {"statusCode": 403, "body": json.dumps({"error": "Unauthorized: missing user_uuid"}), "headers": CORS_HEADERS}

    targetLanguage = (event.get("pathParameters") or {}).get("targetLanguage")
    if not targetLanguage:
        return {"statusCode": 400, "body": json.dumps({"error": "targetLanguage is required"}), "headers": CORS_HEADERS}
        
    # 'jp'를 'ja'로 표준화 및 소문자 통일
    targetLanguage = targetLanguage.lower()
    if targetLanguage == "jp":
        targetLanguage = "ja"

    try:
        init_driver()
        
        with driver.session() as session:
            # 3️. 그래프 데이터 조회 및 JSON 구조로 변환
            graph_data = session.execute_read(get_user_learning_graph, user_id, targetLanguage)

        return {
            "statusCode": 200,
            "body": json.dumps(graph_data, ensure_ascii=False),
            "headers": CORS_HEADERS,
        }

    except exceptions.ServiceUnavailable as e:
        print(f"Neo4j Service Unavailable: {e}")
        return {"statusCode": 503, "body": json.dumps({"error": "Database service unavailable"}), "headers": CORS_HEADERS}
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": "Internal Server Error", "detail": str(e)}), "headers": CORS_HEADERS}