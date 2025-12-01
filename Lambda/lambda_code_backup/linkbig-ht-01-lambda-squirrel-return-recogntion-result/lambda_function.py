import json
import boto3
import os

# DynamoDB 테이블 설정
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("TABLE_NAME")
table = dynamodb.Table(TABLE_NAME)

# 공통 CORS 헤더 정의
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    # "Access-Control-Allow-Methods": "OPTIONS,GET,POST",
    # "Access-Control-Allow-Headers": "Content-Type,Authorization,uuid",
    "Content-Type": "application/json"
}

def lambda_handler(event, context):
        # OPTIONS (preflight) 요청 처리
    # if event.get("httpMethod") == "OPTIONS":
    #     return {
    #         "statusCode": 200,
    #         "headers": CORS_HEADERS,
    #         "body": ""
    #     }

    try:
        # 1. Path Parameter에서 scenarioId 추출
        scenario_id = None

        # HTTP API (v2) 구조
        if event.get("version") == "2.0" and "pathParameters" in event:
            scenario_id = event["pathParameters"].get("scenarioId")

        # REST API (v1) 구조
        elif "pathParameters" in event:
            scenario_id = event["pathParameters"].get("scenarioId")

        if not scenario_id:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "scenarioId is required"})
            }

        # 2. DynamoDB 조회
        response = table.get_item(Key={"scenarioId": scenario_id})
        item = response.get("Item")

        if not item:
            return {
                "statusCode": 404,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "scenarioId not found"})
            }


        # 3. 응답 데이터 구성
        recognized_word = item.get("recognizedWord")
        file_key = item.get("fileKey")

        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "scenarioId": scenario_id,
                "recognizedWord": recognized_word,
                "fileKey": file_key
            }, ensure_ascii=False)
        }

    except Exception as e:
        print(f"Error occurred: {e}")
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)})
        }
