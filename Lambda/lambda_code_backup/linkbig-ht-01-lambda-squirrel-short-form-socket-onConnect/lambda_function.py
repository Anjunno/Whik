import json
import boto3
import os
import time

# DynamoDB 리소스 초기화
dynamodb = boto3.resource('dynamodb')
connections_table = dynamodb.Table(os.environ['CONNECTIONS_TABLE_NAME'])

def lambda_handler(event, context):
    """
    WebSocket $connect 라우트를 처리합니다.
    Authorizer로부터 받은 사용자 정보를 ConnectionsTable에 저장합니다.
    """
    print(f"onConnect received event: {json.dumps(event, ensure_ascii=False)}")
    
    connection_id = event['requestContext']['connectionId']

    try:
        # 1. Authorizer로부터 사용자 정보 추출
        authorizer = event['requestContext'].get('authorizer', {})
        user_uuid = authorizer.get('user_uuid')
        nickname = authorizer.get('nickname', 'unknown')
        gender = authorizer.get('gender', 'unknown')

        # Authorizer를 통과했으므로 user_uuid는 항상 존재해야 합니다.
        if not user_uuid:
            print("Critical Error: user_uuid is missing from authorizer context.")
            return {"statusCode": 403, "body": "Unauthorized"}

        # 2. TTL(Time-To-Live) 타임스탬프 생성 (1시간 후 자동 만료)
        # DynamoDB TTL은 일반 숫자(정수) 타입이어야 합니다.
        ttl_timestamp = int(time.time()) + 3600

        # 3. DynamoDB에 저장할 아이템 구성
        item = {
            "connectionId": connection_id,
            "uuid": user_uuid,
            "nickname": nickname,
            "gender": gender,
            "connectedAt": int(time.time()),
            "ttl": ttl_timestamp
        }

        # 4. DynamoDB에 연결 정보 저장
        connections_table.put_item(Item=item)
        print(f"Connection stored successfully for user: {user_uuid}")

        # 5. 연결 성공 응답 반환
        return {"statusCode": 200, "body": "Connected."}

    except Exception as e:
        # 6. 오류 발생 시 연결 실패 응답 반환
        print(f"Error on connect: {e}")
        return {"statusCode": 500, "body": "Failed to connect."}