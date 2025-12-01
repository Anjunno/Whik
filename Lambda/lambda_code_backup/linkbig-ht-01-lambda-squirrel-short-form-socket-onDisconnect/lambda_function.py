import json
import os
import boto3

# DynamoDB 리소스 초기화
dynamodb = boto3.resource('dynamodb')
connections_table = dynamodb.Table(os.environ['CONNECTIONS_TABLE_NAME'])

def lambda_handler(event, context):
    # 이 함수는 API Gateway WebSocket의 $disconnect 라우트와 연결됩니다.
    # 연결이 끊긴 사용자의 정보를 DynamoDB에서 삭제합니다.
    
    # 1. 이벤트에서 connectionId 추출
    connection_id = event['requestContext']['connectionId']
    print(f"Disconnecting connection: {connection_id}")

    try:
        # 2. DynamoDB에서 해당 connectionId를 가진 항목 삭제
        connections_table.delete_item(
            Key={
                'connectionId': connection_id
            }
        )
        print(f"Successfully deleted connection item from DynamoDB: {connection_id}")
        
        # 3. 성공 응답 반환
        return {'statusCode': 200, 'body': 'Disconnected.'}

    except Exception as e:
        # 4. 오류 발생 시 로그 기록 및 실패 응답 반환
        print(f"Error deleting connection {connection_id}: {e}")
        return {'statusCode': 500, 'body': 'Failed to disconnect.'}