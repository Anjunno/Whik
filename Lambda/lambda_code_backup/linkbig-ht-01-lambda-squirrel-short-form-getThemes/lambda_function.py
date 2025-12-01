import json
import os
import boto3
from decimal import Decimal

# DynamoDB의 Decimal 타입을 JSON으로 변환하기 위한 헬퍼 클래스
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

# DynamoDB 리소스 초기화
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(os.environ['THEMES_TABLE_NAME'])

def lambda_handler(event, context):
    """
    DynamoDB에서 모든 테마 목록을 스캔하여 반환합니다.
    - REST API의 GET /themes 와 연결됩니다.
    """
    print(f"getThemes received event: {json.dumps(event, ensure_ascii=False)}")

    try:
        # table.scan()은 테이블의 모든 항목을 읽어옵니다.
        # 테마의 개수가 적을 때는 괜찮지만, 많아질 경우 비효율적일 수 있습니다.
        response = table.scan()
        items = response.get('Items', [])
        
        # 데이터가 많을 경우, scan은 페이징 처리가 필요할 수 있습니다.
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response.get('Items', []))

        print(f"Found {len(items)} themes.")

        return {
            "statusCode": 200,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps(items, cls=DecimalEncoder, ensure_ascii=False)
        }
        
    except Exception as e:
        print(f"Error scanning themes table: {e}")
        return {
            "statusCode": 500,
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*"
            },
            "body": json.dumps({"message": "Failed to retrieve themes."})
        }