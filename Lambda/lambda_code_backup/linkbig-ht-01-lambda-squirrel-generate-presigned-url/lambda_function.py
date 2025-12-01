import json
import boto3
import uuid
import time
import os
# AWS 클라이언트 초기화
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# S3 버킷명
BUCKET_NAME = os.environ.get("BUCKET_NAME")
# DynamoDB 테이블명
TABLE_NAME = os.environ.get('TABLE_NAME')
table = dynamodb.Table(TABLE_NAME)

def lambda_handler(event, context):
    try:
        # 1️. Authorizer에서 전달된 user_uuid 가져오기
        authorizer_context = (event.get("requestContext") or {}).get("authorizer") or {}
        user_id = authorizer_context.get("user_uuid")

        if not user_id:
            return {
                "statusCode": 403,
                "body": json.dumps({"error": "Unauthorized: missing user_uuid"}),
                "headers": {"Content-Type": "application/json"},
            }

        # 2️. Path Parameter에서 extension 가져오기
        file_ext = (event.get("pathParameters") or {}).get("extension")
        if not file_ext:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "extension is required"}),
                "headers": {"Content-Type": "application/json"}
            }

        # 3️. UUID로 파일 이름 생성
        file_uuid = str(uuid.uuid4())
        file_name = f"temp-uploads/{file_uuid}.{file_ext}"

        # 4️. Presigned URL 생성 (S3 PUT)
        presigned_url = s3_client.generate_presigned_url(
            "put_object",
            Params={"Bucket": BUCKET_NAME, "Key": file_name},
            ExpiresIn=3600,  # 1시간 유효
        )

        # 5️. TTL 설정 (현재 시간 + 3600초)
        ttl = int(time.time()) + 3600

        # 6️. DynamoDB에 임시 아이템 저장
        table.put_item(
            Item={
                "scenarioId": file_uuid,   # PK
                "userId": user_id,         # Authorizer에서 가져온 UUID
                "fileKey": file_name,      # 업로드 예정 파일 경로
                "ttl": ttl,                # 1시간 후 만료
                "status": "PENDING",       # 초기 상태
            }
        )

        # 7️. 결과 반환
        return {
            "statusCode": 200,
            "headers": {
                "Access-Control-Allow-Origin": "*",
                # "Access-Control-Allow-Methods": "OPTIONS,GET,POST",
                # "Access-Control-Allow-Headers": "Content-Type,Authorization,uuid",
                "Content-Type": "application/json"
            },
            "body": json.dumps(
                {
                    "scenarioId": file_uuid,
                    "upload_url": presigned_url,
                    "file_name": file_name,
                },
                ensure_ascii=False,
            )
        }


    except Exception as e:
        # 예외 처리
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {
                "Content-Type": "application/json",
                "Access-Control-Allow-Origin": "*",
            },
        }
