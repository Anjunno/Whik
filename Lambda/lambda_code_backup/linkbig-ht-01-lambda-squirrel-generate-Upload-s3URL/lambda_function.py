import json
import boto3
import uuid
import os
import urllib.parse
from datetime import datetime, timezone
from botocore.exceptions import ClientError

# --- AWS 클라이언트 및 DynamoDB 테이블 객체 초기화 ---
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# --- 환경 변수에서 리소스 이름 가져오기 ---
BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
results_table = dynamodb.Table(RESULTS_TABLE_NAME)

# 업로드를 허용할 파일 확장자
ALLOWED_EXTENSIONS = {
    "mp3": "audio/mpeg",
    "m4a": "audio/mp4"
}

def lambda_handler(event, context):
    
    # CORS 헤더를 미리 정의해 둡니다.
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,uuid',
        'Access-Control-Allow-Methods': 'OPTIONS,POST'
    }

    # OPTIONS 요청(Preflight)을 먼저 처리합니다.
    if event.get("httpMethod") == "OPTIONS":
        return {
            "statusCode": 200,
            "headers": cors_headers,
            "body": ""
        }
    
    """
    S3 Pre-signed URL을 생성하고, 작업 메타데이터를 DynamoDB에 저장합니다.
    """
    try:
        # 1. Authorizer로부터 사용자 정보 추출
        authorizer_context = event.get('requestContext', {}).get('authorizer', {})
        user_id = authorizer_context.get('principalId')
        gender = authorizer_context.get('gender', 'unknown')
        nickname = authorizer_context.get('nickname', 'unknown')

        if not user_id:
            return {
                'statusCode': 401,
                'headers': cors_headers,
                'body': json.dumps({'error': 'Unauthorized'})
            }

        # 2. Request Body에서 파라미터 추출 및 검증
        body = json.loads(event.get("body", "{}"))
        required_params = ["language", "themeId", "videoId", "fileExtension", "inputType"]
        params = {key: body.get(key) for key in required_params}
        
        missing_params = [key for key, value in params.items() if value is None]
        if missing_params:
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": f"Missing required fields: {', '.join(missing_params)}"})
            }
        
        file_extension = params["fileExtension"].lower()
        if file_extension not in ALLOWED_EXTENSIONS:
            return {
                "statusCode": 400,
                "headers": cors_headers,
                "body": json.dumps({"error": f"Unsupported file extension: {file_extension}"})
            }

        # 3. 새로운 작업(Job) ID 생성
        job_id = str(uuid.uuid4())

        # 4. 'results' 테이블에 작업 메타데이터를 'PENDING' 상태로 저장
        print(f"'{job_id}' 작업을 PENDING 상태로 results 테이블에 저장합니다.")
        timestamp = datetime.now(timezone.utc).isoformat()
        
        results_table.put_item(
            Item={
                'PK': job_id,
                'status': 'PENDING',
                'creationTimestamp': timestamp,
                'userId': user_id,
                'gender': gender,
                'nickname': nickname,
                'language': params['language'],
                'themeId': params['themeId'],
                'videoId': params['videoId'],
                'inputType': params['inputType']
            }
        )

        # 5. 단순화된 S3 경로 구조로 파일 키(Key) 생성
        file_key = f"user-uploads/{user_id}/{job_id}.{file_extension}"
        
        # 6. S3 Pre-signed URL 생성
        content_type = ALLOWED_EXTENSIONS[file_extension]
        presigned_url = s3_client.generate_presigned_url(
            'put_object',
            Params={'Bucket': BUCKET_NAME, 'Key': file_key, 'ContentType': content_type},
            ExpiresIn=300
        )

        # 7. 클라이언트에게 CORS 헤더와 함께 URL 및 작업 ID 반환
        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps({'uploadUrl': presigned_url, 'jobId': job_id})
        }

    except Exception as e:
        print(f"An error occurred: {e}")
        error_job_id = locals().get('job_id', 'N/A')
        print(f"Error occurred for Job ID: {error_job_id}")
        
        # 오류 발생 시에도 CORS 헤더를 포함하여 응답
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Could not generate URL.'})
        }