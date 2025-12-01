import json
import boto3
from botocore.exceptions import ClientError

# ==========================
# 설정
# ==========================
S3_BUCKET_NAME = os.environ.get("BUCKET_NAME")
PRESIGNED_URL_EXPIRATION = 3600  # 1시간

dynamodb = boto3.resource("dynamodb")
s3_client = boto3.client("s3")
TABLE_NAME = os.environ.get("HISTORY_TABLE")
table = dynamodb.Table(TABLE_NAME)

# 공통 CORS 헤더 정의
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "application/json"
}

# ==========================
# 헬퍼: Pre-signed URL 생성
# ==========================
def create_presigned_url(bucket_name, object_key, expiration=PRESIGNED_URL_EXPIRATION):
    if not object_key:
        return None
    try:
        return s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': bucket_name, 'Key': object_key},
            ExpiresIn=expiration
        )
    except ClientError as e:
        print(f"Error creating presigned URL for {object_key}: {e}")
        return None

# ==========================
# Lambda Handler
# ==========================
def lambda_handler(event, context):
    try:
        # 1️⃣ Authorizer에서 user_uuid 가져오기
        authorizer_context = (event.get("requestContext") or {}).get("authorizer") or {}
        userId = authorizer_context.get("user_uuid")

        if not userId:
            return {
                "statusCode": 403,
                "body": json.dumps({"error": "Unauthorized: missing user_uuid"}),
                "headers": CORS_HEADERS,
            }

        # 2️⃣ Path Parameter에서 learnedAtTs 가져오기
        learnedAtTs_raw = (event.get("pathParameters") or {}).get("learnedAtTs")
        if not learnedAtTs_raw:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "learnedAtTs is required"}),
                "headers": CORS_HEADERS,
            }

        try:
            learnedAtTs = int(learnedAtTs_raw)
        except ValueError:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "learnedAtTs must be a valid integer"}),
                "headers": CORS_HEADERS,
            }

        # 3️⃣ DynamoDB 조회
        response = table.get_item(
            Key={
                "userId": userId,
                "createdAtTs": learnedAtTs
            }
        )
        item = response.get("Item")

        if not item:
            return {
                "statusCode": 404,
                "body": json.dumps({"error": "해당 학습 기록을 찾을 수 없습니다."}),
                "headers": CORS_HEADERS,
            }

        # 4️⃣ fileKey / 이미지 Pre-signed URL
        file_key = item.get('fileKey')
        image_url = create_presigned_url(S3_BUCKET_NAME, file_key)

        # 5️⃣ audioFileKeys / 오디오 Pre-signed URL
        audio_urls = {}
        for key_alias, audio_key in item.get('audioFileKeys', {}).items():
            audio_urls[key_alias] = create_presigned_url(S3_BUCKET_NAME, audio_key)

        # 6️⃣ 최종 응답
        response_body = {
            "imageUrl": image_url,
            "originalWord": item.get("originalWord"),
            "relatedWords_kr": item.get('relatedWords_kr', {}),       
            "translationDetails": item.get('translationDetails', {}),
            "pronunciation": item.get('pronunciation', {}),
            "audioUrls": audio_urls,
        }

        return {
            "statusCode": 200,
            "body": json.dumps(response_body, ensure_ascii=False),
            "headers": CORS_HEADERS,
        }

    except ClientError as e:
        print(f"DynamoDB or S3 Client Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "AWS 서비스 작업 중 오류가 발생했습니다."}),
            "headers": CORS_HEADERS,
        }

    except Exception as e:
        print(f"Internal Server Error: {e}")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "서버 내부 오류가 발생했습니다."}),
            "headers": CORS_HEADERS,
        }
