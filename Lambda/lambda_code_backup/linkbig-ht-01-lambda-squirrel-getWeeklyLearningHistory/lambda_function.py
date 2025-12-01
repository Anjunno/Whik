import json
import boto3
import os
from decimal import Decimal
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr 
from zoneinfo import ZoneInfo

s3_client = boto3.client("s3")
BUCKET_NAME = os.environ.get("BUCKET_NAME")

# DynamoDB 테이블 설정
dynamodb = boto3.resource("dynamodb")
TABLE_NAME = os.environ.get("TABLE_NAME")
table = dynamodb.Table(TABLE_NAME)

# 공통 CORS 헤더 정의
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Content-Type": "application/json"
}

def decimal_default(obj):
    if isinstance(obj, Decimal):
        return int(obj)
    raise TypeError

def generate_presigned_url(file_key, expiration=3600):
    """S3 presigned URL 생성"""
    return s3_client.generate_presigned_url(
        "get_object",
        Params={"Bucket": BUCKET_NAME, "Key": file_key},
        ExpiresIn=expiration
    )

def lambda_handler(event, context):
    # 1️. Authorizer에서 전달된 user_uuid, gender 가져오기
    authorizer_context = event.get("requestContext", {}).get("authorizer", {})
    user_id = authorizer_context.get("user_uuid")
    gender = authorizer_context.get("gender")

    if not user_id or not gender:
        return {
            "statusCode": 403,
            "body": json.dumps({"error": "Unauthorized: missing user_uuid"}),
            "headers": CORS_HEADERS,
        }

    # 2. 파라미터에서 targetLanguage 가져오기
    targetLanguage = (event.get("pathParameters") or {}).get("targetLanguage")
    if not targetLanguage:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "targetLanguage is required"}),
            "headers": CORS_HEADERS
        }
    
    targetLanguage = targetLanguage.lower()
    if targetLanguage == "jp":
        targetLanguage = "ja"

    # KST 시간대 객체 생성
    KST = ZoneInfo("Asia/Seoul")
    today = datetime.now(KST)
    monday_start = (today - timedelta(days=today.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    next_monday_start = monday_start + timedelta(days=7)
    start_ts = int(monday_start.timestamp())
    end_ts = int(next_monday_start.timestamp())

    print(f"KST 기준 시작 시간: {monday_start}")
    print(f"조회용 시작 타임스탬프: {start_ts}")
    print(f"조회용 종료 타임스탬프 (DynamoDB between용): {end_ts - 1}")

    # Query 실행
    response = table.query(
        KeyConditionExpression=Key("userId").eq(user_id) & Key("createdAtTs").between(start_ts, end_ts - 1),
        FilterExpression=Attr("targetLanguage").eq(targetLanguage)
    )

    items = response.get('Items', [])
    print(f"조회된 아이템 수: {len(items)}")

    result_list = []
    for item in items:
        file_key = item.get("fileKey")
        presigned_url = generate_presigned_url(file_key) if file_key else None

        # 'createdAtIso' (UTC)를 KST 문자열로 변환하는 로직
        created_at_iso_utc = item.get("createdAtIso")
        learned_at_kst_str = created_at_iso_utc # 변환 실패 시 원본 값 사용

        if created_at_iso_utc:
            try:
                # 'Z'를 파싱 가능한 '+00:00'으로 변경하여 datetime 객체 생성
                utc_dt = datetime.fromisoformat(created_at_iso_utc.replace('Z', '+00:00'))
                # KST로 시간대 변환
                kst_dt = utc_dt.astimezone(KST)
                # "YYYY-MM-DD HH:MM:SS" 형식의 문자열로 변환
                learned_at_kst_str = kst_dt.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError) as e:
                print(f"날짜 변환 오류: {e}. 원본 ISO 값 '{created_at_iso_utc}'를 그대로 사용합니다.")

        result_list.append({
            "originalWord": item.get("originalWord"),
            "learnedAtTs": item.get("createdAtTs"),
            "learnedAtIso": learned_at_kst_str, # 변환된 KST 문자열 사용
            "imageUrl": presigned_url
        })

    response_body = json.dumps(result_list, ensure_ascii=False, default=decimal_default, indent=2)

    return {
        "statusCode": 200,
        "body": response_body,
        "headers": CORS_HEADERS
    }