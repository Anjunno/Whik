import json
import boto3
import base64
import uuid
import os
from decimal import Decimal

# AWS 클라이언트 초기화
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")

# 환경 변수
BUCKET_NAME = os.environ.get("BUCKET_NAME")
VIDEO_TABLE = os.environ.get("VIDEO_TABLE")

table = dynamodb.Table(VIDEO_TABLE)

# 언어 경로 매핑
LANG_CODE_MAP = {
    "JAPANESE": "jp",
    "SPANISH": "es",
    "CHINESE": "zh"
}

# CORS 헤더 정의
CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*", 
    "Access-Control-Allow-Headers": "Content-Type",
    "Access-Control-Allow-Methods": "POST"
}

def lambda_handler(event, context):
    try:
        # 1. Body 파싱
        body = json.loads(event.get("body", "{}"))
        lang = body.get("lang")       # JAPANESE / SPANISH / CHINESE
        prompt = body.get("prompt")
        main_script = body.get("main_script")
        translate_script = body.get("translate_script")
        video = body.get("video")     # base64 encoded

        if not lang or not prompt or not video or not main_script or not translate_script:
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "lang, prompt, video, main_script, translate_script are required"})
            }

        #언어코드 매핑
        lang_code = LANG_CODE_MAP[lang]

        # 2. SK 생성
        sk = f"test_{uuid.uuid4()}"
        
        # 3. S3 업로드 경로 생성
        s3_key = f"contents/{lang_code}/test/final/{sk}.mp4"

        # 4. base64 → 바이너리 변환
        video_bytes = base64.b64decode(video)

        # 5. S3 업로드
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=video_bytes,
            ContentType="video/mp4"
        )

        speaker_info = "아무개(테스트용)"
        scene_data = {
            "ko-script": translate_script,

            "lang-script": main_script, 
            
            "speaker": speaker_info 
        }
        

        # 6. DynamoDB 저장
        table.put_item(
            Item={
                "lang": lang,
                "SK": sk,
                "prompt": prompt,
                "s3Url": s3_key,
                "scene": scene_data,
                "status" : "PENDING"
            }
        )

        # 7. 성공 응답
        return {
            "statusCode": 200,
            "headers": CORS_HEADERS,
            "body": json.dumps({
                "message": "Upload success",
                "lang": lang,
                "sk": sk,
                "videoKey": s3_key
            })
        }

    except Exception as e:
        print("Error:", str(e))
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)})
        }