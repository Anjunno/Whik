import json
import os
import boto3
from decimal import Decimal

# S3 클라이언트 초기화 (Presigned URL 생성용)
s3_client = boto3.client('s3')

# DynamoDB 리소스 초기화
dynamodb = boto3.resource('dynamodb')

# 환경 변수
VIDEOS_TABLE_NAME = os.environ['VIDEOS_TABLE_NAME']
VIDEOS_BUCKET_NAME = os.environ['VIDEOS_BUCKET_NAME']

CLOUD_FRONT_URL = os.environ['CLOUD_FRONT']
table = dynamodb.Table(VIDEOS_TABLE_NAME)

# 언어 코드 매핑
LANG_CODE_MAP = {
    "jp": "JAPANESE",
    "zh": "CHINESE",
    "es": "SPANISH"
}

# DynamoDB의 Decimal 타입을 JSON으로 변환하기 위한 헬퍼 클래스
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

def lambda_handler(event, context):
    """
    특정 언어와 테마에 맞는 비디오 목록을 반환합니다.
    - S3 경로는 모두 Presigned URL로 변환하여 보안을 강화합니다.
    - REST API의 GET /videos/{lang}/{themeId} 와 연결됩니다.
    """
    print(f"getVideoList received event: {json.dumps(event, ensure_ascii=False)}")
    
    try:
        # 1. Authorizer와 URL 경로에서 파라미터 추출
        authorizer_context = event.get('requestContext', {}).get('authorizer', {})
        user_gender = authorizer_context.get('gender')
        
        if not user_gender:
            raise ValueError("Gender information is missing from the authorizer context.")

        path_params = event.get('pathParameters', {})
        lang_code = path_params.get('lang')
        theme_id = path_params.get('themeId')
        
        if not lang_code or not theme_id:
            raise ValueError("Language code or Theme ID is missing from the URL path.")
        
        language = LANG_CODE_MAP.get(lang_code.lower())
        if not language:
            raise ValueError(f"Invalid language code provided: {lang_code}")
            
    except (ValueError, KeyError) as e:
        return {
            "statusCode": 400,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"message": f"Bad Request: {str(e)}"})
        }

    try:
        # 2. DynamoDB에서 비디오 목록 조회
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('lang').eq(language) & boto3.dynamodb.conditions.Key('SK').begins_with(theme_id + '#')
        )
        items = response.get('Items', [])
        
    except Exception as e:
        print(f"DynamoDB Query Error: {e}")
        return {
            "statusCode": 500,
            "headers": {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*"},
            "body": json.dumps({"message": "Failed to retrieve video list."})
        }

    # 3. 데이터 가공 (성별 필터링, prompt 제거, Presigned URL 생성)
    processed_items = []
    gender_key = user_gender.lower() 

    for item in items:
        # recommend 필드 필터링
        recommend_data = item.get('recommend')
        if recommend_data and 'male' in recommend_data and 'female' in recommend_data:
            gender_specific_recommend = recommend_data.get(gender_key)
            item['recommend'] = gender_specific_recommend
        
        # prompt 필드 제거
        item.pop('prompt', None)
            
        # s3Url을 다운로드용 Presigned URL로 변환
        # if item.get('s3Url'):
        #     item['s3Url'] = s3_client.generate_presigned_url(
        #         'get_object',
        #         Params={'Bucket': VIDEOS_BUCKET_NAME, 'Key': item['s3Url']},
        #         ExpiresIn=3600  # 1시간 동안 유효
        #     )
        
        #cloud front로 대체함 - 25.11.22
        clean_key = item.get('s3Url').replace("contents/jp/", "", 1)
        item['s3Url'] = "https://" + CLOUD_FRONT_URL + "/" + clean_key
        print("영상 주소 : ",item['s3Url'])

        filename_without_ext = clean_key.rsplit("/", 1)[-1].rsplit(".", 1)[0]

        # 디렉토리만 final → thumbnail 로 변경
        thumbnail_key = clean_key.replace("/final/", "/thumbnail/")

        # 파일명 확장자 mp4 → png 변환
        thumbnail_key = thumbnail_key.rsplit("/", 1)[0] + f"/{filename_without_ext}.png"

        item['thumbnailUrl'] = f"https://{CLOUD_FRONT_URL}/{thumbnail_key}"
        print("썸네일 주소 : ",item['thumbnailUrl'])


        # recommend 안의 s3Url도 Presigned URL로 변환
        if item.get('recommend') and item.get('recommend').get('s3Url'):
            item['recommend']['s3Url'] = s3_client.generate_presigned_url(
                'get_object',
                Params={'Bucket': VIDEOS_BUCKET_NAME, 'Key': item['recommend']['s3Url']},
                ExpiresIn=3600
            )

        processed_items.append(item)

    # 4. 성공 응답 반환
    return {
        "statusCode": 200,
        "headers": {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*"
        },
        "body": json.dumps(processed_items, cls=DecimalEncoder, ensure_ascii=False)
    }