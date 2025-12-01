import json
import boto3
import os
import random
from decimal import Decimal
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key
import traceback # 상세 에러 로깅을 위해 추가

# --- (상수, 헬퍼 클래스, S3/DynamoDB 클라이언트 초기화 등은 기존과 동일) ---
# --- AWS 클라이언트 및 DynamoDB 테이블 객체 초기화 ---
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# --- 환경 변수 ---
VIDEOS_TABLE_NAME = os.environ.get("VIDEOS_TABLE_NAME")
LEARNED_TABLE_NAME = os.environ.get("LEARNED_TABLE_NAME", "linkbig-ht-01-shortform-learned")
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
CLOUD_FRONT_URL = os.environ['CLOUD_FRONT']

# --- DynamoDB 테이블 ---
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# --- 상수 정의 ---
LANGUAGE_MAP = {
    'jp': {'full': 'JAPANESE', 'transcribe': 'ja-JP'},
    'zh': {'full': 'CHINESE', 'transcribe': 'zh-CN'},
    'es': {'full': 'SPANISH', 'transcribe': 'es-ES'}
}
KOREAN_TRANSCRIBE_CODE = 'ko-KR'

# --- 헬퍼 클래스 ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)

def generate_cloudFront_url(s3_key):
    if not s3_key:
        return None
    try:
        # Pre-signed URL을 생성하는 함수는 이미 정의되어 있으므로 그대로 사용
        # url = s3_client.generate_presigned_url(
        #     'get_object',
        #     Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': s3_key},
        #     ExpiresIn=expiration
        # )
        clean_key = s3_key.replace("contents/jp/", "", 1)
        url = f"https://{CLOUD_FRONT_URL}/{clean_key}"
        return url
    except Exception as e:
        print(f"Pre-signed URL 생성 실패 (Key: {s3_key}): {e}")
        return None

def safe_get(data, keys, default=""):
    _data = data
    for key in keys:
        if isinstance(_data, dict):
            _data = _data.get(key)
        else:
            return default
    return _data if _data is not None else default

# ==========================================================
# 학습 활동의 오디오 키를 URL로 변환
# ==========================================================
def process_activities_for_audio_url(activities):
    """
    learning_activities 리스트를 순회하며 audio_key 필드를 찾아 audio_url로 변환합니다.
    """
    if not activities:
        return []

    processed_activities = []
    
    for activity in activities:
        # 1. FOLLOW_THE_SCRIPT 활동 (루트 레벨에 audio_key 존재)
        if activity.get('activity_type') == 'FOLLOW_THE_SCRIPT':
            s3_key = activity.pop('audio_key', None)
            if s3_key:
                activity['audio_url'] = generate_cloudFront_url(s3_key)
                # 원본 키를 유지하고 싶다면: activity['audio_key'] = s3_key

        # 2. RECOMMENDED_RESPONSES 활동 (중첩된 리스트 안에 audio_key 존재)
        elif activity.get('activity_type') == 'RECOMMENDED_RESPONSES':
            recommended_responses = activity.get('recommended_responses', [])
            
            for response in recommended_responses:
                s3_key = response.pop('audio_key', None)
                if s3_key:
                    response['audio_url'] = generate_cloudFront_url(s3_key)
                    # 원본 키를 유지하고 싶다면: response['audio_key'] = s3_key
                    
        # 3. SENTENCE_RECONSTRUCTION 활동 (스크립트 오디오는 따로 처리하지 않음)
        # 만약 SENTENCE_RECONSTRUCTION도 오디오 키를 가지고 있다면 여기에 로직 추가

        processed_activities.append(activity)

    return processed_activities

# --- 메인 Lambda 핸들러 ---

def lambda_handler(event, context):
    print(f"get-feed received event: {json.dumps(event, ensure_ascii=False)}")

    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,uuid',
        'Access-Control-Allow-Methods': 'OPTIONS,GET'
    }

    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers, "body": ""}

    try:
        # 1. 사용자 정보 및 요청 언어 추출 (생략)
        authorizer_context = event.get('requestContext', {}).get('authorizer', {})
        user_uuid = authorizer_context.get('principalId')
        user_gender = authorizer_context.get('gender', 'male')

        if not user_uuid:
            return {'statusCode': 401, 'headers': cors_headers, 'body': json.dumps({'error': 'Unauthorized'})}

        query_params = event.get('queryStringParameters') or {}
        lang_code = query_params.get('lang', 'jp')

        lang_config = LANGUAGE_MAP.get(lang_code)
        if not lang_config:
            return {'statusCode': 400, 'headers': cors_headers, 'body': json.dumps({'error': 'Unsupported language'})}

        language_full_name = lang_config.get('full')
        target_transcribe_code = lang_config.get('transcribe')

        # 2. DB에서 해당 언어 챌린지 전체 조회 (생략)
        try:
            # response = videos_table.query(KeyConditionExpression=Key('lang').eq(language_full_name))
            # 테스트 영상만 나올 수 있게 임시 수정 25.11.12
            response = videos_table.query(
                KeyConditionExpression=Key('lang').eq(language_full_name) & Key('SK').begins_with('test')
            )
            all_challenge_items = response.get('Items', [])
        except Exception as e:
            print(f"Videos 테이블 조회 실패: {e}")
            all_challenge_items = []

        # 3. 학습 완료 여부 확인 (생략)
        learned_set = set()
        if all_challenge_items:
            keys_to_get = [{'userId': user_uuid, 'videoId': f"{item.get('lang')}#{item.get('SK')}"}
                           for item in all_challenge_items if item.get('lang') and item.get('SK')]

            if keys_to_get:
                try:
                    learned_response = dynamodb.batch_get_item(
                        RequestItems={LEARNED_TABLE_NAME: {'Keys': keys_to_get}}
                    )
                    learned_items = learned_response.get('Responses', {}).get(LEARNED_TABLE_NAME, [])
                    learned_set = {item.get('videoId') for item in learned_items if item.get('videoId')}
                except Exception as e:
                    print(f"LearnedStatus 테이블 조회 실패: {e}")

        # 4. 학습 안 한 영상 필터링 및 랜덤 셔플 (생략)
        unlearned_challenges = [item for item in all_challenge_items
                                 if item.get('lang') and item.get('SK') and
                                 f"{item.get('lang')}#{item.get('SK')}" not in learned_set]

        random.shuffle(unlearned_challenges)

        if not unlearned_challenges:
            print(f"사용자 {user_uuid}가 {lang_code}의 모든 영상을 학습했습니다. 학습한 영상 랜덤 피드 제공.")
            random.shuffle(all_challenge_items)
            final_challenges = all_challenge_items
        else:
            final_challenges = unlearned_challenges

        # ==========================================================
        # 5. 최종 피드 가공 (최대 12개)
        # ==========================================================
        feed = []

        for item in final_challenges[:12]:
            lang_full_name = item.get('lang')
            video_id_sk = item.get('SK')
            if not lang_full_name or not video_id_sk:
                continue

            video_id_for_learned_check = f"{lang_full_name}#{video_id_sk}"

            # 퀴즈 데이터 추출
            learning_activities = safe_get(item,['learning_activities'])
            
            # ✨ 핵심 수정: learning_activities 내의 audio_key를 audio_url로 변환
            if learning_activities:
                processed_activities = process_activities_for_audio_url(learning_activities)
            else:
                processed_activities = []
            
            # 1. 's3Url' 추출
            video_s3_key = safe_get(item, ['s3Url'])

            # 2. 'scene' 정보 추출
            question_ko_script = safe_get(item, ['scene', 'ko-script'])
            question_lang_script = safe_get(item, ['scene', 'lang-script'])
            character_info = safe_get(item, ['scene', 'speaker'])

            # 3. 'recommend' 정보 추출 (webSocketContext용)
            model_answer_script = safe_get(item, ['recommend', user_gender, 'script'])

            # 4. 'title' 추출
            video_title = safe_get(item, ['title'])

            # ----------------------------------------------

            feed.append({
                "videoId": video_id_sk,
                "videoUrl": generate_cloudFront_url(video_s3_key), # 영상 URL도 여기서 생성됨
                "title": video_title,
                "characterInfo": character_info,
                "questionKoreanText": question_ko_script,
                "questionForeignText": question_lang_script,
                "hasLearned": (video_id_for_learned_check in learned_set),
                "learning_activities" : processed_activities, # <--- 변환된 데이터 사용
                "webSocketContext": {
                    "langCode": lang_code,
                    "targetForeignLangCode": target_transcribe_code,
                    "modelAnswerScript": model_answer_script,
                    "videoId": video_id_sk,
                    "langFullName": lang_full_name,
                    "themeId": video_id_sk.split('#')[0]
                }
            })

        response_body = {'feed': feed}

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(response_body, cls=DecimalEncoder, ensure_ascii=False)
        }

    except Exception as e:
        print(f"!!! Error in get-feed handler: {e}")
        traceback.print_exc()
        return {
            'statusCode': 500,
            'headers': cors_headers,
            'body': json.dumps({'error': 'Failed to retrieve feed.'})
        }