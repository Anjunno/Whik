import boto3
import os
import json
from datetime import datetime, timezone
from decimal import Decimal

# --- AWS 클라이언트 ---
dynamodb = boto3.resource('dynamodb')
bedrock_runtime = boto3.client('bedrock-runtime', region_name="us-east-1") # 리전 명시 권장

# --- 환경 변수 ---
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
HISTORY_TABLE_NAME = os.environ.get("HISTORY_TABLE_NAME")
VIDEOS_TABLE_NAME = os.environ.get("VIDEOS_TABLE_NAME")
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")

results_table = dynamodb.Table(RESULTS_TABLE_NAME)
history_table = dynamodb.Table(HISTORY_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# 'jp' -> 'JAPANESE' 변환 맵
LANGUAGE_FULL_NAME_MAP = {
    'jp': 'JAPANESE',
    'zh': 'CHINESE',
    'es': 'SPANISH',
    'ko': 'KOREAN'
}

# --- 헬퍼 함수 ---
def get_video_item(language_full_name, video_id):
    try:
        response = videos_table.get_item(Key={'lang': language_full_name, 'SK': video_id})
        return response.get('Item')
    except Exception as e:
        print(f"!!! 비디오 정보 조회 실패: {e}")
        return None

def decimal_default_proc(obj):
    if isinstance(obj, float):
        return Decimal(str(obj))
    return obj

def safe_decimal(obj):
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: safe_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_decimal(elem) for elem in obj]
    return obj

# --- 프롬프트 생성 함수 (기존과 동일) ---
def create_matching_prompt(reference_text, stt_text):
    return f"""You are a strict but fair language tutor. Evaluate if the student's spoken text (STT) matches the reference.
    
    Reference: "{reference_text}"
    Student STT: "{stt_text}"

    Rules:
    1. Check for EXACT semantic match.
    2. Ignore minor punctuation or Kanji/Hiragana differences.
    3. Penalize wrong words or missing core meaning.

    Respond ONLY in JSON:
    {{
        "is_correct": boolean,
        "feedback_comment": "Korean explanation",
        "corrected_sentence": "Reference sentence"
    }}
    """

# --- 메인 로직 ---
def process_and_evaluate(stt_result_text, task_info):
    job_id = task_info.get('PK')
    user_uuid = task_info.get('userId')
    language_code = task_info.get('language')
    theme_id = task_info.get('themeId')
    video_id = task_info.get('videoId')
    original_file_key = task_info.get('originalFileKey')
    
    # 1. 정답 문장 조회
    lang_full_name = LANGUAGE_FULL_NAME_MAP.get(language_code, 'JAPANESE')
    video_item = get_video_item(lang_full_name, video_id)
    
    if not video_item:
        # 비디오 정보가 없으면 에러 대신 로그 찍고 중단 (또는 기본값)
        print(f"!!! Warning: Video Item not found for {video_id}")
        reference_text = "Unknown Reference"
    else:
        # DB 구조에 맞춰 target_sentence 찾기
        reference_text = ""
        try:
            activities = video_item.get('learning_activities', [])
            for act in activities:
                if act.get('activity_type') == 'SENTENCE_RECONSTRUCTION':
                    reference_text = act.get('target_sentence')
                    break
            if not reference_text:
                reference_text = video_item.get('questionForeignText', '').split('\n')[0].strip()
        except:
            reference_text = "Reference Not Found"

    print(f"Job {job_id}: Nova Pro 평가 시작. (Ref: {reference_text} vs STT: {stt_result_text})")

    # 2. Bedrock Nova Pro 호출 (수정됨)
    prompt = create_matching_prompt(reference_text, stt_result_text)
    
    try:
        # ✅ Nova Pro 요청 구조 (inferenceConfig 사용)
        request_body = {
            "inferenceConfig": {
                "max_new_tokens": 1000
            },
            "messages": [
                {
                    "role": "user",
                    "content": [{"text": prompt}]
                }
            ]
        }

        bedrock_response = bedrock_runtime.invoke_model(
            modelId='amazon.nova-pro-v1:0',
            body=json.dumps(request_body),
            contentType='application/json',
            accept='application/json'
        )
        
        # ✅ Nova Pro 응답 파싱 (output -> message -> content -> text)
        response_body = json.loads(bedrock_response.get('body').read())
        response_text = response_body['output']['message']['content'][0]['text']
        
        # JSON 파싱
        feedback_json = json.loads(response_text)

    except Exception as e:
        print(f"!!! Bedrock Nova 호출/파싱 오류: {e}")
        # 오류 발생 시 기본값
        feedback_json = {
            "is_correct": False,
            "feedback_comment": "AI 평가 중 오류가 발생했습니다.",
            "corrected_sentence": reference_text
        }

    # 3. DB 저장 (기존과 동일)
    feedback_json_decimal = json.loads(json.dumps(feedback_json), parse_float=decimal_default_proc)

    results_table.update_item(
        Key={'PK': job_id},
        UpdateExpression="SET #st = :s, #rt = :rt, #fb = :fb, #uvs = :uvs, #uit = :uit, #rtxt = :rtxt",
        ExpressionAttributeNames={
            '#st': 'status', '#rt': 'resultType', '#fb': 'feedback',
            '#uvs': 'userInputVoiceS3Uri', '#uit': 'userInputText', '#rtxt': 'referenceText'
        },
        ExpressionAttributeValues={
            ':s': 'COMPLETED',
            ':rt': 'pronunciation_match',
            ':fb': feedback_json_decimal,
            ':uvs': f"s3://{AUDIO_BUCKET_NAME}/{original_file_key}",
            ':uit': stt_result_text,
            ':rtxt': reference_text
        }
    )
    
    # 4. History 저장
    timestamp = datetime.now(timezone.utc).isoformat()
    sort_key = f"{language_code}#{timestamp}#{job_id}"
    star_rating = 3 if feedback_json.get('is_correct') else 1
    
    history_table.put_item(Item={
        'PK': user_uuid,
        'SK': sort_key,
        'creationTimestamp': timestamp,
        'themeId': theme_id,
        'videoId': video_id,
        'userInput': {
            'language': language_code,
            'script': stt_result_text,
            'voiceS3Uri': f"s3://{AUDIO_BUCKET_NAME}/{original_file_key}"
        },
        'result': {
            'referenceText': reference_text,
            'correctedText': feedback_json.get('corrected_sentence')
        },
        'feedback': {
            'starRating': star_rating,
            'comment': feedback_json.get('feedback_comment'),
            'is_correct': feedback_json.get('is_correct')
        }
    })

    # 5. 결과 반환
    return {
        'status': 'COMPLETED',
        'resultType': 'pronunciation_match',
        'userInputText': stt_result_text,
        'referenceText': reference_text,
        'feedback': safe_decimal(feedback_json)
    }