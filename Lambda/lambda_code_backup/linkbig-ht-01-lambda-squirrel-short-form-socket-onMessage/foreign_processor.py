import boto3
import os
import json
import re
from datetime import datetime, timezone
from decimal import Decimal

# --- AWS 클라이언트 ---
bedrock_runtime = boto3.client('bedrock-runtime', region_name="us-east-1")
polly_client = boto3.client('polly')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# --- 환경 변수 ---
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
VIDEOS_TABLE_NAME = os.environ.get("VIDEOS_TABLE_NAME")
HISTORY_TABLE_NAME = os.environ.get("HISTORY_TABLE_NAME")

results_table = dynamodb.Table(RESULTS_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)
history_table = dynamodb.Table(HISTORY_TABLE_NAME)

# --- 상수 정의 ---
VOICE_MAP = {
    'jp': {'M': 'Takumi', 'F': 'Mizuki'},
    'zh': {'M': 'Zhiyu', 'F': 'Zhiyu'},
    'es': {'M': 'Enrique', 'F': 'Lucia'}
}
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
        print(f"비디오 정보 조회 실패: {e}")
        return None

def generate_presigned_url(s3_uri, expiration=3600):
    if not s3_uri: return None
    try:
        parts = s3_uri.replace("s3://", "").split('/', 1)
        return s3_client.generate_presigned_url(
            'get_object', Params={'Bucket': parts[0], 'Key': parts[1]}, ExpiresIn=expiration
        )
    except Exception:
        return None

def safe_decimal(obj):
    if isinstance(obj, Decimal): return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict): return {k: safe_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list): return [safe_decimal(elem) for elem in obj]
    return obj

def decimal_default_proc(obj):
    if isinstance(obj, float): return Decimal(str(obj))
    return obj

def extract_json_from_text(text: str):
    try:
        start_idx = text.find('{')
        end_idx = text.rfind('}')
        if start_idx != -1 and end_idx != -1 and start_idx <= end_idx:
            return json.loads(text[start_idx : end_idx + 1])
        return json.loads(text)
    except json.JSONDecodeError:
        return None

# ---------------------------------------------------------
# [핵심 수정] 3단계 논리를 수행하는 똑똑한 프롬프트
# ---------------------------------------------------------
def create_bedrock_prompt(model_answer, user_text, user_gender, context, target_language):
    return f"""
    You are a strict language tutor. You must evaluate the student's response according to the rules.

    ⚠️ LANGUAGE RULES (DO NOT BREAK):
    - "corrected_sentence" MUST be written ONLY in **{target_language}**.
    - "feedback_comment" MUST be written ONLY in **KOREAN (한국어)**.
    - Your final output MUST follow the JSON structure exactly. No extra text.

    ---

    [INPUTS]
    - Situation Context: "{context}"
    - Correct Model Answer (Target Language): "{model_answer}"
    - Student's Spoken Text: "{user_text}"
    - Student Gender: "{user_gender}"

    ---

    [SCORING + CORRECTION RULES]

    1. **Relevance**
    - If the student response is unrelated, nonsense, or incorrect context → `"is_correct": false`
    - The `corrected_sentence` must then be the same as `"model_answer"`.

    2. **Tone Requirements**
    - The corrected sentence MUST follow proper formal/honorific tone (존댓말/丁寧語/usted-form depending on the language).
    - FIX grammar, politeness, conjugation, particles, or unnatural expressions.

    3. **Accuracy Rules**
    - If the student's sentence is already natural AND uses the correct tone → return it as-is.

    ---

    [OUTPUT FORMAT — JSON ONLY]

    {{
        "is_correct": true/false,
        "star_rating": 1-3,
        "feedback_comment": "KOREAN ONLY (NO English, Chinese, or Japanese).",
        "corrected_sentence": "TARGET LANGUAGE ONLY: {target_language}"
    }}
    """


# --- 메인 함수 ---
def process_and_get_result(transcribed_text, original_file_key, task_info):
    job_id = task_info.get('PK')
    print(f"외국어 처리 시작: '{transcribed_text}' | Job ID: {job_id}")
    
    try:
        user_uuid = task_info.get('userId')
        user_gender_full = task_info.get('gender')
        language_code = task_info.get('language')
        theme_id = task_info.get('themeId')
        video_id = task_info.get('videoId')
        user_input_voice_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{original_file_key}"

        # 비디오 정보 조회
        language_full_name = LANGUAGE_FULL_NAME_MAP.get(language_code, 'JAPANESE')
        video_item = get_video_item(language_full_name, video_id)
        
        # 추천 문장(Model Answer) 추출
        video_title = "Conversation"
        model_answer_script = "" # 기본값 (없음)
        recommend_data = {}

        if video_item:
            video_title = video_item.get('title', 'Conversation')
            if 'recommend' in video_item:
                recommend_map = video_item.get('recommend')
                if 'male' in recommend_map or 'female' in recommend_map:
                    recommend_data = recommend_map.get(user_gender_full, {})
                else:
                    recommend_data = recommend_map
                # DB에 추천 문장이 있으면 가져오고, 없으면 빈 문자열 유지
                model_answer_script = recommend_data.get('script', '')

        # --- Bedrock Nova Pro 호출 ---
        print(f"Nova Pro 호출... (Ref: {model_answer_script if model_answer_script else 'NONE'})")
        
        # [수정] target_lang_name 추가 전달
        prompt = create_bedrock_prompt(
            model_answer_script, 
            transcribed_text, 
            user_gender_full, 
            video_title,
            LANGUAGE_FULL_NAME_MAP.get(language_code)
        )
        
        request_body = {
            "inferenceConfig": {"max_new_tokens": 1000},
            "messages": [{"role": "user", "content": [{"text": prompt}]}]
        }

        bedrock_response = bedrock_runtime.invoke_model(
            body=json.dumps(request_body),
            modelId='amazon.nova-pro-v1:0',
            contentType='application/json', 
            accept='application/json'
        )
        
        response_body = json.loads(bedrock_response.get('body').read())
        response_text = response_body['output']['message']['content'][0]['text']
        
        feedback_json = extract_json_from_text(response_text)
        if not feedback_json:
            print(f"JSON Parsing Failed. Raw: {response_text}")
            # 파싱 실패 시 최후의 수단: 모델 답변이 있으면 그거라도 쓰고, 없으면 에러 메시지
            safe_fallback = model_answer_script if model_answer_script else "Error generating response."
            feedback_json = {
                "is_correct": False,
                "feedback_comment": "AI 응답을 처리하는 중 오류가 발생했습니다.",
                "star_rating": 1,
                "corrected_sentence": safe_fallback
            }

        # 최종 확정된 문장 (AI가 교정했거나, 추천했거나, 새로 생성한 것)
        final_script = feedback_json.get('corrected_sentence')
        
        # --- Polly 음성 합성 ---
        user_gender_code = 'M' if user_gender_full == 'male' else 'F'
        voice_id = VOICE_MAP.get(language_code, {}).get(user_gender_code, 'Takumi')
        
        # final_script가 비어있을 경우를 대비한 방어 코드
        text_to_speak = final_script if final_script else "Sorry, I could not generate a response."

        polly_response = polly_client.synthesize_speech(
            Text=text_to_speak, OutputFormat='mp3', VoiceId=voice_id
        )

        # S3 저장 및 URL 생성
        audio_output_key = f"processed-audios/{job_id}_correction.mp3"
        correction_audio_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{audio_output_key}"
        s3_client.put_object(
            Bucket=AUDIO_BUCKET_NAME, Key=audio_output_key,
            Body=polly_response['AudioStream'].read(), ContentType='audio/mpeg'
        )
        
        correction_audio_url = generate_presigned_url(correction_audio_s3_uri)
        user_input_voice_url = generate_presigned_url(user_input_voice_s3_uri)
        
        if recommend_data and recommend_data.get('s3Url'):
             # 기존 추천 데이터가 있다면 URL 갱신 (참고용)
             # 하지만 앱은 이제 feedback_json의 corrected_sentence와 correction_audio_url을 주로 써야 함
             rec_s3_key = recommend_data['s3Url']
             rec_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{rec_s3_key}"
             recommend_data['s3Url'] = generate_presigned_url(rec_s3_uri)

        # DB 저장
        feedback_json_decimal = json.loads(json.dumps(feedback_json), parse_float=decimal_default_proc)
        
        results_table.update_item(
            Key={'PK': job_id},
            UpdateExpression="SET #st = :s, #rt = :rt, #fb = :fb, #cas = :cas, #uvs = :uvs",
            ExpressionAttributeNames={
                '#st': 'status', '#rt': 'resultType', '#fb': 'feedback', 
                '#cas': 'correctionAudioS3Uri', '#uvs': 'userInputVoiceS3Uri'
            },
            ExpressionAttributeValues={
                ':s': 'COMPLETED', ':rt': 'feedback',
                ':fb': feedback_json_decimal, ':cas': correction_audio_s3_uri,
                ':uvs': user_input_voice_s3_uri
            }
        )
        
        # History 저장
        timestamp = datetime.now(timezone.utc).isoformat()
        sort_key = f"{language_code}#{timestamp}#{job_id}"
        history_table.put_item(Item={
            'PK': user_uuid, 'SK': sort_key, 'creationTimestamp': timestamp,
            'themeId': theme_id, 'videoId': video_id,
            'userInput': {
                'language': language_code, 'script': transcribed_text,
                'voiceS3Uri': user_input_voice_s3_uri
            },
            'result': {
                'correctedText': final_script, # AI가 결정한 최종 문장
                'correctionAudioS3Uri': correction_audio_s3_uri
            },
            'feedback': {
                'starRating': feedback_json.get('star_rating'),
                'comment': feedback_json.get('feedback_comment'),
                'is_correct': feedback_json.get('is_correct')
            }
        })
        
        # 클라이언트 반환
        result_payload = {
            'status': 'COMPLETED',
            'resultType': 'feedback',
            'userInputText': transcribed_text,
            'feedback': safe_decimal(feedback_json),
            'correctionAudioUrl': correction_audio_url,
            'userInputVoiceUrl': user_input_voice_url,
            'recommendedAnswer': safe_decimal(recommend_data)
        }
        
        print(f"외국어 처리 완료. Job ID: {job_id}")
        return result_payload

    except Exception as e:
        print(f"!!! foreign_processor.py 오류 발생: {e}")
        if job_id:
            results_table.update_item(
                Key={'PK': job_id},
                UpdateExpression="SET #st = :s, #err = :e",
                ExpressionAttributeNames={'#st': 'status', '#err': 'error'},
                ExpressionAttributeValues={':s': 'FAILED', ':e': str(e)}
            )
        raise e