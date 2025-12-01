import boto3
import os
import json
from datetime import datetime, timezone
from decimal import Decimal # DynamoDB의 Decimal 타입 처리용

# --- AWS 클라이언트 및 DynamoDB 테이블 객체 초기화 ---
translate_client = boto3.client('translate')
polly_client = boto3.client('polly')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# --- 환경 변수에서 리소스 이름 가져오기 ---
# (app.py 람다에 설정된 환경 변수를 사용)
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
HISTORY_TABLE_NAME = os.environ.get("HISTORY_TABLE_NAME")
VIDEOS_TABLE_NAME = os.environ.get("VIDEOS_TABLE_NAME")

results_table = dynamodb.Table(RESULTS_TABLE_NAME)
history_table = dynamodb.Table(HISTORY_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# --- 상수 정의 ---
VOICE_MAP = {
    'jp': {'M': 'Takumi', 'F': 'Mizuki'},
    'zh': {'M': 'Zhiyu', 'F': 'Zhiyu'},
    'es': {'M': 'Enrique', 'F': 'Lucia'}
}
# AWS Translate 서비스용 언어 코드
LANGUAGE_CODE_MAP = {'jp': 'ja', 'zh': 'zh-CN', 'es': 'es'}
# DynamoDB 'videos' 테이블 조회를 위한 언어 전체 이름
LANGUAGE_FULL_NAME_MAP = {
    'jp': 'JAPANESE',
    'zh': 'CHINESE',
    'es': 'SPANISH'
}

# --- 헬퍼 함수 ---
def get_video_item(language_full_name, video_id):
    """ 'videos' 테이블에서 원본 학습 콘텐츠 정보를 조회합니다. """
    try:
        response = videos_table.get_item(Key={'lang': language_full_name, 'SK': video_id})
        return response.get('Item')
    except Exception as e:
        print(f"비디오 정보 조회 실패: {e}")
        return None

def generate_presigned_url(s3_uri, expiration=3600):
    """ s3:// URI를 받아서 Pre-signed URL을 생성합니다. """
    if not s3_uri:
        return None
    try:
        # s3://{bucket_name}/{key} 형식 파싱
        parts = s3_uri.replace("s3://", "").split('/', 1)
        bucket_name = parts[0]
        key = parts[1]
        
        # S3 키를 기반으로 Pre-signed URL 생성
        url = s3_client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket_name, 'Key': key}, ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Pre-signed URL 생성 실패 ({s3_uri}): {e}")
        return None

def safe_decimal(obj):
    """ DynamoDB의 Decimal 객체를 JSON 직렬화 가능하게 변환 (재귀) """
    if isinstance(obj, Decimal):
        return int(obj) if obj % 1 == 0 else float(obj)
    if isinstance(obj, dict):
        return {k: safe_decimal(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [safe_decimal(elem) for elem in obj]
    return obj

# --- onMessage(app.py)가 호출할 메인 함수 ---
def process_and_get_result(transcribed_text, original_file_key, task_info):
    """
    한국어 음성을 번역하고, 결과를 DB에 저장하며,
    실시간 응답을 위해 처리 결과를 Python 딕셔너리로 반환합니다.
    """
    job_id = task_info.get('PK')
    print(f"한국어 실시간 처리 시작: '{transcribed_text}' | Job ID: {job_id}")
    
    try:
        # 1. task_info에서 정보 추출
        user_uuid = task_info.get('userId')
        user_gender_full = task_info.get('gender') # 'male' or 'female'
        target_language_code = task_info.get('language') # 'jp', 'zh', 'es'
        theme_id = task_info.get('themeId')
        video_id = task_info.get('videoId')

        # 2. 번역 (Translate)
        translate_target_code = LANGUAGE_CODE_MAP.get(target_language_code, target_language_code)
        translate_response = translate_client.translate_text(
            Text=transcribed_text,
            SourceLanguageCode='ko',
            TargetLanguageCode=translate_target_code
        )
        translated_text = translate_response.get('TranslatedText')

        # 3. 음성 합성 (Polly)
        user_gender_code = 'M' if user_gender_full == 'male' else 'F'
        voice_id = VOICE_MAP.get(target_language_code, {}).get(user_gender_code)
        
        polly_response = polly_client.synthesize_speech(
            Text=translated_text, OutputFormat='mp3', VoiceId=voice_id
        )

        # 4. S3에 Polly 음성 저장
        audio_output_key = f"processed-audios/{job_id}_translated.mp3"
        translated_audio_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{audio_output_key}"
        s3_client.put_object(
            Bucket=AUDIO_BUCKET_NAME, Key=audio_output_key,
            Body=polly_response['AudioStream'].read(), ContentType='audio/mpeg'
        )
        
        # 5. [신규] Polly 음성파일의 Pre-signed URL 즉시 생성
        translated_audio_url = generate_presigned_url(translated_audio_s3_uri)

        # 6. 원본 비디오 정보 (추천 답변) 조회
        language_full_name = LANGUAGE_FULL_NAME_MAP.get(target_language_code)
        video_item = get_video_item(language_full_name, video_id)
        
        recommend_data = {}
        if video_item and 'recommend' in video_item:
            recommend_map = video_item.get('recommend')
            if 'male' in recommend_map or 'female' in recommend_map:
                recommend_data = recommend_map.get(user_gender_full, {})
            else:
                recommend_data = recommend_map
        
        # [신규] 추천 답변의 오디오 S3 URI도 Pre-signed URL로 변환
        if recommend_data and recommend_data.get('s3Url'):
            # recommend.s3Url은 키(key)만 저장되어 있다고 가정
            rec_s3_key = recommend_data['s3Url']
            rec_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{rec_s3_key}"
            recommend_data['s3Url'] = generate_presigned_url(rec_s3_uri)
        else:
            print(f"Warning: {video_id}의 recommend.s3Url이 없습니다.")

        # 7. 'results' 테이블에 최종 결과 업데이트
        results_table.update_item(
            Key={'PK': job_id},
            UpdateExpression=(
                "SET #st = :s, #rt = :rt, #tt = :tt, "
                "#tas = :tas, #ra = :ra"
            ),
            ExpressionAttributeNames={
                '#st': 'status', '#rt': 'resultType',
                '#tt': 'translatedText', '#tas': 'translatedAudioS3Uri', '#ra': 'recommendedAnswer'
            },
            ExpressionAttributeValues={
                ':s': 'COMPLETED', ':rt': 'translation',
                ':tt': translated_text, ':tas': translated_audio_s3_uri, ':ra': recommend_data
            }
        )

        # 8. 'history' 테이블에 영구 기록
        timestamp = datetime.now(timezone.utc).isoformat()
        sort_key = f"{target_language_code}#{timestamp}#{job_id}"
        history_table.put_item(Item={
            'PK': user_uuid,
            'SK': sort_key,
            'creationTimestamp': timestamp,
            'themeId': theme_id,
            'videoId': video_id,
            'userInput': {
                'language': 'ko',
                'script': transcribed_text
                # [수정] voiceS3Uri 저장 안 함 (요구사항 반영)
            },
            'result': {
                'translatedText': translated_text,
                'translatedAudioS3Uri': translated_audio_s3_uri
            },
            'feedback': {'starRating': 1, 'comment': ""} # 기본값
        })
        
        # 9. [신규] app.py가 클라이언트에게 보낼 최종 결과 페이로드 반환
        result_payload = {
            'status': 'COMPLETED',
            'resultType': 'translation',
            'originalText': transcribed_text,
            'translatedText': translated_text,
            'translatedAudioUrl': translated_audio_url, # Pre-signed URL
            'recommendedAnswer': safe_decimal(recommend_data) # Decimal 객체 변환
        }
        
        print(f"한국어 처리 완료. Job ID: {job_id}")
        return result_payload

    except Exception as e:
        print(f"!!! korean_processor.py 오류 발생: {e}")
        # 오류 발생 시 'results' 테이블의 상태를 'FAILED'로 업데이트
        if job_id:
            results_table.update_item(
                Key={'PK': job_id},
                UpdateExpression="SET #st = :s, #err = :e",
                ExpressionAttributeNames={'#st': 'status', '#err': 'error'},
                ExpressionAttributeValues={':s': 'FAILED', ':e': str(e)}
            )
        # app.py가 오류를 잡아서 클라이언트에게 보낼 수 있도록 오류 다시 발생
        raise e