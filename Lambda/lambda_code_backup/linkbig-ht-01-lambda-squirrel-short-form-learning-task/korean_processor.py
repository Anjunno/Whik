import boto3
import os
import json
from datetime import datetime, timezone

# --- AWS 클라이언트 및 DynamoDB 테이블 객체 초기화 ---
translate_client = boto3.client('translate')
polly_client = boto3.client('polly')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# --- 환경 변수에서 리소스 이름 가져오기 ---
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
HISTORY_TABLE_NAME = os.environ.get("HISTORY_TABLE_NAME")
VIDEOS_TABLE_NAME = os.environ.get("VIDEOS_TABLE_NAME")
results_table = dynamodb.Table(RESULTS_TABLE_NAME)
history_table = dynamodb.Table(HISTORY_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# --- 상수 정의 ---
# Polly에서 사용할 언어별, 성별 목소리 ID
VOICE_MAP = {
    'jp': {'M': 'Takumi', 'F': 'Mizuki'},
    'zh': {'M': 'Zhiyu', 'F': 'Zhiyu'},
    'es': {'M': 'Enrique', 'F': 'Lucia'}
}
# AWS Translate 서비스에서 요구하는 언어 코드 매핑
LANGUAGE_CODE_MAP = {
    'jp': 'ja'
}
# DynamoDB 'videos' 테이블 조회를 위한 언어 전체 이름 매핑
LANGUAGE_FULL_NAME_MAP = {
    'jp': 'JAPANESE',
    'zh': 'CHINESE',
    'es': 'SPANISH'
}

def get_video_item(language_full_name, video_id):
    """
    'videos' 테이블에서 원본 학습 콘텐츠 정보를 조회합니다.
    """
    try:
        # lang 파티션 키에 언어 전체 이름을 사용합니다.
        response = videos_table.get_item(Key={'lang': language_full_name, 'SK': video_id})
        return response.get('Item')
    except Exception as e:
        print(f"비디오 정보 조회 실패: {e}")
        return None

def process(transcribed_text, original_file_key, task_info):
    """
    한국어 음성을 번역하고, 결과를 'results' 테이블에 업데이트하며
    학습 히스토리를 'history' 테이블에 새로 생성합니다.
    """
    job_id = task_info.get('PK')
    print(f"한국어 처리 시작: '{transcribed_text}' | Job ID: {job_id}")
    
    try:
        # app.py에서 전달받은 task_info 딕셔너리에서 필요한 정보를 추출합니다.
        user_uuid = task_info.get('userId')
        user_gender_full = task_info.get('gender')
        target_language_code = task_info.get('language')
        theme_id = task_info.get('themeId')
        video_id = task_info.get('videoId')

        # AWS Translate가 요구하는 언어 코드로 변환합니다.
        translate_target_code = LANGUAGE_CODE_MAP.get(target_language_code, target_language_code)
        
        # AWS Translate를 호출하여 텍스트를 번역합니다.
        print(f"'{translate_target_code}' 언어로 번역 중...")
        translate_response = translate_client.translate_text(
            Text=transcribed_text,
            SourceLanguageCode='ko',
            TargetLanguageCode=translate_target_code
        )
        translated_text = translate_response.get('TranslatedText')

        # 'male'/'female'을 Polly가 사용하는 'M'/'F' 코드로 변환합니다.
        user_gender_code = 'M' if user_gender_full == 'male' else 'F'
        voice_id = VOICE_MAP.get(target_language_code, {}).get(user_gender_code)

        # AWS Polly를 호출하여 번역된 텍스트를 음성으로 합성합니다.
        print(f"Polly로 음성 생성 중... (Voice: {voice_id})")
        polly_response = polly_client.synthesize_speech(
            Text=translated_text, OutputFormat='mp3', VoiceId=voice_id
        )

        # 합성된 음성 파일을 S3 버킷에 저장합니다.
        audio_output_key = f"processed-audios/{job_id}_translated.mp3"
        translated_audio_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{audio_output_key}"
        s3_client.put_object(
            Bucket=AUDIO_BUCKET_NAME, Key=audio_output_key,
            Body=polly_response['AudioStream'].read(), ContentType='audio/mpeg'
        )

        # 'videos' 테이블 조회를 위해 언어 코드를 전체 이름으로 변환합니다.
        language_full_name = LANGUAGE_FULL_NAME_MAP.get(target_language_code)
        video_item = get_video_item(language_full_name, video_id)
        
        # 성별에 맞는 추천 답변 데이터를 추출합니다.
        recommend_data = {}
        if video_item and 'recommend' in video_item:
            recommend_map = video_item.get('recommend')
            # 성별 키('male'/'female')가 있으면 해당 데이터를, 없으면 공통 데이터를 사용합니다.
            if 'male' in recommend_map or 'female' in recommend_map:
                recommend_data = recommend_map.get(user_gender_full, {})
            else:
                recommend_data = recommend_map

        # 'results' 테이블의 기존 항목을 'COMPLETED' 상태와 결과로 업데이트합니다.
        print(f"결과 테이블 업데이트 중... (Job ID: {job_id})")
        results_table.update_item(
            Key={'PK': job_id},
            UpdateExpression=(
                "SET #st = :s, #rt = :rt, #ot = :ot, #tt = :tt, "
                "#tas = :tas, #ra = :ra"
            ),
            ExpressionAttributeNames={
                '#st': 'status', '#rt': 'resultType', '#ot': 'originalText',
                '#tt': 'translatedText', '#tas': 'translatedAudioS3Uri', '#ra': 'recommendedAnswer'
            },
            ExpressionAttributeValues={
                ':s': 'COMPLETED', ':rt': 'translation', ':ot': transcribed_text,
                ':tt': translated_text, ':tas': translated_audio_s3_uri, ':ra': recommend_data
            }
        )

        # 'history' 테이블에 사용자의 학습 활동을 영구 기록합니다.
        print(f"히스토리 테이블에 저장 중... (User ID: {user_uuid})")
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
                'script': transcribed_text,
                'voiceS3Uri': f"s3://{AUDIO_BUCKET_NAME}/{original_file_key}"
            },
            'result': {
                'translatedText': translated_text,
                'translatedAudioS3Uri': translated_audio_s3_uri
            },
            'feedback': {'starRating': 1, 'comment': ""}
        })
        
        print("모든 처리 완료!")

    except Exception as e:
        print(f"한국어 처리 중 오류 발생: {e}")
        # 오류 발생 시 'results' 테이블의 상태를 'FAILED'로 업데이트합니다.
        if job_id:
            results_table.update_item(
                Key={'PK': job_id},
                UpdateExpression="SET #st = :s, #err = :e",
                ExpressionAttributeNames={'#st': 'status', '#err': 'error'},
                ExpressionAttributeValues={':s': 'FAILED', ':e': str(e)}
            )
        raise e