import json
import os
import boto3
import uuid
import base64
from datetime import datetime, timezone

# AWS 서비스 클라이언트 초기화
transcribe = boto3.client('transcribe')
dynamodb = boto3.resource('dynamodb')

# 환경 변수
AUDIO_BUCKET_NAME = os.environ['AUDIO_BUCKET_NAME']
HISTORY_TABLE_NAME = os.environ['HISTORY_TABLE_NAME']
VIDEOS_TABLE_NAME = os.environ['VIDEOS_TABLE_NAME']
history_table = dynamodb.Table(HISTORY_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# 언어 코드 매핑
TRANSCRIBE_LANG_MAP = {
    "jp": "ja-JP",
    "zh": "zh-CN",
    "es": "es-US"
}
LANG_CODE_MAP = {
    "jp": "JAPANESE",
    "zh": "CHINESE",
    "es": "SPANISH"
}

def lambda_handler(event, context):
    """
    SQS(foreign-process-queue)로부터 트리거됩니다.
    videoId로 원본 대본을 조회하고, 사용자의 외국어 음성에 대한 Transcribe 작업을 시작합니다.
    """
    print(f"start-foreign received event: {json.dumps(event, ensure_ascii=False)}")

    for record in event.get('Records', []):
        try:
            # 1. SQS 페이로드 파싱
            payload = json.loads(record['body'])
            connection_id = payload.get('connectionId')
            user_uuid = payload.get('uuid')
            nickname = payload.get('nickname')
            gender = payload.get('gender')
            learning_lang = payload.get('learningLang')
            msg_body = payload.get('messageBody', {})
            
            video_id = msg_body.get('videoId')
            theme_id = msg_body.get('themeId')
            user_audio_s3_key = msg_body.get('s3Key')

            if not all([connection_id, user_uuid, nickname, user_audio_s3_key, video_id]):
                raise ValueError("Missing required data in SQS message for Foreign processing.")

            # 2. VideosTable에서 originalScript 조회
            full_lang = LANG_CODE_MAP.get(learning_lang.lower())
            video_item = videos_table.get_item(Key={'lang': full_lang, 'SK': video_id}).get('Item')
            if not video_item or 'scene' not in video_item or 'lang-script' not in video_item['scene']:
                 raise ValueError(f"Could not find originalScript for videoId: {video_id}")
            original_script = video_item['scene']['lang-script']

            # 3. DynamoDB와 Transcribe Job 이름에 사용할 안전한 시간 문자열 생성
            safe_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S-%f")

            # 4. DynamoDB에 '처리 중' 상태의 임시 학습 기록 저장
            pk = f"{nickname}#{user_uuid}"
            sk = f"{learning_lang}#{safe_timestamp}#{uuid.uuid4().hex[:7]}"

            initial_item = {
                "PK": pk, "SK": sk, "videoId": video_id, "themeId": theme_id,
                "lang": learning_lang.upper(), "connectionId": connection_id,
                "status": "PROCESSING_FO", "originalScript": original_script,
                "gender": gender, "createdAt": datetime.now(timezone.utc).isoformat(),
                "userInput": { "voiceS3Key": user_audio_s3_key }
            }
            history_table.put_item(Item=initial_item)
            print(f"Initial Foreign history item created. PK: {pk}, SK: {sk}")

            # 5. Transcribe 작업 시작
            s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{user_audio_s3_key}"
            
            nickname_b64_padded = base64.urlsafe_b64encode(nickname.encode('utf-8')).decode('utf-8')
            nickname_b64 = nickname_b64_padded.rstrip('=')
            safe_pk_for_job = f"{nickname_b64}--{user_uuid}"
            
            job_name = f"foreign--{safe_pk_for_job}___{sk.replace('#','--')}"
            output_key = f"transcripts/{job_name}.json"

            language_code = TRANSCRIBE_LANG_MAP.get(learning_lang)
            if not language_code:
                raise ValueError(f"Unsupported language for Transcribe: {learning_lang}")

            transcribe.start_transcription_job(
                TranscriptionJobName=job_name,
                Media={'MediaFileUri': s3_uri},
                LanguageCode=language_code,
                OutputBucketName=AUDIO_BUCKET_NAME,
                OutputKey=output_key
            )
            
            # 6. 생성된 임시 기록에 transcribeJobName 업데이트
            history_table.update_item(
                Key={'PK': pk, 'SK': sk},
                UpdateExpression="SET transcribeJobName = :tj",
                ExpressionAttributeValues={':tj': job_name}
            )
            print(f"Started Foreign Transcribe job: {job_name}")

        except Exception as e:
            print(f"Error in start-foreign: {e}")
            continue