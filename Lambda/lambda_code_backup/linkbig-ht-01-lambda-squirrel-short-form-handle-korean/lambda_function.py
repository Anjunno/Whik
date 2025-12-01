import json
import os
import boto3
import urllib.parse
import base64
from decimal import Decimal

# Helper Class for JSON serialization
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            if o % 1 == 0:
                return int(o)
            else:
                return float(o)
        return super(DecimalEncoder, self).default(o)

# AWS Service Clients
s3_client = boto3.client('s3')
translate = boto3.client('translate')
polly = boto3.client('polly')
dynamodb = boto3.resource('dynamodb')
apigw_management = boto3.client(
    'apigatewaymanagementapi', 
    endpoint_url=os.environ['WEBSOCKET_ENDPOINT_URL']
)

# Environment Variables & Table Objects
HISTORY_TABLE_NAME = os.environ['HISTORY_TABLE_NAME']
VIDEOS_TABLE_NAME = os.environ['VIDEOS_TABLE_NAME']
AUDIO_BUCKET_NAME = os.environ['AUDIO_BUCKET_NAME']
history_table = dynamodb.Table(HISTORY_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# Language and Voice Mapping
VOICE_ID_MAP = {"ja": "Mizuki", "zh": "Zhiyu", "es": "Lucia"}
LANG_CODE_MAP = {"jp": "JAPANESE", "zh": "CHINESE", "es": "SPANISH"}

# Main Handler
def lambda_handler(event, context):
    print(f"handle-korean received event: {json.dumps(event, ensure_ascii=False)}")
    connection_id = None

    try:
        # 1. S3 이벤트에서 정보 추출 및 Job 이름 파싱
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        s3_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        job_name_from_key = s3_key.split('/')[-1].replace('.json', '')
        
        if not job_name_from_key.startswith('korean--'):
            return

        pk_b64_raw, sk_raw = job_name_from_key.replace('korean--', '').split('___')
        nickname_b64_unpadded, user_uuid_from_job = pk_b64_raw.split('--')
        
        nickname_b64 = nickname_b64_unpadded + '=' * (-len(nickname_b64_unpadded) % 4)
        nickname = base64.urlsafe_b64decode(nickname_b64).decode('utf-8')
        
        pk = f"{nickname}#{user_uuid_from_job}"
        sk = sk_raw.replace('--', '#')

        # 2. DynamoDB에서 '처리 중' 항목 조회
        history_item = history_table.get_item(Key={'PK': pk, 'SK': sk}).get('Item')
        if not history_item:
            raise ValueError(f"History item not found for PK:{pk}, SK:{sk}")
            
        connection_id = history_item.get('connectionId')
        gender = history_item.get('gender')
        learning_lang = history_item.get('lang', '').lower()
        video_id = history_item.get('videoId')

        # 3. Transcribe 결과 파일 읽기
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        transcript_content = s3_object['Body'].read().decode('utf-8')
        transcribed_text = json.loads(transcript_content)['results']['transcripts'][0]['transcript']
        print(f"Transcribed Korean text: {transcribed_text}")
        
        # 4. Amazon Translate로 번역
        target_lang_for_translate = learning_lang.replace('jp', 'ja')
        translate_response = translate.translate_text(Text=transcribed_text, SourceLanguageCode='ko', TargetLanguageCode=target_lang_for_translate)
        translated_text = translate_response['TranslatedText']

        # 5. Amazon Polly로 음성 합성 및 S3 저장
        voice_id = VOICE_ID_MAP.get(target_lang_for_translate)
        polly_response = polly.synthesize_speech(Text=translated_text, OutputFormat='mp3', VoiceId=voice_id)
        
        polly_s3_key = f"user/{user_uuid_from_job}/translations/{learning_lang}/{history_item.get('themeId')}/{sk.replace('#', '-')}.mp3"
        s3_client.put_object(Bucket=AUDIO_BUCKET_NAME, Key=polly_s3_key, Body=polly_response['AudioStream'].read(), ContentType='audio/mpeg')
        
        # 6. 추천 답변 조회
        video_item = get_video_item(learning_lang, video_id)
        recommended_answer = get_recommended_answer(video_item, gender)

        # 7. DynamoDB 학습 기록 '업데이트'
        feedback_payload = {
            "star-rating": 1,
            "comment": "",
            "translatedText": translated_text,
            "translatedAudioKey": polly_s3_key,
            "recommendedAnswer": recommended_answer
        }
        updated_item = history_table.update_item(
            Key={'PK': pk, 'SK': sk},
            UpdateExpression="SET #status = :s, userInput.script = :t, #feedback = :f",
            ExpressionAttributeNames={'#status': 'status', '#userInput': 'userInput', '#script': 'script', '#feedback': 'feedback'},
            ExpressionAttributeValues={':s': 'COMPLETED_KO', ':t': transcribed_text, ':f': feedback_payload},
            ReturnValues="ALL_NEW"
        )['Attributes']

        # 8. 클라이언트에 보낼 데이터에서 S3 Key를 Presigned URL로 변환
        feedback_data = updated_item.get('feedback', {})
        
        if feedback_data.get('translatedAudioKey'):
            key = feedback_data['translatedAudioKey']
            feedback_data['translatedAudioUrl'] = s3_client.generate_presigned_url('get_object', Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': key}, ExpiresIn=3600)
            del feedback_data['translatedAudioKey']

        rec_answer = feedback_data.get('recommendedAnswer', {})
        if rec_answer and isinstance(rec_answer, dict) and rec_answer.get('s3Url'):
            key = rec_answer['s3Url']
            rec_answer['s3Url'] = s3_client.generate_presigned_url('get_object', Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': key}, ExpiresIn=3600)

        # 9. 클라이언트에게 최종 결과 피드백 전송
        feedback_message = {"type": "korean_feedback", "status": "success", "data": updated_item}
        send_ws_message(connection_id, feedback_message)

    except Exception as e:
        print(f"Error in handle-korean: {e}")
        if connection_id:
            send_ws_message(connection_id, {"type": "error", "message": "An error occurred."})

# --- Helper Functions ---
def get_video_item(lang_code, video_id):
    full_lang = LANG_CODE_MAP.get(lang_code.lower())
    if not full_lang: return None
    response = videos_table.get_item(Key={'lang': full_lang, 'SK': video_id})
    return response.get('Item')

def get_recommended_answer(video_item, gender):
    if not video_item: return None
    recommend_data = video_item.get('recommend')
    if not recommend_data: return None
    if 'male' in recommend_data and 'female' in recommend_data:
        return recommend_data.get(gender)
    else:
        return recommend_data

def send_ws_message(connection_id, message):
    try:
        apigw_management.post_to_connection(
            Data=json.dumps(message, cls=DecimalEncoder, ensure_ascii=False).encode('utf-8'),
            ConnectionId=connection_id
        )
    except apigw_management.exceptions.GoneException:
        print(f"Connection {connection_id} no longer exists.")
    except Exception as e:
        print(f"Failed to send message: {e}")