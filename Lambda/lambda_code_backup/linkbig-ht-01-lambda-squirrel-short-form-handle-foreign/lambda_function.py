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
bedrock_runtime = boto3.client('bedrock-runtime')
polly = boto3.client('polly')
dynamodb = boto3.resource('dynamodb')
apigw_management = boto3.client('apigatewaymanagementapi', endpoint_url=os.environ['WEBSOCKET_ENDPOINT_URL'])

# Environment Variables & Table Objects
HISTORY_TABLE_NAME = os.environ['HISTORY_TABLE_NAME']
VIDEOS_TABLE_NAME = os.environ['VIDEOS_TABLE_NAME']
AUDIO_BUCKET_NAME = os.environ['AUDIO_BUCKET_NAME']
history_table = dynamodb.Table(HISTORY_TABLE_NAME)
videos_table = dynamodb.Table(VIDEOS_TABLE_NAME)

# Language and Voice Mapping
VOICE_ID_MAP = {"jp": "Mizuki", "zh": "Zhiyu", "es": "Lucia"}
LANG_CODE_MAP = {"jp": "JAPANESE", "zh": "CHINESE", "es": "SPANISH"}

# Main Handler
def lambda_handler(event, context):
    print(f"handle-foreign received event: {json.dumps(event, ensure_ascii=False)}")
    connection_id = None

    try:
        # 1. S3 이벤트에서 정보 추출 및 Job 이름 파싱
        record = event['Records'][0]
        bucket_name = record['s3']['bucket']['name']
        s3_key = urllib.parse.unquote_plus(record['s3']['object']['key'])
        
        job_name_from_key = s3_key.split('/')[-1].replace('.json', '')
        
        if not job_name_from_key.startswith('foreign--'):
            return

        pk_b64_raw, sk_raw = job_name_from_key.replace('foreign--', '').split('___')
        nickname_b64_unpadded, user_uuid_from_job = pk_b64_raw.split('--')
        
        nickname_b64 = nickname_b64_unpadded + '=' * (-len(nickname_b64_unpadded) % 4)
        nickname = base64.urlsafe_b64decode(nickname_b64).decode('utf-8')
        
        pk = f"{nickname}#{user_uuid_from_job}"
        sk = sk_raw.replace('--', '#')

        # 2. DynamoDB에서 '처리 중'이던 항목 조회
        history_item = history_table.get_item(Key={'PK': pk, 'SK': sk}).get('Item')
        if not history_item:
            raise ValueError(f"History item not found for PK:{pk}, SK:{sk}")
            
        connection_id = history_item.get('connectionId')
        gender = history_item.get('gender')
        learning_lang = history_item.get('lang', '').lower()
        video_id = history_item.get('videoId')
        original_script = history_item.get('originalScript')

        # 3. Transcribe 결과 파일 읽기
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        transcript_content = s3_object['Body'].read().decode('utf-8')
        transcribed_text = json.loads(transcript_content)['results']['transcripts'][0]['transcript']
        
        # 4. Bedrock으로 평가
        prompt = create_bedrock_prompt(original_script, transcribed_text)
        bedrock_response = bedrock_runtime.invoke_model(
            body=json.dumps({"anthropic_version": "bedrock-2023-05-31", "max_tokens": 1000, "messages": [{"role": "user", "content": prompt}]}),
            modelId='anthropic.claude-3-sonnet-20240229-v1:0', contentType='application/json', accept='application/json'
        )
        response_body = json.loads(bedrock_response.get('body').read())
        bedrock_feedback_json = json.loads(response_body['content'][0]['text'])

        # 5. Bedrock이 제안한 교정 문장으로 Polly 음성 합성
        corrected_script = bedrock_feedback_json.get('corrected_sentence', original_script)
        voice_id = VOICE_ID_MAP.get(learning_lang)
        polly_response = polly.synthesize_speech(Text=corrected_script, OutputFormat='mp3', VoiceId=voice_id)
        
        polly_s3_key = f"user/{user_uuid_from_job}/corrections/{learning_lang}/{history_item.get('themeId')}/{sk.replace('#', '-')}.mp3"
        s3_client.put_object(Bucket=AUDIO_BUCKET_NAME, Key=polly_s3_key, Body=polly_response['AudioStream'].read(), ContentType='audio/mpeg')
        bedrock_feedback_json['correctionAudioKey'] = polly_s3_key

        # 6. 추천 답변 조회
        video_item = get_video_item(learning_lang, video_id)
        recommended_answer = get_recommended_answer(video_item, gender)
        bedrock_feedback_json['recommendedAnswer'] = recommended_answer

        # 7. DynamoDB 학습 기록 '업데이트'
        updated_item = history_table.update_item(
            Key={'PK': pk, 'SK': sk},
            UpdateExpression="SET #status = :s, #userInput.#script = :t, #feedback = :f",
            ExpressionAttributeNames={
                '#status': 'status',
                '#userInput': 'userInput',
                '#script': 'script',
                '#feedback': 'feedback'
            },
            ExpressionAttributeValues={
                ':s': 'COMPLETED_FO',
                ':t': transcribed_text,
                ':f': bedrock_feedback_json
            },
            ReturnValues="ALL_NEW"
        )['Attributes']

        # 8. 피드백에 포함된 S3 Key들을 Presigned URL로 변환
        # feedback 객체에서 recommendedAnswer를 꺼내서 data 객체의 최상위로 이동
        if 'feedback' in updated_item and 'recommendedAnswer' in updated_item['feedback']:
            updated_item['recommendedAnswer'] = updated_item['feedback'].pop('recommendedAnswer', None)

        # 사용자 원본 음성 Presigned URL 생성
        if updated_item.get('userInput', {}).get('voiceS3Key'):
            key = updated_item['userInput']['voiceS3Key']
            updated_item['userInput']['voiceS3Url'] = s3_client.generate_presigned_url('get_object', Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': key}, ExpiresIn=3600)
            del updated_item['userInput']['voiceS3Key']

        # Polly 교정 음성 Presigned URL 생성
        feedback_data = updated_item.get('feedback', {})
        if feedback_data.get('correctionAudioKey'):
            key = feedback_data['correctionAudioKey']
            feedback_data['correctionAudioUrl'] = s3_client.generate_presigned_url('get_object', Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': key}, ExpiresIn=3600)
            del feedback_data['correctionAudioKey']

        # 추천 답변 음성 Presigned URL 생성 (이제 최상위 객체)
        rec_answer = updated_item.get('recommendedAnswer', {})
        if rec_answer and isinstance(rec_answer, dict) and rec_answer.get('s3Url'):
            key = rec_answer['s3Url']
            rec_answer['s3Url'] = s3_client.generate_presigned_url('get_object', Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': key}, ExpiresIn=3600)

        # 9. 클라이언트에게 최종 결과 피드백 전송
        feedback_message = {"type": "foreign_feedback", "status": "success", "data": updated_item}
        
        # 테스트를 위해 최종 응답 내용 출력
        print("--- Final message to be sent to client ---")
        print(json.dumps(feedback_message, cls=DecimalEncoder, ensure_ascii=False, indent=2))
        print("------------------------------------------")
        
        send_ws_message(connection_id, feedback_message)

    except Exception as e:
        print(f"Error in handle-foreign: {e}")
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

def create_bedrock_prompt(original, user_text):
    """Bedrock 평가를 위한 프롬프트를 생성합니다."""
    return f"""You are a friendly and precise language tutor. Your ONLY task is to compare two sentences and provide feedback in a strict JSON format.

    Here are your inputs:
    - The original correct sentence: "{original}"
    - The student's spoken sentence: "{user_text}"

    Follow these rules STRICTLY:
    1. Directly compare the student's sentence to the original sentence.
    2. Do NOT invent a context or story. If the sentences are completely different, simply state that they are different.
    3. Provide your feedback ONLY in the following JSON format. Do not add any text before or after the JSON object.

    JSON evaluation rules:
    - "star_rating": Use 3 stars ONLY for a perfect or near-perfect match. Use 2 stars for any deviation.
    - "feedback_comment": If the rating is 2, you MUST provide a specific and helpful comment in Korean explaining what was wrong or awkward. If the rating is 3, provide a short, encouraging comment in Korean.

    JSON format:
    - "is_correct": A boolean value.
    - "feedback_comment": A constructive comment in Korean.
    - "star_rating": An integer, either 2 or 3.
    - "corrected_sentence": The corrected full sentence if there are errors. If the sentence is perfect, return the original sentence.

    --- EXAMPLES ---
    Example 1 (Japanese, Imperfect):
    Original: "何になさいますか？"
    Student: "何をしますか？"
    JSON Response: {{"is_correct": false, "feedback_comment": "'何をしますか'는 '무엇을 합니까?'라는 의미로, 문법은 맞지만 주문받는 상황에서는 어색하게 들릴 수 있어요. '何になさいますか？'가 더 자연스러운 표현입니다.", "star_rating": 2, "corrected_sentence": "何になさいますか？"}}

    Example 2 (Japanese, Perfect):
    Original: "こんにちは"
    Student: "こんにちは"
    JSON Response: {{"is_correct": true, "feedback_comment": "완벽한 발음과 억양입니다!", "star_rating": 3, "corrected_sentence": "こんにちは"}}

    Example 3 (Chinese, Imperfect):
    Original: "请问您想喝点什么？"
    Student: "你想喝什么？"
    JSON Response: {{"is_correct": false, "feedback_comment": "의미는 전달되지만, '您'을 사용하여 더 공손하게 표현하는 것이 좋습니다.", "star_rating": 2, "corrected_sentence": "请问您想喝点什么？"}}
    
    Example 4 (Spanish, Completely Different):
    Original: "Hola, ¿cómo estás?"
    Student: "Gracias."
    JSON Response: {{"is_correct": false, "feedback_comment": "발화하신 문장은 원문과 의미가 완전히 다릅니다. 원문은 '안녕하세요, 어떻게 지내세요?'라는 인사였습니다.", "star_rating": 2, "corrected_sentence": "Hola, ¿cómo estás?"}}
    --- END OF EXAMPLES ---

    Now, provide the evaluation for the student's sentence in the requested JSON format.
    """

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