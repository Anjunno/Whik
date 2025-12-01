import boto3
import os
import json
from datetime import datetime, timezone

# --- AWS 클라이언트 및 DynamoDB 테이블 객체 초기화 ---
bedrock_runtime = boto3.client('bedrock-runtime')
polly_client = boto3.client('polly')
s3_client = boto3.client('s3')
dynamodb = boto3.resource('dynamodb')

# --- 환경 변수에서 리소스 이름 가져오기 ---
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
    'es': 'SPANISH'
}

def get_video_item(language_full_name, video_id):
    """
    'videos' 테이블에서 원본 학습 콘텐츠 정보를 조회합니다.
    """
    try:
        response = videos_table.get_item(Key={'lang': language_full_name, 'SK': video_id})
        return response.get('Item')
    except Exception as e:
        print(f"비디오 정보 조회 실패: {e}")
        return None

def create_bedrock_prompt(model_answer, user_text, user_gender, context):
    """
    대화의 맥락(context)과 사용자 성별 정보를 포함하여, 롤플레잉 상황에 맞는
    정교한 피드백을 유도하는 Bedrock 프롬프트를 생성합니다.
    """
    return f"""You are a helpful language tutor evaluating a student's response in a role-playing scenario. Your primary goal is to provide constructive, context-aware feedback.

    Here are the inputs for this evaluation:
    - Context for this role-play: "{context}"
    - A model answer sentence for this context: "{model_answer}"
    - The student's spoken sentence: "{user_text}"
    - The student's gender: "{user_gender}"

    Follow these rules STRICTLY:
    1.  First, determine if the student's sentence is a contextually appropriate response. It does not need to be identical to the model answer, but it must make sense in the conversation.
    2.  If the response is appropriate for the context, check for grammatical errors, unnatural phrasing, or pronunciation mistakes (based on the transcribed text).
    3.  Crucially, consider the student's gender. If the language has gender-specific expressions, check if the student's sentence is appropriate for their gender.
    4.  When creating the "corrected_sentence", you MUST preserve the student's original intent. Only fix the errors. Do not invent new content or change the core meaning.
    5.  Your response MUST be a single, valid JSON object and nothing else. Do not add any text before or after the JSON.

    JSON evaluation rules:
    - "star_rating": Use 3 stars if the sentence is contextually appropriate and grammatically perfect. Use 2 stars for any deviation.
    - "feedback_comment": Provide a specific and helpful comment in Korean.
    - "corrected_sentence": Provide the corrected full sentence that respects the student's original intent.

    JSON format:
    - "is_correct": A boolean value.
    - "feedback_comment": A constructive comment in Korean.
    - "star_rating": An integer, either 2 or 3.
    - "corrected_sentence": The corrected full sentence.

    --- EXAMPLE ---
    Context: "Ordering a drink"
    Model Answer: "じゃあ、ホットコーヒーをお願いします。"
    Student's (Male) Sentence: "フットのさつまいもラテをください。" (Intended: ホットのさつまいもラテをください。)
    JSON Response: {{"is_correct": false, "feedback_comment": "의미는 잘 전달되지만, 발음에 몇 가지 오류가 있어요. 'フット'는 'ホット'로, 'さつまいも'라고 발음해야 자연스럽습니다. 또한 'ください'보다 'お願いします'가 더 정중한 표현입니다.", "star_rating": 2, "corrected_sentence": "ホットのさつまいもラテをお願いします。"}}
    --- END OF EXAMPLE ---

    Now, provide the evaluation for the student's sentence.
    """

def process(transcribed_text, original_file_key, task_info):
    """
    외국어 음성을 평가하고, 결과를 'results' 테이블에 업데이트하며
    학습 히스토리를 'history' 테이블에 새로 생성합니다.
    """
    job_id = task_info.get('PK')
    print(f"외국어 처리 시작: '{transcribed_text}' | Job ID: {job_id}")
    
    try:
        # app.py에서 전달받은 task_info 딕셔너리에서 필요한 정보를 추출
        user_uuid = task_info.get('userId')
        user_gender_full = task_info.get('gender')
        language_code = task_info.get('language')
        theme_id = task_info.get('themeId')
        video_id = task_info.get('videoId')

        # 'videos' 테이블에서 학습 정보 조회
        language_full_name = LANGUAGE_FULL_NAME_MAP.get(language_code)
        video_item = get_video_item(language_full_name, video_id)
        if not video_item:
            raise Exception(f"Video item not found for lang: {language_full_name}, videoId: {video_id}")
            
        video_title = video_item.get('title', 'a conversation')
        model_answer_script = ""
        recommend_data = {}
        if 'recommend' in video_item:
            recommend_map = video_item.get('recommend')
            if 'male' in recommend_map or 'female' in recommend_map:
                recommend_data = recommend_map.get(user_gender_full, {})
            else:
                recommend_data = recommend_map
            model_answer_script = recommend_data.get('script', '')

        # Bedrock을 호출하여 사용자의 발화를 평가
        print("Bedrock으로 평가 중...")
        prompt = create_bedrock_prompt(model_answer_script, transcribed_text, user_gender_full, video_title)
        bedrock_response = bedrock_runtime.invoke_model(
            body=json.dumps({"anthropic_version": "bedrock-2023-05-31", "max_tokens": 1000, "messages": [{"role": "user", "content": prompt}]}),
            modelId='anthropic.claude-3-5-sonnet-20240620-v1:0', contentType='application/json', accept='application/json'
        )
        response_body = json.loads(bedrock_response.get('body').read())
        response_text = response_body['content'][0]['text']

        # Bedrock의 응답이 유효한 JSON인지 안전하게 파싱
        try:
            feedback_json = json.loads(response_text)
        except json.JSONDecodeError:
            print(f"Warning: Bedrock did not return valid JSON. Response: {response_text}")
            # JSON 파싱 실패 시, 기본 피드백 객체를 생성
            feedback_json = {
                "is_correct": False,
                "feedback_comment": "AI 평가 중 오류가 발생했습니다. 잠시 후 다시 시도해주세요.",
                "star_rating": 1,
                "corrected_sentence": model_answer_script
            }
        
        corrected_script = feedback_json.get('corrected_sentence', model_answer_script)

        # (Polly, S3, DynamoDB 업데이트 등 나머지 로직은 이전과 동일)
        user_gender_code = 'M' if user_gender_full == 'male' else 'F'
        voice_id = VOICE_MAP.get(language_code, {}).get(user_gender_code)
        polly_response = polly_client.synthesize_speech(
            Text=corrected_script, OutputFormat='mp3', VoiceId=voice_id
        )
        audio_output_key = f"processed-audios/{job_id}_correction.mp3"
        correction_audio_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{audio_output_key}"
        s3_client.put_object(
            Bucket=AUDIO_BUCKET_NAME, Key=audio_output_key,
            Body=polly_response['AudioStream'].read(), ContentType='audio/mpeg'
        )
        user_input_voice_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{original_file_key}"
        results_table.update_item(
            Key={'PK': job_id},
            UpdateExpression=(
                "SET #st = :s, #rt = :rt, #uit = :uit, #fb = :fb, "
                "#cas = :cas, #ra = :ra, #uvs = :uvs"
            ),
            ExpressionAttributeNames={
                '#st': 'status', '#rt': 'resultType', '#uit': 'userInputText',
                '#fb': 'feedback', '#cas': 'correctionAudioS3Uri', '#ra': 'recommendedAnswer',
                '#uvs': 'userInputVoiceS3Uri'
            },
            ExpressionAttributeValues={
                ':s': 'COMPLETED', ':rt': 'feedback', ':uit': transcribed_text,
                ':fb': feedback_json, ':cas': correction_audio_s3_uri, ':ra': recommend_data,
                ':uvs': user_input_voice_s3_uri
            }
        )
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
                'correctedText': corrected_script,
                'correctionAudioS3Uri': correction_audio_s3_uri
            },
            'feedback': {
                'starRating': feedback_json.get('star_rating'),
                'comment': feedback_json.get('feedback_comment')
            }
        })

        print("모든 처리 완료!")

    except Exception as e:
        print(f"외국어 처리 중 오류 발생: {e}")
        if job_id:
            results_table.update_item(
                Key={'PK': job_id},
                UpdateExpression="SET #st = :s, #err = :e",
                ExpressionAttributeNames={'#st': 'status', '#err': 'error'},
                ExpressionAttributeValues={':s': 'FAILED', ':e': str(e)}
            )
        raise e

