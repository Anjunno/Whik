import json
import boto3
import os
from decimal import Decimal
from urllib.parse import unquote_plus
import re
import random

# DynamoDB 특수 포맷 변환을 위한 Deserializer (권장)
from boto3.dynamodb.types import TypeDeserializer

BUCKET_NAME = os.environ.get("BUCKET_NAME") 
VIDEO_TABLE = os.environ.get("VIDEO_TABLE")
DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
polly = boto3.client("polly")

# 클라이언트 초기화
s3_client = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(VIDEO_TABLE)
# TTS 로직 제거로 polly_client 제거
bedrock_client = boto3.client("bedrock-runtime", region_name=DEFAULT_REGION) 

deserializer = TypeDeserializer()

# --- 헬퍼 함수 ---

def deserialize_dynamodb_record(dynamo_image):
    """DynamoDB Stream 포맷을 일반 Python 딕셔너리로 변환합니다."""
    if not dynamo_image:
        return {}
    return {k: deserializer.deserialize(v) for k, v in dynamo_image.items()}

def randomize_quiz_options(quiz_data):
    """
    LLM이 생성한 정답과 오답 텍스트를 받아 무작위로 섞고 answer_index를 설정합니다.
    """
    # LLM이 생성한 분리된 필드를 추출
    correct_option = quiz_data.pop('correct_option')
    incorrect_options = quiz_data.pop('incorrect_options')
    
    # 정답과 오답을 하나의 리스트로 합침
    all_options = incorrect_options + [correct_option]
    
    # 1. 옵션 리스트를 무작위로 섞음 (무작위 위치 결정)
    random.shuffle(all_options)
    
    # 2. 섞인 리스트에서 정답의 새로운 위치(인덱스)를 찾음
    try:
        answer_index = all_options.index(correct_option)
    except ValueError:
        # LLM이 정답을 제대로 포함하지 못했을 경우 (매우 드물지만 안전장치)
        raise ValueError("LLM 응답에 정답 텍스트가 포함되지 않았습니다.")
    
    # 3. 최종 구조에 반영
    quiz_data['options'] = all_options
    quiz_data['answer_index'] = answer_index

    return quiz_data


def call_bedrock_for_quiz_generation(lang_script, ko_script):
    """
    Bedrock LLM을 호출하여 learning_activities 리스트를 엄격한 포맷으로 생성합니다.
    (토큰 절약을 위해 prompt_text는 사용하지 않음)
    """
    print("Bedrock 호출: learning_activities 데이터 생성 요청 (최소 토큰 사용)...")
    
    # SYSTEM PROMPT: 규칙 및 출력 포맷 강제 (영어)
    system_prompt = (
        "You are an AI tutor generating Japanese learning content. Your output MUST be a valid JSON object, "
        "adhering strictly to the required schema and rules. "
        "The tip field must **never include the direct translation or definition of the answer**, "
        "only providing grammatical, nuanced, or contextual hints to guide the user's inference."
    )

    # USER QUERY: 지침 및 템플릿 구조 (영어)
    user_query = f"""
    Analyze the following scripts and generate a JSON object containing the 'learning_activities' list:
    - Original Japanese Script: "{lang_script}"
    - Correct Korean Translation: "{ko_script}"

    ## Generation Rules:
    1. activity_id 1 (COMPREHENSION_QUIZ): The correct option must match the Korean translation. Options must be 4.
        **DO NOT generate 'options' or 'answer_index'. Instead, generate 'correct_option' and 3 'incorrect_options' as separate fields.**
    2. activity_id 2 (SENTENCE_RECONSTRUCTION): Generate the 'chunks' list by dynamically splitting target_sentence("{lang_script}") into meaningful units (chunks). The number of chunks should be flexibly determined by the sentence length and complexity.
    3. activity_id 3 (RECOMMENDED_RESPONSES): Generate a 'recommended_responses' list with **exactly 2** example answers relevant to the context of the script. Each response must include 'recommended_answer' (Japanese), 'pronunciation' (Korean Romanization of the **Japanese Answer**), and 'korean_translation'.

    **CRITICAL RULE for activity_id 3 (Pronunciation): The value for 'pronunciation' MUST be the Korean reading/romanization of the 'recommended_answer' (Japanese text), NOT the romanization of the Korean translation. The pronunciation MUST include proper Korean spacing/word breaks for readability (e.g., '와타시모, 키미가 스키.' instead of '와타시모키미가스키.').**

    ## Final Output JSON Template (MUST adhere to this structure):
    {{
      "learning_activities": [
        {{
          "activity_id": 1,
          "activity_type": "COMPREHENSION_QUIZ",
          "correct_option": "<Correct_Translation_String>",
          "incorrect_options": ["<Incorrect_Option_1>", "<Incorrect_Option_2>", "<Incorrect_Option_3>"],
          "question": "영상을 보고 알맞은 의미를 찾아봐요!",
          "tip": "<COMPREHENSION Tip Generation>"
        }},
        {{
          "activity_id": 2,
          "activity_type": "SENTENCE_RECONSTRUCTION",
          "chunks": ["<chunk_1>", "<chunk_2>", "<chunk_n>"], 
          "question": "음성을 듣고, 단어 블록을 드래그하여 문장을 완성하세요。",
          "target_sentence": "{lang_script}",
          "tip": "<RECONSTRUCTION Tip Generation>"
        }},
        {{
          "activity_id": 4,
          "activity_type": "RECOMMENDED_RESPONSES",
          "recommended_responses": [
            {{
              "recommended_answer": "<Japanese_Response_1>",
              "pronunciation": "<Korean_Reading_of_Japanese_1_with_Spacing>",
              "korean_translation": "<Korean_Translation_1>"
            }},
            {{
              "recommended_answer": "<Japanese_Response_2>",
              "pronunciation": "<Korean_Reading_of_Japanese_2_with_Spacing>",
              "korean_translation": "<Korean_Translation_2>"
            }}
          ]
        }}
      ]
    }}

    The response MUST ONLY contain this JSON object.
    """
    try:
        # Claude 3 Sonnet 호출 (Bedrock)
        response = bedrock_client.invoke_model(
            modelId='anthropic.claude-3-sonnet-20240229-v1:0', 
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "messages": [{"role": "user", "content": user_query}],
                "max_tokens": 4096,
                "system": system_prompt,
                "anthropic_version": "bedrock-2023-05-31"
            })
        )
        
        response_body = json.loads(response['body'].read())
        json_string = response_body['content'][0]['text'].strip()
        
        # Bedrock이 생성한 JSON 문자열 파싱
        start_index = json_string.find('{')
        if start_index != -1:
            json_string = json_string[start_index:]
            
        return json.loads(json_string)

    except Exception as e:
        print(f"Bedrock 호출 실패: {e}")
        raise


#따라 말하기 음성파일 생성, 저장
def generate_script_audio(lang_script, sk):
    """
    원본 스크립트(lang_script)에 대해 Polly TTS를 호출하여 음성 파일을 생성하고 S3에 저장한 후, 
    S3 Key를 반환합니다.
    """
    s3_key = f"contents/jp/test/audio/script_audio/{sk}_script_audio.mp3"
    
    try:
        # 추천답변 음성파일 생성
        speech = polly.synthesize_speech(
            Text=lang_script,
            OutputFormat="mp3",
            VoiceId='Mizuki',
            LanguageCode="ja-JP",
        )
        # 스트림에서 오디오 데이터 읽기
        audio_stream = speech["AudioStream"].read()

        # S3 저장 
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=s3_key,
            Body=audio_stream,
            ContentType="audio/mpeg"
        )
        
        return s3_key
        
    except Exception as e:
        print(f"스크립트 오디오 생성 중 오류 발생 (SK: {sk}): {e}")
        raise RuntimeError(f"스크립트 오디오 생성 실패: {e}")
    

# 추천 답변 음성파일 생성 및 파일 키 추가
def generate_audio_response(activity, sk):
    """
    RECOMMENDED_RESPONSES 활동 타입에 대해 Polly TTS를 호출하여
    음성 파일을 생성하고 S3에 저장하며, 활동 데이터에 'audio_key' 필드를 추가합니다.
    Pre-signed URL 생성 로직은 제외합니다.
    """
    
    # 1. 'recommended_responses' 리스트 추출 
    try:
        recommended_responses = activity.pop('recommended_responses', [])
    except Exception:
        recommended_responses = activity.get('recommended_responses', [])

    if not recommended_responses:
        print("경고: 추천 응답 리스트가 비어 있습니다.")
        activity['recommended_responses'] = [] # 빈 리스트로 다시 설정
        return activity
    
    final_responses = []
    
    # 2. 각 추천 답변에 대해 TTS 생성, S3 저장
    for i, item in enumerate(recommended_responses):
        try:
            recommended_answer = item['recommended_answer']
            
            # S3 Key 지정
            s3_key = f"contents/jp/test/audio/recommended_answer/{sk}_recommend_0{i+1}.mp3" 
            
            print(f"Polly 호출: {recommended_answer}")

            # 추천답변 음성파일 생성
            speech = polly.synthesize_speech(
                Text=recommended_answer,
                OutputFormat="mp3",
                VoiceId='Mizuki',
                LanguageCode="ja-JP",
            )
            # 스트림에서 오디오 데이터 읽기
            audio_stream = speech["AudioStream"].read()

            # S3 저장 
            s3_client.put_object(
                Bucket=BUCKET_NAME,
                Key=s3_key,
                Body=audio_stream,
                ContentType="audio/mpeg"
            )

            # S3 Key 추가 (Pre-signed URL을 생성하지 않으므로 Key만 저장)
            item['audio_key'] = s3_key 
            
            final_responses.append(item)
            
        except Exception as e:
            print(f"오디오 처리 중 오류 발생 (Index {i}): {e}")
            # 오류가 발생한 항목도 일단 final_responses에 포함합니다. (Key 필드가 없는 상태)
            final_responses.append(item)
            continue
            
    # 3. 처리 완료된 리스트를 activity에 다시 할당
    activity['recommended_responses'] = final_responses
    
    # 4. 최종 활동 반환
    return activity


# --- 메인 Lambda 핸들러 ---

def lambda_handler(event, context):
    print("--- DynamoDB Stream 이벤트 수신 시작 ---")
    
    for record in event.get('Records', []):
        
        if record['eventName'] not in ['INSERT', 'MODIFY']:
            continue
            
        new_dynamo_image = record['dynamodb'].get('NewImage')
        if not new_dynamo_image:
            continue

        data = deserialize_dynamodb_record(new_dynamo_image)
        
        if data.get('status') != 'PENDING':
            print(f"SK: {data.get('SK')} - 상태가 PENDING이 아니므로 스킵합니다. 현재 상태: {data.get('status')}")
            continue

        try:
            sk = data.get('SK')
            PK = data.get('lang', 'JAPANESE') 
            
            lang_script = data.get('scene', {}).get('lang-script')
            ko_script = data.get('scene', {}).get('ko-script')
            
            if not lang_script or not ko_script:
                 raise ValueError("lang-script 또는 ko-script가 누락되었습니다. 자동화 중단.")

            print(f"✅ 자동화 시작. SK: {sk}, 스크립트: {lang_script}")

            # 1. Bedrock 호출 및 raw 데이터 생성
            bedrock_result = call_bedrock_for_quiz_generation(lang_script, ko_script)
            
            raw_activities = bedrock_result.get('learning_activities', [])
            
            if not raw_activities:
                 raise ValueError("Bedrock이 유효한 learning_activities를 생성하지 못했습니다.")
            
            final_activities = []
            
            # 2. 옵션 무작위화 및 최종 구조 변환
            for activity in raw_activities:
                if activity['activity_type'] == 'COMPREHENSION_QUIZ':
                    # COMPREHENSION_QUIZ만 무작위화 로직 적용
                    final_quiz_data = randomize_quiz_options(activity)
                    final_activities.append(final_quiz_data)
                
                elif activity['activity_type'] == 'SENTENCE_RECONSTRUCTION': 
                    # SENTENCE_RECONSTRUCTION은 그대로 추가
                    final_activities.append(activity)
                
                elif activity['activity_type'] == 'RECOMMENDED_RESPONSES': 
                    processed_activity = generate_audio_response(activity, sk)
                    final_activities.append(processed_activity) # 결과 반영

                # 모든 활동이 위에 정의되지 않은 경우도 대비하여 추가
                else:
                    final_activities.append(activity) 
            
            # --- 1. 스크립트 오디오 생성 및 활동 리스트에 추가 ---
            script_audio_key = generate_script_audio(lang_script, sk)
            
            # FOLLOW_THE_SCRIPT 활동 객체 생성
            follow_activity = {
                "activity_id": 3,
                "activity_type": "FOLLOW_THE_SCRIPT",
                "audio_key": script_audio_key
            }
            final_activities.append(follow_activity)

            # 3. DynamoDB 최종 업데이트 (READY 상태로 전환)
            table.update_item(
                Key={'lang': PK, 'SK': sk},
                UpdateExpression="SET learning_activities = :activities, #st = :new_status",
                ExpressionAttributeNames={'#st': 'status'},
                ExpressionAttributeValues={
                    ':activities': final_activities, # Python List/Dict -> DynamoDB L/M 변환은 boto3가 처리
                    ':new_status': 'READY'
                }
            )
            print(f"✅ 파이프라인 완료. SK: {sk} -> READY (activities 및 오디오 Key 생성 완료)")

        except Exception as e:
            # 4. 오류 발생 시 FAILED 상태로 업데이트
            print(f"❌ 자동화 파이프라인 실행 중 치명적 오류: {e}")
            table.update_item(
                Key={'lang': PK, 'SK': sk},
                UpdateExpression="SET #st = :fail_status, error_message = :error_msg",
                ExpressionAttributeNames={'#st': 'status'},
                ExpressionAttributeValues={':fail_status': 'FAILED', ':error_msg': f"LLM Error/Audio Error during generation: {str(e)}"}
            )
            
    return {'statusCode': 200, 'body': json.dumps('Stream records processed.')}