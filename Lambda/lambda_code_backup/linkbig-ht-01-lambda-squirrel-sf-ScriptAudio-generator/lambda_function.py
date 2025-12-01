import json
import boto3
import os

BUCKET_NAME = os.environ.get("BUCKET_NAME") 
DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
polly = boto3.client("polly")
s3_client = boto3.client("s3")


# --- 헬퍼 함수 (기존 코드 재사용) ---

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
        
        return {'script_audio_key': s3_key}
        
    except Exception as e:
        print(f"스크립트 오디오 생성 중 오류 발생 (SK: {sk}): {e}")
        raise RuntimeError(f"스크립트 오디오 생성 실패: {e}")


def generate_recommended_responses_audio(activities, sk):
    """
    RECOMMENDED_RESPONSES 활동 타입에 대해 Polly TTS를 호출하여 음성 파일을 생성하고 S3에 저장하며, 
    활동 데이터에 'audio_key' 필드를 추가합니다.
    """
    # RECOMMENDED_RESPONSES 활동 찾기
    activity = next((act for act in activities if act['activity_type'] == 'RECOMMENDED_RESPONSES'), None)
    
    if not activity:
        return {'RECOMMENDED_RESPONSES': []} # 활동이 없으면 빈 값 반환

    recommended_responses = activity.get('recommended_responses', [])
    final_responses = []
    
    for i, item in enumerate(recommended_responses):
        try:
            recommended_answer = item['recommended_answer']
            s3_key = f"contents/jp/test/audio/recommended_answer/{sk}_recommend_0{i+1}.mp3" 
            
            # Polly 호출 및 S3 저장 (기존 generate_audio_response 로직과 동일)
            speech = polly.synthesize_speech(
                 Text=recommended_answer,
                 OutputFormat="mp3",
                 VoiceId='Mizuki',
                 LanguageCode="ja-JP",
            )
            audio_stream = speech["AudioStream"].read()
            s3_client.put_object(
                 Bucket=BUCKET_NAME,
                 Key=s3_key,
                 Body=audio_stream,
                 ContentType="audio/mpeg"
            )
            item['audio_key'] = s3_key 
            final_responses.append(item)
            
        except Exception as e:
            print(f"오디오 처리 중 오류 발생 (Index {i}): {e}")
            final_responses.append(item) # 오류가 난 항목도 일단 포함
            continue
            
    # 처리 완료된 리스트를 활동 객체에 다시 할당
    activity['recommended_responses'] = final_responses
    
    # 스크립트 오디오와 분리하기 위해 활동 객체를 그대로 반환
    return activity 


def lambda_handler(event, context):
    iteration_count = event.get('iteration_count', 0)

    # Step Functions의 입력(event)에서 데이터 추출 (sf-content-validator 단계의 출력)
    sk = event.get('SK')
    lang_script = event.get('lang_script')
    validated_activities = event.get('validated_activities', [])
    
    # Step Functions Parallel State의 분기 역할에 따라 다르게 처리
    if context.function_name.endswith('ScriptAudio-generator'):
        # 스크립트 오디오 생성 분기
        audio_result = generate_script_audio(lang_script, sk)
        audio_result['iteration_count'] = iteration_count
        return audio_result
        
    elif context.function_name.endswith('RecommendedAudio-generator'):
        # 추천 답변 오디오 생성 분기
        audio_result = generate_recommended_responses_audio(validated_activities, sk)
        audio_result['iteration_count'] = iteration_count
        return audio_result

    else:
        # 이 Lambda는 병렬 분기로만 호출되어야 함
        raise RuntimeError("잘못된 호출: 이 Lambda는 병렬 분기로만 실행되어야 합니다.")