import json
import boto3
import os

DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
# Bedrock 클라이언트 초기화 (외부 API 호출용)
bedrock_client = boto3.client("bedrock-runtime", region_name=DEFAULT_REGION)

def call_bedrock_for_full_qa(lang_script: str, activities_to_qa: dict) -> str:
    """
    Bedrock LLM을 호출하여 퀴즈 팁의 품질과 추천 답변의 논리적 정확성을 검증합니다.
    (반환: 'PASS' 또는 'FAILED: [이유_TYPE]')
    """
    print("Bedrock 호출: 포괄적 의미/논리 QA 요청...")
    
    system_prompt = (
        "You are an expert AI quality assurance specialist for Japanese language learning content. "
        "Your task is to perform a quality check on the provided learning activities (quiz tip and responses). "
        "Follow the output format strictly."
    )

    # USER QUERY: QA 지침 및 템플릿
    quiz_data = activities_to_qa.get('COMPREHENSION_QUIZ')
    responses_data = activities_to_qa.get('RECOMMENDED_RESPONSES')

    user_query = f"""
    Perform a Quality Assurance (QA) check on the following content against the rules.
    The primary script being taught is: "{lang_script}"

    --- Content to QA ---
    # 1. Comprehension Quiz Tip
    - Correct Answer (Translation): "{quiz_data.get('correct_option', 'N/A')}"
    - Tip Text: "{quiz_data.get('tip', 'N/A')}"

    # 2. Recommended Responses (for speaking practice)
    {json.dumps(responses_data.get('recommended_responses', []), indent=2, ensure_ascii=False)}

    --- Evaluation Rules ---

    ## Rule Set A: Quiz Tip Quality (RELAXED, 의미 힌트 허용)
    1. TIP VALIDITY:
    - The tip is considered PASS unless it **literally contains the full correct answer text** (exact match).
    - Minor hints, grammar markers, sentence endings, 조사, punctuation are ALLOWED.
    - Small meaning cues or usage intent (e.g., 정중한 제안, 요청, 부탁 등) are also ALLOWED.
    - Korean text in tips is fully allowed.
    - Do NOT fail tips for any indirect hint, structural clue, or polite/meaning nuance.

    ## Rule Set B: Recommended Responses Accuracy (STRICT)
    1. Pronunciation Integrity:
    - Only the Korean phonetic transcription of Japanese is allowed.
    - Korean meaning words (네, 좋아요, 예, 아니요) must NOT be included.
    - Any spacing or minor transcription variation is OK.
    - Do NOT fail if pronunciation uses Korean syllables to represent Japanese sounds, even if some syllables look like Korean words.

    2. Translation Accuracy:
    - korean_translation must be an accurate and natural meaning of the recommended_answer.

    --- Output Format (CRITICAL) ---
    If ALL rules are satisfied, output: PASS
    If ANY rule is violated, output: FAILED:<ACTIVITY_TYPE>:<REASON>
    - ACTIVITY_TYPE must be TIP or RESPONSES
    - REASON must be a concise failure cause.

    The response MUST ONLY contain:
    PASS
    OR
    FAILED:<ACTIVITY_TYPE>:<REASON>
    """


    try:
        # Claude 3 Sonnet 호출 (Bedrock)
        response = bedrock_client.invoke_model(
            modelId='anthropic.claude-3-sonnet-20240229-v1:0', 
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "messages": [{"role": "user", "content": user_query}],
                "max_tokens": 512,
                "system": system_prompt,
                "anthropic_version": "bedrock-2023-05-31"
            })
        )
        
        response_body = json.loads(response['body'].read())
        result_text = response_body['content'][0]['text'].strip()
        
        return result_text

    except Exception as e:
        print(f"Bedrock QA API 호출 실패 (API Error): {e}")
        # API 호출 자체 실패는 Runtime Error로 Step Functions Catch로 이동
        raise RuntimeError(f"Bedrock QA API 호출 실패: {e}")


def lambda_handler(event, context):
    """
    Validates Activities를 Bedrock에 전달하여 포괄적인 의미적 QA를 수행합니다.
    """
    # iteration_count 추출 (Check_Iteration_Count에서 사용됨)
    iteration_count = event.get('iteration_count', 0)
    
    validated_activities = event.get('validated_activities', [])
    lang_script = event.get('lang_script')
    
    # 필요한 메타데이터 추출 (다음 단계로 전달하기 위해)
    PK = event.get('PK')
    SK = event.get('SK')
    ko_script = event.get('ko_script')
    
    print(f"Bedrock QA Validation 시작. SK: {SK}")

    # Bedrock에 전달하기 위해 QA가 필요한 활동 데이터를 구조화
    activities_to_qa = {}
    
    for activity in validated_activities:
        activity_type = activity.get('activity_type')
        if activity_type == 'COMPREHENSION_QUIZ':
            activities_to_qa['COMPREHENSION_QUIZ'] = activity
        elif activity_type == 'RECOMMENDED_RESPONSES':
            activities_to_qa['RECOMMENDED_RESPONSES'] = activity

    # 2. Bedrock 호출 (모든 QA 대상 활동을 한 번에 검증)
    qa_result = call_bedrock_for_full_qa(lang_script, activities_to_qa)
    
    if qa_result.startswith('FAILED'):
        # Bedrock 검증 실패 시, 치명적 오류 발생 -> Step Functions Catch로 이동
        raise ValueError(f"교육적 오류 (Bedrock QA): 콘텐츠 품질 검증 실패. 결과: {qa_result}")
    
    if qa_result != 'PASS':
        # PASS, FAILED 외 다른 이상한 결과가 나왔을 때 (LLM 출력 형식 오류)
        raise ValueError(f"Bedrock QA 응답 형식 오류: 'PASS' 또는 'FAILED:'로 시작하지 않습니다. 응답: {qa_result}")

    # 3. 검증 완료된 데이터 반환
    # 이 Lambda는 데이터를 변경하지 않고 다음 단계로 전달합니다.
    return {
        'PK': PK,
        'SK': SK,
        'lang_script': lang_script,
        'ko_script': ko_script,
        'validated_activities': validated_activities, 
        
        # iteration_count를 출력에 포함하여 다음 단계로 전달합니다.
        'iteration_count': iteration_count
    }