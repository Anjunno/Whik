import json
import boto3
import os

DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
bedrock_client = boto3.client("bedrock-runtime", region_name=DEFAULT_REGION)

def call_bedrock_for_quiz_generation(lang_script, ko_script):
  """
  Bedrock LLM을 호출하여 learning_activities 리스트를 엄격한 포맷으로 생성합니다.
  """
  print("Bedrock 호출: learning_activities 데이터 생성 요청 (최소 토큰 사용)...")
  
  system_prompt = (
    "You are an expert AI tutor generating Japanese learning content for BEGINNER LEVEL students. "
    "Your output MUST be a valid JSON object strictly following the required schema. "
    "All tips MUST follow the TIP RULES described in the user instructions. "
    "Tips must be structural, simple, and beginner-friendly, and must never reveal or hint at the sentence's meaning. "
    "Pronunciation MUST always be the Korean reading of the Japanese text (never the Korean translation)."
  )


  # user_query 
  system_prompt = (
    "You are an expert AI tutor generating Japanese learning content for BEGINNER LEVEL students. "
    "Your output MUST be a valid JSON object strictly following the required schema. "
    "Tips MUST be structural, simple, and beginner-friendly. "
    "Tips must NEVER reveal, hint, or reference the sentence's meaning or Korean translation. "
    "Korean text in tips is allowed. "
    "Pronunciation MUST always be the Korean reading of the Japanese text (never the Korean translation)."
  )

  # user_query
  user_query = f"""
  Generate a JSON object containing the 'learning_activities' list.
  - Original Japanese Script: "{lang_script}"
  - Correct Korean Translation: "{ko_script}"

  ---------------------------------------------------------
  ## Activity Generation Rules
  ---------------------------------------------------------

  ### 1) COMPREHENSION_QUIZ (activity_id: 1)
  - correct_option MUST exactly match the Korean translation.
  - Provide 3 incorrect_options (total 4 choices).
  - DO NOT generate 'options' or 'answer_index'.

  ### TIP RULES (QA-FRIENDLY)
  Tips MUST NOT:
  - reveal, paraphrase, or hint at the meaning of the sentence.
  - mention, imply, or be influenced by the Korean translation.
  - reuse or transform vocabulary or semantics from the script.

  Tips MUST:
  - describe only sentence structure (particles, endings, word order, punctuation).
  - be simple and beginner-friendly (JLPT N5–N4).
  - be in Korean.
  - small structural hints (like “문장의 끝이 ‘か’로 끝납니다”) are allowed.

  Examples of allowed tips:
  - "'は'는 주제를 나타내는 조사입니다."
  - "주어와 목적어의 순서를 확인해보세요."
  - "문장의 끝이 ‘か’로 끝나면 의문형입니다."

  ### 2) SENTENCE_RECONSTRUCTION (activity_id: 2)
  - Split the target sentence (“{lang_script}”) into meaningful units ("chunks").
  - Number of chunks is flexible.
  - Tip rules:
      * Provide a structural hint about Japanese sentence formation
      * Focus especially on polite endings or question markers
      * MUST NOT be generic (“Just put the words in order”)
      * These tips MUST also follow all TIP RULES above.

  ### 3) RECOMMENDED_RESPONSES (activity_id: 4)
  - Provide exactly 2 responses.
  - Each response MUST contain:
      * recommended_answer (Japanese)
      * pronunciation (Korean transliteration WITH proper spacing)
      * korean_translation (natural Korean meaning)
  - Pronunciation rules (CRITICAL):
      * MUST be based on the Japanese recommended_answer.
      * MUST NOT contain Korean translation words (“네”, “좋아요”, etc.).
      * MUST use readable spacing (예: “하이, 다이죠부 데스.”).

  ---------------------------------------------------------
  ## Output JSON (MUST FOLLOW EXACTLY)
  ---------------------------------------------------------

  {{
    "learning_activities": [
      {{
        "activity_id": 1,
        "activity_type": "COMPREHENSION_QUIZ",
        "correct_option": "<Correct_Translation_String>",
        "incorrect_options": ["<Incorrect_Option_1>", "<Incorrect_Option_2>", "<Incorrect_Option_3>"],
        "question": "영상을 보고 알맞은 의미를 찾아봐요!",
        "tip": "<Tip text>"
      }},
      {{
        "activity_id": 2,
        "activity_type": "SENTENCE_RECONSTRUCTION",
        "chunks": ["<chunk_1>", "<chunk_2>", "<chunk_n>"],
        "question": "음성을 듣고, 단어 블록을 드래그하여 문장을 완성하세요.",
        "target_sentence": "{lang_script}",
        "tip": "<Tip text>"
      }},
      {{
        "activity_id": 4,
        "activity_type": "RECOMMENDED_RESPONSES",
        "recommended_responses": [
          {{
            "recommended_answer": "<Japanese_Response_1>",
            "pronunciation": "<Korean_Reading_1>",
            "korean_translation": "<Korean_Translation_1>"
          }},
          {{
            "recommended_answer": "<Japanese_Response_2>",
            "pronunciation": "<Korean_Reading_2>",
            "korean_translation": "<Korean_Translation_2>"
          }}
        ]
      }}
    ]
  }}

  The response MUST ONLY contain the JSON object above — no explanations.
  """



  try:
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
    
    # JSON 시작점 찾기 (Claude가 "" 밖 텍스트 넣는 경우 방어)
    start_index = json_string.find('{')
    if start_index != -1:
      json_string = json_string[start_index:]
        
    return json.loads(json_string)

  except Exception as e:
    print(f"Bedrock 호출 실패: {e}")
    raise RuntimeError(f"Bedrock API/JSON 파싱 실패: {e}")


def lambda_handler(event, context):
    
    lang_script = event.get('lang_script')
    ko_script = event.get('ko_script')
    PK = event.get('PK')
    SK = event.get('SK')
    # 루프 관리를 위해 iteration_count를 추출.
    iteration_count = event.get('iteration_count', 0)

    if not lang_script or not ko_script:
      raise ValueError("lang-script 또는 ko-script가 Step Functions 입력에서 누락되었습니다.")

    print(f"Bedrock 호출 시작. SK: {SK}, 스크립트: {lang_script}")

    # 1. Bedrock 호출
    bedrock_result = call_bedrock_for_quiz_generation(lang_script, ko_script)
    
    raw_activities = bedrock_result.get('learning_activities', [])

    if not raw_activities:
      raise ValueError("Bedrock이 유효한 learning_activities를 생성하지 못했습니다.")

    # 다음 단계(Validation)에 필요한 데이터 반환 (모든 데이터를 평탄하게 유지)
    return {
      'PK': PK,
      'SK': SK,
      'lang_script': lang_script,
      'ko_script': ko_script,
      'raw_activities': raw_activities, # LLM이 생성한 Raw 데이터
      
      # 2. 중요: 루프 관리를 위해 iteration_count를 그대로 전달합니다.
      'iteration_count': iteration_count
    }