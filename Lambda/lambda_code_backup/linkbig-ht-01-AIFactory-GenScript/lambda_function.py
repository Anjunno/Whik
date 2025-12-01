import json
import os
import boto3
import logging
import re
from typing import List, Dict, Any

# --- 클라이언트 및 환경 변수 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Bedrock 런타임 클라이언트 (IAM Role을 통해 인증됨)
BEDROCK_REGION = os.environ.get("BEDROCK_REGION", 'us-east-1')
bedrock_runtime = boto3.client(
    service_name='bedrock-runtime', 
    region_name=BEDROCK_REGION
)

# ----------------------------------------------------------------------
# 헬퍼 함수
# ----------------------------------------------------------------------

def extract_json_from_text(text: str):
    """ LLM 응답 텍스트에서 JSON 객체({...}) 또는 배열([...]) 부분만 스마트하게 추출합니다. """
    try:
        # 가장 먼저 발견되는 '{' 또는 '[' 패턴을 찾아 추출
        match = re.search(r'(\{.*\}|\[.*\])', text, re.DOTALL)
        
        if match:
            json_str = match.group(0)
            return json.loads(json_str)
        
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def call_claude_to_generate_scripts(media_type: str, count: int, lang: str, topic: str) -> List[Dict]:
    """
    Claude 3.5 Sonnet을 호출하여 비디오 대본과 프롬프트 배열을 생성합니다.
    (가장 느린 작업이므로 SFN의 긴 타임아웃을 사용합니다.)
    """
    
    # [System Prompt]
    system_prompt = (
        "You are an expert scriptwriter for short educational videos, generating content for a Korean language learning app. "
        "Your task is to generate {count} unique, engaging scene descriptions and dialogues. "
        "The output MUST be a single JSON object with a key 'scripts' containing an array. Ensure all dialogue is in the target language. Respond ONLY with the JSON."
    )
    
    # [User Prompt] - 상세 지침
    user_prompt = (
        f"Generate {count} unique quotes and detailed scene descriptions from globally famous and **Korean-recognized** {media_type}.\n"
        f"**Theme/Topic:** {topic} / Output Language: {lang}.\n\n"
        
        f"**Mandatory Constraints (Verification and High Quality):**\n"
        f"1. **Primary Source:** Select titles based on their **high viewership and strong cross-generational appeal in Korea**.\n"
        f"   - **Acceptable Examples:** Ghibli, One Piece, Naruto, Parasite, Squid Game, or major recent K-Dramas/Movies.\n"
        f"2. **Style Exclusion:** **ABSOLUTELY EXCLUDE** low-resolution, niche, or aesthetically outdated content (e.g., 1990s/early 2000s low-budget animation, Yu-Gi-Oh, very old games).\n" 
        f"3. **Visual Focus:** Every 'scene_prompt' MUST be vivid, highly specific, and suitable for high-resolution cinematic video generation (Veo). The description must focus on a clear, recognizable **character close-up or medium shot**.\n"
        f"4. **Dialogue Length:** Dialogue must be short and impactful (max 15 words).\n\n"
        
        f"**Required Output Fields (ALL FIELDS MUST BE PRESENT):**\n"
        f"(1) 'source_title' (Original title of the work, e.g., '君の名は。'),\n"
        f"(2) 'source_title_kr' (The standard Korean translated title, e.g., '너의 이름은.'),\n" # 👈 [추가됨]
        f"(3) 'character_name' (Character's full or common name, e.g., '立花 瀧'),\n"
        f"(4) 'dialogue_text' (The quote in the target language: {lang}),\n"
        f"(5) 'dialogue_en' (The exact English translation of the dialogue),\n" 
        f"(6) 'emotion_tag' (Single word: ANGER, JOY, SADNESS, CONFUSION, etc.),\n"
        f"(7) 'scene_prompt' (A highly detailed, cinematic description for Veo and L3 image search).\n"
        "**Output Format MUST BE a single JSON object with a key 'scripts': [...]**."
    )

    try:
        response = bedrock_runtime.invoke_model(
            modelId='anthropic.claude-3-5-sonnet-20240620-v1:0', 
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31",
                "system": system_prompt,
                "messages": [{"role": "user", "content": user_prompt}],
                "max_tokens": 4000, 
                "temperature": 0.9 
            })
        )
        
        response_body = json.loads(response.get('body').read())
        content_text = response_body['content'][0]['text'].strip()
        
        # 🚨 [디버깅용 로그] Claude 응답 확인
        logger.info(f"--- Raw LLM Output ---\n{content_text[:500]}...\n----------------------")
        
        # 강력한 JSON 추출 로직 사용
        scripts_data = extract_json_from_text(content_text)
        
        if not scripts_data:
            raise ValueError(f"Failed to parse JSON from LLM. Raw text start: {content_text[:100]}")

        # 결과 반환 (배열 추출)
        if 'scripts' in scripts_data and isinstance(scripts_data['scripts'], list):
            return scripts_data['scripts']
        
        if isinstance(scripts_data, list):
            return scripts_data
            
        raise ValueError("LLM did not return a valid scripts array.")

    except Exception as e:
        logger.error(f"Claude 호출 또는 JSON 파싱 오류: {e}", exc_info=True)
        raise e

# --- 메인 핸들러 ---
def lambda_handler(event: Dict[str, Any], context):
    
    try:
        # 1. Step Function Input 추출
        job_id = event['jobId']
        count = event['videoCount']
        media_type = event['mediaType']
        lang = event['language']
        user_id = event['userId']
        contextual_topic = event['contextualTopic']

        logger.info(f"Job {job_id}: AI 대본 생성 시작. {count}개, Type: {media_type}")

        # 2. Claude 호출
        scripts_list = call_claude_to_generate_scripts(
            media_type, count, lang, contextual_topic
        )

        logger.info(f"Job {job_id}: Claude로부터 {len(scripts_list)}개 대본 수신 성공.")
        
        # 3. 결과 배열 생성 (Map 상태로 반환)
        final_items = []
        for script_item in scripts_list:
            final_items.append({
                "script": script_item, 
                "jobId": job_id, 
                "userId": user_id,
                "mediaType": media_type,
                "videoCount": count,
                "language": lang,
                "contextualTopic": contextual_topic
            })
        
        logger.info(f"Job {job_id}: {len(final_items)}개의 작업 항목 배열 반환.")
        
        return final_items

    except KeyError as e:
        logger.error(f"필수 입력 필드 누락: {e}")
        raise ValueError(f"SFN Input Error: {e}")
    except Exception as e:
        logger.error(f"Job {event.get('jobId', 'N/A')} 오류: {e}", exc_info=True)
        raise e