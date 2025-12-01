import json
import os
import boto3
import logging
import requests
import base64
import re
from typing import List, Dict, Any
from googleapiclient.discovery import build # Google Custom Search API Client

# --- 클라이언트 및 환경 변수 초기화 ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

BEDROCK_REGION = os.environ.get("BEDROCK_REGION", 'us-east-1')
bedrock_runtime = boto3.client(
    service_name='bedrock-runtime', 
    region_name=BEDROCK_REGION
)

# [필수 환경 변수 로드]
SEARCH_API_KEY = os.environ.get("GOOGLE_SEARCH_API_KEY")
SEARCH_CX_ID = os.environ.get("GOOGLE_SEARCH_CX_ID")

if not all([SEARCH_API_KEY, SEARCH_CX_ID]):
    logger.error("!!! CRITICAL: Google Search API 환경 변수가 누락되었습니다.")
    raise EnvironmentError("Missing required environment variables for Google Search/VLM.")


def generate_search_query(script: Dict) -> str:
    """ 
    AI 대본에서 작품명, 캐릭터, 대사를 조합하여 최적의 검색 쿼리를 생성합니다. 
    (한국어 제목이 있다면 최우선으로 사용합니다.)
    """
    title_orig = script.get('source_title', '')      
    title_kr = script.get('source_title_kr', '') # L2에서 넘어온 한국어 제목
    character = script.get('character_name', '') 
    scene_prompt = script.get('scene_prompt', '')
    
    # 1. [최우선] 한국어 제목 + 캐릭터 조합 (한국인 인지도 및 검색 효율 극대화)
    if title_kr and character:
        return f"{title_kr} {character} 스틸컷 (screenshot)" 

    # 2. [차선] 원본 제목 + 캐릭터 조합
    if title_orig and character:
        return f"{title_orig} {character} movie screenshot"
    
    # 3. [차선] 가장 잘 알려진 제목만 사용
    known_title = title_kr or title_orig
    if known_title:
        return f"{known_title} 영화 장면"
        
    # 4. [최후의 수단] (Fallback)
    if scene_prompt:
        return f"{scene_prompt[:100]}, cinematic photo"
        
    return "cinematic movie close-up shot"


def search_images_from_google(query: str) -> List[str]:
    """ 
    Google Custom Search API를 호출하여 10개를 가져온 뒤, 유효한 HTTPS 링크 상위 5개를 반환합니다.
    """
    try:
        service = build("customsearch", "v1", developerKey=SEARCH_API_KEY)
        
        # 넉넉하게 10개 요청
        res = service.cse().list(
            q=query,
            cx=SEARCH_CX_ID,
            searchType='image', 
            num=10, 
            safe='off' 
        ).execute()
        
        raw_items = res.get('items', [])
        
        # [디버깅] 전체 검색 결과 로그 출력
        all_links = [item.get('link', 'N/A') for item in raw_items]
        logger.info(f"🔎 Google Raw Search Results ({len(all_links)}): {json.dumps(all_links, indent=2)}")

        valid_urls = []
        # 필터링 로직: https/http로 시작하는 것만 수집
        for item in raw_items:
            link = item.get('link', '')
            if link.startswith('https://') or link.startswith('http://'):
                valid_urls.append(link)
                
            if len(valid_urls) >= 5:
                break
        
        logger.info(f"✅ Filtered Valid URLs ({len(valid_urls)}): {json.dumps(valid_urls, indent=2)}")
        
        return valid_urls
        
    except Exception as e:
        logger.error(f"Google Search API Fail: {e}")
        return []


def download_and_encode_image(url: str) -> str:
    """ URL에서 이미지를 다운로드하고 Base64 문자열로 반환합니다. """
    try:
        # 타임아웃을 3초로 짧게 설정
        response = requests.get(url, timeout=3)
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생
        return base64.b64encode(response.content).decode('utf-8')
    except Exception as e:
        logger.warning(f"이미지 다운로드 실패 ({url}): {e}")
        return ""


def select_best_image_from_vlm(job_context: Dict, candidate_urls: List[str]) -> str:
    """ Claude 3 Haiku VLM을 호출하여 최적의 이미지 1개 선정 """
    
    # [L2에서 넘어온 상세 정보 추출]
    script_data = job_context['script']
    scene_prompt = script_data.get('scene_prompt', 'Cinematic shot')
    character_name = script_data.get('character_name', 'main character')
    emotion_tag = script_data.get('emotion_tag', 'neutral')
    
    # 1. 다운로드 & 인코딩
    base64_list = []
    working_urls = []
    
    for url in candidate_urls:
        encoded = download_and_encode_image(url)
        if encoded:
            base64_list.append(encoded)
            working_urls.append(url)
    
    if not base64_list:
        logger.error("다운로드 가능한 이미지가 없습니다.")
        return ""
        
    # 2. VLM 프롬프트 구성 (단일 메시지 구조)
    content_list = []
    
    for idx, b64 in enumerate(base64_list):
        content_list.append({
            "type": "image", 
            "source": {
                "type": "base64", 
                "media_type": "image/jpeg", 
                "data": b64
            }
        })
        content_list.append({
            "type": "text", 
            "text": f"Image {idx+1}"
        })
    
    # 3. 최종 질문: VLM에게 강력한 거부 조건과 선택 기준을 부여
    content_list.append({
        "type": "text", 
        "text": (
            f"\n\nReview the {len(working_urls)} images above. Your goal is to select the BEST single image for video generation. "
            f"**SCENE CONTEXT:** Character: '{character_name}', Scene: '{scene_prompt}', Emotion: '{emotion_tag}'. "
            f"**SELECTION CRITERIA (Strict Priority):** "
            f"1. **Relevance:** Image must clearly show the character '{character_name}' and match the emotion/mood '{emotion_tag}'. "
            f"2. **Quality & Focus:** Must be a high-resolution, clean screenshot with the character's face clearly visible and centrally framed. "
            f"**3. ABSOLUTE REJECTION RULE (Reject if ANY are met):** "
            f"   - Contains logos, advertisements, quizzes, overlaid text, or large borders.\n"
            f"   - Is a low-quality webcomic, abstract art, or lacks a recognizable character.\n"
            f"Select the best image number (1 to {len(working_urls)}). Respond ONLY with the number."
        )
    })
    
    # 메시지 구조: User 역할 하나에 모든 컨텐츠 담기
    messages = [{"role": "user", "content": content_list}]
    
    # 4. VLM 호출
    try:
        response = bedrock_runtime.invoke_model(
            modelId='anthropic.claude-3-haiku-20240307-v1:0',
            contentType='application/json',
            accept='application/json',
            body=json.dumps({
                "anthropic_version": "bedrock-2023-05-31", 
                "messages": messages, 
                "max_tokens": 10 
            })
        )
        resp_text = json.loads(response.get('body').read())['content'][0]['text'].strip()
        
        import re
        match = re.search(r'\d+', resp_text)
        idx = int(match.group(0)) if match else 1
        
        selected_url = working_urls[idx-1] if 1 <= idx <= len(working_urls) else working_urls[0]
        logger.info(f"🏆 VLM 최종 선정 이미지 (No.{idx}): {selected_url}")
        return selected_url
        
    except Exception as e:
        logger.error(f"VLM Error: {e}")
        return working_urls[0]


# --- 메인 핸들러 (SFN Task) ---
def lambda_handler(event: Dict[str, Any], context):
    
    try:
        # SFN Input
        job_details = event
        job_id = job_details['jobId']
        worker_id = job_details.get('workerId', f"{job_id}-single")
        
        logger.info(f"Worker {worker_id}: 이미지 검색 시작.")
        
        # 1. 검색 쿼리 생성
        search_query = generate_search_query(job_details['script'])
        
        # 2. 이미지 검색 (1차)
        candidate_urls = search_images_from_google(search_query)

        # 3. 실패 시 재시도 (2차: 단순 검색어)
        if not candidate_urls:
            logger.warning("1차 검색 실패. 작품명 또는 단순 키워드로 재시도합니다.")
            
            # [수정] 2차 검색 쿼리 생성 (Fallback)
            title = job_details['script'].get('source_title', '')
            if title:
                # 한국어 제목이 있을 경우 한국어 검색어로 재시도
                title_kr = job_details['script'].get('source_title_kr', '')
                if title_kr:
                    simple_query = f"{title_kr} 영화 장면"
                else:
                    simple_query = f"{title} movie scene"
            else:
                simple_query = " ".join(job_details['script'].get('scene_prompt', '').split()[:5]) + " cinematic"
            
            logger.info(f"Retry Query: {simple_query}")
            candidate_urls = search_images_from_google(simple_query)
        
        if not candidate_urls:
            logger.error(f"Worker {worker_id}: 이미지 검색 최종 실패.")
            raise Exception("Search API returned no valid image candidates.")

        # 4. VLM 선정
        best_image_url = select_best_image_from_vlm(job_details, candidate_urls)
            
        # 5. 결과 반환
        job_details['best_image_url'] = best_image_url
        return job_details

    except Exception as e:
        logger.error(f"L3 Error: {e}", exc_info=True)
        # Step Function이 오류를 감지하고 Fail 상태로 전환하도록 오류를 전파합니다.
        raise e