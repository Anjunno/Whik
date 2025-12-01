import json
import os
import boto3
import logging
import time
import urllib.request
from google import genai
from google.genai.errors import APIError
from google.genai.types import Image, GenerateVideosConfig, VideoGenerationReferenceImage
from typing import Dict, Any

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- AWS 클라이언트 및 환경 변수 초기화 ---
dynamodb = boto3.resource('dynamodb')
table = None 

# [필수 환경 변수 로드]
VIDEO_JOBS_TABLE = os.environ.get("VIDEO_JOBS_TABLE")
OUTPUT_GCS_BUCKET_URI = os.environ.get("OUTPUT_GCS_BUCKET_URI")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
REGION = "us-central1"
GCP_SA_JSON_CONTENT = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")

if not all([VIDEO_JOBS_TABLE, OUTPUT_GCS_BUCKET_URI, GCP_PROJECT_ID]):
    raise EnvironmentError("Missing required environment variables (DB/GCS/GCP Project).")

# --- GCP 인증 초기화 ---
def initialize_gcp_credentials():
    """
    환경 변수의 GCP 서비스 계정 JSON 내용을 읽어 /tmp에 파일로 생성하고,
    google-genai SDK가 이를 사용하도록 환경 변수를 설정합니다.
    """
    if not GCP_SA_JSON_CONTENT:
        raise EnvironmentError("GCP_SERVICE_ACCOUNT_JSON 환경 변수가 설정되지 않았습니다.")
        
    temp_file_path = "/tmp/gcp_service_account.json"
    
    try:
        with open(temp_file_path, "w") as f:
            f.write(GCP_SA_JSON_CONTENT)
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path
        logger.info("GCP Credentials initialized via /tmp file.")
    except Exception as e:
        logger.error(f"Failed to create credential file: {e}")
        raise

# --- DynamoDB 업데이트 ---
def update_job_status(job_id, status, prompt, user_id, veo_op_name=None):
    """ DynamoDB 테이블에 작업 상태 및 관련 정보를 업데이트합니다. """
    if not table: raise ValueError("DynamoDB table has not been initialized.")
    
    update_expression = "SET #st = :s, #pr = :p, #ui = :u, createdAt = :c"
    expression_attribute_names = {'#st': 'status', '#pr': 'finalPrompt', '#ui': 'userId'}
    expression_attribute_values = {
        ':s': status, ':p': prompt, ':u': user_id, ':c': int(time.time()) 
    }
    
    if veo_op_name:
        update_expression += ", veoOperationName = :vop"
        expression_attribute_values[':vop'] = veo_op_name

    try:
        table.update_item(
            Key={'jobId': job_id},
            UpdateExpression=update_expression,
            ExpressionAttributeNames=expression_attribute_names,
            ExpressionAttributeValues=expression_attribute_values
        )
        logger.info(f"DynamoDB Updated: {job_id} -> {status}")
    except Exception as e:
        logger.error(f"Error updating DynamoDB for jobId {job_id}: {e}")
        raise

# --- 이미지 다운로드 함수 (내장 라이브러리 사용) ---
def download_image_as_bytes(url):
    """ urllib을 사용하여 URL에서 이미지를 다운로드하고 bytes로 반환합니다. """
    try:
        req = urllib.request.Request(
            url, 
            headers={'User-Agent': 'Mozilla/5.0'}
        )
        with urllib.request.urlopen(req, timeout=10) as response:
            return response.read() # bytes 반환
    except Exception as e:
        logger.error(f"Failed to download image from {url}: {e}")
        raise

# --- 메인 핸들러 ---
def lambda_handler(event, context):
    global table
    if not table:
        table = dynamodb.Table(VIDEO_JOBS_TABLE)

    try:
        # 1. GCP 인증
        initialize_gcp_credentials()
        
        # 2. Step Functions 입력 데이터 추출
        job_details = event 
        job_id = job_details.get('jobId')
        user_id = job_details.get('userId', 'UNKNOWN')
        best_image_url = job_details.get('best_image_url')
        
        # L2에서 만든 데이터
        script_data = job_details.get('script', {})
        scene_prompt = script_data.get('scene_prompt', 'A cinematic shot.')
        dialogue_text = script_data.get('dialogue_text', '') # 대사
        emotion_tag = script_data.get('emotion_tag', 'neutral') # 감정 태그

        if not job_id or not best_image_url:
            logger.error(f"Invalid Input: {json.dumps(job_details)}")
            raise ValueError("Job ID or Image URL is missing.")

        # 3. 이미지 다운로드 및 Veo Client 설정
        logger.info("Downloading image...")
        image_bytes = download_image_as_bytes(best_image_url)
        logger.info(f"Image downloaded. Size: {len(image_bytes)} bytes.")

        client = genai.Client(vertexai=True, project=GCP_PROJECT_ID, location=REGION)

        # 4. Veo 생성 요청 및 프롬프트 상세화 (영상 품질 극대화)
        
        # 4-1. 프롬프트 강화 (자막 제거 및 품질/감정 집중)
        final_prompt = (
            # 기본 지시사항
            f"CREATE A CINEMATIC VIDEO SEQUENCE (8 SECONDS) that captures an iconic scene. "
            
            # 영상 품질 스펙
            f"**TECHNICAL SPECIFICATIONS:**\n"
            f"- Resolution: Ultra HD 4K (3840x2160)\n"
            f"- Frame Rate: 24fps for film-like motion\n"
            f"- Lighting: Professional cinematic lighting with emphasis on mood and depth\n"
            f"- Camera: Cinema-grade look with shallow depth of field (f/1.8-2.8)\n"
            f"- Color Grading: Rich, filmic color palette with proper contrast\n"
            
            # 장면 설정
            f"**SCENE COMPOSITION:**\n"
            f"Primary Scene: {scene_prompt}\n"
            f"Emotional Tone: {emotion_tag}\n"
            f"Focus Character: {script_data.get('character_name', 'main character')}\n"
            
            # 캐릭터 연기/표현
            f"**CHARACTER DIRECTION:**\n"
            f"- Maintain clear focus on the character's face and expressions\n"
            f"- Facial expressions must authentically convey '{emotion_tag}' emotion\n"
            f"- Natural lip movements matching dialogue timing (no actual audio needed)\n"
            f"- Body language and gestures should reinforce the emotional state\n"
            
            # 카메라워크
            f"**CAMERA MOVEMENT:**\n"
            f"- Start with an establishing shot (1-2 seconds)\n"
            f"- Smoothly transition to character focus\n"
            f"- Use subtle, cinematic camera movements (slight dolly/tracking)\n"
            f"- End with an emotionally appropriate framing\n"
            
            # 참고 이미지 활용 지침
            f"**REFERENCE IMAGE GUIDANCE:**\n"
            f"- If the provided reference image captures the essence well, use it as inspiration\n"
            f"- If the reference image quality is subpar, prioritize creating a higher quality interpretation\n"
            f"- Focus on capturing the emotional core of the scene rather than exact replication\n"
            
            # 영상미 강화 요소
            f"**ATMOSPHERIC ELEMENTS:**\n"
            f"- Dynamic lighting changes to enhance mood\n"
            f"- Appropriate depth and dimensionality\n"
            f"- Subtle environmental effects (if relevant: light particles, atmospheric haze, etc.)\n"
            f"- Natural film grain (minimal)\n"
            
            # 절대 제외 요소
            f"**STRICT EXCLUSIONS:**\n"
            f"- NO text, subtitles, or any on-screen writing\n"
            f"- NO watermarks or logos\n"
            f"- NO artificial transitions or effects\n"
            f"- NO unrealistic or exaggerated movements\n"
            
            # 품질 보증 사항
            f"**QUALITY ASSURANCE:**\n"
            f"- Maintain consistent high quality throughout all 8 seconds\n"
            f"- Ensure smooth, natural motion without artifacts\n"
            f"- Focus on photorealistic rendering and naturalistic movement\n"
            f"- Prioritize emotional impact and cinematic beauty\n"
            
            # 최종 강조
            f"Create this as if it were a scene from a major studio production, with emphasis on emotional resonance and visual excellence."
        )
        
        # 4-2. Config 설정
        config = GenerateVideosConfig(
            aspect_ratio="9:16",
            output_gcs_uri=OUTPUT_GCS_BUCKET_URI 
        )
        
        # 4-3. Image 객체 생성
        input_image = Image(
            image_bytes=image_bytes, 
            mime_type="image/jpeg"
        )
        
        # 5. Veo API 호출
        logger.info(f"Veo Requesting... Prompt: {final_prompt[:80]}...")
        
        operation = client.models.generate_videos(
            model='veo-3.1-generate-preview', 
            prompt=final_prompt, 
            image=input_image, 
            config=config
        )
        
        veo_op_name = operation.name
        
        # 6. DynamoDB 저장
        update_job_status(job_id, "GENERATING", final_prompt, user_id, veo_op_name)
        
        logger.info(f"✅ Veo Submitted: {veo_op_name}")

        # 7. Step Functions 다음 단계(Wait -> Polling)로 넘길 데이터 반환
        return {
            "veoOperationName": veo_op_name,
            "jobId": job_id,
            "userId": user_id,
            "finalPrompt": final_prompt,
            "gcsUrl": "PENDING"
        }

    except Exception as e:
        logger.error(f"L4 Error: {e}", exc_info=True)
        raise e