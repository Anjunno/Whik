import json
import os
import boto3
import logging
import time
from typing import Dict, Any
from google import genai
from google.longrunning import operations_pb2
import requests

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- 환경 변수 로드 ---
VIDEO_JOBS_TABLE = os.environ.get("VIDEO_JOBS_TABLE")
OUTPUT_S3_BUCKET = os.environ.get("OUTPUT_S3_BUCKET_NAME")
GCP_SA_JSON_CONTENT = os.environ.get("GCP_SERVICE_ACCOUNT_JSON")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")

# --- 클라이언트 초기화 ---
dynamodb = boto3.resource('dynamodb')
table = None 

# ----------------------------------------------------------------------
# 1. GCP 인증 및 초기화 (Lambda 4와 동일)
# ----------------------------------------------------------------------

def initialize_gcp_credentials():
    """ GCP 서비스 계정 JSON 내용을 읽어 /tmp에 파일로 생성하고 환경 변수를 설정합니다. """
    if not GCP_SA_JSON_CONTENT:
        raise EnvironmentError("GCP_SERVICE_ACCOUNT_JSON 환경 변수를 설정해야 합니다.")
        
    temp_file_path = "/tmp/gcp_service_account.json"
    
    try:
        with open(temp_file_path, "w") as f:
            f.write(GCP_SA_JSON_CONTENT)
            
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = temp_file_path
        logger.info("GCP Credentials initialized.")
        
    except Exception as e:
        logger.error(f"Failed to create temp credentials file: {e}")
        raise

# ----------------------------------------------------------------------
# 2. Polling Logic: Veo 작업 상태 확인 (Mode: POLL_CHECK)
# ----------------------------------------------------------------------
def check_veo_status(event_input: Dict) -> Dict:
    """ Veo API를 호출하여 작업 상태를 확인하고 GCS URI를 추출합니다. """
    
    # ⚠️ [수정] Lambda가 실행될 때마다 GCP 인증 초기화
    initialize_gcp_credentials() 
    
    client = genai.Client()
    veo_op_name = event_input.get("veoOperationName")
    job_id = event_input.get("jobId")

    if not veo_op_name:
        return {"isCompleted": True, "status": "ERROR", "message": "Veo Operation Name is missing."}

    try:
        operation = client.operations.get(name=veo_op_name)
    except Exception as e:
        logger.error(f"Veo API call failed during polling (Op: {veo_op_name}): {e}")
        return {"isCompleted": False, "status": "API_ERROR"} # SFN이 재시도하도록 유도

    # 기본 반환값 (아직 진행 중)
    status_result = {"isCompleted": False, "status": "POLLING"}
    
    if operation.done:
        status_result["isCompleted"] = True
        
        if operation.error:
            # 작업 실패 (Veo 측)
            logger.error(f"Veo Job Failed: {operation.error.message}")
            status_result["status"] = "VEOGCP_FAILED"
            status_result["veoError"] = operation.error.message
        else:
            # 작업 성공 -> GCS URI 추출
            status_result["status"] = "QC_PENDING"
            
            try:
                # operations_pb2.GenerateVideosResponse 파싱 (GCS URI 추출)
                response_proto = operations_pb2.GenerateVideosResponse()
                operation.response.Unpack(response_proto)
                
                if response_proto.generated_videos:
                    gcs_uri = response_proto.generated_videos[0].video.uri
                    status_result["gcsUrl"] = gcs_uri
                    logger.info(f"Veo Job Succeeded. GCS URL: {gcs_uri}")
                else:
                    raise ValueError("No generated videos found in response.")
                    
            except Exception as e:
                logger.error(f"GCS URI Parsing Failed: {e}")
                status_result["status"] = "FAILED"
                status_result["veoError"] = "GCS URI parsing failure."
                
    return status_result

# ----------------------------------------------------------------------
# 3. Finalization Logic: QC 및 저장 (Mode: QC_SAVE)
# ----------------------------------------------------------------------

def run_gemini_vision_qc(gcs_uri: str, prompt: str) -> Dict[str, Any]:
    """ [TODO] Gemini Vision QC 시뮬레이션 """
    # ⚠️ 이 로직은 Lambda 6으로 이동되어야 하나, SFN 루프 테스트를 위해 여기에 남겨둠
    return {"pass": True, "score": 9.5, "detail": "QC Passed (Simulated)."}


def perform_qc_and_route(job_details: Dict) -> Dict:
    """ QC 수행 결과를 SFN에 반환하여 L6 (저장) 또는 L4 (재시도)를 결정합니다. """
    
    # 이 람다는 QC만 수행하고, 저장은 L6이 합니다.
    gcs_url = job_details.get('gcsUrl')
    
    # 1. QC 수행
    qc_result = run_gemini_vision_qc(gcs_url, job_details['finalPrompt'])
    
    # 2. 다음 단계 결정 플래그 반환
    if qc_result['pass']:
        # ✅ 검수 통과 시: L6으로 진행하도록 SFN에 지시
        return {"qcStatus": "PASS", "s3Target": f"s3://{OUTPUT_S3_BUCKET}/{job_details['jobId']}/final.mp4"}
    else:
        # ❌ 검수 실패 시: L4로 돌아가 재시도하도록 SFN에 지시
        return {"qcStatus": "FAIL", "retryCount": job_details.get('retryCount', 0) + 1}


# ----------------------------------------------------------------------
# 4. 메인 핸들러 (SFN 라우터)
# ----------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context):
    
    global table
    if not table:
        table = dynamodb.Table(VIDEO_JOBS_TABLE)
        
    # SFN Task Input에서 'mode'를 추출
    mode = event.get("mode", "POLL_CHECK") 
    
    if mode == "POLL_CHECK":
        # Polling Logic 실행
        return check_veo_status(event)
        
    elif mode == "QC_CHECK":
        # QC 및 라우팅 로직 실행
        return perform_qc_and_route(event) 
        
    else:
        logger.error(f"Invalid mode specified: {mode}")
        raise ValueError(f"Invalid mode: {mode}. Must be POLL_CHECK or QC_CHECK.")