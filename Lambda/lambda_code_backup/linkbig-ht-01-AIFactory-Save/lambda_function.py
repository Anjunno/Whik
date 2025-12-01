import json
import os
import boto3
import logging
import time
import requests
from typing import Dict, Any
from urllib.parse import urlparse

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- 환경 변수 로드 ---
VIDEO_JOBS_TABLE = os.environ.get("VIDEO_JOBS_TABLE")
OUTPUT_S3_BUCKET = os.environ.get("OUTPUT_S3_BUCKET_NAME")

# --- 클라이언트 초기화 ---
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
table = None


# ----------------------------------------------------------------------
# 1. GCS -> S3 복사 로직
# ----------------------------------------------------------------------

def copy_gcs_to_s3_via_download(gcs_uri: str, job_id: str, s3_bucket: str) -> str:
    """
    GCP GCS URI에서 파일을 다운로드하여 AWS S3에 업로드합니다.
    (가장 확실하지만, Lambda의 /tmp (512MB) 제한이 있는 방식입니다.)
    """
    local_path = f"/tmp/{job_id}_final.mp4"
    final_s3_key = f"{job_id}/final.mp4"
    final_s3_url = f"s3://{s3_bucket}/{final_s3_key}"
    
    # 1. GCS 파일 다운로드
    logger.info(f"Starting download from GCS: {gcs_uri}")
    try:
        # GCS URI는 일반 HTTPS URL처럼 requests로 접근 가능하다고 가정합니다.
        # (GCP 설정에 따라 인증이 필요할 수 있으나, 여기서는 공개 URL로 가정)
        parsed_uri = urlparse(gcs_uri)
        # GCS URI를 HTTPS로 변환 (gs:// -> https://storage.googleapis.com/)
        http_url = f"https://storage.googleapis.com/{parsed_uri.netloc}{parsed_uri.path}"
        
        with requests.get(http_url, stream=True) as r:
            r.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
        logger.info(f"Download complete. Size: {os.path.getsize(local_path)} bytes.")

        # 2. S3에 업로드
        s3_client.upload_file(local_path, s3_bucket, final_s3_key)
        
        return final_s3_url
        
    except requests.exceptions.HTTPError as e:
        logger.error(f"GCS Download Failed (HTTP Error): {e}")
        raise Exception(f"GCS file transfer failed (Source: {http_url})")
    except Exception as e:
        logger.error(f"File handling error: {e}")
        raise e
    finally:
        # 3. /tmp 파일 삭제 (람다 재사용 시 공간 확보)
        if os.path.exists(local_path):
            os.remove(local_path)


# ----------------------------------------------------------------------
# 2. 메인 핸들러
# ----------------------------------------------------------------------

def lambda_handler(event: Dict[str, Any], context):
    
    global table
    if not table:
        table = dynamodb.Table(VIDEO_JOBS_TABLE)
    
    # SFN의 최종 Input은 QC/Save Task를 위한 최종 데이터입니다.
    # 이 데이터는 L5 Poller의 최종 출력과 이전 상태 데이터가 병합된 상태입니다.
    
    # 1. 데이터 추출 및 검증
    try:
        job_id = event['jobId']
        user_id = event['userId']
        qc_status = event['qcStatus'] # QC 결과: PASS 또는 FAIL
        gcs_url = event['gcsUrl']     # Veo가 최종적으로 저장한 GCS URI
        final_prompt = event['finalPrompt']
        
        if qc_status != "PASS":
            logger.warning(f"Job {job_id}: QC FAILED status received. Skipping S3 upload.")
            # QC 실패 시 DB 상태만 FAILED로 최종 업데이트하고 종료합니다.
            table.update_item(
                Key={'jobId': job_id},
                UpdateExpression="SET #st = :s, completedAt = :ca",
                ExpressionAttributeNames={'#st': 'status'},
                ExpressionAttributeValues={':s': 'QC_FAILED', ':ca': int(time.time())}
            )
            return {"status": "QC_FAILED", "message": "QC check failed. Video not saved to S3."}

    except KeyError as e:
        logger.error(f"Invalid SFN Input: Missing key {e} for final save.")
        raise ValueError("SFN Input Error: Missing required data for finalization.")


    # 2. GCS -> S3 최종 복사 (QC PASS 시)
    try:
        final_s3_url = copy_gcs_to_s3_via_download(gcs_url, job_id, OUTPUT_S3_BUCKET)
        
        # 3. DynamoDB 최종 완료 상태 업데이트
        table.update_item(
            Key={'jobId': job_id},
            UpdateExpression="SET #st = :s, s3Url = :s3, completedAt = :ca",
            ExpressionAttributeNames={'#st': 'status'},
            ExpressionAttributeValues={
                ':s': 'COMPLETED',
                ':s3': final_s3_url,
                ':ca': int(time.time())
            }
        )
        
        logger.info(f"✅ Job {job_id}: S3 저장 및 DB 커밋 완료. URL: {final_s3_url}")
        
        return {
            "status": "SUCCESS",
            "jobId": job_id,
            "s3Url": final_s3_url
        }

    except Exception as e:
        logger.error(f"Finalization failed for Job {job_id}: {e}", exc_info=True)
        # 최종 단계에서 오류가 발생해도 재시도는 의미가 없으므로, 상태만 FAILED로 기록
        table.update_item(Key={'jobId': job_id}, UpdateExpression="SET #st = :s", ExpressionAttributeNames={'#st': 'status'}, ExpressionAttributeValues={':s': 'SAVE_FAILED'})
        raise