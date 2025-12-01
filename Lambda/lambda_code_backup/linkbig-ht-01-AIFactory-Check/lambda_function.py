import json
import os
import boto3
import uuid
import time

# --- AWS 클라이언트 및 환경 변수 초기화 ---
# [수정] SQS 대신 Step Functions 클라이언트를 사용합니다.
sfn_client = boto3.client('stepfunctions')

# [필수 설정] Step Function의 ARN을 환경 변수에서 가져옵니다.
# ⚠️ 이 ARN은 SFN을 생성한 후 AWS 콘솔에서 얻어야 합니다.
STEP_FUNCTION_ARN = os.environ.get("STEP_FUNCTION_ARN")
if not STEP_FUNCTION_ARN:
    raise EnvironmentError("STEP_FUNCTION_ARN 환경 변수가 설정되지 않았습니다.")


def lambda_handler(event, context):
    print("--- 람다 1: Step Functions 실행 요청 시작 ---")
    
    # [인증 제외] 고정된 개발자 UUID를 사용합니다.
    user_uuid = "DEV-ADMIN-A01" 
    
    # --- 1. 입력 (Request) 데이터 파싱 및 유효성 검사 ---
    try:
        # API Gateway (프록시 통합) Body를 파싱합니다.
        body = json.loads(event.get("body", "{}"))
    except json.JSONDecodeError:
        return {'statusCode': 400, 'body': json.dumps({'message': 'Invalid JSON format in request body'})}

    count = body.get('count')
    media_type = body.get('mediaType')
    language = body.get('language')

    if not all([count, media_type, language]):
        return {'statusCode': 400, 'body': json.dumps({'message': 'Missing required parameters: count, mediaType, or language'})}
    
    if not isinstance(count, int) or count <= 0 or count > 1000:
        return {'statusCode': 400, 'body': json.dumps({'message': 'Count must be an integer between 1 and 1000.'})}


    # --- 2. Step Function 실행 입력 (Input) 생성 ---
    job_id = str(uuid.uuid4())
    
    # Step Function의 Input Payload로 사용될 JSON
    execution_input = {
        'jobId': job_id,
        'userId': user_uuid,
        'videoCount': count,
        'mediaType': media_type,
        'language': language,
        'contextualTopic': f"{media_type} 장르 명대사 생성",
        'creationTimestamp': int(time.time()),
        'executionName': f'VideoJob-{job_id}-{int(time.time())}' # SFN 실행의 고유 이름
    }
    
    # --- 3. Step Functions 실행 (핵심 동작) ---
    try:
        response = sfn_client.start_execution(
            stateMachineArn=STEP_FUNCTION_ARN,
            name=execution_input['executionName'],
            input=json.dumps(execution_input)
        )
        
        execution_arn = response['executionArn']
        print(f"✅ Step Function 실행 성공. Execution ARN: {execution_arn}")

    except Exception as e:
        print(f"!!! CRITICAL: Step Function 실행 실패: {e}")
        # IAM 권한 문제 등이 발생하면 500 오류 반환
        return {'statusCode': 500, 'body': json.dumps({'message': f'Internal service error: SFN execution failed. Detail: {str(e)}'})}

    # --- 4. 응답 (Response) 반환 (30초 타임아웃 방어) ---
    return {
        'statusCode': 202,
        'body': json.dumps({
            'message': f'{count}개 {media_type} 영상 파이프라인이 백그라운드에서 시작되었습니다.',
            'jobId': job_id,
            'executionArn': execution_arn,
            'statusCheckEndpoint': f'/video-jobs/{job_id}' 
        })
    }