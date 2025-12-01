import json
import boto3
import os
import uuid
import time
from botocore.exceptions import ClientError
from decimal import Decimal

# --- AWS 클라이언트 초기화 ---
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')

# --- 환경 변수 ---
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
WEBSOCKET_ENDPOINT_URL = os.environ.get("WEBSOCKET_ENDPOINT_URL")
AWS_REGION = os.environ.get("AWS_REGION", 'us-east-1')
GOOGLE_CREDENTIALS_JSON = os.environ.get("GOOGLE_CREDENTIALS")

# --- 테이블 객체 ---
results_table = dynamodb.Table(RESULTS_TABLE_NAME)

# --- API Gateway 클라이언트 ---
apigw_management_client = boto3.client(
    'apigatewaymanagementapi',
    endpoint_url=WEBSOCKET_ENDPOINT_URL
)

# --- JSON 변환용 ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super().default(o)

# --- Google STT 액세스 토큰 발급 헬퍼 ---
def get_google_stt_token():
    """
    GOOGLE_CREDENTIALS 환경변수를 사용해 Google STT용 액세스 토큰 생성.
    """
    if not GOOGLE_CREDENTIALS_JSON:
        print("!!! CRITICAL: GOOGLE_CREDENTIALS 환경 변수가 설정되지 않았습니다.")
        raise ValueError("Google STT credentials not configured on server.")

    try:
        import google.auth
        import google.auth.transport.requests
        from google.oauth2 import service_account
    except ImportError:
        print("!!! CRITICAL: Google Auth 라이브러리가 Lambda Layer에 없습니다.")
        raise ValueError("Server configuration error: Missing Google Auth library.")

    try:
        # 1. JSON 문자열 → dict 변환
        credentials_info = json.loads(GOOGLE_CREDENTIALS_JSON)

        # 2. Service Account Credential 생성
        credentials = service_account.Credentials.from_service_account_info(credentials_info)

        # 3. 필요한 스코프 추가
        scoped_credentials = credentials.with_scopes([
            'https://www.googleapis.com/auth/cloud-platform'
        ])

        # 4. 액세스 토큰 갱신
        request = google.auth.transport.requests.Request()
        scoped_credentials.refresh(request)

        print(f"✅ Google STT 액세스 토큰 발급 성공 (만료: {scoped_credentials.expiry})")
        return scoped_credentials.token

    except Exception as e:
        print(f"!!! Google STT 토큰 생성 중 오류 발생: {e}")
        raise ValueError(f"Google STT Token generation failed: {e}")

# --- WebSocket 메시지 전송 헬퍼 ---
def send_to_client(connection_id, payload):
    """
    특정 connectionId에 JSON 페이로드를 전송합니다.
    """
    try:
        apigw_management_client.post_to_connection(
            ConnectionId=connection_id,
            Data=json.dumps(payload, cls=DecimalEncoder, ensure_ascii=False)
        )
        print(f"Sent payload to {connection_id} (Action: {payload.get('action')})")

    except ClientError as e:
        if e.response['Error']['Code'] == 'GoneException':
            print(f"Client {connection_id} is gone, skipping message.")
        else:
            print(f"!!! Failed to send to {connection_id}: {e}")
    except Exception as e:
        print(f"!!! Unknown error in send_to_client: {e}")

# --- 메인 핸들러 ---
def lambda_handler(event, context):
    """
    WebSocket $default 라우트 핸들러.
    action 값에 따라 Google STT 토큰/S3 URL 생성 또는 결과 처리를 수행합니다.
    """
    connection_id = event['requestContext']['connectionId']
    authorizer_context = event['requestContext'].get('authorizer', {})
    user_uuid = authorizer_context.get('user_uuid')

    if not user_uuid:
        print(f"Warning: Authorizer context에 user_uuid가 없습니다. connection_id: {connection_id}")
        return {'statusCode': 403, 'body': 'Unauthorized'}

    print(f"Received event from {user_uuid} ({connection_id})...")
    
    # --- [수정] 모든 프로세서를 여기서 Lazy Import (Cold Start 문제 해결) ---
    try:
        import korean_processor
        import foreign_processor
        import follow_speech
    except ImportError:
        print("!!! CRITICAL: Helper/Processor 모듈을 import할 수 없습니다.")
        send_to_client(connection_id, {'action': 'error', 'message': 'Internal server error: Missing core modules.'})
        return {'statusCode': 500, 'body': 'Internal server error: Missing core modules.'}

    try:
        body = json.loads(event.get("body", "{}"))
        action = body.get("action")

        # --- 1. S3 업로드 URL + Google 토큰 요청 (공용) ---
        if action == "requestStream":
            print(f"Action 'requestStream' (S3 Upload URL + Google Token) for user {user_uuid}")

            context_from_client = body.get('context')
            file_extension = body.get('fileExtension', 'm4a')

            if not context_from_client:
                raise ValueError("webSocketContext ('context')가 누락되었습니다.")

            job_id = str(uuid.uuid4())
            original_file_key = f"user-uploads/{user_uuid}/{job_id}.{file_extension}"
            content_type = 'audio/mp4' if file_extension == 'm4a' else 'audio/mpeg'

            upload_url = s3_client.generate_presigned_url(
                'put_object',
                Params={'Bucket': AUDIO_BUCKET_NAME, 'Key': original_file_key, 'ContentType': content_type},
                ExpiresIn=600
            )

            # ✅ Google STT 액세스 토큰 발급
            google_stt_token = get_google_stt_token()

            # DynamoDB에 작업 저장
            task_info = {
                'PK': job_id,
                'status': 'PENDING',
                'creationTimestamp': int(time.time()),
                'userId': user_uuid,
                'gender': authorizer_context.get('gender', 'male'),
                'nickname': authorizer_context.get('nickname', 'unknown'),
                'language': context_from_client.get('langCode'),
                'themeId': context_from_client.get('themeId'),
                'videoId': context_from_client.get('videoId'),
                'originalFileKey': original_file_key
                # (참고: context_from_client의 modelAnswerScript 등도 여기에 저장됨)
            }
            results_table.put_item(Item=task_info)

            response_payload = {
                'action': 'streamReady',
                'jobId': job_id,
                'uploadUrl': upload_url,
                'googleSttToken': google_stt_token
            }
            send_to_client(connection_id, response_payload)

        # --- 2. STT 결과 처리 (자유 대화 - 대답해보기) ---
        elif action == "processResult":
            print(f"Action 'processResult' (Free Talk Flow) for user {user_uuid}")

            job_id = body.get('jobId')
            stt_result_text = body.get('sttResult')
            detected_language = body.get('detectedLanguage')

            if not all([job_id, stt_result_text is not None, detected_language]):
                raise ValueError("jobId, sttResult, detectedLanguage가 누락되었습니다.")

            task_info = results_table.get_item(Key={'PK': job_id}).get('Item')
            if not task_info:
                raise ValueError(f"유효하지 않은 jobId입니다: {job_id}")

            results_table.update_item(
                Key={'PK': job_id},
                UpdateExpression="SET #uit = :uit, #dl = :dl",
                ExpressionAttributeNames={'#uit': 'userInputText', '#dl': 'detectedLanguage'},
                ExpressionAttributeValues={':uit': stt_result_text, ':dl': detected_language}
            )

            original_file_key = task_info['originalFileKey']

            if 'ko-KR' in detected_language:
                print(f"Job {job_id}: 한국어 감지. korean_processor 호출.")
                final_result_payload = korean_processor.process_and_get_result(
                    stt_result_text, original_file_key, task_info
                )
            else:
                print(f"Job {job_id}: 외국어 감지. foreign_processor 호출.")
                final_result_payload = foreign_processor.process_and_get_result(
                    stt_result_text, original_file_key, task_info
                )

            send_to_client(connection_id, {
                'action': 'finalResult',
                'data': final_result_payload
            })
            
        # --- 3. [신규] 발음 평가 결과 처리 (따라 말하기) ---
        elif action == "processPronunciation":
            print(f"Action 'processPronunciation' (Follow-Along Flow) for user {user_uuid}")

            job_id = body.get('jobId')
            stt_result_text = body.get('sttResult') # "따라 말하기"의 STT 텍스트

            if not all([job_id, stt_result_text is not None]):
                raise ValueError("jobId, sttResult가 누락되었습니다.")

            # jobId로 'task_info' (videoId 등) 로드
            task_info = results_table.get_item(Key={'PK': job_id}).get('Item')
            if not task_info:
                raise ValueError(f"유효하지 않은 jobId입니다: {job_id}")
            
            final_result_payload = follow_speech.process_and_evaluate(
                stt_result_text, task_info
            )
            
            # 클라이언트에게 "평가 완료" 신호 + Bedrock 피드백 전송
            send_to_client(connection_id, {
                'action': 'pronunciationResult',
                'data': final_result_payload
            })

        else:
            raise ValueError(f"지원하지 않는 action입니다: {action}")

        return {'statusCode': 200, 'body': 'Message processed.'}

    except Exception as e:
        print(f"!!! Error in lambda_handler (app.py): {e}")
        send_to_client(connection_id, {'action': 'error', 'message': str(e)})
        return {'statusCode': 500, 'body': f'Error: {str(e)}'}