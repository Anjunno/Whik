import json
import boto3
import os
import time
import urllib.parse
import urllib.request
from korean_processor import process as process_korean
from foreign_processor import process as process_foreign

# --- AWS 클라이언트 및 DynamoDB 테이블 객체 초기화 ---
s3_client = boto3.client('s3')
transcribe_client = boto3.client('transcribe')
dynamodb = boto3.resource('dynamodb')
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
results_table = dynamodb.Table(RESULTS_TABLE_NAME)


def lambda_handler(event, context):

    """
    SQS에서 S3 이벤트 메시지를 수신합니다.
    S3 경로에서 jobId를 추출하고, DynamoDB에서 작업 정보를 조회한 뒤,
    STT 작업 후 결과를 각 전문 처리 함수로 전달합니다.
    """
    print(f"Received SQS event: {json.dumps(event)}")

    for record in event['Records']:
        job_id = None # 예외 처리를 위해 job_id를 미리 선언
        try:
            # SQS 메시지 본문에서 S3 이벤트 정보를 파싱합니다.
            s3_event = json.loads(record['body'])
            bucket_name = s3_event['Records'][0]['s3']['bucket']['name']
            input_key = urllib.parse.unquote_plus(s3_event['Records'][0]['s3']['object']['key'])

            print(f"Processing new file: s3://{bucket_name}/{input_key}")
            
            # 1. 단순화된 S3 파일 경로에서 jobId를 추출합니다.
            # 경로 구조: user-uploads/{user_uuid}/{jobId}.ext
            try:
                # 파일명(예: fa4ff8d0...m4a)만 가져옵니다.
                base_name = os.path.basename(input_key)
                # 확장자를 제외한 부분(jobId)만 추출합니다.
                job_id = os.path.splitext(base_name)[0]
                print(f"Extracted Job ID: {job_id}")
            except Exception as e:
                print(f"Error: S3 키에서 jobId를 추출하지 못했습니다: {input_key}, Error: {e}")
                continue # 다음 메시지 처리

            # 2. jobId를 사용하여 'results' 테이블에서 작업 메타데이터를 조회합니다.
            print(f"jobId '{job_id}'에 대한 메타데이터를 results 테이블에서 조회합니다.")
            response = results_table.get_item(Key={'PK': job_id})
            task_info = response.get('Item')

            if not task_info:
                print(f"Error: results 테이블에서 jobId '{job_id}'에 해당하는 작업을 찾을 수 없습니다.")
                continue

            # 3. 조회한 메타데이터에서 필요한 정보를 추출합니다.
            input_type = task_info.get('inputType')
            language = task_info.get('language')

            # 4. AWS Transcribe 작업을 시작합니다.
            job_name = f"transcribe-job-{context.aws_request_id}"
            s3_uri = f"s3://{bucket_name}/{input_key}"
            language_code_map = {
                'jp': 'ja-JP', 'es': 'es-ES', 'zh': 'zh-CN', 'ko': 'ko-KR'
            }
            # inputType에 따라 음성 인식(STT) 언어를 설정합니다.
            transcribe_language_code = 'ko-KR' if input_type == 'korean' else language_code_map.get(language)

            if not transcribe_language_code:
                raise ValueError(f"지원하지 않는 Transcribe 언어 코드입니다: {language}")

            transcribe_client.start_transcription_job(
                TranscriptionJobName=job_name,
                LanguageCode=transcribe_language_code,
                Media={'MediaFileUri': s3_uri}
            )

            # 5. Transcribe 작업이 완료될 때까지 대기합니다 (Polling).
            while True:
                status = transcribe_client.get_transcription_job(TranscriptionJobName=job_name)
                job_status = status['TranscriptionJob']['TranscriptionJobStatus']
                if job_status in ['COMPLETED', 'FAILED']:
                    break
                print(f"Waiting for Transcribe job '{job_name}' to complete...")
                time.sleep(5)

            if job_status == 'FAILED':
                raise Exception(f"Transcribe job failed. Reason: {status['TranscriptionJob'].get('FailureReason')}")

            # 6. Transcribe 결과(JSON)에서 텍스트를 추출합니다.
            transcript_uri = status['TranscriptionJob']['Transcript']['TranscriptFileUri']
            with urllib.request.urlopen(transcript_uri) as response:
                transcript_data = json.loads(response.read())
            transcript_text = transcript_data['results']['transcripts'][0]['transcript']
            print(f"Transcription result: {transcript_text}")
            
            # 7. inputType에 따라 적절한 처리 함수를 호출합니다.
            #    이제 DynamoDB에서 조회한 전체 메타데이터(task_info)를 전달합니다.
            if input_type == 'korean':
                process_korean(transcript_text, input_key, task_info)
            elif input_type == 'foreign':
                process_foreign(transcript_text, input_key, task_info)
            else:
                print(f"Error: Unknown inputType: {input_type}")

        except Exception as e:
            print(f"An unexpected error occurred while processing a record: {e}")
            # 오류 발생 시 'results' 테이블의 상태를 'FAILED'로 업데이트합니다.
            if job_id:
                results_table.update_item(
                    Key={'PK': job_id},
                    UpdateExpression="SET #st = :s, #err = :e",
                    ExpressionAttributeNames={'#st': 'status', '#err': 'error'},
                    ExpressionAttributeValues={':s': 'FAILED', ':e': str(e)}
                )
            continue # 다음 SQS 메시지 처리
            
    return {
        'statusCode': 200, 
        'body': json.dumps('Processing of all messages completed.')
    }