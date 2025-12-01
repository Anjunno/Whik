import json
import boto3
import os
from boto3.dynamodb.types import TypeDeserializer

# 환경 변수: 생성한 Step Functions State Machine ARN
STATE_MACHINE_ARN = os.environ.get("STATE_MACHINE_ARN")
DEFAULT_REGION = os.environ.get("AWS_REGION", "ap-northeast-1")
stepfunctions = boto3.client("stepfunctions", region_name=DEFAULT_REGION)
deserializer = TypeDeserializer()

def deserialize_dynamodb_record(dynamo_image):
    """DynamoDB Stream 포맷을 일반 Python 딕셔너리로 변환합니다."""
    if not dynamo_image:
        return {}
    return {k: deserializer.deserialize(v) for k, v in dynamo_image.items()}

def lambda_handler(event, context):
    print("--- Step Functions 트리거 시작 (DynamoDB Stream 수신) ---")
    
    for record in event.get('Records', []):
        if record['eventName'] not in ['INSERT', 'MODIFY']:
            continue
            
        new_dynamo_image = record['dynamodb'].get('NewImage')
        if not new_dynamo_image:
            continue

        data = deserialize_dynamodb_record(new_dynamo_image)
        
        # PENDING 상태 확인
        if data.get('status') != 'PENDING':
            print(f"SK: {data.get('SK')} - 상태가 PENDING이 아니므로 스킵합니다.")
            continue

        # Step Functions 실행 시작
        try:
            sk = data.get('SK', 'UNKNOWN_SK') 
            
            if sk == 'UNKNOWN_SK':
                print("경고: SK가 데이터에서 누락되었습니다. 실행을 건너뜁니다.")
                continue

            print(f"SK: {sk} - Step Functions 실행 시작.")
            
            # 1. 고유성 확보를 위해 Request ID의 마지막 8자만 사용
            short_request_id = context.aws_request_id[-8:] 
            
            # 2. 실행 이름 생성 및 유효성 검사 
            # SK에 포함된 '#' 문자를 Step Functions에서 허용되는 '-' 문자로 대체합니다.
            safe_sk = sk.replace('#', '-')
            
            # Step Functions 실행 이름 생성
            execution_name = f"ContentGen-{safe_sk}-{short_request_id}"
            
            
            # 3. 최대 길이 80자 초과 방지 (추가 안전장치)
            if len(execution_name) > 80:
                # SK가 매우 길 경우, 실행 이름을 80자 미만으로 맞추기 위해 SK를 잘라냅니다.
                # 80 - (len("ContentGen-") + len(short_request_id) + 1) = 60
                max_sk_len = 80 - (len("ContentGen-") + len(short_request_id) + 1) 
                trimmed_sk = safe_sk[-max_sk_len:] # SK의 마지막 60자 사용
                execution_name = f"ContentGen-{trimmed_sk}-{short_request_id}"

            # Step 1: Step Functions에 전달할 입력 데이터 준비
            execution_input = {
                'PK': data.get('lang', 'JAPANESE'),
                'SK': sk,
                'lang_script': data.get('scene', {}).get('lang-script'),
                'ko_script': data.get('scene', {}).get('ko-script'),
            }
            
            stepfunctions.start_execution(
                stateMachineArn=STATE_MACHINE_ARN,
                name=execution_name, 
                input=json.dumps(execution_input)
            )
            print(f"Step Functions 실행 요청 완료. 이름: {execution_name}")

        except Exception as e:
            print(f"Step Functions 실행 시작 중 오류 발생: {e}")
            
    return {'statusCode': 200, 'body': json.dumps('Stream records processed.')}