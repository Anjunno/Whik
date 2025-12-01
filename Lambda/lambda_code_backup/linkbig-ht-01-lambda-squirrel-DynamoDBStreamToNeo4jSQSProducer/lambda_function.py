import json
import os
import boto3
from boto3.dynamodb.types import TypeDeserializer
from decimal import Decimal

# --- 설정 및 초기화 ---
SQS_QUEUE_URL = os.environ.get('SQS_QUEUE_URL')

sqs = boto3.client('sqs')
deserializer = TypeDeserializer()

def default_serializer(obj):
    """
    json.dumps()가 Decimal 객체를 만났을 때 이를 문자열로 변환하는 사용자 정의 함수
    """
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

def lambda_handler(event, context):
    if not SQS_QUEUE_URL:
        print("Error: SQS_QUEUE_URL environment variable is not set.")
        raise EnvironmentError("SQS_QUEUE_URL is missing. Cannot send messages.")

    messages_to_send = []

    for record in event.get('Records', []):
        if record.get('eventName') == 'INSERT':
            new_image = record.get('dynamodb', {}).get('NewImage')
            if not new_image:
                continue

            try:
                # 1. DynamoDB JSON을 Python 딕셔너리로 언마샬링
                python_data = {k: deserializer.deserialize(v) for k, v in new_image.items()}

                # 2. SQS에 보낼 필수 데이터 필드만 추출 
                essential_data = {
                    'userId': python_data.get('userId'),
                    'scenarioId': python_data.get('scenarioId'),
                    'createdAtTs': python_data.get('createdAtTs'),
                    'createdAtIso': python_data.get('createdAtIso'),
                    'targetLanguage': python_data.get('targetLanguage'),
                    'originalWord': python_data.get('originalWord'),
                    'relatedWords_KR': python_data.get('relatedWords_kr', {}),
                }

                # 3. 필수 필드만 담은 딕셔너리를 SQS 메시지 바디 JSON 문자열로 변환
                message_body = json.dumps(essential_data,
                                          default=default_serializer,
                                          ensure_ascii=False)

                messages_to_send.append({
                    'Id': record['eventID'],
                    'MessageBody': message_body
                })

            except Exception as e:
                print(f"Skipping record {record.get('eventID')}: Data processing error: {e}")
                continue

    if messages_to_send:
        try:
            # SQS SendMessageBatch를 사용하여 메시지를 효율적으로 일괄 전송
            response = sqs.send_message_batch(
                QueueUrl=SQS_QUEUE_URL,
                Entries=messages_to_send
            )

            failed_count = len(response.get('Failed', []))
            if failed_count > 0:
                print(f"SQS 전송 중 {failed_count}건 실패: {response.get('Failed')}")
                raise RuntimeError("SQS batch send partial failure occurred. Rerunning batch.")

            print(f"SQS 전송 성공: {len(response.get('Successful', []))} 건.")

        except Exception as e:
            # 치명적인 오류 발생 시 DynamoDB Stream 재처리 유도
            print(f"Fatal SQS Batch Send error: {e}")
            raise e

    return {'statusCode': 200}