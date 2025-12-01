import json
import boto3
import os

# 환경 변수는 람다 환경에서 가져옵니다.
VIDEO_TABLE = os.environ.get("VIDEO_TABLE")
dynamodb = boto3.resource("dynamodb")
# 테이블 이름이 설정되지 않았다면 안전하게 처리합니다.
if VIDEO_TABLE:
    table = dynamodb.Table(VIDEO_TABLE)
else:
    print("경고: VIDEO_TABLE 환경 변수가 설정되지 않았습니다.")
    table = None # DynamoDB 호출 시 오류 발생 방지

def lambda_handler(event, context):
    
    # 1. 입력 데이터 소스 결정 (루프/직접 Catch 구분)
    # 'execution_data' 키가 있으면 루프를 통해 온 데이터로 간주하고 중첩된 키를 사용합니다.
    # Check_Iteration_Count 또는 Increment_Counter에서 전달된 데이터는 이 경로를 탑니다.
    if 'execution_data' in event:
        # 실행 데이터 (PK, SK)와 오류 데이터 (Error, Cause)를 분리
        data_source = event.get('execution_data', {})
        error_data = event.get('error', {})
    else:
        # Parallel/Finalize Catch 등 직접적인 Catch로 온 데이터로 간주합니다.
        # 이 경우 event 자체가 데이터 소스 역할을 합니다.
        data_source = event
        error_data = event 
        
    # 2. Key 추출
    # data_source는 이제 최상위 PK/SK를 포함하거나, 중첩된 데이터에서 추출된 PK/SK를 포함합니다.
    PK = data_source.get('PK') 
    SK = data_source.get('SK')
    
    # 3. 오류 메시지 정리
    # 'Error' 필드나 'errorType' 필드에서 오류 유형 추출
    error_type = error_data.get('Error', error_data.get('errorType', 'UnknownError'))
    
    # Step Functions Catch의 'Cause' 필드에서 상세 메시지 추출 시도
    cause_str = error_data.get('Cause', 'No detailed cause provided.')
    
    error_message = cause_str
    if isinstance(cause_str, str) and cause_str.startswith('{'):
        try:
            # 중첩된 JSON을 파싱하여 실제 오류 메시지 추출
            cause_detail = json.loads(cause_str)
            error_message = cause_detail.get('errorMessage', cause_str)
        except json.JSONDecodeError:
            pass 
    
    # 4. Key 누락 시 Early Exit
    if not SK or not PK:
        print(f"❌ 오류 상태 업데이트 실패: PK/SK 누락. 오류: {error_type}")
        # 이 경우, DynamoDB 업데이트 자체를 시도할 수 없습니다.
        return {'status': 'Error', 'message': 'PK/SK missing in error payload', 'input': event}
        
    # 메시지를 DynamoDB에 맞게 255자 내외로 제한합니다.
    final_error_msg = f"Type: {error_type[:50]} | Msg: {error_message[:200]}"
    
    print(f"❌ 파이프라인 실패 감지. SK: {SK} -> FAILED. 메시지: {final_error_msg}")

    # 5. DynamoDB 업데이트
    if not table:
        raise RuntimeError("DynamoDB 테이블 초기화 실패 (환경 변수 확인)")
        
    try:
        table.update_item(
             Key={'lang': PK, 'SK': SK},
             UpdateExpression="SET #st = :fail_status, error_message = :error_msg",
             ExpressionAttributeNames={'#st': 'status'},
             ExpressionAttributeValues={
                 ':fail_status': 'FAILED', 
                 ':error_msg': final_error_msg
             }
        )
        return {'PK': PK, 'SK': SK, 'status': 'FAILED', 'error': final_error_msg}
        
    except Exception as e:
        print(f"❌ DynamoDB FAILED 상태 업데이트 중 오류 발생: {e}")
        # 복구 경로의 치명적인 실패이므로 예외를 발생시킵니다.
        raise RuntimeError(f"DB 업데이트 실패: {e}")