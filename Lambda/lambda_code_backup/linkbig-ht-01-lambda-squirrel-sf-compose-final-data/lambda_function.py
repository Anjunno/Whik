import json
import boto3
import os
from decimal import Decimal

VIDEO_TABLE = os.environ.get("VIDEO_TABLE")
dynamodb = boto3.resource("dynamodb")
# 테이블 초기화 안전화
if VIDEO_TABLE:
    table = dynamodb.Table(VIDEO_TABLE)
else:
    table = None

def lambda_handler(event, context):
    # Step Functions Parallel State의 출력은 항상 리스트입니다: [Branch1_Output, Branch2_Output]
    parallel_outputs = event.get('ParallelOutputs', [])
    
    # 이전 단계에서 넘어온 메타데이터
    PK = event.get('PK')
    SK = event.get('SK')
    validated_activities = event.get('validated_activities', [])
    
    # iteration_count추출합니다. 
    iteration_count = parallel_outputs[0].get('iteration_count', 0) if parallel_outputs and len(parallel_outputs) > 0 else 0
    
    if len(parallel_outputs) != 2:
        # 이 오류는 Finalize_Data의 Catch로 잡힙니다.
        raise ValueError(f"병렬 출력 결과가 2개가 아닙니다. 실제 개수: {len(parallel_outputs)}")
        
    # 병렬 결과 추출 및 취합
    script_audio_output = parallel_outputs[0] # 스크립트 오디오 분기
    responses_audio_output = parallel_outputs[1] # 추천 답변 오디오 분기
    
    script_audio_key = script_audio_output.get('script_audio_key')
    
    # 1. FOLLOW_THE_SCRIPT 활동 객체 생성 및 리스트에 추가
    if not script_audio_key:
        raise ValueError("스크립트 오디오 Key가 병렬 출력에서 누락되었습니다.")

    follow_activity = {
        "activity_id": 3,
        "activity_type": "FOLLOW_THE_SCRIPT",
        "audio_key": script_audio_key
    }
    
    final_activities = []
    
    # 2. 기존 활동 리스트를 순회하며 RECOMMENDED_RESPONSES를 업데이트하고, 나머지를 추가
    for activity in validated_activities:
        if activity['activity_type'] == 'RECOMMENDED_RESPONSES':
            # 오디오 생성된 RECOMMENDED_RESPONSES 활동으로 대체 (병렬 분기 결과)
            
            # responses_audio_output에서 iteration_count를 제거합니다.
            clean_response_activity = responses_audio_output.copy()
            clean_response_activity.pop('iteration_count', None)
            
            final_activities.append(clean_response_activity)
        else:
            final_activities.append(activity)

    # FOLLOW_THE_SCRIPT 활동 최종 추가
    final_activities.append(follow_activity)

    # 3. DynamoDB 최종 업데이트 (READY 상태로 전환)
    if not table:
        raise RuntimeError("DynamoDB 테이블 초기화 실패 (환경 변수 확인)")
        
    try:
        table.update_item(
             Key={'lang': PK, 'SK': SK},
             UpdateExpression="SET learning_activities = :activities, #st = :new_status",
             ExpressionAttributeNames={'#st': 'status'},
             ExpressionAttributeValues={
                 ':activities': final_activities, 
                 ':new_status': 'READY'
             }
        )
        print(f"파이프라인 완료. SK: {SK} -> READY (DB 업데이트 완료, 재시도 횟수: {iteration_count})")
        
        return {'PK': PK, 'SK': SK, 'status': 'READY'}
        
    except Exception as e:
        print(f"DynamoDB 최종 업데이트 실패: {e}")
        raise RuntimeError(f"최종 DB 업데이트 실패: {e}")