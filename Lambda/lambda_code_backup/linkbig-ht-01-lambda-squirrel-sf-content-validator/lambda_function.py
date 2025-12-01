import json
import os
import random
import re

def randomize_quiz_options(quiz_data):
    """
    LLM이 생성한 정답과 오답 텍스트를 받아 무작위로 섞고 answer_index를 설정합니다.
    """
    # LLM이 생성한 분리된 필드를 추출
    correct_option = quiz_data.pop('correct_option')
    incorrect_options = quiz_data.pop('incorrect_options')
    
    # 정답과 오답을 하나의 리스트로 합침
    all_options = incorrect_options + [correct_option]
    
    # 1. 옵션 리스트를 무작위로 섞음 (무작위 위치 결정)
    random.shuffle(all_options)
    
    # 2. 섞인 리스트에서 정답의 새로운 위치(인덱스)를 찾음
    try:
        answer_index = all_options.index(correct_option)
    except ValueError:
        # LLM이 정답을 제대로 포함하지 못했을 경우 (매우 드물지만 안전장치)
        raise ValueError("LLM 응답에 정답 텍스트가 포함되지 않았습니다.")
    
    # 3. 최종 구조에 반영
    quiz_data['options'] = all_options
    quiz_data['answer_index'] = answer_index

    return quiz_data

def validate_bedrock_output(raw_activities, lang_script, ko_script):
    """Bedrock의 raw 출력을 검증하고 무작위화된 activities 리스트를 반환합니다."""
    
    final_activities = []
    found_types = set()
    
    if not isinstance(raw_activities, list) or not raw_activities:
        raise ValueError("형식 오류: 'learning_activities'가 유효한 리스트가 아니거나 비어 있습니다.")

    for activity in raw_activities:
        activity_type = activity.get('activity_type')
        found_types.add(activity_type)
        
        # 1. 형식적 검증 (필수 키)
        if not activity_type or 'activity_id' not in activity:
            raise ValueError(f"형식 오류: 활동에 필수 키(type/id)가 누락되었습니다. ({activity})")

        if activity_type == 'COMPREHENSION_QUIZ':
            correct = activity.get('correct_option', '')
            incorrect = activity.get('incorrect_options', [])
            tip = activity.get('tip', '')
            
            # 1. 형식적 검증 (퀴즈 옵션 개수)
            if len(incorrect) != 3:
                 raise ValueError(f"형식 오류: 퀴즈 오답이 3개가 아닙니다. 실제 개수: {len(incorrect)}")

            # 2. 논리적 검증 (정답/오답 중복)
            if correct in incorrect:
                 raise ValueError("논리적 오류: 퀴즈 정답과 오답이 중복됩니다.")
                 
            # 3. 학습 효과 검증 (Tip에 정답 포함 금지)
            if correct in tip or ko_script in tip:
                 raise ValueError("교육적 오류: 퀴즈 Tip에 정답 또는 번역이 포함되어 있습니다.")
                 
            # 검증 통과 후, 무작위화 수행 및 최종 리스트에 추가
            final_activities.append(randomize_quiz_options(activity))

        elif activity_type == 'SENTENCE_RECONSTRUCTION':
            chunks = activity.get('chunks', [])
            
            # 2. 논리적 검증 (청크 재구성 일치)
            reconstructed = "".join(chunks)
            if reconstructed != lang_script:
                 raise ValueError(f"논리적 오류: 청크 재구성 결과가 원본 스크립트와 일치하지 않습니다. ({reconstructed} != {lang_script})")
                 
            final_activities.append(activity)

        elif activity_type == 'RECOMMENDED_RESPONSES':
            responses = activity.get('recommended_responses', [])
            
            # 3. 학습 효과 검증 (추천 답변 개수)
            if len(responses) != 2:
                 # 개수가 달라도 치명적 오류가 아닐 수 있으므로 일단 경고만 하고 진행
                 print(f"경고: 추천 답변이 2개가 아닙니다. 실제 개수: {len(responses)}") 
                 
            for response in responses:
                pronunciation = response.get('pronunciation', '')
                # 2. 논리적 검증 (발음 필드 한글 외 문자 검사 - 정규식)
                # 한글, 공백, 기본 구두점(. , ! ? ,) 외 문자 포함 시 오류
                if re.search(r'[^\s\.\,!\?\가-힣]', pronunciation): 
                    raise ValueError(f"논리적 오류: 발음 필드에 허용되지 않은 문자(한글 외)가 포함되어 있습니다. ({pronunciation})")
            
            final_activities.append(activity)
        
        else:
             # 정의되지 않은 활동 타입은 일단 통과시킵니다.
             final_activities.append(activity)

    # 1. 형식적 검증 (최종 필수 활동 포함 여부)
    required_types = {'COMPREHENSION_QUIZ', 'SENTENCE_RECONSTRUCTION', 'RECOMMENDED_RESPONSES'}
    if not required_types.issubset(found_types):
        raise ValueError(f"형식 오류: 필수 활동이 누락되었습니다. 누락된 활동: {required_types - found_types}")

    return final_activities

def lambda_handler(event, context):
    # iteration_count를 이벤트에서 추출.
    iteration_count = event.get('iteration_count', 0)

    # Step Functions의 입력(event)에서 데이터 추출 (sf-content-generator 단계의 출력)
    raw_activities = event.get('raw_activities', [])
    lang_script = event.get('lang_script')
    ko_script = event.get('ko_script')
    PK = event.get('PK')
    SK = event.get('SK')
    
    print(f"Validation 시작. SK: {SK}")

    # 1. 검증 및 무작위화 수행
    # 실패 시 ValueError 발생하여 Step Functions Catch로 이동
    validated_activities = validate_bedrock_output(raw_activities, lang_script, ko_script)
    
    # 다음 단계(TTS 생성)에 필요한 데이터 반환
    return {
        'PK': PK,
        'SK': SK,
        'lang_script': lang_script,
        'ko_script': ko_script,
        'validated_activities': validated_activities, # 검증이 완료된 최종 리스트
        'iteration_count': iteration_count
    }