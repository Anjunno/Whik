from db import get_connection
from datetime import datetime, timedelta #
import boto3
import os
import json
import random
import firebase_admin
from firebase_admin import credentials
from firebase_admin import messaging

# DynamoDB 테이블 
dynamodb = boto3.resource("dynamodb")
NOTIFICATION_TABLE = os.environ.get("NOTIFICATION_TABLE")
notification_table = dynamodb.Table(NOTIFICATION_TABLE)

# FCM 인증 정보 로드 및 초기화
SERVICE_ACCOUNT_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON")

if SERVICE_ACCOUNT_JSON:
    # JSON 문자열을 딕셔너리로 변환하여 인증 정보 로드
    cred_dict = json.loads(SERVICE_ACCOUNT_JSON)
    cred = credentials.Certificate(cred_dict)
    # 앱 초기화 (앱이 이미 초기화되지 않은 경우에만)
    if not firebase_admin._apps:
        firebase_admin.initialize_app(cred)
    print("Firebase Admin SDK initialized successfully.")
else:
    # Lambda 배포 시 이 부분을 확인해야 합니다.
    print("WARNING: FIREBASE_SERVICE_ACCOUNT_JSON environment variable not found.")
    
#----------------------------------------------------------------------

def lambda_handler(event, context):
    try:
        # short_form_messages = [
        #     {"category_time_key": "WEEKDAY#08:00", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "출근", "message": "출근길 1분! ☕ 오늘의 숏폼으로 외국어 한 문장 말해볼까요? 🎤"},
        #     {"category_time_key": "WEEKDAY#12:30", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "점심", "message": "점심 시간 1분! 🍱 오늘의 숏폼으로 외국어 한 문장 말하며 연습해요 🎤"},
        #     {"category_time_key": "WEEKDAY#18:30", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "퇴근", "message": "퇴근길, 짧게 1문장 🎬 오늘의 숏폼으로 말하기 연습해요 🎤"},
        #     {"category_time_key": "WEEKDAY#19:30", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "저녁", "message": "저녁 시간, 숏폼으로 외국어 한 문장 말하며 즐기기 🍴🎤"},
        #     {"category_time_key": "WEEKDAY#22:00", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "취침", "message": "하루 마무리 1분 🌙 숏폼으로 오늘 배운 문장 말하며 복습 🎤"},
        #     {"category_time_key": "WEEKEND#08:00", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "아침", "message": "좋은 아침! ☀️ 주말 숏폼으로 외국어 한 문장 말하며 하루 시작 🎤"},
        #     {"category_time_key": "WEEKEND#12:30", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "점심", "message": "점심 후 잠깐 🍔 숏폼으로 오늘의 문장 말하며 학습 📱🎤"},
        #     {"category_time_key": "WEEKEND#14:00", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "활동", "message": "주말 여유 시간 🎬 숏폼으로 한 문장 말하기 연습해요 🎤"},
        #     {"category_time_key": "WEEKEND#19:00", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "저녁", "message": "주말 저녁, 숏폼으로 외국어 한 문장 말하며 학습 🌙🎤"},
        #     {"category_time_key": "WEEKEND#23:00", "message_id": 2, "action_type": "SHORT_FORM", "active": True, "category": "취침", "message": "포근한 주말 밤 🌌 숏폼으로 오늘 배운 문장 말하며 복습 🎤"},
        # ]

        # # DynamoDB에 삽입
        # for msg in short_form_messages:
        #     notification_table.put_item(Item=msg)

        # print("SHORT_FORM 메시지 10개 추가 완료 ✅")



        # FCM 초기화 실패 시 예외 처리
        if not firebase_admin._apps:
            return {"statusCode": 500, "body": "Firebase Admin SDK is not initialized."}

        # 1️⃣ 현재 시간 기반으로 category_time_key 계산 (변경 없음)
        now = datetime.utcnow()
        now_kst = now + timedelta(hours=9)

        # 분 단위 보정: 항상 00 또는 30으로 "내림"
        minute = now_kst.minute
        if minute < 30:
            minute = 0
        else:
            minute = 30

        # 보정된 시각으로 hour_min 구성
        hour_min = f"{now_kst.hour:02d}:{minute:02d}"

        weekday = now_kst.weekday()
        category_prefix = "WEEKDAY" if weekday < 5 else "WEEKEND"
        category_time_key = f"{category_prefix}#{hour_min}"

        print(f'조회할 시간 : {category_time_key}')

        # 2️⃣ DynamoDB에서 해당 시간대 메시지 조회
        response = notification_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('category_time_key').eq(category_time_key)
            # KeyConditionExpression=boto3.dynamodb.conditions.Key('category_time_key').eq('WEEKDAY#08:00')
        )
        messages = response.get("Items", [])
        if not messages:
            print("No messages for this time slot.")
            return {"statusCode": 200, "body": "No messages."}

        selected_message = random.choice(messages)
        print(f"Selected Message: {selected_message}")

        # 3️⃣ MySQL에서 FCM 토큰 조회 (변경 없음)
        conn = get_connection()
        with conn.cursor() as cursor:
            # 닉네임은 전송에 필요 없으므로 토큰만 가져옵니다.
            cursor.execute("SELECT fcm_token FROM user WHERE fcm_token IS NOT NULL")
            user_tokens_raw = cursor.fetchall()
        conn.close()
        print(f'user_tokens_raw : {user_tokens_raw}')
        # 토큰 리스트 추출
        fcm_tokens = [token[0] for token in user_tokens_raw]
        print(f'추출한 사용자 fcm_token : {fcm_tokens}')

        if not fcm_tokens:
            print("No users with FCM token.")
            return {"statusCode": 200, "body": "No users."}

        # 4️⃣ FCM 푸시 전송 (v1 프로토콜, Admin SDK 사용)
        multicast_message = messaging.MulticastMessage(
            notification=messaging.Notification(
                # title=selected_message["category"],
                title="람다람쥐",
                body=selected_message["message"]
            ),
            data={
                "action_type": selected_message.get("action_type", "OPEN_APP")
            },
            tokens=fcm_tokens, # 토큰 리스트 그대로 전달 (SDK가 알아서 분할 처리)
        )

        # send_each_for_multicast를 사용하여 전송 (내부적으로 500개씩 자동 분할)
        response = messaging.send_each_for_multicast(multicast_message)

        print(f"Successfully sent {response.success_count} messages.")

        if response.failure_count > 0:
            responses = response.responses
            failed_tokens = []
            
            # 실패한 토큰들을 찾습니다.
            for idx, resp in enumerate(responses):
                if not resp.success:
                    failed_token = fcm_tokens[idx]
                    failed_tokens.append(failed_token)
                    
                    # 여기서 토큰 무효화(삭제) 로직을 추가할 수 있습니다.
                    if resp.exception and resp.exception.code in ['NOT_FOUND', 'INVALID_ARGUMENT']:
                        print(f"Token to delete: {failed_token}") 
                        # TODO: MySQL에서 이 토큰을 삭제하는 로직 구현

            print(f"Failed to send {response.failure_count} messages.")
            print(f"List of tokens that caused failures: {failed_tokens[:10]}...") # 처음 10개만 출력

        return {"statusCode": 200, "body": f"Attempted to send message to {len(fcm_tokens)} users. Successes: {response.success_count}"}

    except Exception as e:
        print(f"Error: {e}")
        return {"statusCode": 500, "body": str(e)}