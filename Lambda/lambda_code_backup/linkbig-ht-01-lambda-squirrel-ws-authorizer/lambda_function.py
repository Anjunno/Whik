import json
import os
from db import get_connection

def lambda_handler(event, context):

    print("--- ws-authorizer started ---")

    print(f"[WS Authorizer] Incoming event: {json.dumps(event, ensure_ascii=False)}")

    # --- [수정됨] 헤더와 쿼리 파라미터에서 UUID 동시 확인 ---

    # 1. 헤더에서 uuid 추출 (Postman, 네이티브 앱용)
    headers = event.get("headers", {}) or {}
    user_uuid = headers.get("user_uuid")
    source = "headers"

    # 2. 헤더에 없으면, 쿼리 파라미터에서 확인 (웹 브라우저, FlutterFlow Web용)
    if not user_uuid:
        query_params = event.get("queryStringParameters", {}) or {}
        user_uuid = query_params.get("user_uuid")
        source = "query params"

    print(f"[WS Authorizer] Received UUID from {source}: {user_uuid}")
    
    # --- [수정됨] 3. 두 곳 모두 uuid가 없으면 인증 거부 ---
    if not user_uuid:
        print("[WS Authorizer] No user_uuid in headers or query params. Denying.")
        return generate_policy(
            principal_id="anonymous",
            effect="Deny",
            resource=event.get("methodArn", "*"),
            reason="Missing user_uuid"
        )

    connection = None
    cursor = None
    try:
        # 4. 데이터베이스 연결 및 사용자 정보 조회
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)
        cursor.execute("SELECT gender, nickname FROM user WHERE uuid = %s", (user_uuid,))
        user = cursor.fetchone()
        print(f"[WS Authorizer] DB Query Result: {user}")

        if not user:
            print(f"[WS Authorizer] No user found for UUID {user_uuid}")
            return generate_policy(
                principal_id=user_uuid,
                effect="Deny",
                resource=event.get("methodArn", "*"),
                reason="Invalid user_uuid"
            )

        # 5. gender 값 변환 (M/F -> male/female)
        gender_from_db = user.get("gender")
        nickname_from_db = user.get("nickname", "unknown")

        # (기존 코드보다 간결하게 수정)
        gender_for_context = "female" if gender_from_db == "F" else "male"
        
        if gender_from_db not in ("M", "F"):
             print(f"[WS Authorizer] Warning: Unexpected gender '{gender_from_db}', defaulting to 'male'.")

        # 6. 인증 성공 정책 생성 및 반환
        # context에 포함된 정보는 후속 Lambda 함수(onConnect, onMessage 등)에서 사용 가능합니다.
        return generate_policy(
            principal_id=user_uuid,
            effect="Allow",
            resource=event.get("methodArn", "*"),
            reason="Authorized",
            extra_context={
                "user_uuid": user_uuid,
                "gender": gender_for_context,
                "nickname": nickname_from_db
            }
        )

    except Exception as e:
        # 오류 발생 시 인증 거부
        print(f"[WS Authorizer] Error: {e}")
        return generate_policy(
            principal_id="anonymous",
            effect="Deny",
            resource=event.get("methodArn", "*"),
            reason=f"Internal error: {str(e)}"
        )

    finally:
        # 7. 데이터베이스 연결 해제
        if connection and connection.is_connected():
            if cursor:
                cursor.close()
            connection.close()

def generate_policy(principal_id, effect, resource, reason="", extra_context=None):
    """API Gateway가 요구하는 형식의 정책 문서를 생성하는 헬퍼 함수"""
    context = {"reason": reason}
    if extra_context:
        context.update(extra_context)

    policy = {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [{
                "Action": "execute-api:Invoke",
                "Effect": effect,
                "Resource": resource
            }]
        },
        "context": context
    }
    print(f"[WS Authorizer] Generated policy: {json.dumps(policy, ensure_ascii=False)}")
    return policy