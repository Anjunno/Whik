import json
import os
from db import get_connection

def lambda_handler(event, context):
    print("[Authorizer] Incoming event:")
    print(json.dumps(event, ensure_ascii=False))

    db_host_value = os.environ.get("DB_HOST", "NOT_FOUND")
    user_uuid = None

    # ✅ 1️⃣ REST API 요청 (TOKEN Authorizer 형식)
    if "authorizationToken" in event:
        print("[Authorizer] Detected REST TOKEN type event")
        user_uuid = event["authorizationToken"]

    # ✅ 2️⃣ REST API 요청 (REQUEST Authorizer 형식)
    elif "headers" in event:
        headers = event.get("headers", {})
        user_uuid = headers.get("uuid") or headers.get("Authorization")

    print(f"[Authorizer] Extracted user_uuid: {user_uuid}")

    # ✅ UUID가 없으면 Deny
    if not user_uuid:
        return generate_policy(
            principal_id="anonymous",
            effect="Deny",
            resource=event.get("methodArn", "*"),
            reason="Missing user_uuid header"
        )

    connection = None
    cursor = None

    try:
        connection = get_connection()
        cursor = connection.cursor(dictionary=True)

        # ✅ uuid로 gender, nickname 조회
        cursor.execute("SELECT gender, nickname FROM user WHERE uuid = %s", (user_uuid,))
        user = cursor.fetchone()
        print(f"[Authorizer] DB Query Result: {user}")

        if not user:
            return generate_policy(
                principal_id=user_uuid,
                effect="Deny",
                resource=event.get("methodArn", "*"),
                reason="Invalid user_uuid"
            )

        gender_from_db = user.get("gender")
        nickname_from_db = user.get("nickname", "unknown")

        # ✅ gender 변환 (M/F → male/female)
        if gender_from_db == 'M':
            gender_for_context = 'male'
        elif gender_from_db == 'F':
            gender_for_context = 'female'
        else:
            gender_for_context = 'male'
            print(f"[Authorizer] Warning: Unexpected gender value '{gender_from_db}' for user {user_uuid}. Defaulting to 'male'.")

        # ✅ 최종 허용 정책 리턴
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
        print(f"[Authorizer] Error: {e}")
        return generate_policy(
            principal_id="anonymous",
            effect="Deny",
            resource=event.get("methodArn", "*"),
            reason=f"Internal error: {str(e)}"
        )

    finally:
        if connection and connection.is_connected():
            if cursor:
                cursor.close()
            connection.close()


def generate_policy(principal_id, effect, resource, reason="", extra_context=None):
    context = {"reason": reason}
    if extra_context:
        context.update(extra_context)

    policy = {
        "principalId": principal_id,
        "policyDocument": {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Action": "execute-api:Invoke",
                    "Effect": effect,
                    "Resource": resource
                }
            ]
        },
        "context": context
    }

    print(f"[Authorizer] Generated policy: {json.dumps(policy, ensure_ascii=False)}")
    return policy
