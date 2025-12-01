# # 파일명: transcribe_stream_helper.py (Canonical Request 수정 버전)

# import json
# import boto3
# import datetime
# import hmac
# import hashlib
# import urllib.parse
# import os

# # --- SigV4 서명 보조 함수들 ---
# def sign(key, msg):
#     return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

# def get_signature_key(key, date_stamp, region_name, service_name):
#     kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
#     kRegion = sign(kDate, region_name)
#     kService = sign(kRegion, service_name)
#     kSigning = sign(kService, 'aws4_request')
#     return kSigning

# # --- 메인 함수 (app.py가 호출할 함수) ---
# def generate_transcribe_presigned_url(language_candidates: list, sample_rate: int = 16000):
#     aws_region = os.environ.get('AWS_REGION','us-east-1')
#     if not aws_region:
#         print("!!! CRITICAL: AWS_REGION 환경 변수가 설정되지 않았습니다.")
#         raise Exception("AWS_REGION 환경 변수가 설정되지 않았습니다.")
#     print(f"[Helper] Using AWS Region for signing: {aws_region}")

#     session = boto3.Session(region_name=aws_region)
#     credentials = session.get_credentials()

#     if credentials.access_key is None:
#         raise Exception("AWS 자격 증명을 찾을 수 없습니다.")

#     access_key = credentials.access_key
#     secret_key = credentials.secret_key
#     session_token = credentials.token
#     print(f"[Helper] Credentials obtained: Access Key ID starts with: {access_key[:5]}...")
#     print(f"[Helper] Session Token present: {'Yes' if session_token else 'No'}")

#     service = 'transcribe'
#     host_without_port = f'transcribestreaming.{aws_region}.amazonaws.com'
#     host = f'{host_without_port}:8443'
#     canonical_uri = '/stream-transcription-websocket'
#     t = datetime.datetime.utcnow()
#     amz_date = t.strftime('%Y%m%dT%H%M%SZ')
#     datestamp = t.strftime('%Y%m%d')
#     language_options_str = ",".join(language_candidates)
#     print(f"[Helper] Language options for URL: {language_options_str}")

#     query_params = {
#         'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
#         'X-Amz-Credential': f"{access_key}/{datestamp}/{aws_region}/{service}/aws4_request",
#         'X-Amz-Date': amz_date,
#         'X-Amz-Expires': '300',
#         'X-Amz-Security-Token': session_token,
#         'X-Amz-SignedHeaders': 'host',

#         'language-code': 'ja-JP',

#         # 'identify-language': 'true',
#         # 'language-options': language_options_str,
#         'media-encoding': 'pcm',
#         'sample-rate': str(sample_rate)
#     }

#     sorted_params = sorted(query_params.items())
#     canonical_querystring = urllib.parse.urlencode(sorted_params)
#     print(f"[Helper] Canonical Query String: {canonical_querystring}")

#     method = 'GET'
#     canonical_headers = 'host:' + host + '\n' # 포트 포함, 끝에 개행
#     signed_headers = 'host'
#     payload_hash = hashlib.sha256(('').encode('utf-8')).hexdigest()

#     # --- 👇 [수정됨] Canonical Request 생성 ---
#     canonical_request = (
#         f"{method}\n"
#         f"{canonical_uri}\n"
#         f"{canonical_querystring}\n"
#         f"{canonical_headers}" # 헤더 끝에 이미 \n 포함됨
#         f"\n"
#         f"{signed_headers}\n"
#         f"{payload_hash}"
#     )
#     # --- 수정 끝 ---
#     print(f"[Helper] Canonical Request (Raw):\n---\n{repr(canonical_request)}\n---") # repr()로 정확한 문자열 로깅

#     algorithm = 'AWS4-HMAC-SHA256'
#     credential_scope = f"{datestamp}/{aws_region}/{service}/aws4_request"
#     print(f"[Helper] Hashing this Canonical Request for StringToSign...")
#     hashed_canonical_request = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
#     print(f"[Helper] Hashed Canonical Request: {hashed_canonical_request}")

#     string_to_sign = (
#         f"{algorithm}\n"
#         f"{amz_date}\n"
#         f"{credential_scope}\n"
#         f"{hashed_canonical_request}"
#     )
#     print(f"[Helper] StringToSign:\n---\n{string_to_sign}\n---")

#     signing_key = get_signature_key(secret_key, datestamp, aws_region, service)
#     signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()
#     print(f"[Helper] Calculated Signature: {signature}")

#     canonical_querystring += f'&X-Amz-Signature={signature}'
#     final_url = f"wss://{host}{canonical_uri}?{canonical_querystring}"

#     print(f"[transcribe_helper] Pre-signed URL 생성 완료 (PCM, 자동 식별: {language_options_str})")
#     return final_url