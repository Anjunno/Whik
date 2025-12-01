# 파일명: transcribe_stream_helper.py
# (이 코드를 ...stream-s3url 람다에 넣고, Layer로 만드시면 됩니다)

import json
import boto3
import datetime
import hmac
import hashlib
import urllib.parse
import os

# --- SigV4 서명 보조 함수들 ---

def sign(key, msg):
    """HMAC-SHA256 서명을 생성합니다."""
    return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

def get_signature_key(key, date_stamp, region_name, service_name):
    """AWS SigV4 서명 키를 생성합니다."""
    kDate = sign(('AWS4' + key).encode('utf-8'), date_stamp)
    kRegion = sign(kDate, region_name)
    kService = sign(kRegion, service_name)
    kSigning = sign(kService, 'aws4_request')
    return kSigning

# --- 메인 함수 (onMessage가 호출할 함수) ---

def generate_transcribe_presigned_url(language_candidates: list, sample_rate: int = 16000):
    """
    Amazon Transcribe Streaming (WebSocket)용 Pre-signed URL을 생성합니다.
    자동 언어 식별('identify-language=True')을 사용하고, 
    제공된 언어 후보('language-options')를 사용하도록 설정합니다.
    """
    
    # 이 람다(또는 onMessage 람다)가 실행되는 리전을 가져옵니다.
    aws_region = os.environ.get('AWS_REGION', 'us-east-1')
    if not aws_region:
        raise Exception("AWS_REGION 환경 변수가 설정되지 않았습니다.")
    
    session = boto3.Session(region_name=aws_region)
    credentials = session.get_credentials()
    
    if credentials.access_key is None:
        raise Exception("AWS 자격 증명을 찾을 수 없습니다. Lambda 실행 역할(Role)을 확인하세요.")

    access_key = credentials.access_key
    secret_key = credentials.secret_key
    session_token = credentials.token # IAM Role을 사용할 때 필수

    # Transcribe Streaming 엔드포인트 정보
    service = 'transcribe'
    host_without_port = f'transcribestreaming.{aws_region}.amazonaws.com'
    host = f'{host_without_port}:8443' # WSS는 8443 포트 사용
    canonical_uri = '/stream-transcription-websocket'

    # 시간 및 날짜 스탬프
    t = datetime.datetime.utcnow()
    amz_date = t.strftime('%Y%m%dT%H%M%SZ')
    datestamp = t.strftime('%Y%m%d')

    # 쿼리 파라미터 (자동 언어 식별)
    language_options_str = ",".join(language_candidates)
    
    query_params = {
        'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
        'X-Amz-Credential': f"{access_key}/{datestamp}/{aws_region}/{service}/aws4_request",
        'X-Amz-Date': amz_date,
        'X-Amz-Expires': '300', # 5분
        'X-Amz-Security-Token': session_token,
        'X-Amz-SignedHeaders': 'host',
        'identify-language': 'True',
        'language-options': language_options_str,
        'media-encoding': 'pcm',
        'sample-rate': str(sample_rate)
    }
    
    sorted_params = sorted(query_params.items())
    canonical_querystring = urllib.parse.urlencode(sorted_params)

    # 서명 생성 (SigV4)
    method = 'GET'
    canonical_headers = 'host:' + host_without_port + '\n'
    signed_headers = 'host'
    payload_hash = hashlib.sha256(('').encode('utf-8')).hexdigest()
    
    canonical_request = (
        f"{method}\n"
        f"{canonical_uri}\n"
        f"{canonical_querystring}\n"
        f"{canonical_headers}\n"
        f"{signed_headers}\n"
        f"{payload_hash}"
    )

    algorithm = 'AWS4-HMAC-SHA256'
    credential_scope = f"{datestamp}/{aws_region}/{service}/aws4_request"
    string_to_sign = (
        f"{algorithm}\n"
        f"{amz_date}\n"
        f"{credential_scope}\n"
        f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
    )

    signing_key = get_signature_key(secret_key, datestamp, aws_region, service)
    signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()

    # 최종 URL 조립
    canonical_querystring += f'&X-Amz-Signature={signature}'
    final_url = f"wss://{host}{canonical_uri}?{canonical_querystring}"
    
    print(f"[transcribe_helper] Pre-signed URL 생성 완료 (언어: {language_options_str})")
    return final_url