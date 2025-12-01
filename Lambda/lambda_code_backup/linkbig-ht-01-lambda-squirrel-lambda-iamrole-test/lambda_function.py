import json
import boto3
import os
from botocore.exceptions import ClientError

# --- 환경 변수 로드 ---
# 2단계에서 설정한 값을 읽어옵니다.
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
AWS_REGION = os.environ.get("AWS_REGION", 'us-east-1')

# --- 서비스 클라이언트 초기화 ---
# 리전을 명시적으로 설정
try:
    transcribe = boto3.client('transcribe', region_name=AWS_REGION)
    dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)
    s3 = boto3.client('s3', region_name=AWS_REGION)
    translate = boto3.client('translate', region_name=AWS_REGION)
    polly = boto3.client('polly', region_name=AWS_REGION)
    bedrock = boto3.client('bedrock', region_name=AWS_REGION) # ListModels용
    
    clients_initialized = True
except Exception as e:
    clients_initialized = False
    initialization_error = str(e)


def lambda_handler(event, context):
    
    test_results = {}
    
    if not clients_initialized:
        return {
            'statusCode': 500,
            'body': json.dumps({"ERROR": f"클라이언트 초기화 실패: {initialization_error}"})
        }

    # === 1. Transcribe 테스트 ===
    # (transcribe:StartStreamTranscription의 프록시 테스트)
    try:
        # 가장 간단한 읽기 권한인 ListVocabularies를 테스트합니다.
        transcribe.list_vocabularies(MaxResults=1)
        test_results['Transcribe (ListVocabularies)'] = "✅ SUCCESS"
    except Exception as e:
        test_results['Transcribe (ListVocabularies)'] = f"❌ FAILED: {e}"

    # === 2. DynamoDB 테스트 ===
    try:
        if not RESULTS_TABLE_NAME:
            test_results['DynamoDB'] = "SKIPPED (RESULTS_TABLE_NAME 변수 없음)"
        else:
            dynamodb.describe_table(TableName=RESULTS_TABLE_NAME)
            test_results['DynamoDB (DescribeTable)'] = f"✅ SUCCESS ({RESULTS_TABLE_NAME})"
    except Exception as e:
        test_results['DynamoDB (DescribeTable)'] = f"❌ FAILED: {e}"

    # === 3. S3 테스트 ===
    try:
        if not AUDIO_BUCKET_NAME:
            test_results['S3'] = "SKIPPED (AUDIO_BUCKET_NAME 변수 없음)"
        else:
            # 버킷 접근 권한(s3:ListBucket 등)이 아닌, HeadBucket 권한 테스트
            s3.head_bucket(Bucket=AUDIO_BUCKET_NAME)
            test_results['S3 (HeadBucket)'] = f"✅ SUCCESS ({AUDIO_BUCKET_NAME})"
    except Exception as e:
        test_results['S3 (HeadBucket)'] = f"❌ FAILED: {e}"

    # === 4. Translate 테스트 ===
    try:
        translate.translate_text(Text="test", SourceLanguageCode="en", TargetLanguageCode="ko")
        test_results['Translate (TranslateText)'] = "✅ SUCCESS"
    except Exception as e:
        test_results['Translate (TranslateText)'] = f"❌ FAILED: {e}"

    # === 5. Polly 테스트 (수정됨) ===
    try:
        # describe_voices는 기본 권한 중 하나입니다.
        polly.describe_voices(LanguageCode='ko-KR')
        test_results['Polly (DescribeVoices)'] = "✅ SUCCESS"
    except Exception as e:
        test_results['Polly (DescribeVoices)'] = f"❌ FAILED: {e}"

    # === 6. Bedrock 테스트 (수정됨) ===
    try:
        # bedrock:ListFoundationModels 권한을 테스트합니다.
        # maxResults -> maxItems로 수정
        bedrock.list_foundation_models(maxItems=1) 
        test_results['Bedrock (ListFoundationModels)'] = "✅ SUCCESS"
    except Exception as e:
        test_results['Bedrock (ListFoundationModels)'] = f"❌ FAILED: {e}"

    # === 최종 결과 반환 ===
    return {
        'statusCode': 200,
        'body': json.dumps(test_results, indent=2, ensure_ascii=False)
    }