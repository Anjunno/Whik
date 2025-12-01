import json
import boto3
import os
from decimal import Decimal
from botocore.exceptions import ClientError

# --- 초기화 ---
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
RESULTS_TABLE_NAME = os.environ.get("RESULTS_TABLE_NAME")
AUDIO_BUCKET_NAME = os.environ.get("AUDIO_BUCKET_NAME")
results_table = dynamodb.Table(RESULTS_TABLE_NAME)

# --- 헬퍼 클래스 및 함수 ---
class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return int(o) if o % 1 == 0 else float(o)
        return super(DecimalEncoder, self).default(o)

def generate_presigned_url_from_s3_uri(s3_uri, expiration=3600):
    try:
        parts = s3_uri.replace("s3://", "").split('/', 1)
        bucket_name = parts[0]
        key = parts[1]
        url = s3_client.generate_presigned_url(
            'get_object', Params={'Bucket': bucket_name, 'Key': key}, ExpiresIn=expiration
        )
        return url
    except Exception as e:
        print(f"Pre-signed URL 생성 실패 ({s3_uri}): {e}")
        return None

def lambda_handler(event, context):
    # 모든 응답에 포함될 CORS 헤더
    cors_headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,uuid',
        'Access-Control-Allow-Methods': 'OPTIONS,GET'
    }

    # Preflight 요청 처리
    if event.get("httpMethod") == "OPTIONS":
        return {"statusCode": 200, "headers": cors_headers, "body": ""}

    try:
        job_id = event['pathParameters']['jobId']
        response = results_table.get_item(Key={'PK': job_id})
        item = response.get('Item')

        if not item:
            return {
                'statusCode': 202, 'headers': cors_headers,
                'body': json.dumps({'status': 'PENDING', 'message': 'Task is being processed or does not exist.'})
            }
        
        status = item.get('status')
        if status != 'COMPLETED':
            return {
                'statusCode': 202 if status == 'PENDING' else 200,
                'headers': cors_headers,
                'body': json.dumps(item, cls=DecimalEncoder)
            }

        result_data = item
        
        # S3 URI들을 Pre-signed URL로 변환
        if 'translatedAudioS3Uri' in result_data:
            result_data['translatedAudioUrl'] = generate_presigned_url_from_s3_uri(result_data['translatedAudioS3Uri'])
            del result_data['translatedAudioS3Uri']
        if 'correctionAudioS3Uri' in result_data:
            result_data['correctionAudioUrl'] = generate_presigned_url_from_s3_uri(result_data['correctionAudioS3Uri'])
            del result_data['correctionAudioS3Uri']
        if 'userInputVoiceS3Uri' in result_data:
            result_data['userInputVoiceUrl'] = generate_presigned_url_from_s3_uri(result_data['userInputVoiceS3Uri'])
            del result_data['userInputVoiceS3Uri']
        if 'recommendedAnswer' in result_data and 's3Url' in result_data['recommendedAnswer']:
            rec_s3_path = result_data['recommendedAnswer'].get('s3Url')
            if isinstance(rec_s3_path, str):
                full_s3_uri = f"s3://{AUDIO_BUCKET_NAME}/{rec_s3_path}"
                result_data['recommendedAnswer']['s3Url'] = generate_presigned_url_from_s3_uri(full_s3_uri)

        return {
            'statusCode': 200,
            'headers': cors_headers,
            'body': json.dumps(result_data, cls=DecimalEncoder)
        }
    except Exception as e:
        print(f"FATAL_ERROR: {e}")
        return {
            'statusCode': 500, 'headers': cors_headers,
            'body': json.dumps({'error': 'Failed to get task result.'})
        }