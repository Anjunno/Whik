import json
import boto3
import base64
from urllib.parse import unquote_plus
from decimal import Decimal
import os

# AWS 클라이언트 초기화
s3_client = boto3.client("s3")
rekognition_client = boto3.client("rekognition")
translate_client = boto3.client("translate")
dynamodb = boto3.resource("dynamodb")

TABLE_NAME = os.environ.get("TABLE_NAME")

def lambda_handler(event, context):
    try:
        # 1️. S3 이벤트에서 파일 정보 추출
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        file_key = unquote_plus(record["s3"]["object"]["key"])
        print(f"Reading image from s3://{bucket_name}/{file_key}")

        # 파일명 기반 시나리오 ID
        scenario_id = os.path.splitext(os.path.basename(file_key))[0]
        print(f"Scenario ID: {scenario_id}")

        # 2️. S3에서 이미지 바이트 읽기 
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        img_bytes = s3_object["Body"].read()

        # 3️. Rekognition 객체 탐지 수행
        labels = rekognition_client.detect_labels(
            Image={"Bytes": img_bytes},
            MaxLabels=2
        )["Labels"]

        if not labels:
            raise Exception("No labels detected in image")

        # Rekognition 레이블을 confidence 기준 내림차순 정렬
        sorted_labels = sorted(labels, key=lambda x: x["Confidence"], reverse=True)

        # 신뢰도 threshold 설정
        # threshold = 70.0
        # filtered_labels = [l for l in sorted_labels if l["Confidence"] >= threshold]

        # 두 번째로 높은 confidence label 선택 (없으면 첫 번째 선택)
        if len(sorted_labels) >= 2:
            selected_label = sorted_labels[1]
        else:
            selected_label = sorted_labels[0]

        detected_name = selected_label["Name"]
        confidence = Decimal(str(selected_label["Confidence"]))
        print(f"Detected object (EN): {detected_name} ({confidence:.2f}%)")

        # 4️. Translate로 한국어 번역
        translated = translate_client.translate_text(
            Text=detected_name,
            SourceLanguageCode="en",
            TargetLanguageCode="ko"
        )
        object_name_kr = translated["TranslatedText"]
        print(f"Translated object (KR): {object_name_kr}")

        # 5️. DynamoDB 업데이트 (영문 + 한글 + 신뢰도 저장)
        table = dynamodb.Table(TABLE_NAME)
        table.update_item(
            Key={"scenarioId": scenario_id},
            UpdateExpression=(
                "SET recognizedWord = :rw, recognizedWordEn = :rwe, "
                "#st = :st, confidence = :cf"
            ),
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues={
                ":rw": object_name_kr,
                ":rwe": detected_name,
                ":st": "COMPLETED",
                ":cf": confidence
            }
        )

        # 6️. Lambda 응답
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "scenarioId": scenario_id,
                    "recognizedWord": object_name_kr,
                    "recognizedWordEn": detected_name,
                    "confidence": round(float(confidence), 2),
                    "fileKey": file_key,
                },
                ensure_ascii=False,
            ),
        }

    except Exception as e:
        print(f"Error occurred: {e}")

        # 실패 시 DynamoDB 상태 업데이트
        try:
            table = dynamodb.Table(TABLE_NAME)
            table.update_item(
                Key={"scenarioId": scenario_id},
                UpdateExpression="SET #st = :st",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues={":st": "FAILED"},
            )
        except Exception as inner_e:
            print(f"Failed to update DynamoDB with error status: {inner_e}")

        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
