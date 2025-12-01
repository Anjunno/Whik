import json
import boto3
import uuid
import time
import os
from datetime import datetime
import hashlib
from botocore.exceptions import ClientError
from concurrent.futures import ThreadPoolExecutor, as_completed
from zoneinfo import ZoneInfo

s3 = boto3.client("s3")
dynamodb = boto3.resource("dynamodb")
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")
polly = boto3.client("polly")

BUCKET_NAME = os.environ.get("BUCKET_NAME")
LEARNING_TABLE = os.environ.get("LEARNING_TABLE")
MASTER_TABLE = os.environ.get("MASTER_TABLE")

learning_table = dynamodb.Table(LEARNING_TABLE)
master_table = dynamodb.Table(MASTER_TABLE)

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "OPTIONS,GET,POST",
    "Access-Control-Allow-Headers": "Content-Type,Authorization,uuid",
    "Content-Type": "application/json"
}

def lambda_handler(event, context):
    try:
        # OPTIONS 요청 처리
        if event.get("httpMethod") == "OPTIONS":
            return {
                "statusCode": 200,
                "headers": {
                    "Access-Control-Allow-Origin": "*",
                    "Access-Control-Allow-Methods": "OPTIONS,POST",
                    "Access-Control-Allow-Headers": "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,uuid",
                },
                "body": ""
            }

        # 1. Authorizer에서 user_uuid 가져오기
        authorizer_context = event.get("requestContext", {}).get("authorizer", {})
        user_id = authorizer_context.get("user_uuid")
        gender = authorizer_context.get("gender")

        if not user_id or not gender:
            return {
                "statusCode": 403,
                "body": json.dumps({"error": "Unauthorized: missing user_uuid"}),
                "headers": CORS_HEADERS,
            }

        # 2. 요청 Body 파싱
        body = json.loads(event.get("body", "{}"))
        scenario_id = body.get("scenarioId")
        file_key = body.get("fileKey")
        original_word = body.get("originalWord")
        target_lang = body.get("targetLanguage")

        if not all([user_id, scenario_id, file_key, original_word, target_lang, gender]):
            return {
                "statusCode": 400,
                "headers": CORS_HEADERS,
                "body": json.dumps({"error": "Missing required fields"})
            }

        if target_lang == 'jp':
            target_lang = 'ja'

        # 3. word_lang_gender 키 생성
        key_base = f"{original_word}#{target_lang}"
        if target_lang == "es":
            key_base += f"#{gender}"
        word_hash = hashlib.sha256(key_base.encode()).hexdigest()

        # 4. 마스터 테이블 기존 학습 기록 조회
        existing = master_table.get_item(Key={"userId": user_id, "word_lang_gender": word_hash}).get("Item")
        prev_record = None
        if existing:
            last_learned_at_iso_utc = existing.get("lastLearnedAtIso")
            last_learned_at_kst_str = ""
            if last_learned_at_iso_utc:
                try:
                    utc_dt = datetime.fromisoformat(last_learned_at_iso_utc.replace('Z', '+00:00'))
                    kst_dt = utc_dt.astimezone(ZoneInfo("Asia/Seoul"))
                    last_learned_at_kst_str = kst_dt.strftime("%Y-%m-%d %H:%M:%S")
                except (TypeError, ValueError):
                    last_learned_at_kst_str = str(last_learned_at_iso_utc)

            prev_record = {
                "originalWord": existing.get("originalWord"),
                "lastLearnedAtIso": last_learned_at_kst_str,
                "lastLearnedAtTs": int(existing.get("lastLearnedAtTs", 0)),
                "totalCount": int(existing.get("totalCount", 0)),
            }

        # 5. Bedrock 프롬프트 (발음 포함)
        gender_instruction = f" Since the target language is Spanish, please ensure the translation and related words reflect the '{gender}' gender." if target_lang == "es" else ""
        prompt = f"""
        Translate the Korean word '{original_word}' into {target_lang}.
        {gender_instruction}
        Also generate 4 related Korean words with their translations in {target_lang}.
        Additionally, provide the pronunciation for the main word and each related word in the target language.
        For Japanese, use Hiragana. For Chinese, use Pinyin. For Spanish, use standard phonetic spelling.
        Format response strictly as JSON:
        {{
            "main": "<translation>",
            "related_kr": ["word1","word2","word3","word4"],
            "related_translations": ["t1","t2","t3","t4"],
            "pronunciation": {{
                "main": "<pronunciation of main word>",
                "related_1": "<pronunciation of word1>",
                "related_2": "<pronunciation of word2>",
                "related_3": "<pronunciation of word3>",
                "related_4": "<pronunciation of word4>"
            }}
        }}
        """

        request_body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 300,
            "messages": [{"role": "user", "content": [{"type": "text", "text": prompt}]}],
        }

        response = bedrock.invoke_model(
            body=json.dumps(request_body),
            modelId="anthropic.claude-3-5-sonnet-20240620-v1:0",
            contentType="application/json",
            accept="application/json",
        )

        result = json.loads(response["body"].read())
        translation_data = json.loads(result["content"][0]["text"])

        # 6. S3 이미지 이동
        file_ext = file_key.split(".")[-1]
        new_key = f"user/{user_id}/{scenario_id}.{file_ext}"
        s3.copy_object(Bucket=BUCKET_NAME, CopySource={"Bucket": BUCKET_NAME, "Key": file_key}, Key=new_key)
        s3.delete_object(Bucket=BUCKET_NAME, Key=file_key)

        # 7. Polly 음성 생성 (ja, zh, es)
        audio_urls = {}
        audio_file_keys = {}
        if target_lang in ["ja", "zh", "es"]:
            voices = {
                "ja": "Mizuki",
                "zh": "Zhiyu",
                "es_male": "Enrique",
                "es_female": "Conchita",
            }
            voice_id = (
                voices["es_male"] if target_lang == "es" and gender == "male"
                else voices["es_female"] if target_lang == "es" and gender == "female"
                else voices[target_lang]
            )

            def synthesize_audio(text, word_key):
                file_hash = hashlib.sha256(word_key.encode()).hexdigest()
                s3_key = f"audios/{file_hash}.mp3"
                reused = False
                try:
                    s3.head_object(Bucket=BUCKET_NAME, Key=s3_key)
                    reused = True
                except ClientError:
                    speech = polly.synthesize_speech(
                        Text=text,
                        OutputFormat="mp3",
                        VoiceId=voice_id,
                        LanguageCode={"ja": "ja-JP", "zh": "cmn-CN", "es": "es-ES"}[target_lang],
                    )
                    audio_stream = speech["AudioStream"].read()
                    s3.put_object(
                        Bucket=BUCKET_NAME,
                        Key=s3_key,
                        Body=audio_stream,
                        ContentType="audio/mpeg"
                    )
                presigned_url = s3.generate_presigned_url(
                    "get_object",
                    Params={"Bucket": BUCKET_NAME, "Key": s3_key},
                    ExpiresIn=3600
                )
                print(f"[Polly Stats] reused={1 if reused else 0}, generated={0 if reused else 1}, key={word_key}")
                return s3_key, presigned_url

            # main + related 단어 순차 처리
            tasks = [("main", translation_data["main"], f"{original_word}#{target_lang}" + (f"#{gender}" if target_lang=="es" else ""))]
            for i, text in enumerate(translation_data["related_translations"]):
                related_key_str = f"{translation_data['related_kr'][i]}#{target_lang}" + (f"#{gender}" if target_lang=="es" else "")
                tasks.append((f"related_{i+1}", text, related_key_str))

            # 순차 실행 (재활용 + 순차 방식)
            start = time.time()
            for name, text, key_str in tasks:
                s3_key, url = synthesize_audio(text, key_str)
                audio_urls[name] = url
                audio_file_keys[name] = s3_key
            end = time.time()
            print(f"[Polly Stats] total_duration={end - start:.3f}s (재활용+순차)")


        # 8. DynamoDB 저장
        ts = int(time.time())
        created_at_iso = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        related_words_kr_dict = {f"related_{i+1}": w for i, w in enumerate(translation_data["related_kr"])}
        translation_details_dict = {"main": translation_data["main"]}
        translation_details_dict.update({f"related_{i+1}": t for i, t in enumerate(translation_data["related_translations"])})
        pronunciation_dict = translation_data.get("pronunciation", {})
        item = {
            "userId": user_id,
            "createdAtTs": ts,
            "createdAtIso": created_at_iso,
            "scenarioId": scenario_id,
            "originalWord": original_word,
            "relatedWords_kr": related_words_kr_dict,
            "translationDetails": translation_details_dict,
            "fileKey": new_key,
            "targetLanguage": target_lang,
            "pronunciation": translation_data.get("pronunciation", {})
        }
        if audio_file_keys:
            item["audioFileKeys"] = audio_file_keys

        learning_table.put_item(Item=item)

        # 9. 이미지 presigned URL
        image_url = s3.generate_presigned_url("get_object", Params={"Bucket": BUCKET_NAME, "Key": new_key}, ExpiresIn=3600)

        # 10. 마스터 테이블 업데이트
        if existing:
            master_table.update_item(
                Key={"userId": user_id, "word_lang_gender": word_hash},
                UpdateExpression="SET lastLearnedAtTs = :ts, lastLearnedAtIso = :iso, totalCount = totalCount + :inc",
                ExpressionAttributeValues={":ts": ts, ":iso": created_at_iso, ":inc": 1}
            )
        else:
            master_table.put_item(
                Item={"userId": user_id, "word_lang_gender": word_hash, "originalWord": original_word, "lastLearnedAtTs": ts, "lastLearnedAtIso": created_at_iso, "totalCount": 1}
            )

        # 11. 응답 반환
        response_body = {
            "imageUrl": image_url,
            "originalWord": original_word,
            "relatedWords_kr": related_words_kr_dict,
            "translationDetails": translation_details_dict,
            "pronunciation": translation_data.get("pronunciation", {})
        }
        if audio_urls:
            response_body["audioUrls"] = audio_urls
        if prev_record:
            response_body["previousLearning"] = prev_record

        return {
            "statusCode": 200,
            "body": json.dumps(response_body, ensure_ascii=False),
            "headers": CORS_HEADERS,
        }

    except Exception as e:
        print("Error:", e)
        return {
            "statusCode": 500,
            "headers": CORS_HEADERS,
            "body": json.dumps({"error": str(e)})
        }
