# Whik!

<p align="center">
  <img src="assets/whik_logo.png" alt="Whik Logo" width="160" />
</p>

<p align="center"><strong>휙! 넘기다 보면 말이 트인다!</strong></p>

<br>

## 간단 소개
**Whik(휙!)** 은 숏폼 기반 외국어 학습 모바일 앱으로, 외국어 학습에 숏폼 콘텐츠의 몰입성을 결합하여  
높은 이탈률이라는 기존 언어 학습 서비스의 한계를 개선하고자 개발했습니다.

<br>

## 주요 기능

### 사용자 기능
- **숏폼 콘텐츠 제공** — 사용자의 흥미를 끄는 영상 기반 학습
- **테마별 학습** — 카페, 편의점 등 실생활 테마 중심 콘텐츠
- **청해력 향상** — 영상 시청 후 의미 추론 방식으로 듣기 능력 향상
- **문법 학습** — 문장 청크 배치 기반 구조적 이해
- **말하기 학습** — 영상 속 인물 대사 따라하기
- **응답 학습** — 영상 속 질문에 직접 대답하여 스피킹 훈련

### 영상 자동 생성 파이프라인
1. html 폼을 통해 입력
2. Bedrock로 스크립트 생성 
3. 레퍼런스 이미지 크롤링
4. Veo3 영상 생성
5. 가이드라인 기반 검증
6. 영상 및 스크립트 S3 · DynamoDB 자동 저장

### 학습 데이터 자동 생성 파이프라인
1. DynamoDB Streams 이벤트 감지
2. Lambda Trigger 및 Step Functions 연동
3. Bedrock 학습 데이터 생성
4. json 구조 검증, Bedrock을 통한 교육적 품질 검증
5. 학습 관련 음성 병렬 생성
6. 최종 학습 데이터 S3 · DynamoDB 자동 저장

<br>

## 시연

|  |  |  |
|:---:|:---:|:---:|
| ![전체 흐름](assets/demo_flow.gif) | ![짧은 데모](assets/demo_short.gif) | ![엔딩](assets/demo_ending.gif) |

<br>

## 시스템 아키텍처

<p align="center">
  <img src="assets/whik_architecture.png" width="800" />
</p>

### Step Functions 워크플로우

| 영상 자동 생성 | 학습 데이터 자동 생성 |
|:---:|:---:|
| ![영상 자동생성](assets/영상생성.png) | ![학습 데이터 자동생성](assets/학습데이터생성.png) |

<br>

## 기술 스택

| 카테고리 | 기술 |
| --- | --- |
| **Frontend & App** | Flutter, Dart |
| **Backend & Cloud** | AWS, Python, Lambda, API Gateway |
| **Data & Storage** | DynamoDB, RDS(MySQL), S3 |
| **Messaging & Orchestration** | SQS, Step Functions |
| **AI / ML & Media** | Bedrock, Polly, Transcribe, Google TTS |
| **Deployment & CDN** | CloudFront, EC2 |

<br>

## 폴더 구조
- `API_Gateway/` — API Gateway 설정 및 통합
- `CloudFront/` — CloudFront 배포 구성
- `DynamoDB/` — DynamoDB 테이블 스키마
- `EC2/` — EC2 인스턴스 구성
- `EventBridge/` — EventBridge 스케줄러 설정
- `Lambda/` — Lambda 함수 코드
- `RDS/` — 데이터베이스 스키마
- `S3/` — 버킷 정책 및 CORS 설정
- `SQS/` — 큐 설정
- `Step_Functions/` — Step Functions 워크플로우 정의

<br>

<details>
<summary><strong>제거된 기능 & 관련 자료 보기</strong></summary>

<br>

## 제거된 기능
> 초기 설계 대비, 집중 기능 강화를 위해 아래 기능을 제외했습니다.

1. 사진 기반 단어 학습
2. 주간 단어 학습 기록
3. 단어 학습 그래프

<br>

## 단어학습 아키텍처 구성도
<p align="center">
  <img src="assets/word.png" width="720" />
</p>

<br>

## 단어학습 그래프
<p align="center">
  <img src="assets/graph.png" width="520" />
</p>

<br>

## 사진 기반 단어 학습 데모
<p align="center">
  <img src="assets/rrr.gif" width="400" />
</p>

<br>

</details>

