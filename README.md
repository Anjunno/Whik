# Whik!

<p align="center">
  <img src="assets/whik_logo.png" alt="Whik Logo" width="160" />
</p>

<p align="center"><strong>휙! 넘기다 보면 말이 트인다!</strong></p>

간단 소개
==========

**Whik(휙!)** 은 숏폼 기반 외국어 학습 모바일 앱으로, 외국어 학습에 숏폼 콘텐츠의 몰입성을 결합하여 높은 이탈률이라는 기존 언어 학습 앱의 한계를 개선하고자 개발했습니다.

주요 기능
==========

### 사용자 기능

- **숏폼 콘텐츠 제공** - 사용자의 흥미를 끄는 숏폼 콘텐츠 제공
- **테마별 학습** - 카페, 편의점 등 실생활 테마별 콘텐츠 제공
- **청해력 향상** - 영상 시청 후 의미 추론으로 듣기 능력 강화
- **문법 학습** - 문장 청크 배치로 문법 이해
- **말하기 학습** - 영상 속 인물 대사 따라해보기
- **응답 학습** - 영상 속 인물의 질문에 대답해보기

### 시스템 자동화 기능

- **Step Functions 기반 영상 자동생성 및 검증** 
  - AI(Bedrock)가 스크립트 자동 생성
  - 검증 후 비디오/오디오 자동 생성 및 S3, DynamoDB 저장

- **DynamoDB Streams 트리거 기반 학습 데이터 자동생성**
  - 영상 생성 완료 → DynamoDB 저장
  - DynamoDB Streams 이벤트 감지 → Lambda 자동 트리거
  - Step Functions으로 영상별 학습 데이터 자동 생성 및 검증

시연
==========
| |  |  |
|---|---|---|
| ![전체 흐름](<assets/demo_flow.gif>) | ![짧은 데모](assets/short.gif)| ![엔딩](<assets/demo_ending.gif>)  |

시스템 아키텍처
-------------

![Architecture](assets/whik_architecture.png)

**Step Functions 워크플로우**

| 영상 자동생성 | 학습 데이터 자동생성 |
|---|---|
| ![영상 자동생성](assets/영상생성.png) | ![학습 데이터 자동생성](assets/학습데이터생성.png) |

기술 스택
==========
| 카테고리 | 기술 |
| :--- | :--- |
| **Backend & Cloud** | ![AWS](https://img.shields.io/badge/AWS-FF9900?style=flat&logo=amazon&logoColor=white) ![Lambda](https://img.shields.io/badge/AWS%20Lambda-FF9900?style=flat&logo=aws-lambda&logoColor=white) ![API Gateway](https://img.shields.io/badge/Amazon%20API%20Gateway-FF4F8B?style=flat&logo=amazon&logoColor=white) |
| **Data & Storage** | ![DynamoDB](https://img.shields.io/badge/Amazon%20DynamoDB-4053D6?style=flat&logo=amazondynamodb&logoColor=white) ![RDS](https://img.shields.io/badge/Amazon%20RDS-527FFF?style=flat&logo=amazon-rds&logoColor=white) ![S3](https://img.shields.io/badge/Amazon%20S3-569A31?style=flat&logo=amazon-s3&logoColor=white)  |
| **Messaging & Orchestration** | ![SQS](https://img.shields.io/badge/Amazon%20SQS-FF4F8B?style=flat&logo=amazon-sqs&logoColor=white) ![Step Functions](https://img.shields.io/badge/AWS%20Step%20Functions-FF9900?style=flat&logo=amazon&logoColor=white) |
| **AI/ML & Processing** | ![Bedrock](https://img.shields.io/badge/Amazon%20Bedrock-FF9900?style=flat&logo=amazon&logoColor=white) ![Polly](https://img.shields.io/badge/Amazon%20Polly-FF9900?style=flat&logo=amazon&logoColor=white) ![Transcribe](https://img.shields.io/badge/Amazon%20Transcribe-FF9900?style=flat&logo=amazon&logoColor=white) ![Google TTS](https://img.shields.io/badge/Google%20TTS-4285F4?style=flat&logo=google&logoColor=white) |
| **Deployment & CDN** | ![CloudFront](https://img.shields.io/badge/Amazon%20CloudFront-FF9900?style=flat&logo=amazon&logoColor=white) ![EC2](https://img.shields.io/badge/Amazon%20EC2-FF9900?style=flat&logo=amazon-ec2&logoColor=white) |
| **Languages & Tools** | ![Python](https://img.shields.io/badge/Python-3776AB?style=flat&logo=python&logoColor=white) ![GitHub](https://img.shields.io/badge/GitHub-%23121011.svg?style=flat&logo=github&logoColor=white) |

폴더 구조
==========

- `API_Gateway/` - API Gateway 설정 및 통합
- `CloudFront/` - CloudFront 배포 구성
- `DynamoDB/` - DynamoDB 테이블 스키마
- `EC2/` - EC2 인스턴스 구성
- `EventBridge/` - EventBridge 스케줄러 설정
- `Lambda/` - Lambda 함수 코드 및 백업
- `RDS/` - RDS 데이터베이스 스키마
- `S3/` - S3 버킷 정책 및 CORS 설정
- `SQS/` - SQS 큐 설정
- `Step_Functions/` - Step Functions 워크플로우 정의
