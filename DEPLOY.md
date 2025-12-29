# Render 배포 가이드

## 사전 준비

### 1. GitHub 저장소
이 프로젝트를 GitHub에 푸시합니다.

### 2. Render 계정
[Render.com](https://render.com)에서 계정을 생성합니다.

## 배포 단계

### 1. Render에서 새 Web Service 생성

1. Render 대시보드에서 **New** → **Web Service** 클릭
2. GitHub 저장소 연결
3. 다음 설정 입력:

**Basic Settings:**
- **Name**: `jtwolab-deadline-notifier` (원하는 이름)
- **Runtime**: `Docker` (✨ Playwright 지원을 위해 Docker 사용)
- **Instance Type**: `Free` (또는 원하는 플랜)

> **Note**: Dockerfile을 사용하므로 Build Command와 Start Command는 자동으로 설정됩니다.

### 2. 환경변수 설정

Render 대시보드의 **Environment** 탭에서 다음 환경변수를 추가합니다:

#### 필수 환경변수

```bash
# 구글 시트 인증 (둘 중 하나)
SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"...전체 JSON 내용..."}
# 또는
GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/service_account.json

# 스프레드시트 ID
SPREADSHEET_ID=your_spreadsheet_id_here
JTWOLAB_SHEET_ID=1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y
ILRYU_SHEET_ID=1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM

# 애드로그 로그인
ADLOG_ID=jtwolab
ADLOG_PASSWORD=1234
ADLOG_URL=https://www.adlog.kr/adlog/naver_place_rank_check.php?sca=&sfl=api_memo&stx=%EC%9B%94%EB%B3%B4%EC%9E%A5&page_rows=100

# Flask 설정
FLASK_HOST=0.0.0.0
FLASK_PORT=8080
FLASK_DEBUG=false
```

#### 선택 환경변수

```bash
# 데이터 암호화 (권장)
DATA_ENCRYPTION_KEY=your_32_character_encryption_key

# 시트 컬럼 설정 (기본값이 있음)
AGENCY_COLUMN=대행사 명
INTERNAL_COLUMN=내부 진행건
REMAINING_DAYS_COLUMN=마감 잔여일
CHECKED_COLUMN=마감 안내 체크
BIZNAME_COLUMN=상호명
```

### 3. 배포 실행

**Deploy** 버튼을 클릭하면 자동으로 빌드 및 배포가 시작됩니다.

빌드 로그에서 다음 단계를 확인할 수 있습니다:
1. Python 패키지 설치
2. Playwright Chromium 브라우저 설치
3. 시스템 의존성 설치
4. 애플리케이션 시작

### 4. 배포 후 확인

배포가 완료되면 Render가 제공하는 URL로 접속합니다:
- 예: `https://jtwolab-deadline-notifier.onrender.com`

## 중요 참고사항

### ⚠️ 데이터 영속성 문제

**Render Free Tier의 제한사항:**
- 파일 시스템이 임시(ephemeral)입니다
- 재배포 또는 재시작 시 모든 파일이 삭제됩니다
- SQLite DB, 캐시 파일 등이 모두 사라집니다

**해결 방안:**

#### 옵션 1: 외부 DB 사용 (권장)
- Render의 PostgreSQL 서비스 사용 (무료 플랜 있음)
- 또는 다른 클라우드 DB 서비스 (Supabase, Railway 등)

#### 옵션 2: 클라우드 스토리지
- Google Cloud Storage, AWS S3 등에 DB 파일 백업/복원

#### 옵션 3: 현재 상태 유지
- 재시작마다 구글 시트에서 데이터 재동기화
- 순위 크롤링도 다시 실행
- 임시 사용에는 문제없지만 히스토리 손실

### 🔧 Playwright 설정

Docker 이미지를 사용하여 Playwright를 실행합니다:
1. ✅ Playwright 공식 Docker 이미지 사용 (mcr.microsoft.com/playwright/python)
2. ✅ 모든 브라우저 의존성이 미리 설치되어 있음
3. ✅ Chromium 자동 설치
4. ⚠️ 메모리 제한 주의 (Free tier: 512MB)
5. ✅ headless 모드로 실행 (rank_crawler.py에 설정됨)

### 📦 환경변수 보안

민감한 정보는 GitHub에 올리지 말고 Render 대시보드에서만 설정:
- ✅ `.env` 파일은 `.gitignore`에 포함됨
- ✅ `service_account.json`도 제외됨
- ⚠️ Render에서 `SERVICE_ACCOUNT_JSON`을 환경변수로 설정

## 배포 체크리스트

- [ ] GitHub에 코드 푸시 (민감 정보 제외)
- [ ] Render에서 Web Service 생성
- [ ] 환경변수 설정
- [ ] 빌드 성공 확인
- [ ] 애플리케이션 접속 테스트
- [ ] 구글 시트 동기화 테스트
- [ ] 순위 크롤링 테스트
- [ ] 자동 스케줄러 동작 확인

## 문제 해결

### 빌드 실패 시
- Docker 이미지 빌드 로그 확인
- `Dockerfile`의 베이스 이미지가 올바른지 확인
- 메모리 부족이면 유료 플랜 고려

### Playwright 브라우저 오류 시
- ✅ Docker를 사용하면 해결됨 (모든 의존성 포함)
- 이전 방식(Python runtime)에서는 시스템 의존성 설치 실패 가능
- "Executable doesn't exist" 오류 → Docker 런타임으로 전환

### 크롤링 실패 시
- headless 브라우저 설정 확인 (rank_crawler.py)
- 메모리 제한 확인 (Free tier: 512MB)
- 로그인 셀렉터가 변경되었는지 확인
- 애드로그 사이트 접근 가능 여부 확인

### DB 데이터 손실 시
- PostgreSQL 마이그레이션 고려
- 또는 주기적으로 구글 시트에 백업

## 로컬 테스트

Docker를 사용하여 로컬에서 테스트하려면:

```bash
# Docker 이미지 빌드
docker build -t deadline-notifier .

# 컨테이너 실행 (.env 파일 사용)
docker run --rm -p 8080:8080 --env-file .env deadline-notifier

# 브라우저에서 접속
# http://localhost:8080
```

