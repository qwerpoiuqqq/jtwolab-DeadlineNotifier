# Render 배포 시 주의사항

## 🚨 중요: 데이터 영속성 문제

Render Free Tier는 **임시 파일시스템(Ephemeral Filesystem)**을 사용합니다.
- 재배포 또는 재시작 시 모든 로컬 파일이 삭제됩니다
- SQLite DB, 캐시 파일 등이 사라집니다

### 영향받는 파일들
- `rank_history.db` - 순위 크롤링 데이터
- `workload_cache.json` - 작업량 캐시
- `guarantee_data.json` / `guarantee_data.enc` - 보장건 데이터
- `pricebook.json` - 단가 데이터
- `internal_cache.json` - 내부 진행건 캐시

## 해결 방안

### 방안 1: PostgreSQL 사용 (권장)
Render에서 무료 PostgreSQL 인스턴스 생성:
1. Render 대시보드에서 **New** → **PostgreSQL** 선택
2. `rank_crawler.py`를 PostgreSQL용으로 수정
3. SQLAlchemy 등 ORM 사용

### 방안 2: 매 재시작마다 재동기화
현재 구조 유지하되:
- 앱 시작 시 자동으로 구글 시트 동기화
- 순위 크롤링 자동 실행
- 작업량 캐시 자동 갱신

### 방안 3: 외부 스토리지
- Google Cloud Storage에 DB 백업
- 앱 시작 시 자동 복원

## 추천 설정

### 1. 앱 시작 시 자동 초기화 추가

`app.py`의 `create_app()` 함수에:
```python
# 앱 시작 시 자동 동기화 (Render용)
if os.getenv("AUTO_SYNC_ON_START", "false").lower() == "true":
    try:
        logger.info("Auto-syncing on startup...")
        gm = GuaranteeManager()
        gm.sync_from_google_sheets()
        logger.info("Auto-sync completed")
    except Exception as e:
        logger.error(f"Auto-sync failed: {e}")
```

### 2. 환경변수 추가
```bash
AUTO_SYNC_ON_START=true
```

### 3. 메모리 최적화
Render Free Tier는 512MB 제한:
- Playwright Chromium 실행 시 메모리 사용량 높음
- 필요시 유료 플랜($7/month) 고려

## 배포 후 모니터링

### 로그 확인
Render 대시보드의 **Logs** 탭에서:
- 빌드 로그 확인
- 런타임 로그 확인
- 스케줄러 실행 로그 확인

### 상태 확인
- `/manage` 페이지 접속
- "🔄 동기화" 버튼으로 데이터 로드
- "🏆 순위 갱신" 버튼으로 크롤링 테스트

## 비용 최적화

### Free Tier 활용
- 1개 Web Service 무료
- 750시간/월 무료 (항상 켜놓기 가능)
- 100GB 대역폭 무료

### 주의사항
- 15분 동안 요청이 없으면 sleep 모드
- 첫 요청 시 cold start (10-20초)
- 스케줄러는 sleep 중에도 앱을 깨움

### Sleep 방지 (선택사항)
외부 모니터링 서비스로 주기적 ping:
- UptimeRobot (무료)
- Cronitor 등

## 🔧 문제 해결 (Troubleshooting)

### Playwright 브라우저 설치 오류

**증상:**
```
ERROR: BrowserType.launch: Executable doesn't exist at /opt/render/.cache/ms-playwright/chromium-1140/chrome-linux/chrome
```

**해결 방법:**

#### 1. Docker 런타임 사용 (권장)
Render 대시보드에서:
- Settings → Runtime을 **Docker**로 변경
- `render.yaml`에 `runtime: docker` 설정 확인
- Docker 이미지에 Playwright 브라우저가 포함됨

#### 2. Python 런타임 사용 시
`build.sh`에 이미 Playwright 설치 명령이 포함되어 있습니다:
```bash
playwright install chromium
playwright install-deps chromium
```

빌드 로그에서 위 명령이 성공했는지 확인하세요.

#### 3. 메모리 부족 문제
Free Tier (512MB)에서 Playwright 실행 시 메모리 부족 발생 가능:
- **Starter 플랜** ($7/month, 2GB)으로 업그레이드 권장
- 또는 순위 갱신 빈도 줄이기

#### 4. 자동 복구 로직
`rank_crawler.py`에 자동 설치 로직이 추가되어 있어, 브라우저가 없으면 자동으로 설치 시도합니다.

### 데이터베이스 초기화 문제

**증상:** 재배포 시 데이터가 사라짐

**해결 방법:**
- 방안 1: Render PostgreSQL 사용 (무료)
- 방안 2: `AUTO_SYNC_ON_START=true` 설정 (자동 동기화)
- 방안 3: Google Cloud Storage 백업 사용

