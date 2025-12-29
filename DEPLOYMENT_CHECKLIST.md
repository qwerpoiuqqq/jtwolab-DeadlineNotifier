# 🚀 Render 배포 체크리스트

## 배포 전 준비

### 1. GitHub 저장소 준비
- [ ] 코드를 GitHub 저장소에 푸시
- [ ] `.env` 파일이 커밋되지 않았는지 확인 (.gitignore 포함됨)
- [ ] `service_account.json` 파일이 커밋되지 않았는지 확인

### 2. 구글 서비스 계정 준비
- [ ] 서비스 계정 JSON 키 파일 준비
- [ ] JSON 내용을 한 줄로 압축 (공백 제거)
- [ ] 스프레드시트에 서비스 계정 이메일 공유 완료

### 3. 애드로그 계정 확인
- [ ] 애드로그 로그인 정보 확인 (ID: jtwolab, PW: 1234)
- [ ] 월보장 페이지 접속 가능 확인

## Render 배포

### 1. Web Service 생성
- [ ] [Render.com](https://render.com) 로그인
- [ ] **New** → **Web Service** 클릭
- [ ] GitHub 저장소 선택
- [ ] Branch: `main` (또는 배포할 브랜치)

### 2. 기본 설정
- **Name**: `jtwolab-deadline-notifier`
- **Runtime**: `Docker` (권장) 또는 `Python 3`
- **Build Command**: `bash build.sh` (Python 3인 경우) / Docker는 자동
- **Start Command**: `bash start.sh` (Python 3인 경우) / Docker는 자동
- **Instance Type**: `Free` (또는 `Starter - $7/month` 권장)

⚠️ **Free Tier 제한사항**:
- 메모리: 512MB (Playwright 사용 시 부족할 수 있음)
- 15분 무활동 시 sleep (첫 요청 시 10-20초 지연)
- 파일시스템 임시 (재배포 시 DB 초기화)

💡 **Starter 플랜 권장** ($7/month):
- 메모리: 2GB (Playwright 안정적 실행)
- Sleep 없음
- 더 빠른 응답

🎭 **Playwright 브라우저 설치**:
- Docker 런타임 사용 시: Dockerfile에서 자동 설치
- Python 런타임 사용 시: build.sh에서 자동 설치
- 런타임에도 자동 설치 로직 포함 (rank_crawler.py)
- 순위 갱신 기능은 브라우저가 정상 설치되어야 작동

### 3. 환경변수 설정

**Environment** 탭에서 다음 변수 추가:

#### 필수 환경변수

```bash
# 구글 서비스 계정 (JSON 한 줄로)
SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"...전체내용..."}

# 스프레드시트 ID
SPREADSHEET_ID=your_main_spreadsheet_id
JTWOLAB_SHEET_ID=1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y
ILRYU_SHEET_ID=1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM

# 애드로그 로그인
ADLOG_ID=jtwolab
ADLOG_PASSWORD=1234
```

#### 권장 환경변수

```bash
# 데이터 암호화 (보안 강화)
DATA_ENCRYPTION_KEY=your_32_character_random_key_here

# 자동 초기화 (Render용)
AUTO_SYNC_ON_START=true
```

### 4. 배포 실행
- [ ] **Create Web Service** 버튼 클릭
- [ ] 빌드 로그 확인 (5-10분 소요)
- [ ] 배포 완료 확인

## 배포 후 테스트

### 1. 기본 접속
- [ ] Render URL 접속 (`https://your-app.onrender.com`)
- [ ] 메인 페이지 로딩 확인

### 2. 월보장 관리 테스트
- [ ] `/manage` 페이지 접속
- [ ] 데이터가 자동 로드되는지 확인 (AUTO_SYNC_ON_START 설정 시)
- [ ] "🔄 동기화" 버튼 클릭하여 수동 동기화 테스트

### 3. 순위 크롤링 테스트
- [ ] "🏆 순위 갱신" 버튼 클릭
- [ ] 크롤링 성공 메시지 확인
- [ ] 테이블의 "현재순위" 컬럼에 데이터 표시 확인
- [ ] "실시간 노출 현황" 클릭하여 순위 확인

### 4. 작업량 캐시 테스트
- [ ] "⚡ 작업량 갱신" 버튼 클릭
- [ ] 캐시 생성 확인
- [ ] 업체의 "📊 작업량" 버튼 클릭하여 즉시 로딩 확인

### 5. 스케줄러 확인
Render 로그에서 다음 스케줄 실행 확인:
- [ ] 09:00 보장건 동기화
- [ ] 11:30 작업량 캐시 갱신
- [ ] 14:30 순위 크롤링
- [ ] 16:00 보장건 동기화

## 문제 해결

### 빌드 실패
- Playwright 설치 실패 → 로그 확인, 메모리 부족이면 유료 플랜
- Python 의존성 오류 → requirements.txt 확인

### 런타임 오류
- 503 Service Unavailable → 로그에서 에러 확인
- Playwright 크롤링 실패 → 메모리 부족, 브라우저 실행 오류
- DB 데이터 없음 → 재배포 시 초기화됨 (정상)

### 메모리 부족 (Free Tier)
```
Error: browser closed unexpectedly
```
→ Starter 플랜으로 업그레이드 권장

### Sleep 모드 방지 (선택사항)
UptimeRobot 등으로 5분마다 ping:
- URL: `https://your-app.onrender.com`
- Method: GET

## 유지보수

### 로그 모니터링
Render 대시보드의 **Logs** 탭:
- 실시간 로그 확인
- 에러 발생 시 알림 설정

### 데이터 백업 (선택사항)
재배포 시 DB가 초기화되므로:
1. `/api/guarantee/ranks/export`로 순위 데이터 백업
2. 로컬에 JSON 저장
3. 재배포 후 `/api/guarantee/ranks/import`로 복원

### 환경변수 업데이트
- Render 대시보드의 **Environment** 탭에서 수정
- 변경 후 자동 재배포됨

## 비용 최적화

### Free Tier 활용
- ✅ 무료로 사용 가능
- ✅ 750시간/월 (충분함)
- ⚠️ Sleep 모드 (15분 무활동 시)
- ⚠️ 메모리 512MB (Playwright 부족할 수 있음)

### Starter 플랜 ($7/month)
- ✅ 2GB 메모리
- ✅ Sleep 없음
- ✅ 더 빠른 응답
- ✅ Playwright 안정적 실행

## 추가 개선 사항 (선택)

### PostgreSQL 마이그레이션
데이터 영속성을 위해:
1. Render PostgreSQL 인스턴스 생성 (무료 90일)
2. `rank_crawler.py`를 PostgreSQL용으로 수정
3. SQLAlchemy ORM 사용

### Google Cloud Storage 백업
- DB 파일을 주기적으로 GCS에 백업
- 앱 시작 시 자동 복원

