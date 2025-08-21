# Place Manager (마감 안내 자동화)

구글 스프레드시트의 각 작업 탭(워크시트)에서 공통 컬럼들을 읽어, 대행사별/작업별로 복사 가능한 카톡 마감 안내 문구를 자동 생성합니다.

## 요구 사항
- Python 3.10+
- 구글 서비스 계정 (해당 스프레드시트에 조회 권한 공유 필요)

## 설치
```powershell
cd place_manager
python -m venv .venv
. .venv\Scripts\activate
pip install -r requirements.txt
```

## 환경 설정
1) `env.example` 파일을 복사하여 `.env` 파일을 만듭니다. (Windows 탐색기에서 파일 이름 앞에 점을 붙여 저장하세요)
2) 서비스 계정 키를 준비합니다. 두 가지 방법 중 하나를 사용하세요.
   - 키 파일 사용: `GOOGLE_APPLICATION_CREDENTIALS`에 파일 경로 설정 (예: `service_account.json`)
   - 인라인 JSON 사용: `SERVICE_ACCOUNT_JSON`에 키 JSON 문자열 전체를 넣기
3) 스프레드시트 ID를 `SPREADSHEET_ID`에 설정합니다. 주소가 `https://docs.google.com/spreadsheets/d/<ID>/edit`면 `<ID>` 부분입니다.
4) 시트별로 열 구조가 달라도 아래 5개 컬럼명이 있어야 합니다.
   - 대행사 명
   - 내부 진행건
   - 마감 잔여일
   - 마감 안내 체크
   - 상호명

`.env` 예시는 `env.example` 참고.

## 실행
개발 실행(Flask 내장 서버):
```powershell
python app.py
```
프로덕션 실행(Waitress):
```powershell
python app.py --prod
```
기본 주소: `http://localhost:8080`

## 사용 방법
- 상단 입력창에 만료 기준 일수(0=오늘, 1=1일뒤, 2=2일뒤 …)를 콤마(,)로 입력하고 조회
- 결과는 [대행사] 묶음으로 보이며, 각 대행사 카드의 복사 버튼을 눌러 바로 카톡에 붙여넣기
- 실제 복사되는 내용에는 대행사명은 포함되지 않고, 아래 형식으로 출력됩니다:

```
<작업명>
상호명1
상호명2

<작업명>
상호명1
상호명2
상호명3
```

## 배포/상시 구동 팁
- 사내 24시간 구동 PC에 설치 후 `--prod`로 실행하고, Windows 서비스(작업 스케줄러)로 등록하면 자동 재시작에 유리합니다.
- 외부 무료 호스팅은 장기 상시 구동 제약이 많습니다. 내부 PC 또는 자체 서버 사용을 권장합니다.
