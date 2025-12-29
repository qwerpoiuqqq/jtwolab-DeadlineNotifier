# 월보장 관리 시스템 - 보안 및 데이터 관리 가이드

## 📊 시스템 개요

- **제이투랩 보장건**: https://docs.google.com/spreadsheets/d/1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y
- **일류기획 보장건**: https://docs.google.com/spreadsheets/d/1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM
- 각 시트의 '보장건' 탭에서 데이터 동기화

## 🔐 데이터 보안 구조

### 1. 암호화 저장
```
secure_data/
├── guarantee_data.enc      # 암호화된 보장건 데이터
├── .encryption_key         # 암호화 키 (자동 생성)
└── backups/               # 암호화된 백업 파일들
    ├── guarantee_data.enc.20241022_090000.bak
    └── ...
```

### 2. 암호화 방식
- **알고리즘**: Fernet (AES 128-bit)
- **키 관리**: 
  - 환경변수 `DATA_ENCRYPTION_KEY` (우선순위 1)
  - 로컬 파일 `.encryption_key` (우선순위 2)
  - 자동 생성 (첫 실행 시)

### 3. 백업 정책
- 데이터 수정 시 자동 백업
- 최근 10개 백업 유지
- 백업도 암호화 상태로 저장

## 🚀 Render 배포 보안 설정

### 환경변수 필수 설정
```bash
# 서비스 계정 (JSON 한 줄로)
SERVICE_ACCOUNT_JSON={"type":"service_account","project_id":"..."}

# 시트 ID
JTWOLAB_SHEET_ID=1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y
ILRYU_SHEET_ID=1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM

# 암호화 키 (고정값 사용 권장)
DATA_ENCRYPTION_KEY=<생성된 Fernet 키>
```

### 암호화 키 생성 방법
```python
from cryptography.fernet import Fernet
key = Fernet.generate_key()
print(key.decode())  # 이 값을 환경변수에 설정
```

## 📋 데이터 구조

### 보장건 필드 (17개)
1. **구분**: 신규/연장
2. **계약일**: YYYY-MM-DD
3. **대행사명**: 대행사 이름
4. **작업 여부**: 진행중/완료/중단/환불/세팅대기
5. **상호명**: 업체명 (필수)
6. **메인 키워드**: SEO 키워드
7. **입금액/마진**: VAT 제외
8. **총 계약금**: VAT 제외
9. **상품**: 플레이스/쇼핑/자동완성
10. **담당자**: 담당자명
11. **메모**: 특이사항
12. **플 계정**: ID/PW (암호화 저장)
13. **URL**: 플레이스 URL
14. **계약 당시 순위**: 초기 순위
15. **보장 순위**: 목표 순위
16. **작업 시작일**: YYYY-MM-DD
17. **1~25일차**: 일별 순위 추적

## 🔄 동기화 스케줄

### 자동 동기화
- **시간**: 매일 09:00, 16:00 (한국시간)
- **대상**: 제이투랩, 일류기획 시트
- **방식**: 증분 업데이트 (신규/수정만)

### 수동 동기화
- UI에서 "수동 동기화" 버튼 클릭
- API: `POST /api/guarantee/sync`

## 🛠️ 관리 API

### 보안 상태 확인
```bash
GET /api/guarantee/security-status
```
응답:
```json
{
  "encryption_enabled": true,
  "encryption_key_source": "environment",
  "data_info": {
    "data_files": [...],
    "backup_count": 10,
    "total_size": 102400
  }
}
```

### 데이터 내보내기
```bash
# 전체 데이터
GET /api/guarantee/export

# 민감정보 제외
GET /api/guarantee/export?remove_sensitive=true
```

## ⚠️ 주의사항

### Git 제외 파일
`.gitignore`에 포함된 파일들 (절대 커밋 금지):
- `guarantee_data.json`
- `guarantee_data.enc`
- `secure_data/`
- `.encryption_key`
- `*.bak`
- `service_account.json`

### 데이터 복구
1. 최근 백업에서 자동 복구
2. 수동 복구:
   ```python
   from data_security import DataSecurity
   ds = DataSecurity()
   data = ds.restore_from_backup("guarantee_data.enc", backup_index=0)
   ```

### 로그 확인
- Render: Dashboard > Logs
- 로컬: 콘솔 출력
- 동기화 실패 시 상세 로그 확인

## 📝 트러블슈팅

### 문제: 동기화는 되는데 데이터가 안 보임
**해결**:
1. 브라우저 콘솔에서 에러 확인
2. `/api/guarantee/items` 응답 확인
3. 로그에서 "Loaded N items" 메시지 확인

### 문제: '보장건' 탭을 찾을 수 없음
**해결**:
1. 시트에 정확히 '보장건' 탭이 있는지 확인
2. 서비스 계정에 뷰어 권한이 있는지 확인
3. 로그에서 "Available worksheets" 확인

### 문제: 암호화 키 분실
**해결**:
1. 백업 파일도 같은 키로 암호화됨
2. 환경변수 `DATA_ENCRYPTION_KEY` 백업 필수
3. 키 분실 시 데이터 복구 불가

## 📞 지원

문제 발생 시:
1. `/api/guarantee/sync-status` 확인
2. `/api/guarantee/security-status` 확인
3. Render 로그 확인
4. 최근 백업 확인
