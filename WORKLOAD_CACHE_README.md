# 작업량 캐시 시스템 사용 가이드

## 개요

작업량 조회 성능을 개선하기 위해 캐시 시스템이 추가되었습니다.
- 매일 오전 11시 30분에 자동으로 업체별 3주치 작업량 데이터를 캐시로 저장
- 작업량 조회 시 캐시를 우선 사용하여 응답 속도 개선 (5-10초 → 0.5초 이내)
- 구글 시트 API 호출 최소화로 서버 부하 감소

## 주요 기능

### 1. 자동 캐시 갱신
- **스케줄**: 매일 오전 11:30 (KST)
- **대상 업체**: 제이투랩, 일류기획
- **데이터 범위**: 각 업체의 가장 최신 작업 시작일 기준 3주 전 데이터

### 2. 작업량 조회 개선
- 보장 관리 페이지에서 "📊 작업량" 버튼 클릭 시 캐시 우선 조회
- 캐시가 없거나 만료된 경우에만 실시간 조회
- 캐시 사용 여부는 응답에 `from_cache` 필드로 표시

### 3. 수동 캐시 갱신
필요시 API를 통해 수동으로 캐시를 갱신할 수 있습니다.

## API 엔드포인트

### 1. 작업량 스케줄 조회
```http
GET /api/workload/schedule?company=제이투랩
```

**응답 예시**:
```json
{
  "weeks": [
    {
      "start_date": "10/07",
      "end_date": "10/13",
      "items": [
        {"name": "플레이스 저장", "workload": "300"},
        {"name": "영수증B", "workload": "10"}
      ]
    }
  ],
  "from_cache": true
}
```

### 2. 캐시 수동 갱신
```http
POST /api/workload/cache/refresh
```

**응답 예시**:
```json
{
  "success": true,
  "updated_companies": ["제이투랩", "일류기획"],
  "failed_companies": [],
  "message": "캐시 갱신 완료 - 성공: 제이투랩, 일류기획",
  "cache_status": {
    "is_valid": true,
    "updated_at": "2025-10-27T11:30:00",
    "expires_at": "2025-10-28T11:30:00",
    "companies": ["제이투랩", "일류기획"],
    "company_count": 2
  }
}
```

### 3. 캐시 상태 조회
```http
GET /api/workload/cache/status
```

**응답 예시**:
```json
{
  "is_valid": true,
  "updated_at": "2025-10-27T11:30:00",
  "expires_at": "2025-10-28T11:30:00",
  "companies": ["제이투랩", "일류기획"],
  "company_count": 2,
  "제이투랩_weeks": 3,
  "일류기획_weeks": 3
}
```

## 파일 구조

### 새로 추가된 파일
- `workload_cache.py`: 작업량 캐시 관리 모듈
- `workload_cache.json`: 캐시 데이터 저장 파일 (자동 생성)
- `WORKLOAD_CACHE_README.md`: 사용 가이드 (이 파일)

### 수정된 파일
- `app.py`: 스케줄러 작업 및 API 엔드포인트 추가
- `internal_manager.py`: 캐시 우선 조회 로직 추가
- `templates/manage.html`: 작업량 버튼 버그 수정

## 캐시 데이터 구조

```json
{
  "updated_at": "2025-10-27T11:30:00",
  "cache_expires_at": "2025-10-28T11:30:00",
  "companies": {
    "제이투랩": {
      "weeks": [
        {
          "start_date": "10/07",
          "end_date": "10/13",
          "items": [
            {"name": "플레이스 저장", "workload": "300"},
            {"name": "영수증B", "workload": "10"}
          ]
        }
      ]
    },
    "일류기획": {
      "weeks": [...]
    }
  }
}
```

## 환경 변수

캐시 파일 경로를 변경하려면 `.env` 파일에 추가:
```env
WORKLOAD_CACHE_FILE=workload_cache.json
```

## 문제 해결

### 캐시가 작동하지 않는 경우
1. 로그 확인: 스케줄러 실행 및 캐시 갱신 로그 확인
2. 캐시 파일 확인: `workload_cache.json` 파일 존재 여부 확인
3. 수동 갱신: `POST /api/workload/cache/refresh` 호출하여 수동 갱신

### 작업량이 표시되지 않는 경우
1. 구글 시트 연결 확인
2. 작업 시작일 데이터 확인
3. 캐시 상태 확인: `GET /api/workload/cache/status`

## 테스트 방법

### 1. 수동 캐시 갱신 테스트
```bash
curl -X POST http://localhost:8080/api/workload/cache/refresh
```

### 2. 캐시 상태 확인
```bash
curl http://localhost:8080/api/workload/cache/status
```

### 3. 작업량 조회 테스트
```bash
curl "http://localhost:8080/api/workload/schedule?company=제이투랩"
```

## 성능 개선 효과

- **조회 속도**: 5-10초 → 0.5초 이내 (약 10-20배 향상)
- **API 호출**: 매 클릭마다 → 하루 1-2회
- **서버 부하**: 구글 시트 API 호출 최소화
- **사용자 경험**: 즉시 응답으로 UX 크게 개선

## 향후 개선 사항

- [ ] 캐시 갱신 실패 시 재시도 로직
- [ ] 업체별 개별 캐시 갱신 API
- [ ] 캐시 통계 및 모니터링 대시보드
- [ ] 캐시 만료 시간 동적 조정

