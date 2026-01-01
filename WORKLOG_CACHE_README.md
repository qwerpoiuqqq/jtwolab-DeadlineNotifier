# Worklog Cache 시스템

학습용 데이터셋 구축을 위한 작업 로그 캐시 시스템입니다.

## 개요

마감 체킹 시트의 작업 데이터를 정규화하여 캐시로 저장하고,
순위 스냅샷(rank_snapshots)과 조인하여 학습용 데이터셋을 생성합니다.

## 핵심 모듈

### worklog_cache.py

- 마감 체킹 시트 전체 탭 스캔
- 정규화 레코드 생성 (company, agency, business_name, task_name, workload, start_date, end_date)
- `/var/data/worklog_cache.json` 저장

### training_dataset_builder.py

- rank_snapshots + worklog_cache 조인
- training_rows 생성 (N2 delta 포함)
- recipe_stats 생성 (작업별 효과 통계)

## API 엔드포인트

| Endpoint | Method | 설명 |
|----------|--------|------|
| `/api/worklog/cache/refresh` | POST | Worklog 캐시 갱신 |
| `/api/worklog/cache/status` | GET | 캐시 상태 조회 |
| `/api/training/build?weeks=3` | POST | 학습 데이터셋 빌드 |
| `/api/recipe/top?weeks=3` | GET | 상위 레시피 조회 |

## 스케줄

| 시간 (KST) | 작업 |
|-----------|------|
| 11:20 | Worklog 캐시 갱신 |
| 15:10 | 순위 크롤링 → 학습 데이터셋 빌드 |

## 환경변수

```bash
WORKLOG_CACHE_FILE=/var/data/worklog_cache.json
WORKLOG_CACHE_TTL_HOURS=24
TRAINING_ROWS_FILE=/var/data/training_rows.json
RECIPE_STATS_FILE=/var/data/recipe_stats.json
```

## 데이터 구조

### Training Row

```json
{
  "date": "2026-01-01",
  "business_name": "상호명",
  "keyword": "키워드",
  "n2_score": 0.4523,
  "n2_delta_3d": 0.032,
  "tasks_active": ["저장", "영수증B"],
  "task_totals": {"저장": 300, "영수증B": 10}
}
```

### Recipe Stats

```json
{
  "name": "저장+영수증B",
  "avg_delta": 0.035,
  "count": 45,
  "up_rate": 0.72
}
```

## 사용 예시

```bash
# Worklog 캐시 갱신
curl -X POST http://localhost:8080/api/worklog/cache/refresh

# 캐시 상태 확인
curl http://localhost:8080/api/worklog/cache/status

# 학습 데이터셋 빌드
curl -X POST "http://localhost:8080/api/training/build?weeks=3"

# 상위 레시피 조회
curl "http://localhost:8080/api/recipe/top?weeks=3"
```
