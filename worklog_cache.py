"""
작업 로그 캐시 관리 모듈
마감 체킹 시트의 작업 데이터를 정규화하여 캐시로 저장

핵심 기능:
- worksheet.get_all_values() 1회/탭으로 API 호출 최소화
- 429/5xx 대응: 지수 백오프 재시도
- MID(place_id) 기반 조회 지원 (fallback: business_name)
"""
import os
import json
import time
import hashlib
import re
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any
import pytz

logger = logging.getLogger(__name__)

# 기본 설정
KST = pytz.timezone('Asia/Seoul')
CACHE_FILE = os.getenv("WORKLOG_CACHE_FILE", "worklog_cache.json")
CACHE_TTL_HOURS = int(os.getenv("WORKLOG_CACHE_TTL_HOURS", "24"))

# Render Disk 경로 우선 사용
DISK_PATH = "/var/data"
if os.path.isdir(DISK_PATH):
    DEFAULT_CACHE_PATH = os.path.join(DISK_PATH, "worklog_cache.json")
else:
    DEFAULT_CACHE_PATH = os.path.join(os.getcwd(), "worklog_cache.json")


def _with_retry(func, *args, max_attempts: int = 5, base_delay: float = 1.0, **kwargs):
    """지수 백오프 재시도 (429/5xx 완화)"""
    import gspread
    
    for attempt in range(max_attempts):
        try:
            return func(*args, **kwargs)
        except gspread.exceptions.APIError as e:
            status_code = getattr(e, 'response', None)
            if status_code:
                status_code = status_code.status_code
            
            if status_code in (429, 500, 502, 503, 504) and attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt) + (0.1 * attempt)
                logger.warning(f"API error {status_code}, retry {attempt + 1}/{max_attempts} after {delay:.1f}s")
                time.sleep(delay)
            else:
                raise
        except Exception as e:
            if attempt < max_attempts - 1:
                delay = base_delay * (2 ** attempt)
                logger.warning(f"Error: {e}, retry {attempt + 1}/{max_attempts} after {delay:.1f}s")
                time.sleep(delay)
            else:
                raise
    return None


def extract_mid_from_url(url: str) -> Optional[str]:
    """URL에서 MID(place_id) 추출
    
    Args:
        url: place.naver.com URL
        
    Returns:
        MID 문자열 (예: "1234567890") 또는 None
    """
    if not url:
        return None
    
    # place.naver.com/restaurant/1234567890 형식
    match = re.search(r'place\.naver\.com/[^/]+/(\d{5,})', url)
    if match:
        return match.group(1)
    
    # /1234567890 형식 (숫자만)
    match = re.search(r'/(\d{7,})', url)
    if match:
        return match.group(1)
    
    return None


class WorklogCache:
    """작업 로그 캐시 관리 클래스
    
    마감 체킹 시트의 모든 탭을 스캔하여 정규화된 작업 로그를 캐시로 저장.
    학습용 데이터셋 조인에 활용.
    """
    
    def __init__(self, cache_file: str = None):
        """초기화
        
        Args:
            cache_file: 캐시 파일 경로 (기본: /var/data/worklog_cache.json)
        """
        self.cache_file = cache_file or os.getenv("WORKLOG_CACHE_FILE", DEFAULT_CACHE_PATH)
        self.cache_data = self._load_cache()
    
    def _load_cache(self) -> Dict:
        """캐시 파일 로드"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, "r", encoding="utf-8") as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"캐시 로드 실패: {e}")
        
        return {
            "updated_at": None,
            "expires_at": None,
            "records": [],
            "stats": {}
        }
    
    def _save_cache(self) -> bool:
        """캐시 파일 저장"""
        try:
            # 디렉토리 생성
            cache_dir = os.path.dirname(self.cache_file)
            if cache_dir and not os.path.exists(cache_dir):
                os.makedirs(cache_dir, exist_ok=True)
            
            with open(self.cache_file, "w", encoding="utf-8") as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            logger.error(f"캐시 저장 실패: {e}")
            return False
    
    def is_cache_valid(self) -> bool:
        """캐시가 유효한지 확인"""
        if not self.cache_data.get("updated_at"):
            return False
        
        expires_at = self.cache_data.get("expires_at")
        if not expires_at:
            return False
        
        try:
            exp_dt = datetime.fromisoformat(expires_at)
            now = datetime.now(KST)
            if exp_dt.tzinfo is None:
                exp_dt = KST.localize(exp_dt)
            return now < exp_dt
        except Exception:
            return False
    
    def get_cache_status(self) -> Dict:
        """캐시 상태 정보 조회"""
        records = self.cache_data.get("records", [])
        
        # 회사별/업체별 통계
        companies = set()
        businesses = set()
        for r in records:
            if r.get("company"):
                companies.add(r["company"])
            if r.get("business_name"):
                businesses.add(r["business_name"])
        
        return {
            "is_valid": self.is_cache_valid(),
            "updated_at": self.cache_data.get("updated_at"),
            "expires_at": self.cache_data.get("expires_at"),
            "records_count": len(records),
            "companies": list(companies),
            "business_count": len(businesses),
            "stats": self.cache_data.get("stats", {})
        }
    
    def get_worklog_by_business(
        self, 
        business_name: str, 
        date_from: date = None, 
        date_to: date = None
    ) -> List[Dict]:
        """특정 업체의 작업 로그 조회
        
        Args:
            business_name: 상호명
            date_from: 시작일 (포함)
            date_to: 종료일 (포함)
            
        Returns:
            해당 업체의 작업 로그 리스트
        """
        results = []
        
        for record in self.cache_data.get("records", []):
            if record.get("business_name") != business_name:
                continue
            
            # 날짜 필터링
            if date_from or date_to:
                rec_start = record.get("start_date")
                rec_end = record.get("end_date")
                
                if rec_start:
                    try:
                        rec_start_dt = date.fromisoformat(rec_start)
                        if date_to and rec_start_dt > date_to:
                            continue
                    except:
                        pass
                
                if rec_end:
                    try:
                        rec_end_dt = date.fromisoformat(rec_end)
                        if date_from and rec_end_dt < date_from:
                            continue
                    except:
                        pass
            
            results.append(record)
        
        return results
    
    def get_active_tasks_on_date(self, business_name: str, target_date: date) -> List[Dict]:
        """특정 날짜에 활성화된 작업 목록 조회
        
        Args:
            business_name: 상호명
            target_date: 대상 날짜
            
        Returns:
            해당 날짜에 활성 상태인 작업 리스트
        """
        results = []
        
        for record in self.cache_data.get("records", []):
            if record.get("business_name") != business_name:
                continue
            
            # 날짜 범위 확인
            start_str = record.get("start_date")
            end_str = record.get("end_date")
            
            try:
                if start_str:
                    start_dt = date.fromisoformat(start_str)
                    if target_date < start_dt:
                        continue
                
                if end_str:
                    end_dt = date.fromisoformat(end_str)
                    if target_date > end_dt:
                        continue
                
                results.append(record)
            except Exception:
                # 날짜 파싱 실패 시 포함
                results.append(record)
        
        return results
    
    def get_active_tasks_by_mid(self, mid: str, target_date: date) -> List[Dict]:
        """MID(place_id) 기준으로 활성 작업 조회
        
        Args:
            mid: place_id (숫자 문자열)
            target_date: 대상 날짜
            
        Returns:
            해당 날짜에 활성 상태인 작업 리스트
        """
        results = []
        
        for record in self.cache_data.get("records", []):
            rec_mid = record.get("mid")
            if not rec_mid or rec_mid != mid:
                continue
            
            # 날짜 범위 확인
            start_str = record.get("start_date")
            end_str = record.get("end_date")
            
            try:
                if start_str:
                    start_dt = date.fromisoformat(start_str)
                    if target_date < start_dt:
                        continue
                
                if end_str:
                    end_dt = date.fromisoformat(end_str)
                    if target_date > end_dt:
                        continue
                
                results.append(record)
            except Exception:
                results.append(record)
        
        return results
    
    def get_active_tasks_smart(
        self, 
        mid: str = None, 
        business_name: str = None, 
        target_date: date = None
    ) -> List[Dict]:
        """MID 우선, business_name fallback으로 활성 작업 조회
        
        Args:
            mid: place_id (우선 사용)
            business_name: 상호명 (MID 없을 때 fallback)
            target_date: 대상 날짜
            
        Returns:
            해당 날짜에 활성 상태인 작업 리스트
        """
        if not target_date:
            target_date = datetime.now(KST).date()
        
        # 1. MID로 먼저 시도
        if mid:
            results = self.get_active_tasks_by_mid(mid, target_date)
            if results:
                return results
        
        # 2. Fallback: business_name으로 조회
        if business_name:
            return self.get_active_tasks_on_date(business_name, target_date)
        
        return []
    
    def get_task_totals_on_date(self, business_name: str, target_date: date) -> Dict[str, int]:
        """특정 날짜의 작업별 workload 합계
        
        Returns:
            {"작업명": workload합계, ...}
        """
        tasks = self.get_active_tasks_on_date(business_name, target_date)
        
        totals = {}
        for task in tasks:
            task_name = task.get("task_name", "Unknown")
            try:
                workload = int(task.get("workload", 0) or 0)
            except:
                workload = 0
            
            if task_name in totals:
                totals[task_name] += workload
            else:
                totals[task_name] = workload
        
        return totals
    
    def refresh_cache(self) -> Dict:
        """전체 캐시 갱신
        
        마감 체킹 시트의 모든 탭을 스캔하여 정규화된 작업 로그 생성
        
        Returns:
            갱신 결과 {"success": bool, "records_count": int, "message": str}
        """
        logger.info("🔄 Worklog 캐시 갱신 시작...")
        start_time = time.time()
        
        try:
            from sheet_client import (
                load_settings, _get_client, _find_header_row, _build_records,
                _get_value_flexible, _normalize_key, _parse_int_maybe
            )
            from internal_manager import _is_internal_or_postpaid, parse_date_flexible
            from guarantee_manager import GuaranteeManager
            
            settings = load_settings()
            if not settings.spreadsheet_id:
                return {"success": False, "records_count": 0, "message": "SPREADSHEET_ID 미설정"}
            
            client = _get_client()
            
            # 보장건 데이터에서 회사-상호명 매핑 로드
            company_map = {}  # business_name -> company
            guarantee_map = {}  # business_name -> guarantee_item
            try:
                gm = GuaranteeManager()
                for item in gm.get_items():
                    biz = item.get("business_name")
                    if biz:
                        company_map[biz] = item.get("company", "기타")
                        guarantee_map[biz] = item
                logger.info(f"📋 보장건 매핑 로드: {len(company_map)}개 업체")
            except Exception as e:
                logger.warning(f"보장건 매핑 로드 실패: {e}")
            
            # 스프레드시트 열기
            ss = _with_retry(client.open_by_key, settings.spreadsheet_id)
            ws_list = ss.worksheets()
            
            today = datetime.now(KST).date()
            all_records = []
            stats = {
                "worksheets_scanned": 0,
                "worksheets_failed": 0,
                "total_rows": 0,
                "internal_rows": 0,
                "companies": {}
            }
            
            logger.info(f"📊 워크시트 {len(ws_list)}개 스캔 시작")
            
            for idx, ws in enumerate(ws_list, 1):
                tab_title = (ws.title or "").strip()
                
                try:
                    # 헤더 찾기
                    header_row, headers = _find_header_row(ws, settings)
                    
                    # 전체 데이터 1회 읽기
                    records = _with_retry(_build_records, ws, header_row, headers)
                    stats["worksheets_scanned"] += 1
                    stats["total_rows"] += len(records)
                    
                    if idx % 5 == 0 or idx == len(ws_list):
                        logger.info(f"   [{idx}/{len(ws_list)}] {tab_title}: {len(records)}행")
                    
                except Exception as e:
                    logger.warning(f"   [{idx}] {tab_title} 실패: {e}")
                    stats["worksheets_failed"] += 1
                    continue
                
                # 각 행 처리
                for row in records:
                    row_norm = {_normalize_key(k): v for k, v in row.items()}
                    
                    # 내부 진행건/후불 필터
                    is_internal = _is_internal_or_postpaid(
                        _get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN")
                    )
                    if not is_internal:
                        continue
                    
                    stats["internal_rows"] += 1
                    
                    # 필드 추출
                    bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
                    if not bizname:
                        continue
                    
                    agency = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
                    workload = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
                    product = str(_get_value_flexible(row_norm, settings.product_col, "PRODUCT_COLUMN") or "").strip()
                    product_name = str(_get_value_flexible(row_norm, settings.product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
                    remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
                    
                    # 회사 결정
                    company = company_map.get(bizname, "기타")
                    
                    # 작업명 생성
                    if tab_title.lower() == "기타" and product_name:
                        task_name = product_name
                    else:
                        task_name = f"{tab_title} {product}".strip() if product else tab_title
                    
                    # 날짜 계산
                    end_date = (today + timedelta(days=remain)).isoformat() if remain is not None else None
                    
                    # 작업 시작일 추출
                    start_date = None
                    for col in ["작업 시작일", "작업시작일", "시작일", "세팅일"]:
                        val = _get_value_flexible(row_norm, col, "")
                        if val:
                            parsed = parse_date_flexible(str(val).strip())
                            if parsed:
                                start_date = parsed.isoformat()
                                break
                    
                    # URL/MID 추출 (새 컬럼)
                    place_url = None
                    mid = None
                    for col in ["URL", "url", "플레이스 URL", "플레이스URL", "장소 URL", "장소URL"]:
                        val = _get_value_flexible(row_norm, col, "")
                        if val:
                            place_url = str(val).strip()
                            mid = extract_mid_from_url(place_url)
                            break
                    
                    # MID 컬럼 직접 읽기 (URL 없을 때 대비)
                    if not mid:
                        for col in ["MID", "mid", "place_id", "플레이스ID"]:
                            val = _get_value_flexible(row_norm, col, "")
                            if val:
                                mid_str = str(val).strip()
                                if mid_str.isdigit() and len(mid_str) >= 5:
                                    mid = mid_str
                                    break
                    
                    # 레코드 생성
                    record = {
                        "company": company,
                        "agency": agency,
                        "business_name": bizname,
                        "mid": mid,  # MID 추가
                        "place_url": place_url,  # URL 추가
                        "task_name": task_name,
                        "workload": workload,
                        "start_date": start_date,
                        "end_date": end_date,
                        "source_tab": tab_title,
                    }
                    all_records.append(record)
                    
                    # 회사별 통계
                    if company not in stats["companies"]:
                        stats["companies"][company] = 0
                    stats["companies"][company] += 1
            
            # 캐시 업데이트
            now = datetime.now(KST)
            expires = now + timedelta(hours=CACHE_TTL_HOURS)
            
            self.cache_data = {
                "updated_at": now.isoformat(),
                "expires_at": expires.isoformat(),
                "records": all_records,
                "stats": stats
            }
            
            if self._save_cache():
                elapsed = time.time() - start_time
                message = f"Worklog 캐시 갱신 완료 - {len(all_records)}건 ({elapsed:.1f}초)"
                logger.info(f"✅ {message}")
                return {
                    "success": True,
                    "records_count": len(all_records),
                    "message": message,
                    "stats": stats
                }
            else:
                return {"success": False, "records_count": 0, "message": "캐시 저장 실패"}
            
        except Exception as e:
            logger.error(f"❌ Worklog 캐시 갱신 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "records_count": 0,
                "message": f"갱신 실패: {str(e)}"
            }
    
    def clear_cache(self):
        """캐시 초기화"""
        self.cache_data = {
            "updated_at": None,
            "expires_at": None,
            "records": [],
            "stats": {}
        }
        self._save_cache()
        logger.info("Worklog 캐시 초기화 완료")


# 전역 인스턴스
_worklog_cache = None


def get_worklog_cache() -> WorklogCache:
    """WorklogCache 싱글턴 인스턴스 반환"""
    global _worklog_cache
    if _worklog_cache is None:
        _worklog_cache = WorklogCache()
    return _worklog_cache


def refresh_worklog_cache() -> Dict:
    """Worklog 캐시 갱신 (외부 호출용)"""
    cache = get_worklog_cache()
    return cache.refresh_cache()


def get_worklog_cache_status() -> Dict:
    """캐시 상태 조회 (외부 호출용)"""
    cache = get_worklog_cache()
    return cache.get_cache_status()
