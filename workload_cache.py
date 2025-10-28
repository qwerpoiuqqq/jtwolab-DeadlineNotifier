"""
작업량 캐시 관리 모듈
업체별 3주치 작업량 데이터를 캐시로 관리하여 성능 개선
"""
import os
import json
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any
import logging

logger = logging.getLogger(__name__)

CACHE_FILE = os.getenv("WORKLOAD_CACHE_FILE", "workload_cache.json")


class WorkloadCache:
    """작업량 캐시 관리 클래스"""
    
    def __init__(self, cache_file: str = None):
        """초기화
        Args:
            cache_file: 캐시 파일 경로 (기본: workload_cache.json)
        """
        self.cache_file = cache_file or CACHE_FILE
        self.cache_data = self._load_cache()
    
    def _load_cache(self) -> Dict[str, Any]:
        """캐시 파일 로드"""
        try:
            if os.path.exists(self.cache_file):
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded workload cache: {len(data.get('companies', {}))} companies")
                    return data
        except Exception as e:
            logger.error(f"Failed to load cache: {e}")
        
        return {
            "updated_at": None,
            "cache_expires_at": None,
            "companies": {}
        }
    
    def _save_cache(self) -> bool:
        """캐시 파일 저장"""
        try:
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(self.cache_data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved workload cache: {len(self.cache_data.get('companies', {}))} companies")
            return True
        except Exception as e:
            logger.error(f"Failed to save cache: {e}")
            return False
    
    def is_cache_valid(self) -> bool:
        """캐시가 유효한지 확인"""
        if not self.cache_data.get("cache_expires_at"):
            return False
        
        try:
            expires_at = datetime.fromisoformat(self.cache_data["cache_expires_at"])
            return datetime.now() < expires_at
        except Exception as e:
            logger.error(f"Cache validation error: {e}")
            return False
    
    def get_company_workload(self, company: str) -> Optional[Dict[str, Any]]:
        """특정 회사의 작업량 스케줄 조회
        
        Args:
            company: 회사명 (제이투랩, 일류기획)
            
        Returns:
            작업량 스케줄 데이터 또는 None (캐시 없음/만료)
        """
        if not self.is_cache_valid():
            logger.info("Cache expired or invalid")
            return None
        
        companies = self.cache_data.get("companies", {})
        return companies.get(company)
    
    def get_business_workload(self, company: str, business_name: str) -> Optional[Dict[str, Any]]:
        """특정 업체의 작업량 스케줄 조회
        
        Args:
            company: 회사명 (제이투랩, 일류기획)
            business_name: 상호명
            
        Returns:
            작업량 스케줄 데이터 또는 None (캐시 없음/만료)
        """
        if not self.is_cache_valid():
            logger.info("Cache expired or invalid")
            return None
        
        companies = self.cache_data.get("companies", {})
        businesses = companies.get("businesses", {})
        business_key = f"{company}:{business_name}"
        return businesses.get(business_key)
    
    def get_all_businesses_workload(self, company: str) -> Dict[str, Dict[str, Any]]:
        """특정 회사의 모든 업체별 작업량 조회
        
        Args:
            company: 회사명 (제이투랩, 일류기획)
            
        Returns:
            업체별 작업량 데이터 {"업체명": {"weeks": [...]}, ...}
        """
        if not self.is_cache_valid():
            logger.info("Cache expired or invalid")
            return {}
        
        companies = self.cache_data.get("companies", {})
        businesses = companies.get("businesses", {})
        
        # 해당 회사의 업체만 필터링
        result = {}
        prefix = f"{company}:"
        for key, data in businesses.items():
            if key.startswith(prefix):
                business_name = key[len(prefix):]
                result[business_name] = data
        
        return result
    
    def update_cache(self, workload_data: Dict[str, Any]) -> bool:
        """캐시 업데이트
        
        Args:
            workload_data: {
                "제이투랩": {"weeks": [...]},
                "일류기획": {"weeks": [...]}
            }
            
        Returns:
            성공 여부
        """
        try:
            now = datetime.now()
            
            # 다음날 11:30까지 유효
            tomorrow = now + timedelta(days=1)
            expires_at = tomorrow.replace(hour=11, minute=30, second=0, microsecond=0)
            
            # 이미 오늘 11:30이 지났다면, 내일 11:30
            if now.hour >= 11 and now.minute >= 30:
                expires_at = (now + timedelta(days=1)).replace(hour=11, minute=30, second=0, microsecond=0)
            else:
                expires_at = now.replace(hour=11, minute=30, second=0, microsecond=0)
            
            self.cache_data = {
                "updated_at": now.isoformat(),
                "cache_expires_at": expires_at.isoformat(),
                "companies": workload_data
            }
            
            return self._save_cache()
        except Exception as e:
            logger.error(f"Cache update error: {e}")
            return False
    
    def clear_cache(self) -> bool:
        """캐시 초기화"""
        try:
            self.cache_data = {
                "updated_at": None,
                "cache_expires_at": None,
                "companies": {}
            }
            return self._save_cache()
        except Exception as e:
            logger.error(f"Cache clear error: {e}")
            return False
    
    def get_cache_status(self) -> Dict[str, Any]:
        """캐시 상태 정보 조회"""
        is_valid = self.is_cache_valid()
        companies_data = self.cache_data.get("companies", {})
        
        # 회사 목록 (businesses 키 제외)
        companies = [k for k in companies_data.keys() if k != "businesses"]
        
        # 업체별 캐시 정보
        businesses_dict = companies_data.get("businesses", {})
        business_count = len(businesses_dict)
        
        status = {
            "is_valid": is_valid,
            "updated_at": self.cache_data.get("updated_at"),
            "expires_at": self.cache_data.get("cache_expires_at"),
            "companies": companies,
            "company_count": len(companies),
            "business_count": business_count
        }
        
        # 각 회사별 주차 수 정보
        for company in companies:
            company_data = companies_data[company]
            if isinstance(company_data, dict):
                weeks = company_data.get("weeks", [])
                status[f"{company}_weeks"] = len(weeks)
        
        return status




def refresh_all_workload_cache() -> Dict[str, Any]:
    """모든 회사의 작업량 캐시를 갱신 (회사 전체 + 업체별)
    
    Returns:
        갱신 결과 {
            "success": bool,
            "updated_companies": [],
            "failed_companies": [],
            "message": str
        }
    """
    from internal_manager import fetch_workload_schedule_direct
    from guarantee_manager import GuaranteeManager
    
    logger.info("Starting workload cache refresh for all companies and businesses...")
    
    companies = ["제이투랩", "일류기획"]
    workload_data = {}
    updated_companies = []
    failed_companies = []
    business_workloads = {}  # 업체별 작업량 저장
    
    for company in companies:
        try:
            logger.info(f"🚀 Fetching raw workload data for {company}...")
            
            # RAW 데이터를 한 번만 가져오기 (가장 효율적)
            from internal_manager import fetch_internal_items_for_company, process_raw_items_to_schedule
            
            raw_items = fetch_internal_items_for_company(company)
            logger.info(f"  ✅ Raw data fetched: {len(raw_items)} items (단 1회 API 호출!)")
            
            # 회사 전체 작업량 계산
            schedule = process_raw_items_to_schedule(raw_items, company, None)
            workload_data[company] = schedule
            logger.info(f"  📊 {company} 전체: {len(schedule.get('weeks', []))} weeks")
            
            # 해당 회사의 진행중인 업체 목록
            from datetime import date, timedelta
            gm = GuaranteeManager()
            guarantee_items = gm.get_items({"company": company})
            
            # 진행중/후불/세팅대기 상태인 업체 선택
            active_statuses = ["진행중", "후불", "세팅대기"]
            
            filtered_guarantee_items = []
            for item in guarantee_items:
                status = item.get("status")
                business_name = item.get("business_name")
                
                if not business_name:
                    continue
                
                # 진행중/후불/세팅대기 업체만 포함
                if status not in active_statuses:
                    continue
                
                filtered_guarantee_items.append(item)
            
            business_names = [item.get("business_name") for item in filtered_guarantee_items]
            
            logger.info(f"  📋 Processing {len(business_names)} active businesses (메모리 필터링)...")
            
            # 메모리에서 업체별로 분할 (초고속, API 호출 없음!)
            cached_count = 0
            skipped_count = 0
            failed_count = 0
            
            for idx, business_name in enumerate(business_names, 1):
                try:
                    # Raw 데이터에서 해당 업체만 필터링
                    business_raw_items = [
                        item for item in raw_items 
                        if item.get("bizname") == business_name
                    ]
                    
                    if not business_raw_items:
                        logger.info(f"  [{idx}/{len(business_names)}] ⊘ {business_name}: no data")
                        skipped_count += 1
                        continue
                    
                    # 업체별 스케줄 계산
                    business_schedule = process_raw_items_to_schedule(business_raw_items, company, business_name)
                    
                    # 작업이 있는 업체만 캐싱
                    if business_schedule.get("weeks"):
                        business_key = f"{company}:{business_name}"
                        business_workloads[business_key] = business_schedule
                        logger.info(f"  [{idx}/{len(business_names)}] ✅ {business_name}: {len(business_schedule.get('weeks', []))} weeks")
                        cached_count += 1
                    else:
                        logger.info(f"  [{idx}/{len(business_names)}] ⊘ {business_name}: no weeks data")
                        skipped_count += 1
                except Exception as e:
                    logger.warning(f"  [{idx}/{len(business_names)}] ❌ {business_name}: {e}")
                    failed_count += 1
            
            updated_companies.append(company)
            logger.info(f"✅ {company} complete - Cached: {cached_count}, Skipped: {skipped_count}, Failed: {failed_count}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch workload for {company}: {e}")
            failed_companies.append(company)
            # 빈 데이터라도 추가
            workload_data[company] = {"weeks": []}
    
    # 캐시 저장 (회사 전체 + 업체별)
    cache = WorkloadCache()
    cache_data = {
        **workload_data,
        "businesses": business_workloads  # 업체별 데이터 추가
    }
    success = cache.update_cache(cache_data)
    
    if success:
        message = f"✅ 캐시 갱신 완료 - {', '.join(updated_companies)} ({len(business_workloads)}개 업체 캐싱됨)"
        if failed_companies:
            message += f", 실패: {', '.join(failed_companies)}"
    else:
        message = "❌ 캐시 저장 실패"
    
    logger.info(message)
    logger.info(f"📊 Total cached: {len(business_workloads)} businesses")
    
    return {
        "success": success,
        "updated_companies": updated_companies,
        "failed_companies": failed_companies,
        "business_count": len(business_workloads),
        "message": message,
        "cache_status": cache.get_cache_status()
    }

