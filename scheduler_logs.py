"""
스케줄러 로그 관리 모듈
모든 스케줄러 작업의 실행 로그를 저장하고 조회

핵심 기능:
- 메모리 + 파일 기반 로그 저장 (최근 100개)
- 작업별 실행 상태, 시간, 결과 기록
- API 엔드포인트를 통한 조회
"""
import os
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from collections import deque
import pytz
import threading

logger = logging.getLogger(__name__)

# 기본 설정
KST = pytz.timezone('Asia/Seoul')
MAX_LOG_ENTRIES = 100

# Render Disk 경로 우선 사용
DISK_PATH = "/var/data"
if os.path.isdir(DISK_PATH):
    DEFAULT_LOG_PATH = os.path.join(DISK_PATH, "scheduler_logs.json")
else:
    DEFAULT_LOG_PATH = os.path.join(os.getcwd(), "scheduler_logs.json")


class SchedulerLogManager:
    """스케줄러 로그 관리 클래스"""
    
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
        
        self.log_file = os.getenv("SCHEDULER_LOG_FILE", DEFAULT_LOG_PATH)
        self.logs = deque(maxlen=MAX_LOG_ENTRIES)
        self._load_logs()
        self._initialized = True
    
    def _load_logs(self):
        """파일에서 로그 로드"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    logs = data.get("logs", [])
                    for log in logs[-MAX_LOG_ENTRIES:]:
                        self.logs.append(log)
                logger.info(f"Loaded {len(self.logs)} scheduler logs")
        except Exception as e:
            logger.warning(f"Failed to load scheduler logs: {e}")
    
    def _save_logs(self):
        """파일에 로그 저장"""
        try:
            dir_path = os.path.dirname(self.log_file)
            if dir_path and not os.path.exists(dir_path):
                os.makedirs(dir_path, exist_ok=True)
            
            with open(self.log_file, "w", encoding="utf-8") as f:
                json.dump({
                    "updated_at": datetime.now(KST).isoformat(),
                    "logs": list(self.logs)
                }, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save scheduler logs: {e}")
    
    def add_log(
        self,
        job_id: str,
        job_name: str,
        status: str,  # "started", "success", "failed"
        message: str = "",
        details: Dict = None
    ) -> Dict:
        """로그 추가
        
        Args:
            job_id: 작업 ID (예: "daily_rank_crawl")
            job_name: 작업 이름 (예: "순위 크롤링")
            status: 상태 ("started", "success", "failed")
            message: 결과 메시지
            details: 추가 상세 정보
        """
        now = datetime.now(KST)
        
        log_entry = {
            "id": f"{job_id}_{now.strftime('%Y%m%d_%H%M%S')}",
            "job_id": job_id,
            "job_name": job_name,
            "status": status,
            "message": message,
            "details": details or {},
            "timestamp": now.isoformat(),
            "date": now.strftime("%Y-%m-%d"),
            "time": now.strftime("%H:%M:%S"),
        }
        
        self.logs.append(log_entry)
        self._save_logs()
        
        # 콘솔 로그도 출력
        status_emoji = {"started": "🚀", "success": "✅", "failed": "❌"}.get(status, "📝")
        logger.info(f"{status_emoji} [{job_name}] {status}: {message}")
        
        return log_entry
    
    def get_logs(
        self,
        job_id: str = None,
        status: str = None,
        limit: int = 50,
        date_from: str = None
    ) -> List[Dict]:
        """로그 조회
        
        Args:
            job_id: 특정 작업만 필터링
            status: 특정 상태만 필터링
            limit: 최대 개수
            date_from: 시작 날짜 (YYYY-MM-DD)
        """
        results = []
        
        for log in reversed(list(self.logs)):
            if job_id and log.get("job_id") != job_id:
                continue
            if status and log.get("status") != status:
                continue
            if date_from and log.get("date", "") < date_from:
                continue
            
            results.append(log)
            if len(results) >= limit:
                break
        
        return results
    
    def get_latest_by_job(self) -> Dict[str, Dict]:
        """각 작업별 최신 로그 조회"""
        latest = {}
        
        for log in reversed(list(self.logs)):
            job_id = log.get("job_id")
            if job_id and job_id not in latest:
                latest[job_id] = log
        
        return latest
    
    def get_summary(self) -> Dict:
        """로그 요약 통계"""
        total = len(self.logs)
        success = sum(1 for l in self.logs if l.get("status") == "success")
        failed = sum(1 for l in self.logs if l.get("status") == "failed")
        
        # 오늘 로그
        today = datetime.now(KST).strftime("%Y-%m-%d")
        today_logs = [l for l in self.logs if l.get("date") == today]
        
        return {
            "total_logs": total,
            "success_count": success,
            "failed_count": failed,
            "today_count": len(today_logs),
            "latest_by_job": self.get_latest_by_job()
        }
    
    def clear_old_logs(self, days: int = 7):
        """오래된 로그 정리"""
        cutoff = (datetime.now(KST) - timedelta(days=days)).strftime("%Y-%m-%d")
        old_count = len(self.logs)
        
        self.logs = deque(
            (l for l in self.logs if l.get("date", "") >= cutoff),
            maxlen=MAX_LOG_ENTRIES
        )
        
        removed = old_count - len(self.logs)
        if removed > 0:
            self._save_logs()
            logger.info(f"Removed {removed} old scheduler logs")


# 전역 인스턴스
_log_manager = None


def get_scheduler_log_manager() -> SchedulerLogManager:
    """싱글턴 인스턴스 반환"""
    global _log_manager
    if _log_manager is None:
        _log_manager = SchedulerLogManager()
    return _log_manager


def log_scheduler_event(
    job_id: str,
    job_name: str,
    status: str,
    message: str = "",
    details: Dict = None
) -> Dict:
    """스케줄러 이벤트 로깅 (외부 호출용)"""
    manager = get_scheduler_log_manager()
    return manager.add_log(job_id, job_name, status, message, details)


def get_scheduler_logs(
    job_id: str = None,
    status: str = None,
    limit: int = 50
) -> List[Dict]:
    """스케줄러 로그 조회 (외부 호출용)"""
    manager = get_scheduler_log_manager()
    return manager.get_logs(job_id=job_id, status=status, limit=limit)


def get_scheduler_summary() -> Dict:
    """스케줄러 요약 조회 (외부 호출용)"""
    manager = get_scheduler_log_manager()
    return manager.get_summary()


# timedelta import 추가
from datetime import timedelta
