"""
순위 스냅샷 관리 모듈
Google Sheets를 1차 원장(DB)으로 사용하여 순위/N2 데이터를 누적 저장

핵심 설계:
- unique_key = sha1(date|time_slot|keyword|place_url)로 중복 판별
- batch_update + append_rows로 효율적 upsert
- date는 '화면에서 파싱한 날짜', collected_at는 '실제 수집 시각'
"""
import os
import json
import hashlib
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import gspread
from google.oauth2.service_account import Credentials
import pytz

logger = logging.getLogger(__name__)

# 기본 설정
KST = pytz.timezone('Asia/Seoul')
DEFAULT_TAB_NAME = "rank_snapshots"
LOG_TAB_NAME = "rank_update_logs"

# 원장 헤더 정의
SNAPSHOT_HEADERS = [
    "unique_key",     # sha1(date|time_slot|keyword|place_url) - upsert 키
    "date",           # 화면에서 파싱한 날짜 (예: 2025-12-28)
    "time_slot",      # 09:00 / 15:00
    "agency",         # 대행사명 (밴스마케팅, 흐름 등)
    "client_name",    # 상호명
    "group",          # 회사 그룹 (제이투랩/일류기획)
    "keyword",        # 메인 키워드
    "place_url",      # 네이버 플레이스 URL
    "place_id",       # 플레이스 ID (URL에서 추출한 숫자, 상호명 변경 추적용)
    "rank",           # 현재 순위
    "saves",          # 저장 수
    "blog_reviews",   # 블로그 리뷰 수
    "visitor_reviews",# 방문자 리뷰 수
    "n2_score",       # N2 지수
    "collected_at",   # 실제 수집 타임스탬프
    "source",         # 데이터 출처 (adlog_crawl / manual)
]

LOG_HEADERS = [
    "executed_at",    # 실행 시간
    "time_slot",      # 09:00 / 16:00
    "success_count",  # 성공 건수
    "failed_count",   # 실패 건수
    "elapsed_seconds",# 소요 시간
    "message",        # 결과 메시지
    "failed_details", # 실패 상세 (JSON)
]


def generate_unique_key(date: str, time_slot: str, keyword: str, place_url: str) -> str:
    """Upsert용 고유 키 생성 (sha1 해시)
    
    Args:
        date: 날짜 (YYYY-MM-DD)
        time_slot: 시간대 (09:00 / 16:00)
        keyword: 키워드
        place_url: 플레이스 URL
        
    Returns:
        sha1 해시 (40자)
    """
    raw = f"{date}|{time_slot}|{keyword}|{place_url}"
    return hashlib.sha1(raw.encode()).hexdigest()


class RankSnapshotManager:
    """Google Sheets 기반 순위 스냅샷 관리 클래스
    
    핵심 기능:
    - unique_key 기반 upsert (중복 방지)
    - batch_update + append_rows (효율적 쓰기)
    - 전체 읽기 후 행마다 개별 update 금지
    """
    
    def __init__(self, spreadsheet_id: str = None):
        """초기화
        
        Args:
            spreadsheet_id: 대상 스프레드시트 ID 
                - 우선순위: 인자 > RANK_SHEET_ID > JTWOLAB_SHEET_ID
        """
        # 별도 순위 시트 ID 우선 사용
        self.spreadsheet_id = spreadsheet_id or os.getenv("RANK_SHEET_ID") or os.getenv(
            "JTWOLAB_SHEET_ID", 
            "1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y"
        )
        self.tab_name = os.getenv("RANK_SNAPSHOT_TAB", DEFAULT_TAB_NAME)
        self.log_tab_name = os.getenv("RANK_LOG_TAB", LOG_TAB_NAME)
        self._client = None
        self._spreadsheet = None
        
        # 사용 중인 시트 로그
        sheet_source = "RANK_SHEET_ID" if os.getenv("RANK_SHEET_ID") else "JTWOLAB_SHEET_ID"
        logger.info(f"RankSnapshotManager using {sheet_source}: {self.spreadsheet_id[:20]}...")
    
    def _get_credentials(self) -> Credentials:
        """Google API 인증 정보 로드"""
        scope = [
            'https://spreadsheets.google.com/feeds', 
            'https://www.googleapis.com/auth/drive'
        ]
        
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
        
        if creds_path and os.path.exists(creds_path):
            logger.info(f"Using credentials file: {creds_path}")
            return Credentials.from_service_account_file(creds_path, scopes=scope)
        elif service_account_json:
            logger.info("Using SERVICE_ACCOUNT_JSON from environment")
            service_account_info = json.loads(service_account_json)
            return Credentials.from_service_account_info(service_account_info, scopes=scope)
        else:
            raise ValueError(
                "No Google credentials found. "
                "Set GOOGLE_APPLICATION_CREDENTIALS or SERVICE_ACCOUNT_JSON"
            )
    
    def _get_client(self) -> gspread.Client:
        """gspread 클라이언트 반환 (캐시)"""
        if self._client is None:
            creds = self._get_credentials()
            self._client = gspread.authorize(creds)
        return self._client
    
    def _get_spreadsheet(self) -> gspread.Spreadsheet:
        """스프레드시트 객체 반환 (캐시)"""
        if self._spreadsheet is None:
            client = self._get_client()
            self._spreadsheet = client.open_by_key(self.spreadsheet_id)
        return self._spreadsheet
    
    def get_or_create_worksheet(self, tab_name: str = None) -> gspread.Worksheet:
        """워크시트 가져오기 (없으면 생성)
        
        Args:
            tab_name: 탭 이름 (기본: rank_snapshots)
            
        Returns:
            gspread.Worksheet
        """
        tab_name = tab_name or self.tab_name
        spreadsheet = self._get_spreadsheet()
        
        try:
            worksheet = spreadsheet.worksheet(tab_name)
            logger.info(f"Found existing worksheet: {tab_name}")
            
            # 헤더 확인 (첫 행)
            first_row = worksheet.row_values(1)
            if not first_row or first_row[0] != "unique_key":
                # 헤더가 없거나 다르면 추가
                logger.info(f"Adding headers to worksheet: {tab_name}")
                worksheet.insert_row(SNAPSHOT_HEADERS, 1)
                
        except gspread.WorksheetNotFound:
            logger.info(f"Creating new worksheet: {tab_name}")
            worksheet = spreadsheet.add_worksheet(
                title=tab_name, 
                rows=1000, 
                cols=len(SNAPSHOT_HEADERS)
            )
            # 헤더 추가
            worksheet.append_row(SNAPSHOT_HEADERS)
            logger.info(f"Created worksheet with headers: {SNAPSHOT_HEADERS}")
        
        return worksheet
    
    def _build_key_row_map(self, ws: gspread.Worksheet) -> Tuple[Dict[str, int], List[List[str]], List[str]]:
        """unique_key -> row_number 맵 생성 (효율적 upsert용)
        
        Returns:
            (key_to_row, all_values, headers)
            - key_to_row: {unique_key: row_number} (1-based)
            - all_values: 전체 시트 데이터
            - headers: 헤더 행
        """
        all_values = ws.get_all_values()
        
        if len(all_values) == 0:
            return {}, [], []
        
        headers = all_values[0]
        
        # unique_key 컬럼 인덱스 찾기
        try:
            key_idx = headers.index("unique_key")
        except ValueError:
            logger.warning("unique_key column not found, using first column")
            key_idx = 0
        
        key_to_row = {}
        for row_num, row in enumerate(all_values[1:], start=2):  # 1-based, 헤더 제외
            if len(row) > key_idx:
                unique_key = row[key_idx]
                if unique_key:
                    key_to_row[unique_key] = row_num
        
        return key_to_row, all_values, headers
    
    def upsert_bulk(self, records: List[Dict[str, Any]]) -> Dict[str, int]:
        """대량 스냅샷 저장 (batch_update + append_rows)
        
        핵심 로직:
        1. unique_key 컬럼만 읽어 key->row 맵 생성
        2. 기존 키는 batch_update로 업데이트
        3. 새 키는 append_rows로 추가
        4. get_all_values 후 행마다 개별 update 금지
        
        Args:
            records: 스냅샷 데이터 리스트
            
        Returns:
            {"success": N, "failed": M, "updated": X, "added": Y}
        """
        result = {"success": 0, "failed": 0, "updated": 0, "added": 0}
        
        if not records:
            return result
        
        try:
            ws = self.get_or_create_worksheet()
            
            # 1. key->row 맵 생성 (한 번만 읽기)
            key_to_row, all_values, headers = self._build_key_row_map(ws)
            
            if not headers:
                headers = SNAPSHOT_HEADERS
                ws.append_row(headers)
                key_to_row = {}
            
            logger.info(f"Loaded {len(key_to_row)} existing keys from sheet")
            
            # 2. 업데이트/추가 분류
            updates = []  # [(row_num, row_data), ...]
            new_rows = []  # [row_data, ...]
            
            collected_at = datetime.now(KST).isoformat()
            
            for data in records:
                try:
                    # 필수 필드 확인
                    if not all(data.get(f) for f in ["date", "time_slot", "keyword", "place_url"]):
                        result["failed"] += 1
                        continue
                    
                    # unique_key 생성
                    unique_key = generate_unique_key(
                        data["date"],
                        data["time_slot"],
                        data["keyword"],
                        data["place_url"]
                    )
                    data["unique_key"] = unique_key
                    
                    # place_id 자동 추출 (URL에서 숫자 ID)
                    if not data.get("place_id"):
                        place_url = data.get("place_url", "")
                        if place_url:
                            import re
                            match = re.search(r'/(\d{5,})', place_url)
                            if match:
                                data["place_id"] = match.group(1)
                    
                    # collected_at 설정 (실제 수집 시각)
                    if not data.get("collected_at"):
                        data["collected_at"] = collected_at
                    
                    # source 기본값
                    if not data.get("source"):
                        data["source"] = "adlog_crawl"
                    
                    # 행 데이터 생성
                    row_data = [str(data.get(h, "") or "") for h in headers]
                    
                    if unique_key in key_to_row:
                        # 업데이트 대상
                        row_num = key_to_row[unique_key]
                        updates.append((row_num, row_data))
                        result["updated"] += 1
                    else:
                        # 추가 대상
                        new_rows.append(row_data)
                        result["added"] += 1
                    
                    result["success"] += 1
                    
                except Exception as e:
                    logger.error(f"Failed to process record: {e}")
                    result["failed"] += 1
            
            # 3. 배치 업데이트 실행
            if updates:
                # gspread batch_update 사용
                batch_data = []
                for row_num, row_data in updates:
                    end_col = chr(ord('A') + len(headers) - 1)
                    range_str = f"A{row_num}:{end_col}{row_num}"
                    batch_data.append({
                        'range': range_str,
                        'values': [row_data]
                    })
                
                # 100개씩 나눠서 처리 (API 제한 대응)
                batch_size = 100
                for i in range(0, len(batch_data), batch_size):
                    chunk = batch_data[i:i+batch_size]
                    ws.batch_update(chunk)
                    logger.info(f"Batch updated {len(chunk)} rows")
            
            # 4. 새 행 추가 (append_rows 사용)
            if new_rows:
                # gspread append_rows 사용 (한 번에 추가)
                ws.append_rows(new_rows)
                logger.info(f"Appended {len(new_rows)} new rows")
            
            logger.info(f"Upsert complete: updated={result['updated']}, added={result['added']}, failed={result['failed']}")
            
        except Exception as e:
            logger.error(f"Bulk upsert failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
        
        return result
    
    def get_latest_by_client(self, client_id: str) -> Optional[Dict[str, Any]]:
        """특정 클라이언트의 최신 스냅샷 조회"""
        try:
            ws = self.get_or_create_worksheet()
            all_values = ws.get_all_values()
            
            if len(all_values) <= 1:
                return None
            
            headers = all_values[0]
            client_id_idx = headers.index("client_id") if "client_id" in headers else 3
            
            # 역순으로 탐색 (최신 데이터 먼저)
            for row in reversed(all_values[1:]):
                if len(row) > client_id_idx and row[client_id_idx] == client_id:
                    return dict(zip(headers, row))
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to get latest snapshot: {e}")
            return None
    
    def get_history(
        self,
        date_from: str = None,
        date_to: str = None,
        client_id: str = None, 
        keyword: str = None,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """스냅샷 히스토리 조회"""
        from datetime import timedelta
        
        try:
            ws = self.get_or_create_worksheet()
            all_values = ws.get_all_values()
            
            if len(all_values) <= 1:
                return []
            
            headers = all_values[0]
            
            # 인덱스 찾기 (헤더 이름 기반)
            try:
                date_idx = headers.index("date") 
            except ValueError:
                date_idx = 1
                
            # client_id 대신 place_id나 agency 사용 가능하게
            # 여기서는 호출 하위 호환성을 위해 client_id 인자가 들어오면 place_id나 client_name 등으로 매칭 시도 가능하지만
            # 현재 요구사항인 날짜 필터링 위주로 구현
            
            client_id_idx = -1
            if "client_id" in headers:
                client_id_idx = headers.index("client_id")
            elif "place_id" in headers:
                client_id_idx = headers.index("place_id")
            
            keyword_idx = headers.index("keyword") if "keyword" in headers else 6
            
            # 기준 날짜 계산 (days 인자 사용 시)
            cutoff_date = None
            if days > 0 and not date_from:
                cutoff_date = (datetime.now(KST) - timedelta(days=days)).strftime("%Y-%m-%d")
            
            results = []
            for row in all_values[1:]:
                if len(row) <= date_idx:
                    continue
                
                row_date = row[date_idx]
                
                # 날짜 필터링 (date_from ~ date_to)
                if date_from and row_date < date_from:
                    continue
                if date_to and row_date > date_to:
                    continue
                
                # days 필터링 (date_from이 없을 때만)
                if not date_from and cutoff_date and row_date < cutoff_date:
                    continue
                
                # client_id 필터
                if client_id and client_id_idx >= 0:
                    if len(row) > client_id_idx and row[client_id_idx] != client_id:
                        continue
                
                # keyword 필터
                if keyword:
                    if len(row) > keyword_idx and row[keyword_idx] != keyword:
                        continue
                
                # 결과 딕셔너리 생성
                item = {}
                for idx, val in enumerate(row):
                    if idx < len(headers):
                        item[headers[idx]] = val
                results.append(item)
            
            # 날짜 역순 정렬
            results.sort(key=lambda x: (x.get("date", ""), x.get("time_slot", "")), reverse=True)
            
            return results
            
        except Exception as e:
            logger.error(f"Failed to get history: {e}")
            return []
    
    def log_execution(self, result: Dict[str, Any]) -> None:
        """크롤링 실행 로그 기록"""
        try:
            spreadsheet = self._get_spreadsheet()
            
            try:
                log_ws = spreadsheet.worksheet(self.log_tab_name)
            except gspread.WorksheetNotFound:
                log_ws = spreadsheet.add_worksheet(
                    title=self.log_tab_name,
                    rows=500,
                    cols=len(LOG_HEADERS)
                )
                log_ws.append_row(LOG_HEADERS)
            
            now = datetime.now(KST)
            current_hour = now.hour
            time_slot = "09:00" if current_hour < 12 else "15:00"
            
            # 실패 상세 JSON 변환
            failed_details = result.get("failed_details", [])
            if isinstance(failed_details, list):
                failed_json = json.dumps(failed_details[:10], ensure_ascii=False)
            else:
                failed_json = str(failed_details)
            
            log_row = [
                now.isoformat(),
                time_slot,
                str(result.get("success_count", 0)),
                str(result.get("failed_count", 0)),
                str(result.get("elapsed_seconds", 0)),
                result.get("message", ""),
                failed_json,
            ]
            
            log_ws.append_row(log_row)
            logger.info(f"Logged execution: success={result.get('success_count')}, failed={result.get('failed_count')}")
            
        except Exception as e:
            logger.error(f"Failed to log execution: {e}")
    
    def get_last_crawl_time(self) -> Optional[Dict[str, Any]]:
        """마지막 크롤링 실행 시간 조회"""
        try:
            spreadsheet = self._get_spreadsheet()
            
            try:
                log_ws = spreadsheet.worksheet(self.log_tab_name)
            except gspread.WorksheetNotFound:
                return None
            
            all_values = log_ws.get_all_values()
            if len(all_values) <= 1:
                return None
            
            # 마지막 행 (가장 최근)
            last_row = all_values[-1]
            headers = all_values[0] if all_values else []
            
            if headers and last_row:
                return {
                    "executed_at": last_row[0] if len(last_row) > 0 else "",
                    "time_slot": last_row[1] if len(last_row) > 1 else "",
                    "success_count": last_row[2] if len(last_row) > 2 else "",
                    "failed_count": last_row[3] if len(last_row) > 3 else "",
                }
            return None
            
        except Exception as e:
            logger.error(f"Failed to get last crawl time: {e}")
            return None


def get_current_time_slot() -> str:
    """현재 시간대 반환 (09:00 또는 15:00)"""
    now = datetime.now(KST)
    return "09:00" if now.hour < 12 else "15:00"


def get_today_date() -> str:
    """오늘 날짜 반환 (YYYY-MM-DD)"""
    return datetime.now(KST).strftime("%Y-%m-%d")
