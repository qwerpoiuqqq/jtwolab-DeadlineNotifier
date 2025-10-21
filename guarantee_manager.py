"""
월보장 관리 모듈
제이투랩, 일류기획 보장건 데이터 관리
"""
import os
import json
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
import logging

logger = logging.getLogger(__name__)


class GuaranteeManager:
    """월보장 데이터 관리 클래스"""
    
    def __init__(self, storage_path: str = None):
        """초기화
        Args:
            storage_path: 데이터 저장 경로 (기본: guarantee_data.json)
        """
        if storage_path is None:
            storage_path = os.path.join(os.getcwd(), "guarantee_data.json")
        self.storage_path = storage_path
        self.data = self._load_data()
    
    def _load_data(self) -> Dict[str, List[Dict]]:
        """저장된 데이터 로드"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {
            "items": [],
            "updated_at": None
        }
    
    def _save_data(self) -> bool:
        """데이터 저장"""
        try:
            self.data["updated_at"] = datetime.now().isoformat()
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            return True
        except Exception as e:
            print(f"Save error: {e}")
            return False
    
    def create_item(self, item: Dict) -> Dict:
        """새 보장건 생성"""
        # ID 자동 생성
        item_id = datetime.now().strftime("%Y%m%d%H%M%S") + str(len(self.data["items"]))
        item["id"] = item_id
        item["created_at"] = datetime.now().isoformat()
        item["updated_at"] = datetime.now().isoformat()
        
        # 필수 필드 기본값 설정
        item.setdefault("type", "신규")  # 구분: 신규/연장
        item.setdefault("status", "세팅대기")  # 작업 여부
        item.setdefault("product", "플레이스")  # 상품
        item.setdefault("manager", "김찬영")  # 담당자
        item.setdefault("daily_ranks", {})  # 1~25일차 순위
        
        self.data["items"].append(item)
        self._save_data()
        return item
    
    def get_items(self, filters: Dict = None) -> List[Dict]:
        """보장건 목록 조회
        Args:
            filters: 필터 조건 (company, status, product 등)
        """
        items = self.data.get("items", [])
        
        if not filters:
            return items
        
        filtered = items
        
        # 회사별 필터
        if "company" in filters:
            filtered = [i for i in filtered if i.get("company") == filters["company"]]
        
        # 상태별 필터
        if "status" in filters:
            filtered = [i for i in filtered if i.get("status") == filters["status"]]
        
        # 상품별 필터
        if "product" in filters:
            filtered = [i for i in filtered if i.get("product") == filters["product"]]
        
        # 날짜 범위 필터
        if "date_from" in filters:
            filtered = [i for i in filtered if i.get("contract_date", "") >= filters["date_from"]]
        
        if "date_to" in filters:
            filtered = [i for i in filtered if i.get("contract_date", "") <= filters["date_to"]]
        
        # 진행중인 건만 필터
        if filters.get("active_only"):
            active_statuses = ["진행중", "세팅대기"]
            filtered = [i for i in filtered if i.get("status") in active_statuses]
        
        return filtered
    
    def get_item(self, item_id: str) -> Optional[Dict]:
        """특정 보장건 조회"""
        for item in self.data.get("items", []):
            if item.get("id") == item_id:
                return item
        return None
    
    def update_item(self, item_id: str, updates: Dict) -> Optional[Dict]:
        """보장건 수정"""
        for idx, item in enumerate(self.data.get("items", [])):
            if item.get("id") == item_id:
                # 수정 불가 필드 보호
                updates.pop("id", None)
                updates.pop("created_at", None)
                
                # 업데이트
                item.update(updates)
                item["updated_at"] = datetime.now().isoformat()
                self.data["items"][idx] = item
                self._save_data()
                return item
        return None
    
    def delete_item(self, item_id: str) -> bool:
        """보장건 삭제"""
        items = self.data.get("items", [])
        for idx, item in enumerate(items):
            if item.get("id") == item_id:
                del self.data["items"][idx]
                self._save_data()
                return True
        return False
    
    def update_daily_rank(self, item_id: str, day: int, rank: int) -> Optional[Dict]:
        """일차별 순위 업데이트
        Args:
            item_id: 보장건 ID
            day: 일차 (1~25)
            rank: 순위
        """
        item = self.get_item(item_id)
        if not item:
            return None
        
        if "daily_ranks" not in item:
            item["daily_ranks"] = {}
        
        item["daily_ranks"][str(day)] = rank
        return self.update_item(item_id, {"daily_ranks": item["daily_ranks"]})
    
    def get_statistics(self) -> Dict:
        """통계 정보 조회"""
        items = self.data.get("items", [])
        
        # 회사별 통계
        by_company = {}
        for item in items:
            company = item.get("company", "기타")
            if company not in by_company:
                by_company[company] = {"total": 0, "active": 0, "completed": 0}
            
            by_company[company]["total"] += 1
            
            status = item.get("status", "")
            if status in ["진행중", "세팅대기"]:
                by_company[company]["active"] += 1
            elif status == "완료":
                by_company[company]["completed"] += 1
        
        # 상품별 통계
        by_product = {}
        for item in items:
            product = item.get("product", "기타")
            by_product[product] = by_product.get(product, 0) + 1
        
        # 월별 계약 통계
        by_month = {}
        for item in items:
            contract_date = item.get("contract_date", "")
            if contract_date:
                month_key = contract_date[:7]  # YYYY-MM
                by_month[month_key] = by_month.get(month_key, 0) + 1
        
        return {
            "total": len(items),
            "by_company": by_company,
            "by_product": by_product,
            "by_month": by_month,
            "updated_at": self.data.get("updated_at")
        }
    
    def get_latest_activities(self, limit: int = 10) -> List[Dict]:
        """최근 활동 내역 조회"""
        items = sorted(
            self.data.get("items", []),
            key=lambda x: x.get("updated_at", ""),
            reverse=True
        )
        return items[:limit]
    
    def search(self, query: str) -> List[Dict]:
        """통합 검색
        Args:
            query: 검색어 (상호명, 키워드, 메모 등)
        """
        if not query:
            return []
        
        query_lower = query.lower()
        results = []
        
        for item in self.data.get("items", []):
            # 검색 대상 필드들
            searchable = [
                item.get("business_name", ""),
                item.get("main_keyword", ""),
                item.get("agency", ""),
                item.get("memo", ""),
                item.get("id", "")
            ]
            
            # 하나라도 매치되면 결과에 포함
            if any(query_lower in str(field).lower() for field in searchable):
                results.append(item)
        
        return results
    
    def get_last_sync_time(self) -> Optional[str]:
        """마지막 동기화 시간 조회"""
        return self.data.get("last_sync")


    def sync_from_google_sheets(self) -> Dict[str, int]:
        """구글 시트에서 보장건 데이터 동기화
        Returns:
            동기화 결과 (추가/수정/실패 건수)
        """
        # 시트 ID 설정
        sheets_config = {
            "제이투랩": os.getenv("JTWOLAB_SHEET_ID", "1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y"),
            "일류기획": os.getenv("ILRYU_SHEET_ID", "1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM")
        }
        
        result = {"added": 0, "updated": 0, "failed": 0}
        
        for company, sheet_id in sheets_config.items():
            try:
                items = self._fetch_sheet_data(sheet_id, company)
                for item in items:
                    # 기존 데이터 확인 (상호명과 계약일로 중복 체크)
                    existing = self._find_existing_item(item["business_name"], item["contract_date"], company)
                    
                    if existing:
                        # 업데이트
                        self.update_item(existing["id"], item)
                        result["updated"] += 1
                    else:
                        # 신규 추가
                        item["company"] = company
                        self.create_item(item)
                        result["added"] += 1
            except Exception as e:
                logger.error(f"Sync failed for {company}: {e}")
                result["failed"] += 1
        
        # 마지막 동기화 시간 업데이트
        self.data["last_sync"] = datetime.now().isoformat()
        self._save_data()
        
        return result
    
    def _find_existing_item(self, business_name: str, contract_date: str, company: str) -> Optional[Dict]:
        """중복 데이터 확인"""
        for item in self.data.get("items", []):
            if (item.get("business_name") == business_name and 
                item.get("contract_date") == contract_date and
                item.get("company") == company):
                return item
        return None
    
    def _fetch_sheet_data(self, sheet_id: str, company: str) -> List[Dict]:
        """구글 시트에서 데이터 가져오기"""
        try:
            # 인증 설정
            scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
            
            # 서비스 계정 키 로드
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
            if creds_path and os.path.exists(creds_path):
                creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            else:
                # 환경변수에서 JSON 직접 로드
                import json
                service_account_info = json.loads(os.getenv("SERVICE_ACCOUNT_JSON", "{}"))
                creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
            
            # 시트 연결
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet("보장건")
            
            # 데이터 가져오기
            rows = worksheet.get_all_values()
            if len(rows) < 2:
                return []
            
            headers = rows[0]
            data_rows = rows[1:]
            
            # 헤더 인덱스 매핑
            header_map = {}
            for idx, header in enumerate(headers):
                header_lower = header.strip().lower()
                if "구분" in header:
                    header_map["type"] = idx
                elif "계약일" in header:
                    header_map["contract_date"] = idx
                elif "대행사" in header:
                    header_map["agency"] = idx
                elif "작업" in header and "여부" in header:
                    header_map["status"] = idx
                elif "상호" in header:
                    header_map["business_name"] = idx
                elif "키워드" in header:
                    header_map["main_keyword"] = idx
                elif "입금" in header or "마진" in header:
                    header_map["deposit_amount"] = idx
                elif "계약금" in header:
                    header_map["contract_amount"] = idx
                elif "상품" in header:
                    header_map["product"] = idx
                elif "담당" in header:
                    header_map["manager"] = idx
                elif "메모" in header:
                    header_map["memo"] = idx
                elif "플" in header and "계정" in header:
                    header_map["place_account"] = idx
                elif "URL" in header:
                    header_map["url"] = idx
                elif "보장" in header and "순위" in header:
                    header_map["guarantee_rank"] = idx
                elif "시작일" in header:
                    header_map["start_date"] = idx
            
            # 데이터 파싱
            items = []
            for row in data_rows:
                if not row or not any(row):  # 빈 행 건너뛰기
                    continue
                
                item = {}
                
                # 필수 필드 체크
                business_name = self._get_cell_value(row, header_map.get("business_name"))
                if not business_name:
                    continue
                
                item["business_name"] = business_name
                
                # 나머지 필드 매핑
                for field_name, col_idx in header_map.items():
                    if field_name != "business_name":
                        value = self._get_cell_value(row, col_idx)
                        if value:
                            # 날짜 형식 변환
                            if "date" in field_name and value:
                                value = self._parse_date(value)
                            # 금액 필드 숫자 변환
                            elif "amount" in field_name:
                                value = self._parse_amount(value)
                            
                            item[field_name] = value
                
                # 일차별 순위 파싱 (1~25일차)
                daily_ranks = {}
                for i in range(1, 26):
                    for idx, header in enumerate(headers):
                        if str(i) in header and idx < len(row):
                            rank = self._get_cell_value(row, idx)
                            if rank and rank.isdigit():
                                daily_ranks[str(i)] = int(rank)
                
                if daily_ranks:
                    item["daily_ranks"] = daily_ranks
                
                items.append(item)
            
            return items
            
        except Exception as e:
            logger.error(f"Failed to fetch sheet data: {e}")
            raise
    
    def _get_cell_value(self, row: List, index: Optional[int]) -> str:
        """셀 값 안전하게 가져오기"""
        if index is None or index >= len(row):
            return ""
        return str(row[index]).strip()
    
    def _parse_date(self, date_str: str) -> str:
        """날짜 형식 파싱 (YYYY-MM-DD 형식으로 통일)"""
        if not date_str:
            return ""
        
        # 이미 YYYY-MM-DD 형식인 경우
        if len(date_str) == 10 and date_str[4] == "-" and date_str[7] == "-":
            return date_str
        
        # 다양한 날짜 형식 처리
        import re
        
        # MM/DD/YYYY 또는 MM-DD-YYYY
        match = re.match(r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", date_str)
        if match:
            month, day, year = match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        # YYYY/MM/DD 또는 YYYY.MM.DD
        match = re.match(r"(\d{4})[/.-](\d{1,2})[/.-](\d{1,2})", date_str)
        if match:
            year, month, day = match.groups()
            return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
        
        return date_str
    
    def _parse_amount(self, amount_str: str) -> float:
        """금액 파싱 (숫자만 추출)"""
        if not amount_str:
            return 0
        
        import re
        # 숫자와 소수점만 추출
        numbers = re.findall(r"[\d.]+", amount_str)
        if numbers:
            try:
                return float(numbers[0].replace(",", ""))
            except:
                pass
        return 0
