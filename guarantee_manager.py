"""
월보장 관리 모듈
제이투랩, 일류기획 보장건 데이터 관리
"""
import os
import json
import pytz
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from pathlib import Path
import gspread
from google.oauth2.service_account import Credentials
import logging

logger = logging.getLogger(__name__)

try:
    from data_security import DataSecurity
    USE_ENCRYPTION = True
except ImportError:
    USE_ENCRYPTION = False
    logger.warning("DataSecurity module not available. Using plain storage.")


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
        
        # 암호화 모듈 초기화
        if USE_ENCRYPTION:
            self.security = DataSecurity()
            self.encrypted_filename = "guarantee_data.enc"
        else:
            self.security = None
        
        self.data = self._load_data()
    
    def _load_data(self) -> Dict[str, List[Dict]]:
        """저장된 데이터 로드"""
        # 암호화된 데이터 우선 로드
        if USE_ENCRYPTION and self.security:
            try:
                data = self.security.load_encrypted(self.encrypted_filename)
                if data and "items" in data:
                    logger.info(f"Loaded {len(data.get('items', []))} encrypted items")
                    return data
            except Exception as e:
                logger.warning(f"Failed to load encrypted data: {e}")
        
        # 일반 파일 로드 (호환성)
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded {len(data.get('items', []))} items from plain file")
                    
                    # 암호화 저장소로 마이그레이션
                    if USE_ENCRYPTION and self.security and data.get("items"):
                        self.security.save_encrypted(data, self.encrypted_filename)
                        logger.info("Migrated data to encrypted storage")
                        # 원본 파일 삭제 (선택사항)
                        # os.remove(self.storage_path)
                    
                    return data
            except Exception as e:
                logger.error(f"Failed to load plain data: {e}")
        
        return {
            "items": [],
            "updated_at": None,
            "last_sync": None
        }
    
    def _save_data(self) -> bool:
        """데이터 저장"""
        try:
            kst = pytz.timezone('Asia/Seoul')
            self.data["updated_at"] = datetime.now(kst).isoformat()
            
            # 암호화 저장
            if USE_ENCRYPTION and self.security:
                success = self.security.save_encrypted(self.data, self.encrypted_filename)
                if success:
                    logger.info(f"Saved {len(self.data.get('items', []))} items (encrypted)")
                return success
            
            # 일반 저장 (호환성)
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            logger.info(f"Saved {len(self.data.get('items', []))} items (plain)")
            return True
        except Exception as e:
            logger.error(f"Save error: {e}")
            return False
    
    def create_item(self, item: Dict) -> Dict:
        """새 보장건 생성"""
        kst = pytz.timezone('Asia/Seoul')
        now_kst = datetime.now(kst)
        
        # ID 자동 생성
        item_id = now_kst.strftime("%Y%m%d%H%M%S") + str(len(self.data["items"]))
        item["id"] = item_id
        item["created_at"] = now_kst.isoformat()
        item["updated_at"] = now_kst.isoformat()
        
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
                kst = pytz.timezone('Asia/Seoul')
                item["updated_at"] = datetime.now(kst).isoformat()
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
    
    def get_deadline_status(self, company: str = None) -> Dict:
        """마감 임박 현황 조회 (오늘부터 5일 이내)
        
        Args:
            company: 회사명 (제이투랩, 일류기획) - None이면 전체
            
        Returns:
            {
                "items_by_days_remaining": {
                    0: [...],  # 오늘 만료 (25일차)
                    1: [...],  # 1일 남음 (24일차)
                    2: [...],  # 2일 남음 (23일차)
                    3: [...],  # 3일 남음 (22일차)
                    4: [...],  # 4일 남음 (21일차)
                    5: [...],  # 5일 남음 (20일차)
                },
                "total": 총 개수
            }
        """
        items = self.get_items()
        if company:
            items = [i for i in items if i.get("company") == company]
        
        # 진행중 또는 후불만
        items = [i for i in items if i.get("status") in ["진행중", "후불"]]
        
        items_by_days_remaining = {0: [], 1: [], 2: [], 3: [], 4: [], 5: []}
        
        for item in items:
            daily_ranks = item.get("daily_ranks", {})
            if not daily_ranks:
                continue
            
            # 최대 일차 찾기
            max_day = 0
            for day_str in daily_ranks.keys():
                try:
                    day_num = int(day_str)
                    if day_num > max_day:
                        max_day = day_num
                except:
                    continue
            
            if max_day == 0:
                continue
            
            # 20일차 이상 (5일 이내 마감) 항목만 수집
            if max_day >= 20 and max_day <= 25:
                days_remaining = 25 - max_day
                
                item_info = {
                    "business_name": item.get("business_name"),
                    "main_keyword": item.get("main_keyword"),
                    "current_day": max_day,
                    "days_remaining": days_remaining
                }
                
                items_by_days_remaining[days_remaining].append(item_info)
        
        # 총 개수 계산
        total = sum(len(items) for items in items_by_days_remaining.values())
        
        return {
            "items_by_days_remaining": items_by_days_remaining,
            "total": total
        }
    
    def get_exposure_status(self, company: str = None) -> Dict:
        """실시간 노출 현황 조회
        
        Args:
            company: 회사명 (제이투랩, 일류기획) - None이면 전체
            
        Returns:
            {
                "exposed": 노출 중인 건수,
                "not_exposed": 미노출 건수,
                "exposure_details": [
                    {
                        "business_name": "상호명",
                        "current_rank": 현재순위,
                        "trend_1d": 1일전 대비 증감,
                        "trend_2d": 2일전 대비 증감,
                        "trend_3d": 3일전 대비 증감,
                        "last_updated": "마지막 업데이트 날짜"
                    }
                ]
            }
        """
        from datetime import date, timedelta
        
        items = self.get_items()
        if company:
            items = [i for i in items if i.get("company") == company]
        
        # 진행중 또는 후불만
        items = [i for i in items if i.get("status") in ["진행중", "후불"]]
        
        today = date.today()
        exposed_count = 0
        not_exposed_count = 0
        exposure_details = []
        
        for item in items:
            daily_ranks = item.get("daily_ranks", {})
            if not daily_ranks:
                not_exposed_count += 1
                continue
            
            # 일차별 순위를 날짜 순으로 정렬 (날짜가 있는 것 우선)
            days_with_dates = []
            days_without_dates = []
            
            for day_str, rank_data in daily_ranks.items():
                if isinstance(rank_data, dict) and rank_data.get("date"):
                    days_with_dates.append((day_str, rank_data))
                else:
                    days_without_dates.append((day_str, rank_data))
            
            # 날짜가 있는 데이터를 날짜 순으로 정렬
            if days_with_dates:
                days_with_dates.sort(key=lambda x: x[1]["date"])
                
                # 가장 최신 데이터
                latest_day_str, latest_rank_data = days_with_dates[-1]
                latest_date_str = latest_rank_data.get("date")
                current_rank = latest_rank_data.get("rank")
                
                # 날짜로 노출 여부 판단 (오늘 또는 어제까지는 노출로 간주)
                try:
                    latest_date = date.fromisoformat(latest_date_str)
                    days_ago = (today - latest_date).days
                    is_exposed = (days_ago <= 1)  # 오늘 또는 어제면 노출
                except:
                    is_exposed = False
                
                # 증감 계산
                trend_1d = None
                trend_2d = None
                trend_3d = None
                
                if len(days_with_dates) >= 2:
                    prev_rank_data = days_with_dates[-2][1]
                    prev_rank = prev_rank_data.get("rank")
                    if current_rank and prev_rank:
                        trend_1d = prev_rank - current_rank  # 순위가 올랐으면 양수
                
                if len(days_with_dates) >= 3:
                    prev_rank_data_2 = days_with_dates[-3][1]
                    prev_rank_2 = prev_rank_data_2.get("rank")
                    if current_rank and prev_rank_2:
                        trend_2d = prev_rank_2 - current_rank
                
                if len(days_with_dates) >= 4:
                    prev_rank_data_3 = days_with_dates[-4][1]
                    prev_rank_3 = prev_rank_data_3.get("rank")
                    if current_rank and prev_rank_3:
                        trend_3d = prev_rank_3 - current_rank
                
            else:
                # 날짜 정보가 없는 경우 (레거시 데이터)
                # 일차별 데이터를 숫자 순으로 정렬
                sorted_days = sorted([int(k) for k in daily_ranks.keys()])
                if not sorted_days:
                    not_exposed_count += 1
                    continue
                
                latest_day = sorted_days[-1]
                rank_data = daily_ranks.get(str(latest_day))
                
                # 레거시 형식: 정수 또는 딕셔너리
                if isinstance(rank_data, dict):
                    current_rank = rank_data.get("rank")
                else:
                    current_rank = rank_data
                
                latest_date_str = None
                is_exposed = False  # 날짜 정보 없으면 미노출로 간주
                trend_1d = None
                trend_2d = None
                trend_3d = None
            
            if is_exposed:
                exposed_count += 1
            else:
                not_exposed_count += 1
            
            exposure_details.append({
                "business_name": item.get("business_name"),
                "main_keyword": item.get("main_keyword"),  # 메인 키워드 추가
                "current_rank": current_rank,
                "is_exposed": is_exposed,
                "trend_1d": trend_1d,
                "trend_2d": trend_2d,
                "trend_3d": trend_3d,
                "latest_date": latest_date_str
            })
        
        return {
            "exposed": exposed_count,
            "not_exposed": not_exposed_count,
            "exposure_details": exposure_details
        }


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
                logger.info(f"✅ Fetched {len(items)} items from {company} sheet")
                
                # 전체 데이터 수 로그
                if items:
                    logger.info(f"First item from {company}: {items[0]}")
                
                for idx, item in enumerate(items):
                    try:
                        # 상호명이 필수
                        if not item.get("business_name"):
                            logger.warning(f"Skipping item without business_name at index {idx}: {item}")
                            continue
                        
                        # 기존 데이터 확인 (상호명과 계약일로 중복 체크)
                        existing = self._find_existing_item(
                            item.get("business_name", ""),
                            item.get("contract_date", ""),
                            company
                        )
                        
                        if existing:
                            # 업데이트
                            self.update_item(existing["id"], item)
                            result["updated"] += 1
                            if idx < 3:  # 처음 몇 개만 로그
                                logger.info(f"Updated: {item.get('business_name')}")
                        else:
                            # 신규 추가
                            item["company"] = company
                            self.create_item(item)
                            result["added"] += 1
                            if idx < 3:  # 처음 몇 개만 로그
                                logger.info(f"Added: {item.get('business_name')}")
                    except Exception as e:
                        logger.error(f"Failed to process item at index {idx}: {item}. Error: {e}")
                        result["failed"] += 1
                        continue
            except Exception as e:
                logger.error(f"❌ Sync failed for {company}: {str(e)}")
                logger.error(f"Sheet ID: {sheet_id}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                result["failed"] += 1
        
        # 마지막 동기화 시간 업데이트 (한국 시간)
        kst = pytz.timezone('Asia/Seoul')
        self.data["last_sync"] = datetime.now(kst).isoformat()
        self._save_data()
        
        logger.info(f"📊 Sync complete - Added: {result['added']}, Updated: {result['updated']}, Failed: {result['failed']}")
        logger.info(f"💾 Total items in database: {len(self.data.get('items', []))}")
        
        return result
    
    def _find_existing_item(self, business_name: str, contract_date: str, company: str) -> Optional[Dict]:
        """중복 데이터 확인"""
        if not business_name:
            return None
            
        for item in self.data.get("items", []):
            # 계약일이 없는 경우 상호명과 회사로만 체크
            if not contract_date:
                if (item.get("business_name") == business_name and
                    item.get("company") == company):
                    return item
            else:
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
            service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
            
            if creds_path and os.path.exists(creds_path):
                logger.info(f"Using credentials file: {creds_path}")
                creds = Credentials.from_service_account_file(creds_path, scopes=scope)
            elif service_account_json:
                # 환경변수에서 JSON 직접 로드
                import json
                logger.info("Using SERVICE_ACCOUNT_JSON from environment")
                service_account_info = json.loads(service_account_json)
                creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
            else:
                raise ValueError("No Google credentials found. Set GOOGLE_APPLICATION_CREDENTIALS or SERVICE_ACCOUNT_JSON")
            
            # 시트 연결
            logger.info(f"Connecting to sheet: {sheet_id}")
            client = gspread.authorize(creds)
            spreadsheet = client.open_by_key(sheet_id)
            
            # 탭 이름 확인
            worksheets = spreadsheet.worksheets()
            worksheet_names = [ws.title for ws in worksheets]
            logger.info(f"Available worksheets: {worksheet_names}")
            
            # '보장건' 탭 찾기 (대소문자 무시)
            target_sheet = None
            for ws in worksheets:
                if ws.title == "보장건":
                    target_sheet = ws
                    break
            
            if not target_sheet:
                logger.error(f"'보장건' 탭을 찾을 수 없습니다. 사용 가능한 탭: {worksheet_names}")
                raise ValueError(f"'보장건' 탭이 없습니다. 사용 가능한 탭: {', '.join(worksheet_names)}")
            
            worksheet = target_sheet
            
            # 데이터 가져오기
            rows = worksheet.get_all_values()
            if len(rows) < 2:
                return []
            
            # 헤더 행 자동 감지 (상위 10행 내에서 찾기)
            header_row_idx = 0
            max_score = 0
            header_keywords = ["구분", "계약일", "대행사", "상호", "키워드", "작업", "상품", "입금", "마진"]
            
            for idx in range(min(10, len(rows))):
                row = rows[idx]
                score = 0
                for cell in row:
                    cell_str = str(cell).strip()
                    for keyword in header_keywords:
                        if keyword in cell_str:
                            score += 1
                if score > max_score:
                    max_score = score
                    header_row_idx = idx
            
            if max_score == 0:
                logger.warning(f"Could not find header row with keywords. Using first row.")
                header_row_idx = 0
            else:
                logger.info(f"Found header row at index {header_row_idx} (row {header_row_idx + 1}) with score {max_score}")
            
            headers = rows[header_row_idx]
            data_rows = rows[header_row_idx + 1:]
            
            logger.info(f"Using headers: {headers[:10]}...")  # 처음 10개만 로그
            
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
                elif "총" in header and "계약" in header:
                    header_map["total_contract"] = idx
                elif "상품" in header:
                    header_map["product"] = idx
                elif "담당" in header:
                    header_map["manager"] = idx
                elif "메모" in header:
                    header_map["memo"] = idx
                elif "플" in header and "계정" in header:
                    header_map["place_account"] = idx
                elif "URL" in header.upper():
                    header_map["url"] = idx
                elif "계약" in header and "당시" in header and "순위" in header:
                    header_map["initial_rank"] = idx
                elif "보장" in header and "순위" in header:
                    header_map["guarantee_rank"] = idx
                elif "작업" in header and "시작" in header:
                    header_map["work_start_date"] = idx
            
            # 헤더 매핑 로그
            logger.info(f"Header mapping created: {header_map}")
            logger.info(f"Total headers: {len(headers)}, Headers: {headers}")
            
            # 데이터 파싱
            items = []
            for row_idx, row in enumerate(data_rows):
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
                            elif "amount" in field_name or field_name == "total_contract":
                                value = self._parse_amount(value)
                            # 순위 필드 숫자 변환
                            elif "rank" in field_name:
                                try:
                                    value = int(value) if value.isdigit() else value
                                except:
                                    pass
                            
                            item[field_name] = value
                
                # 일차별 순위 파싱 (1~25일차)
                # 데이터 형식: "25. 07. 083등" (YY. MM. DD순위)
                daily_ranks = {}
                for i in range(1, 26):
                    for idx, header in enumerate(headers):
                        # 숫자만 있는 헤더 또는 "일" 포함 헤더 찾기
                        header_str = str(header).strip()
                        if (header_str == str(i) or f"{i}일" in header_str) and idx < len(row):
                            cell_value = self._get_cell_value(row, idx)
                            if cell_value:
                                # "25. 07. 083등" 형식 파싱
                                parsed = self._parse_daily_rank_cell(cell_value)
                                if parsed:
                                    daily_ranks[str(i)] = parsed
                            break  # 하나 찾으면 종료
                
                if daily_ranks:
                    item["daily_ranks"] = daily_ranks
                
                items.append(item)
                
                # 첫 몇 개 항목은 로깅
                if row_idx < 3:
                    logger.info(f"Parsed item {row_idx}: {item.get('business_name')}, contract_date: {item.get('contract_date')}, status: {item.get('status')}")
            
            logger.info(f"Total items parsed from {company} sheet: {len(items)}")
            return items
            
        except Exception as e:
            logger.error(f"Failed to fetch sheet data from {sheet_id}: {str(e)}")
            logger.error(f"Company: {company}")
            if "worksheet" in str(e).lower() or "not found" in str(e).lower():
                logger.error("'보장건' 탭을 찾을 수 없습니다. 시트에 '보장건' 탭이 있는지 확인하세요.")
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
    
    def _parse_daily_rank_cell(self, cell_value: str) -> Optional[Dict]:
        """일차별 순위 셀 파싱
        Args:
            cell_value: 다양한 형식 지원:
                - "25. 07. 08\n3등" (줄바꿈으로 날짜와 순위 분리)
                - "25. 07. 083등" (날짜와 순위가 붙어있음)
                - "3등" (순위만)
                - "3" (숫자만)
        Returns:
            {"date": "2025-07-08", "rank": 3} 또는 None
        """
        if not cell_value:
            return None
        
        import re
        cell_str = str(cell_value).strip()
        
        # 패턴 1: 줄바꿈으로 날짜와 순위 분리 (예: "25. 07. 08\n3등")
        match = re.match(r"(\d{2})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*[\n\r]+\s*(\d+)등?", cell_str)
        if match:
            year, month, day, rank = match.groups()
            full_year = f"20{year}"
            date_str = f"{full_year}-{month.zfill(2)}-{day.zfill(2)}"
            try:
                rank_int = int(rank)
                return {"date": date_str, "rank": rank_int}
            except:
                return None
        
        # 패턴 2: 날짜 뒤에 바로 순위 (예: "25. 07. 083등", "25. 09. 024등")
        match = re.match(r"(\d{2})\.\s*(\d{1,2})\.\s*(\d{1,2})(\d+)등?", cell_str)
        if match:
            year, month, day, rank = match.groups()
            full_year = f"20{year}"
            date_str = f"{full_year}-{month.zfill(2)}-{day.zfill(2)}"
            try:
                rank_int = int(rank)
                return {"date": date_str, "rank": rank_int}
            except:
                return None
        
        # 패턴 3: 날짜만 있는 경우 (예: "25. 07. 08")
        match = re.match(r"(\d{2})\.\s*(\d{1,2})\.\s*(\d{1,2})\s*$", cell_str)
        if match:
            year, month, day = match.groups()
            full_year = f"20{year}"
            date_str = f"{full_year}-{month.zfill(2)}-{day.zfill(2)}"
            return {"date": date_str, "rank": None}  # 순위 정보 없음
        
        # 패턴 4: "N등" 형식 (예: "3등", "5등")
        match = re.match(r"(\d+)등", cell_str)
        if match:
            try:
                rank_int = int(match.group(1))
                return {"rank": rank_int}  # 날짜 정보 없음
            except:
                return None
        
        # 패턴 5: 숫자만 (예: "3", "5")
        if cell_str.isdigit():
            try:
                rank_int = int(cell_str)
                return {"rank": rank_int}  # 날짜 정보 없음
            except:
                return None
        
        return None
