"""
보장건 시트 자동 순위 업데이트 서비스

15시 순위 크롤링 후 제이투랩/일류기획 시트의 '보장건' 탭에 자동 기입
"""
import os
import re
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# 한국 시간대
KST = ZoneInfo("Asia/Seoul")

# 시트 ID
JTWOLAB_SHEET_ID = os.getenv("JTWOLAB_SHEET_ID", "1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y")
ILRYU_SHEET_ID = os.getenv("ILRYU_SHEET_ID", "1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM")

# 보장건 탭 열 구조 (1-indexed)
COL_PRODUCT = 10      # J열: 상품
COL_BUSINESS_NAME = 6  # F열: 상호명
COL_KEYWORD = 7       # G열: 메인 키워드
COL_URL = 14          # N열: URL
COL_GUARANTEE_RANK = 16  # P열: 보장 순위
COL_DAILY_START = 18  # R열부터: 일별 순위 (1, 2, 3...)

# 상단 헤더 행 (날짜 열 번호: 1, 2, 3...)
HEADER_ROW = 2


class GuaranteeSheetUpdater:
    """보장건 시트 순위 업데이트 클래스"""
    
    def __init__(self):
        self.gc = self._get_gspread_client()
    
    def _get_gspread_client(self) -> gspread.Client:
        """gspread 클라이언트 초기화"""
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        # 환경변수에서 서비스 계정 JSON 로드
        service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
        if service_account_json:
            import json
            creds_dict = json.loads(service_account_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        
        return gspread.authorize(creds)
    
    def update_all_sheets(self, rank_data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """모든 보장건 시트 업데이트
        
        Args:
            rank_data: 크롤링된 순위 데이터 리스트
                      [{"client_name": ..., "keyword": ..., "rank": ..., "place_url": ...}, ...]
        
        Returns:
            업데이트 결과 {"jtwolab": {...}, "ilryu": {...}}
        """
        results = {}
        
        # URL → 순위 데이터 맵 생성
        url_to_rank = {}
        name_keyword_to_rank = {}
        
        for item in rank_data:
            place_url = item.get("place_url", "")
            if place_url:
                # URL에서 place_id 추출
                match = re.search(r'/(\d{5,})', place_url)
                if match:
                    place_id = match.group(1)
                    url_to_rank[place_id] = item
            
            # 상호명 + 키워드로도 매핑
            name = item.get("client_name", "")
            keyword = item.get("keyword", "")
            if name and keyword:
                key = f"{name}|{keyword}"
                name_keyword_to_rank[key] = item
        
        logger.info(f"Prepared {len(url_to_rank)} URL mappings, {len(name_keyword_to_rank)} name+keyword mappings")
        
        # 각 시트 업데이트
        sheets_config = [
            ("jtwolab", JTWOLAB_SHEET_ID),
            ("ilryu", ILRYU_SHEET_ID),
        ]
        
        for sheet_name, sheet_id in sheets_config:
            try:
                result = self._update_sheet(sheet_id, sheet_name, url_to_rank, name_keyword_to_rank)
                results[sheet_name] = result
            except Exception as e:
                logger.error(f"Failed to update {sheet_name} sheet: {e}")
                results[sheet_name] = {"success": False, "error": str(e)}
        
        return results
    
    def _update_sheet(
        self, 
        sheet_id: str, 
        sheet_name: str,
        url_to_rank: Dict[str, Dict],
        name_keyword_to_rank: Dict[str, Dict]
    ) -> Dict[str, Any]:
        """단일 시트 업데이트 (동적 헤더 매핑 적용)"""
        try:
            spreadsheet = self.gc.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet("보장건")
            
            # 전체 데이터 가져오기 (헤더 포함)
            all_values = worksheet.get_all_values()
            
            # 헤더 행 인덱스 (1행 or 2행 확인)
            # 보통 2행이 실제 헤더
            header_row_idx = 1 # 0-indexed (2행)
            if len(all_values) <= header_row_idx:
                return {"success": False, "error": "Sheet has insufficient rows"}
            
            headers = all_values[header_row_idx]
            
            # 헤더 매핑
            col_map = {}
            daily_start_idx = -1
            
            for idx, header in enumerate(headers):
                h = str(header).strip()
                
                if "작업" in h and "여부" in h: 
                    col_map["status"] = idx
                elif "상호" in h or ("플레이스" in h and "자동완성" in h): 
                    col_map["business_name"] = idx
                elif "키워드" in h and "메인" in h: 
                    col_map["keyword"] = idx
                elif "상품" in h: 
                    col_map["product"] = idx
                elif "URL" in h.upper(): 
                    col_map["url"] = idx
                elif "보장" in h and "순위" in h: 
                    col_map["guarantee_rank"] = idx
                
                # 일별 순위 시작 열 찾기 ("1" 또는 "1일")
                # 단, 날짜 열은 보통 뒤쪽에 위치하므로 앞에서 발견된 '1'이 다른 의미일 수 있음에 주의
                # 여기서는 '순위'나 '계약' 등이 아닌 순수 숫자 '1' 또는 '1일'을 찾음
                if daily_start_idx == -1:
                    if h == "1" or h == "1일":
                        daily_start_idx = idx

            # 필수 컬럼 체크
            required_cols = ["business_name", "status", "product", "guarantee_rank"]
            missing = [c for c in required_cols if c not in col_map]
            if missing:
                logger.error(f"[{sheet_name}] Missing headers: {missing}")
                return {"success": False, "error": f"Missing headers: {missing}"}
            
            if daily_start_idx == -1:
                # 못 찾으면 기본값 사용 (R열 = 17)
                daily_start_idx = 17 
                logger.warning(f"[{sheet_name}] Could not find daily rank start column '1'. Using default index 17 (R column).")

            # 오늘 날짜 문자열 (형식: 25. 12. 28)
            today = datetime.now(KST)
            today_str = today.strftime("%y. %m. %d")
            
            VALID_STATUSES = ["진행중", "후불", "반불"]
            MAX_DAILY_COUNT = 25
            
            updates = []
            matched_count = 0
            skipped_count = 0
            filter_skipped_count = 0
            
            # 데이터 행 순회 (헤더 다음 행부터)
            start_row = header_row_idx + 1
            for i, row in enumerate(all_values[start_row:]):
                row_num = start_row + i + 1 # 1-based row number
                
                # 안전한 값 가져오기 함수
                def get_val(col_name):
                    idx = col_map.get(col_name)
                    if idx is not None and idx < len(row):
                        return row[idx].strip()
                    return ""

                # 1. 작업 여부 필터링
                status = get_val("status")
                if status not in VALID_STATUSES:
                    filter_skipped_count += 1
                    continue

                # 2. 상품 확인 (플레이스 포함 여부)
                product = get_val("product")
                if "플레이스" not in product:
                    continue
                
                # 3. 매칭 데이터 찾기
                business_name = get_val("business_name")
                keyword = get_val("keyword") # 메인 키워드
                url = get_val("url")
                
                # 보장 순위 파싱
                guarantee_rank_str = get_val("guarantee_rank")
                guarantee_rank = None
                try:
                    guarantee_rank = int(re.sub(r'[^\d]', '', guarantee_rank_str))
                except ValueError:
                    continue
                
                if not guarantee_rank:
                    continue
                
                # 매칭 시도
                rank_item = None
                if url:
                    match = re.search(r'/(\d{5,})', url)
                    if match:
                        place_id = match.group(1)
                        rank_item = url_to_rank.get(place_id)
                
                # 상호명 매칭 (키워드는 옵션일 수 있음, 하지만 보통 키워드 필수)
                # 시트에 메인 키워드가 없으면 매칭 키 생성 시 주의
                if not rank_item and business_name:
                    # 키워드가 있으면 함께, 없으면 상호명으로만 시도해볼 수도 있지만
                    # rank_data는 보통 (상호명|키워드) 키를 가짐
                    if keyword:
                        key = f"{business_name}|{keyword}"
                        rank_item = name_keyword_to_rank.get(key)
                
                if not rank_item:
                    continue
                
                # 순위 값 확인
                raw_rank = rank_item.get("rank")
                if raw_rank is None:
                    continue
                    
                try:
                    current_rank = int(str(raw_rank).replace('위', '').strip())
                except ValueError:
                    logger.warning(f"Invalid rank value: {raw_rank}")
                    continue
                
                # 4. 보장 순위 이내 확인
                if current_rank > guarantee_rank:
                    skipped_count += 1
                    continue
                
                # 5. 기입 위치 찾기 (동일 날짜 기록 시 패스)
                target_col_idx = -1
                already_recorded_today = False
                
                # 일별 순위 열 검사
                for offset in range(MAX_DAILY_COUNT):
                    check_idx = daily_start_idx + offset
                    
                    cell_value = ""
                    if check_idx < len(row):
                        cell_value = row[check_idx].strip()
                    
                    if not cell_value:
                        # 빈 셀 발견 → 타겟
                        target_col_idx = check_idx
                        break
                    
                    # 오늘 날짜가 이미 기록되어 있는지 확인
                    if today_str in cell_value:
                        already_recorded_today = True
                        break
                
                # 오늘 날짜가 이미 기록되어 있으면 패스
                if already_recorded_today:
                    skipped_count += 1
                    continue
                
                # 25개가 꽉 찼으면 26번째 위치 사용
                if target_col_idx == -1:
                    target_col_idx = daily_start_idx + MAX_DAILY_COUNT
                
                # 업데이트 추가 (1-indexed col로 변환)
                cell_value = f"{today_str}\n{current_rank}등"
                
                updates.append({
                    "row": row_num,
                    "col": target_col_idx + 1,
                    "value": cell_value,
                })
                matched_count += 1
            
            # 배치 업데이트
            if updates:
                cells_to_update = []
                for update in updates:
                    cell = gspread.Cell(update["row"], update["col"], update["value"])
                    cells_to_update.append(cell)
                
                worksheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
                logger.info(f"{sheet_name}: Updated {len(updates)} cells")
            
            return {
                "success": True,
                "matched": matched_count,
                "skipped_rank": skipped_count,
                "skipped_filter": filter_skipped_count,
                "updated": len(updates),
            }
            
        except gspread.WorksheetNotFound:
            return {"success": False, "error": "보장건 tab not found"}
        except Exception as e:
            logger.error(f"Error updating {sheet_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}


def update_guarantee_sheets_from_snapshots() -> Dict[str, Any]:
    """rank_snapshots에서 최신 데이터를 가져와 보장건 시트 업데이트
    
    15시 크롤링 후 호출됨
    """
    try:
        from rank_snapshot_manager import RankSnapshotManager
        
        # 최신 순위 데이터 가져오기
        manager = RankSnapshotManager()
        today = datetime.now(KST).strftime("%Y-%m-%d")
        
        # 오늘 데이터 가져오기
        history = manager.get_history(date_from=today, date_to=today)
        
        if not history:
            logger.warning("No rank data found for today")
            return {"success": False, "error": "No data for today"}
        
        logger.info(f"Found {len(history)} rank records for today")
        
        # 보장건 시트 업데이트
        updater = GuaranteeSheetUpdater()
        results = updater.update_all_sheets(history)
        
        total_updated = sum(
            r.get("updated", 0) for r in results.values() if isinstance(r, dict)
        )
        
        logger.info(f"✅ Guarantee sheets updated: {total_updated} total cells")
        
        return {
            "success": True,
            "results": results,
            "total_updated": total_updated,
        }
        
    except Exception as e:
        logger.error(f"Failed to update guarantee sheets: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {"success": False, "error": str(e)}
