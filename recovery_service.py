"""
크롤링 실패 복구 서비스

rank_update_logs에서 실패한 날짜를 찾아 재크롤링하고,
월보장 시트에 누락된 순위만 선택적으로 업데이트

핵심 기능:
1. 실패한 크롤링 날짜 조회 (rank_update_logs)
2. 과거 날짜 데이터 재크롤링 (Adlog)
3. 월보장 시트 선택적 업데이트 (이미 채워진 셀 건너뛰기)
"""
import os
import re
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from zoneinfo import ZoneInfo

import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

KST = ZoneInfo("Asia/Seoul")

# 시트 ID
JTWOLAB_SHEET_ID = os.getenv("JTWOLAB_SHEET_ID", "1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y")
ILRYU_SHEET_ID = os.getenv("ILRYU_SHEET_ID", "1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM")


class RecoveryService:
    """크롤링 실패 복구 서비스"""

    def __init__(self):
        self.gc = self._get_gspread_client()

    def _get_gspread_client(self) -> gspread.Client:
        """gspread 클라이언트 초기화"""
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]

        service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
        if service_account_json:
            creds_dict = json.loads(service_account_json)
            creds = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        else:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
            creds = Credentials.from_service_account_file(creds_path, scopes=scopes)

        return gspread.authorize(creds)

    # =========================================================================
    # 1. 실패한 크롤링 날짜 조회
    # =========================================================================

    def get_failed_crawl_dates(self, days_back: int = 7) -> List[Dict[str, Any]]:
        """rank_update_logs에서 실패한 크롤링 날짜 조회

        Args:
            days_back: 조회할 과거 일수 (기본: 7일)

        Returns:
            실패 기록 리스트 [{date, time_slot, failed_count, message, failed_details}, ...]
        """
        try:
            from rank_snapshot_manager import RankSnapshotManager
            manager = RankSnapshotManager()
            spreadsheet = manager._get_spreadsheet()

            try:
                log_ws = spreadsheet.worksheet("rank_update_logs")
            except gspread.WorksheetNotFound:
                logger.warning("rank_update_logs 탭을 찾을 수 없습니다")
                return []

            all_values = log_ws.get_all_values()
            if len(all_values) <= 1:
                return []

            headers = all_values[0]

            # 헤더 인덱스 찾기
            idx_map = {}
            for i, h in enumerate(headers):
                idx_map[h] = i

            cutoff_date = (datetime.now(KST) - timedelta(days=days_back)).strftime("%Y-%m-%d")

            failed_records = []
            for row in all_values[1:]:
                try:
                    executed_at = row[idx_map.get("executed_at", 0)] if len(row) > idx_map.get("executed_at", 0) else ""
                    failed_count_str = row[idx_map.get("failed_count", 3)] if len(row) > idx_map.get("failed_count", 3) else "0"
                    message = row[idx_map.get("message", 5)] if len(row) > idx_map.get("message", 5) else ""
                    failed_details_str = row[idx_map.get("failed_details", 6)] if len(row) > idx_map.get("failed_details", 6) else "[]"
                    time_slot = row[idx_map.get("time_slot", 1)] if len(row) > idx_map.get("time_slot", 1) else ""

                    # 날짜 추출 (ISO 형식에서)
                    if "T" in executed_at:
                        log_date = executed_at.split("T")[0]
                    else:
                        log_date = executed_at[:10] if len(executed_at) >= 10 else ""

                    # 날짜 필터
                    if log_date < cutoff_date:
                        continue

                    # 실패 건수 확인
                    failed_count = int(failed_count_str) if failed_count_str.isdigit() else 0

                    # 실패가 있거나 메시지에 '실패'가 포함된 경우
                    is_failed = failed_count > 0 or "실패" in message or "failed" in message.lower()

                    if is_failed:
                        # failed_details JSON 파싱
                        try:
                            failed_details = json.loads(failed_details_str) if failed_details_str else []
                        except json.JSONDecodeError:
                            failed_details = []

                        failed_records.append({
                            "date": log_date,
                            "time_slot": time_slot,
                            "executed_at": executed_at,
                            "failed_count": failed_count,
                            "message": message,
                            "failed_details": failed_details,
                        })

                except Exception as e:
                    logger.warning(f"로그 행 파싱 오류: {e}")
                    continue

            # 날짜 역순 정렬
            failed_records.sort(key=lambda x: x["date"], reverse=True)

            logger.info(f"최근 {days_back}일 중 {len(failed_records)}개의 실패 기록 발견")
            return failed_records

        except Exception as e:
            logger.error(f"실패 기록 조회 오류: {e}")
            return []

    def get_dates_missing_in_snapshots(self, target_dates: List[str]) -> List[str]:
        """rank_snapshots에서 데이터가 없는 날짜 찾기

        Args:
            target_dates: 확인할 날짜 리스트 ['2025-01-08', '2025-01-09', ...]

        Returns:
            데이터가 없는 날짜 리스트
        """
        try:
            from rank_snapshot_manager import RankSnapshotManager
            manager = RankSnapshotManager()

            # 전체 기간의 데이터 조회
            if not target_dates:
                return []

            min_date = min(target_dates)
            max_date = max(target_dates)

            history = manager.get_history(date_from=min_date, date_to=max_date)

            # 날짜별 데이터 존재 여부 확인
            dates_with_data = set()
            for record in history:
                record_date = record.get("date", "")
                if record_date:
                    dates_with_data.add(record_date)

            # 데이터가 없는 날짜
            missing_dates = [d for d in target_dates if d not in dates_with_data]

            logger.info(f"확인 요청 {len(target_dates)}일 중 {len(missing_dates)}일 데이터 누락")
            return missing_dates

        except Exception as e:
            logger.error(f"스냅샷 확인 오류: {e}")
            return target_dates  # 오류 시 모든 날짜 반환

    # =========================================================================
    # 2. 과거 날짜 데이터 재크롤링
    # =========================================================================

    def crawl_historical_date(self, target_date: str) -> Dict[str, Any]:
        """특정 과거 날짜의 데이터 크롤링

        Adlog 페이지에는 여러 날짜의 데이터가 표시되므로,
        현재 크롤링 후 target_date에 해당하는 데이터만 필터링

        Args:
            target_date: 크롤링할 날짜 (YYYY-MM-DD)

        Returns:
            크롤링 결과
        """
        try:
            from rank_crawler import AdlogCrawler

            logger.info(f"🔄 {target_date} 날짜 데이터 재크롤링 시작")

            crawler = AdlogCrawler()

            # 전체 크롤링 수행 (Adlog는 여러 날짜 데이터를 동시에 보여줌)
            result = crawler.crawl_ranks(None)

            if not result.get("success"):
                return {
                    "success": False,
                    "date": target_date,
                    "message": result.get("message", "크롤링 실패"),
                    "data": []
                }

            # target_date에 해당하는 데이터만 필터링
            all_data = result.get("data", [])
            filtered_data = [
                record for record in all_data
                if record.get("date") == target_date
            ]

            logger.info(f"✅ {target_date}: 총 {len(all_data)}건 중 {len(filtered_data)}건 매칭")

            return {
                "success": True,
                "date": target_date,
                "message": f"{len(filtered_data)}건 크롤링 완료",
                "data": filtered_data,
                "total_crawled": len(all_data)
            }

        except Exception as e:
            logger.error(f"과거 날짜 크롤링 오류: {e}")
            return {
                "success": False,
                "date": target_date,
                "message": str(e),
                "data": []
            }

    # =========================================================================
    # 3. 월보장 시트 선택적 업데이트 (이미 채워진 셀 건너뛰기)
    # =========================================================================

    def update_guarantee_sheets_selective(
        self,
        rank_data: List[Dict[str, Any]],
        target_date: str
    ) -> Dict[str, Any]:
        """월보장 시트에 선택적으로 순위 업데이트

        담당자가 이미 수동으로 입력한 경우 건너뛰고,
        비어있는 셀만 업데이트

        Args:
            rank_data: 순위 데이터 리스트
            target_date: 업데이트할 날짜 (YYYY-MM-DD)

        Returns:
            업데이트 결과
        """
        results = {}

        # 날짜 문자열 변환 (2025-01-08 → 25. 01. 08)
        try:
            dt = datetime.strptime(target_date, "%Y-%m-%d")
            date_str = dt.strftime("%y. %m. %d")
        except ValueError:
            logger.error(f"잘못된 날짜 형식: {target_date}")
            return {"success": False, "error": "Invalid date format"}

        # URL → 순위 데이터 맵 생성
        url_to_rank = {}
        name_keyword_to_rank = {}

        for item in rank_data:
            place_url = item.get("place_url", "")
            if place_url:
                match = re.search(r'/(\d{5,})', place_url)
                if match:
                    place_id = match.group(1)
                    url_to_rank[place_id] = item

            name = item.get("client_name", "")
            keyword = item.get("keyword", "")
            if name and keyword:
                key = f"{name}|{keyword}"
                name_keyword_to_rank[key] = item

        logger.info(f"[{target_date}] {len(url_to_rank)} URL 매핑, {len(name_keyword_to_rank)} 이름+키워드 매핑")

        # 각 시트 업데이트
        sheets_config = [
            ("jtwolab", JTWOLAB_SHEET_ID),
            ("ilryu", ILRYU_SHEET_ID),
        ]

        for sheet_name, sheet_id in sheets_config:
            try:
                result = self._update_sheet_selective(
                    sheet_id,
                    sheet_name,
                    url_to_rank,
                    name_keyword_to_rank,
                    date_str,
                    target_date
                )
                results[sheet_name] = result
            except Exception as e:
                logger.error(f"{sheet_name} 시트 업데이트 오류: {e}")
                results[sheet_name] = {"success": False, "error": str(e)}

        total_updated = sum(
            r.get("updated", 0) for r in results.values() if isinstance(r, dict)
        )
        total_skipped_existing = sum(
            r.get("skipped_existing", 0) for r in results.values() if isinstance(r, dict)
        )

        return {
            "success": True,
            "date": target_date,
            "results": results,
            "total_updated": total_updated,
            "total_skipped_existing": total_skipped_existing,
        }

    def _update_sheet_selective(
        self,
        sheet_id: str,
        sheet_name: str,
        url_to_rank: Dict[str, Dict],
        name_keyword_to_rank: Dict[str, Dict],
        date_str: str,
        target_date: str
    ) -> Dict[str, Any]:
        """단일 시트 선택적 업데이트 (이미 채워진 날짜는 건너뛰기)"""
        try:
            spreadsheet = self.gc.open_by_key(sheet_id)
            worksheet = spreadsheet.worksheet("보장건")

            all_values = worksheet.get_all_values()

            # 헤더 행 (보통 2행)
            header_row_idx = 1
            if len(all_values) <= header_row_idx:
                return {"success": False, "error": "시트에 데이터가 부족합니다"}

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

                if daily_start_idx == -1:
                    if h == "1" or h == "1일":
                        daily_start_idx = idx

            # 필수 컬럼 체크
            required_cols = ["business_name", "status", "product", "guarantee_rank"]
            missing = [c for c in required_cols if c not in col_map]
            if missing:
                return {"success": False, "error": f"필수 헤더 누락: {missing}"}

            if daily_start_idx == -1:
                daily_start_idx = 17

            VALID_STATUSES = ["진행중", "후불", "반불"]
            MAX_DAILY_COUNT = 25

            updates = []
            matched_count = 0
            skipped_existing = 0  # 이미 채워진 셀 (담당자가 입력)
            skipped_rank = 0  # 보장 순위 초과

            start_row = header_row_idx + 1
            for i, row in enumerate(all_values[start_row:]):
                row_num = start_row + i + 1

                def get_val(col_name):
                    idx = col_map.get(col_name)
                    if idx is not None and idx < len(row):
                        return row[idx].strip()
                    return ""

                # 필터링
                status = get_val("status")
                if status not in VALID_STATUSES:
                    continue

                product = get_val("product")
                if "플레이스" not in product:
                    continue

                # 매칭 데이터 찾기
                business_name = get_val("business_name")
                keyword = get_val("keyword")
                url = get_val("url")

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

                if not rank_item and business_name and keyword:
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
                    continue

                # 보장 순위 이내 확인
                if current_rank > guarantee_rank:
                    skipped_rank += 1
                    continue

                # ====== 핵심: 해당 날짜가 이미 채워져 있는지 확인 ======
                target_col_idx = -1
                date_already_exists = False

                for offset in range(MAX_DAILY_COUNT):
                    check_idx = daily_start_idx + offset

                    cell_value = ""
                    if check_idx < len(row):
                        cell_value = row[check_idx].strip()

                    if not cell_value:
                        # 빈 셀 발견 - 여기에 기입 가능
                        if target_col_idx == -1:
                            target_col_idx = check_idx
                        continue

                    # 해당 날짜가 이미 기록되어 있는지 확인
                    # 형식: "25. 01. 08\n3등" 또는 "25. 01. 08 3등"
                    if date_str in cell_value:
                        date_already_exists = True
                        break

                # 이미 해당 날짜 데이터가 있으면 건너뛰기 (담당자가 입력)
                if date_already_exists:
                    skipped_existing += 1
                    logger.debug(f"[{sheet_name}] {business_name}: {target_date} 이미 기록됨 (건너뛰기)")
                    continue

                # 빈 셀이 없으면 마지막 위치에 추가
                if target_col_idx == -1:
                    target_col_idx = daily_start_idx + MAX_DAILY_COUNT

                # 업데이트 추가
                cell_value = f"{date_str}\n{current_rank}등"

                updates.append({
                    "row": row_num,
                    "col": target_col_idx + 1,
                    "value": cell_value,
                    "business_name": business_name,
                })
                matched_count += 1

            # 배치 업데이트
            if updates:
                cells_to_update = []
                for update in updates:
                    cell = gspread.Cell(update["row"], update["col"], update["value"])
                    cells_to_update.append(cell)

                worksheet.update_cells(cells_to_update, value_input_option='USER_ENTERED')
                logger.info(f"[{sheet_name}] {len(updates)}개 셀 업데이트 완료")

            return {
                "success": True,
                "matched": matched_count,
                "updated": len(updates),
                "skipped_existing": skipped_existing,
                "skipped_rank": skipped_rank,
            }

        except gspread.WorksheetNotFound:
            return {"success": False, "error": "보장건 탭을 찾을 수 없습니다"}
        except Exception as e:
            logger.error(f"[{sheet_name}] 업데이트 오류: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return {"success": False, "error": str(e)}

    # =========================================================================
    # 4. 통합 복구 기능
    # =========================================================================

    def recover_failed_crawls(self, days_back: int = 7) -> Dict[str, Any]:
        """실패한 크롤링 복구 (전체 프로세스)

        1. 실패한 날짜 조회
        2. rank_snapshots에서 누락 확인
        3. 누락된 날짜 재크롤링
        4. 월보장 시트 선택적 업데이트

        Args:
            days_back: 조회할 과거 일수

        Returns:
            복구 결과
        """
        logger.info(f"🔄 크롤링 실패 복구 시작 (최근 {days_back}일)")

        result = {
            "failed_dates_found": [],
            "missing_dates": [],
            "crawl_results": [],
            "update_results": [],
            "summary": {},
        }

        # 1. 실패한 날짜 조회
        failed_records = self.get_failed_crawl_dates(days_back)
        failed_dates = list(set([r["date"] for r in failed_records]))
        result["failed_dates_found"] = failed_dates

        if not failed_dates:
            logger.info("✅ 실패한 크롤링 기록이 없습니다")
            result["summary"] = {"status": "no_failures", "message": "실패 기록 없음"}
            return result

        logger.info(f"📋 실패한 날짜: {failed_dates}")

        # 2. rank_snapshots에서 실제로 누락된 날짜 확인
        missing_dates = self.get_dates_missing_in_snapshots(failed_dates)
        result["missing_dates"] = missing_dates

        if not missing_dates:
            logger.info("✅ 모든 실패 날짜의 데이터가 이미 복구되어 있습니다")
            result["summary"] = {"status": "already_recovered", "message": "이미 복구됨"}
            return result

        logger.info(f"📋 데이터 누락 날짜: {missing_dates}")

        # 3. 누락된 날짜 재크롤링
        total_crawled = 0
        for target_date in missing_dates:
            crawl_result = self.crawl_historical_date(target_date)
            result["crawl_results"].append(crawl_result)

            if crawl_result.get("success") and crawl_result.get("data"):
                total_crawled += len(crawl_result["data"])

                # 4. 월보장 시트 업데이트
                update_result = self.update_guarantee_sheets_selective(
                    crawl_result["data"],
                    target_date
                )
                result["update_results"].append(update_result)

        # 요약
        total_updated = sum(
            r.get("total_updated", 0) for r in result["update_results"]
        )
        total_skipped = sum(
            r.get("total_skipped_existing", 0) for r in result["update_results"]
        )

        result["summary"] = {
            "status": "completed",
            "failed_dates_count": len(failed_dates),
            "missing_dates_count": len(missing_dates),
            "total_crawled": total_crawled,
            "total_updated": total_updated,
            "total_skipped_existing": total_skipped,
            "message": f"{len(missing_dates)}개 날짜 복구, {total_updated}개 셀 업데이트, {total_skipped}개 건너뜀 (이미 입력됨)"
        }

        logger.info(f"✅ 복구 완료: {result['summary']['message']}")

        return result

    def recover_specific_date(self, target_date: str) -> Dict[str, Any]:
        """특정 날짜만 복구

        Args:
            target_date: 복구할 날짜 (YYYY-MM-DD)

        Returns:
            복구 결과
        """
        logger.info(f"🔄 {target_date} 날짜 복구 시작")

        result = {
            "date": target_date,
            "crawl_result": None,
            "update_result": None,
            "summary": {},
        }

        # 1. 크롤링
        crawl_result = self.crawl_historical_date(target_date)
        result["crawl_result"] = crawl_result

        if not crawl_result.get("success"):
            result["summary"] = {
                "status": "crawl_failed",
                "message": crawl_result.get("message", "크롤링 실패")
            }
            return result

        if not crawl_result.get("data"):
            result["summary"] = {
                "status": "no_data",
                "message": f"{target_date} 날짜의 데이터를 찾을 수 없습니다"
            }
            return result

        # 2. 시트 업데이트
        update_result = self.update_guarantee_sheets_selective(
            crawl_result["data"],
            target_date
        )
        result["update_result"] = update_result

        result["summary"] = {
            "status": "completed",
            "crawled_count": len(crawl_result["data"]),
            "updated_count": update_result.get("total_updated", 0),
            "skipped_existing": update_result.get("total_skipped_existing", 0),
            "message": f"{len(crawl_result['data'])}건 크롤링, {update_result.get('total_updated', 0)}건 업데이트, {update_result.get('total_skipped_existing', 0)}건 건너뜀"
        }

        logger.info(f"✅ {target_date} 복구 완료: {result['summary']['message']}")

        return result


# =============================================================================
# 외부 호출용 함수
# =============================================================================

def get_failed_crawl_dates(days_back: int = 7) -> List[Dict[str, Any]]:
    """실패한 크롤링 날짜 조회 (외부 호출용)"""
    service = RecoveryService()
    return service.get_failed_crawl_dates(days_back)


def recover_failed_crawls(days_back: int = 7) -> Dict[str, Any]:
    """실패한 크롤링 복구 (외부 호출용)"""
    service = RecoveryService()
    return service.recover_failed_crawls(days_back)


def recover_specific_date(target_date: str) -> Dict[str, Any]:
    """특정 날짜 복구 (외부 호출용)"""
    service = RecoveryService()
    return service.recover_specific_date(target_date)
