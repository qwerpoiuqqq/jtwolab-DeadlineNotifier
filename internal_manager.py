import os
import json
import pytz
from datetime import datetime, timezone, date, timedelta
from typing import Any, Dict, List, Tuple

from sheet_client import (
	load_settings,
	_get_client,
	_find_header_row,
	_build_records,
	_get_value_flexible,
	_normalize_key,
	_collapse_spaces,
	_parse_int_maybe,
	_is_truthy,
)


CACHE_FILE = os.getenv("INTERNAL_CACHE_FILE", "internal_cache.json")


def _is_internal_or_postpaid(value: Any) -> bool:
	"""내부 진행건 또는 후불 건인지 확인
	
	Args:
		value: '작업 여부' 컬럼 값
		
	Returns:
		True if 값이 '진행중', '후불' 또는 truthy 값
	"""
	if value is None:
		return False
	
	s = str(value).strip().lower()
	
	# 명시적 상태값 체크
	if s in ["진행중", "후불", "진행 중", "후불건"]:
		return True
	
	# 기존 truthy 값도 허용 (하위 호환성)
	from sheet_client import _is_truthy
	return _is_truthy(value)


def parse_date_flexible(date_str: str):
	"""다양한 날짜 형식을 파싱 (매우 관대하게)"""
	from datetime import date, datetime, timedelta
	import re
	
	if not date_str:
		return None
	
	# datetime 객체가 이미 들어온 경우
	if isinstance(date_str, datetime):
		return date_str.date()
	
	# date 객체가 이미 들어온 경우
	if isinstance(date_str, date):
		return date_str
	
	date_str = str(date_str).strip()
	
	# 빈 문자열 체크
	if not date_str or date_str.lower() in ['none', 'null', 'n/a', '-']:
		return None
	
	try:
		# 1. 한국어 날짜 형식 먼저 처리 (예: "10월 31일", "08월 04일")
		match = re.match(r"^(\d{1,2})월\s*(\d{1,2})일$", date_str)
		if match:
			month, day = match.groups()
			year = date.today().year
			try:
				return date(year, int(month), int(day))
			except ValueError:
				pass
		
		# 2. Google Sheets 시리얼 넘버 처리 (1900-01-01 기준)
		# 예: 45582 = 2024-10-10
		if re.match(r"^\d{5,6}$", date_str):
			try:
				serial = int(date_str)
				base_date = datetime(1899, 12, 30)
				result_date = base_date + timedelta(days=serial)
				return result_date.date()
			except:
				pass
		
		# 3. YYYY-MM-DD, YYYY.MM.DD, YYYY/MM/DD 형식
		match = re.match(r"^(\d{4})[./-](\d{1,2})[./-](\d{1,2})$", date_str)
		if match:
			year, month, day = match.groups()
			try:
				return date(int(year), int(month), int(day))
			except ValueError:
				pass
		
		# 4. YYYY-MM-DD HH:MM:SS 형식 (datetime 문자열)
		match = re.match(r"^(\d{4})[./-](\d{1,2})[./-](\d{1,2})\s+\d{1,2}:\d{1,2}(:\d{1,2})?$", date_str)
		if match:
			year, month, day = match.groups()[:3]
			try:
				return date(int(year), int(month), int(day))
			except ValueError:
				pass
		
		# 5. YYYYMMDD 형식 (예: 20251027)
		match = re.match(r"^(\d{4})(\d{2})(\d{2})$", date_str)
		if match:
			year, month, day = match.groups()
			try:
				return date(int(year), int(month), int(day))
			except ValueError:
				pass
		
		# 6. YY. M. D 형식 (예: 25. 10. 27, 25.10.27, 25-10-27)
		# 구분자가 2개 있어야 함 (3개 부분)
		match = re.match(r"^(\d{2})[./ -](\d{1,2})[./ -](\d{1,2})$", date_str)
		if match:
			year_short, month, day = match.groups()
			year = 2000 + int(year_short)
			try:
				return date(year, int(month), int(day))
			except ValueError:
				pass
		
		# 7. M/D 또는 MM/DD 형식 (예: 8/1, 10/27) - 현재 연도 기준
		match = re.match(r"^(\d{1,2})/(\d{1,2})$", date_str)
		if match:
			month, day = match.groups()
			month_int, day_int = int(month), int(day)
			if 1 <= month_int <= 12 and 1 <= day_int <= 31:
				year = date.today().year
				try:
					return date(year, month_int, day_int)
				except ValueError:
					pass
		
		# 8. M-D 또는 MM-DD 형식 (예: 8-1, 10-24) - 현재 연도 기준
		match = re.match(r"^(\d{1,2})-(\d{1,2})$", date_str)
		if match:
			month, day = match.groups()
			month_int, day_int = int(month), int(day)
			if 1 <= month_int <= 12 and 1 <= day_int <= 31:
				year = date.today().year
				try:
					return date(year, month_int, day_int)
				except ValueError:
					pass
		
		# 9. M.D 또는 MM.DD 형식 (예: 8.1, 10.27) - 현재 연도 기준
		match = re.match(r"^(\d{1,2})\.(\d{1,2})$", date_str)
		if match:
			month, day = match.groups()
			month_int, day_int = int(month), int(day)
			if 1 <= month_int <= 12 and 1 <= day_int <= 31:
				year = date.today().year
				try:
					return date(year, month_int, day_int)
				except ValueError:
					pass
		
		# 10. dateutil 사용 (매우 복잡한 형식만 - 위에서 실패한 경우)
		try:
			from dateutil import parser as dateutil_parser
			# dayfirst=False로 미국식 해석 (MM-DD-YYYY)
			parsed = dateutil_parser.parse(date_str, dayfirst=False)
			return parsed.date()
		except ImportError:
			pass  # dateutil 없으면 스킵
		except Exception:
			pass  # dateutil 파싱 실패시 스킵
			
	except Exception as e:
		import logging
		logging.getLogger(__name__).warning(f"날짜 파싱 실패: '{date_str}' - {e}")
	
	return None


def _build_task_display(tab_title: str, product: str, product_name: str) -> str:
	base_task = (tab_title or "").strip()
	is_misc = _collapse_spaces(base_task) == _collapse_spaces("기타")
	if is_misc:
		return (product_name or base_task).strip() or base_task
	else:
		return (f"{base_task} {product}".strip() if product else base_task) or base_task


def fetch_internal_items_for_company(company: str) -> List[Dict[str, Any]]:
	"""특정 회사의 raw 내부 진행건 데이터 가져오기 (단 1회 API 호출)
	
	Args:
		company: 회사명 (제이투랩, 일류기획)
	
	Returns:
		raw items 리스트: [{agency, bizname, task_display, workload, start_date, end_date, has_real_start_date}]
	"""
	import logging
	logger = logging.getLogger(__name__)
	from datetime import date, timedelta
	
	settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")
	
	# 보장건 데이터에서 해당 회사의 상호명 리스트 + 작업 시작일 매핑
	guarantee_data_map = {}
	company_business_names = None
	try:
		from guarantee_manager import GuaranteeManager
		gm = GuaranteeManager()
		items = gm.get_items({"company": company})
		company_business_names = {item.get("business_name") for item in items if item.get("business_name")}
		for item in items:
			biz = item.get("business_name")
			if biz:
				guarantee_data_map[biz] = item
		logger.info(f"📋 {company} 보장건: {len(company_business_names)}개 업체")
	except Exception as e:
		logger.warning(f"보장건 로드 실패: {e}")
		company_business_names = None
	
	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)
	
	# 한국 시간 기준 (KST)
	kst = pytz.timezone('Asia/Seoul')
	today = datetime.now(kst).date()
	logger.info(f"📅 오늘 날짜 (KST): {today}")
	
	all_items = []
	ws_list = ss.worksheets()
	tab_titles = [ws.title for ws in ws_list]
	logger.info(f"📊 {company} raw 데이터 조회 - 워크시트: {len(ws_list)}개")
	logger.info(f"   탭 목록: {', '.join(tab_titles)}")
	
	# 워크시트별 처리 통계
	tab_stats = {}
	
	for idx, ws in enumerate(ws_list, 1):
		tab_title = (ws.title or "").strip()
		logger.info(f"   [{idx}/{len(ws_list)}] 처리 중: {tab_title}")
		
		try:
			header_row, headers = _find_header_row(ws, settings)
			records = _build_records(ws, header_row, headers)
			logger.info(f"      ✓ {len(records)}개 행 읽음")
			
			tab_stats[tab_title] = {"total_rows": len(records), "internal_count": 0, "company_match": 0}
		except Exception as e:
			logger.error(f"      ❌ 탭 읽기 실패: {e}")
			tab_stats[tab_title] = {"error": str(e)}
			continue
		
		for row in records:
			row_norm = { _normalize_key(k): v for k, v in row.items() }
			
			# 내부 진행건 또는 후불 건 필터
			is_internal = _is_internal_or_postpaid(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			if not is_internal:
				continue
			
			tab_stats[tab_title]["internal_count"] += 1
			
			# 기본 정보
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
			if not bizname:
				continue
			
			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			workload = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
			product = str(_get_value_flexible(row_norm, settings.product_col, "PRODUCT_COLUMN") or "").strip()
			product_name = str(_get_value_flexible(row_norm, settings.product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
			
			# 회사 필터 (상호명 기준)
			if company_business_names is not None:
				if bizname not in company_business_names:
					continue
			
			tab_stats[tab_title]["company_match"] += 1
			
			# 마감일 계산: 오늘 + 남은일수 (remain이 None이어도 일단 수집)
			end_date = today + timedelta(days=remain) if remain is not None else None
			
			# 작업 시작일: 각 행의 '작업 시작일' 컬럼 읽기 (다양한 컬럼명 시도)
			start_date_str = None
			start_col_found = None
			for possible_col in ["작업 시작일", "작업시작일", "시작일", "세팅일", "작업시작", "시작"]:
				start_val = _get_value_flexible(row_norm, possible_col, "")
				if start_val:
					start_date_str = str(start_val).strip()
					start_col_found = possible_col
					# 디버깅: 원본 값 확인
					if len(all_items) < 10:
						logger.info(f"  🔍 {bizname}/{product}: '{possible_col}' 컬럼 원본값 = '{start_val}' (타입: {type(start_val).__name__})")
					break
			
			start_date = None
			parse_success = False
			if start_date_str:
				start_date = parse_date_flexible(start_date_str)
				if start_date:
					parse_success = True
					# 디버깅: 처음 10개만 상세 로그
					if len(all_items) < 10:
						logger.info(f"  ✓ 파싱 성공: '{start_date_str}' → {start_date}")
						if end_date:
							logger.info(f"     마감일: {end_date.strftime('%Y-%m-%d')} (오늘={today}, remain={remain}일)")
				else:
					# 파싱 실패 - 경고 로그
					logger.warning(f"  ⚠️ 날짜 파싱 실패: {bizname}/{product} - '{start_date_str}' (컬럼: {start_col_found})")
			
			# 시작일이 없거나 파싱 실패한 경우 보장건 데이터에서 가져오기 시도
			if not start_date and bizname in guarantee_data_map:
				guarantee_item = guarantee_data_map[bizname]
				work_start = guarantee_item.get("work_start_date")
				if work_start:
					fallback_date = parse_date_flexible(work_start)
					if fallback_date:
						start_date = fallback_date
						parse_success = True
						if len(all_items) < 10:
							logger.info(f"  ✓ 보장건 데이터에서 시작일 가져옴: {bizname} → {work_start} → {start_date}")
			
			# 그래도 시작일이 없으면 None으로 유지 (필터링하지 않고 포함)
			if not start_date:
				if len(all_items) < 10 and end_date:
					logger.info(f"  ○ {bizname}/{product}: 시작일(없음), 마감일={end_date.strftime('%Y-%m-%d')} (remain={remain}일) - 그대로 포함")
			
			# 작업명 생성
			is_review_tab = _collapse_spaces(tab_title) == _collapse_spaces("영수증리뷰")
			if is_review_tab:
				item_col_value = None
				for possible_col in ["항목", "항목명"]:
					val = _get_value_flexible(row_norm, possible_col, "")
					if val:
						item_col_value = str(val).strip()
						break
				task_display = item_col_value if item_col_value else _build_task_display(tab_title, product, product_name)
			else:
				task_display = _build_task_display(tab_title, product, product_name)
			
			all_items.append({
				"agency": agency_raw or "내부 진행",
				"bizname": bizname,
				"task_display": task_display,
				"workload": workload,
				"start_date": start_date,
				"end_date": end_date,
				"has_real_start_date": bool(start_date_str),  # 시트에 시작일 컬럼이 있었는지
				"tab_title": tab_title,  # 업종 분류용
				"product": product,  # 업종 분류용
				"product_name": product_name  # 업종 분류용
			})
	
	logger.info(f"✅ {company} raw 데이터: {len(all_items)}개")
	
	# 워크시트별 통계 출력
	logger.info(f"📋 워크시트별 상세 통계:")
	for tab_title, stats in tab_stats.items():
		if "error" in stats:
			logger.error(f"   ❌ {tab_title}: {stats['error']}")
		else:
			logger.info(f"   ✓ {tab_title}: 전체 {stats['total_rows']}행 → 내부진행 {stats['internal_count']}건 → {company} 매칭 {stats['company_match']}건")
	
	# 중복 제거 없음! 각 행을 그대로 유지
	# 같은 작업이라도 시작일-마감일이 다르면 별도로 표시
	return all_items


def process_raw_items_to_schedule(raw_items: List[Dict[str, Any]], company: str, business_name: str = None) -> Dict[str, Any]:
	"""Raw 데이터를 스케줄 형식으로 변환 (메모리 작업, API 호출 없음)
	
	Args:
		raw_items: raw 데이터 리스트
		company: 회사명
		business_name: 업체명 (None이면 전체)
	
	Returns:
		{"weeks": [...]}
	"""
	import logging
	logger = logging.getLogger(__name__)
	
	# 한국 시간 기준 (KST)
	kst = pytz.timezone('Asia/Seoul')
	today = datetime.now(kst).date()
	
	if not raw_items:
		logger.debug(f"⊘ {business_name or company}: raw_items 없음")
		return {"weeks": []}
	
	# 모든 데이터를 읽어온 후 4주 기준으로 필터링
	four_weeks_ago = today - timedelta(days=28)
	logger.info(f"  📅 오늘: {today}, 4주 전: {four_weeks_ago}")
	
	# 시작일이 4주 이내이거나, 시작일이 없으면 마감일이 오늘 이후인 작업만 표시
	filtered_items = [
		item for item in raw_items 
		if (item["start_date"] is not None and item["start_date"] >= four_weeks_ago) or
		   (item["start_date"] is None and item["end_date"] is not None and item["end_date"] >= today)
	]
	
	logger.info(f"  📊 필터 결과: {len(raw_items)}개 → {len(filtered_items)}개 (4주 필터 적용, 시작일 {four_weeks_ago.strftime('%m/%d')} 이후)")
	
	# 기간별 그룹핑 (같은 시작일-마감일을 가진 작업들을 묶음)
	period_groups = {}
	
	# 디버깅: 각 고유 기간 확인
	if business_name and len(filtered_items) > 0:
		unique_periods = {}
		for item in filtered_items:
			key = (item["start_date"], item["end_date"])
			if key not in unique_periods:
				unique_periods[key] = []
			unique_periods[key].append(f"{item['task_display']}:{item.get('workload', 0)}")
		
		logger.info(f"  🔍 {business_name} 고유 기간: {len(unique_periods)}개")
		# None 처리하여 정렬 (None은 맨 뒤로)
		sorted_periods = sorted(unique_periods.items(), key=lambda x: (x[0][0] is None, x[0][0] if x[0][0] else date.max, x[0][1]))
		for (s, e), tasks in sorted_periods[:10]:
			s_str = s.strftime('%m/%d') if s else "미정"
			logger.info(f"    [{s_str} ~ {e.strftime('%m/%d')}] {len(tasks)}개 작업")
	
	for item in filtered_items:
		start_dt = item["start_date"]  # None 가능
		end_dt = item["end_date"]  # None 가능
		
		# end_dt가 None이면 스킵
		if end_dt is None:
			continue
		
		# 날짜 검증 (시작일이 있는 경우만)
		if start_dt is not None and start_dt > end_dt:
			logger.error(f"❌ {item['bizname']}: 시작일({start_dt}) > 마감일({end_dt}) - 논리 오류!")
			continue  # 잘못된 데이터는 제외
		
		# 같은 기간(시작일-마감일)끼리 그룹핑 (시작일 None도 허용)
		key = (start_dt, end_dt)
		if key not in period_groups:
			period_groups[key] = {}
		
		task_name = item["task_display"]
		
		try:
			wl_num = _parse_int_maybe(item["workload"]) or 0
		except:
			wl_num = 0
		
		# 같은 기간 내에서 같은 작업명은 합산
		if task_name in period_groups[key]:
			period_groups[key][task_name] += wl_num
		else:
			period_groups[key][task_name] = wl_num
	
	# 스케줄 포맷팅 (시작일 기준 정렬 - 오래된 것부터, None은 맨 뒤)
	weeks = []
	sorted_groups = sorted(period_groups.items(), key=lambda x: (x[0][0] is None, x[0][0] if x[0][0] else date.max, x[0][1]))
	
	for (start_dt, end_dt), tasks in sorted_groups:
		items = []
		# 시트 순서 유지를 위해 정렬하지 않고 원래 순서대로 (삽입 순서 유지)
		for task_name, total_workload in tasks.items():
			items.append({
				"name": task_name,
				"workload": str(total_workload) if total_workload > 0 else "0"
			})
		
		weeks.append({
			"start_date": start_dt.strftime("%m/%d") if start_dt else "",
			"end_date": end_dt.strftime("%m/%d"),
			"items": items
		})
	
	# 디버깅: 처음 몇 개만 로그
	if business_name and len(weeks) > 0:
		logger.info(f"  📊 총 {len(weeks)}개 기간:")
		for idx, week in enumerate(weeks[:5]):
			items_str = ", ".join([f"{item['name'][:20]}:{item['workload']}" for item in week['items']])
			logger.info(f"    [{idx+1}] {week['start_date']} ~ {week['end_date']}: {items_str}")
	
	return {"weeks": weeks}


def fetch_internal_items() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
	"""구글 시트 전 탭을 순회하여 내부 진행건만 평탄화하여 반환한다.

	반환: (items, stats)
	- items: [{tab_title, agency, bizname, task_display, remain_days, daily_workload, checked}]
	- stats: {worksheets: int, items: int}
	"""
	settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	# 중복 상호 병합 및 작업량 합산을 위한 집계: key=(agency, tab_title, task_display, bizname) -> sum(workload)
	aggregator: Dict[Tuple[str, str, str, str], Dict[str, Any]] = {}
	ws_list = ss.worksheets()
	for ws in ws_list:
		tab_title = (ws.title or "").strip()
		header_row, headers = _find_header_row(ws, settings)
		records = _build_records(ws, header_row, headers)
		for row in records:
			row_norm = { _normalize_key(k): v for k, v in row.items() }
			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			is_checked = _is_truthy(_get_value_flexible(row_norm, settings.checked_col, "CHECKED_COLUMN"))
			is_internal = _is_internal_or_postpaid(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
			product = str(_get_value_flexible(row_norm, settings.product_col, "PRODUCT_COLUMN") or "").strip()
			product_name = str(_get_value_flexible(row_norm, settings.product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
			workload = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()

			if not is_internal:
				continue
			if not bizname:
				continue

			display_task = _build_task_display(tab_title, product, product_name)
			label_agency = agency_raw or "내부 진행"
			try:
				wl_num = _parse_int_maybe(workload) or 0
			except Exception:
				wl_num = 0
			key = (label_agency, tab_title, display_task, bizname)
			entry = aggregator.get(key)
			if not entry:
				aggregator[key] = {
					"tab_title": tab_title,
					"agency": label_agency,
					"bizname": bizname,
					"task_display": display_task,
					"remain_days": remain,
					"daily_workload_sum": wl_num,
					"checked": bool(is_checked),
				}
			else:
				entry["daily_workload_sum"] = int(entry.get("daily_workload_sum", 0)) + wl_num

	# 출력 리스트 구성 (정렬은 상호명 기준)
	items: List[Dict[str, Any]] = []
	for entry in sorted(aggregator.values(), key=lambda e: (e["agency"], e["task_display"], e["bizname"])):
		wl_sum = int(entry.get("daily_workload_sum", 0))
		items.append({
			"tab_title": entry["tab_title"],
			"agency": entry["agency"],
			"bizname": entry["bizname"],
			"task_display": entry["task_display"],
			"remain_days": entry["remain_days"],
			"daily_workload": (str(wl_sum) if wl_sum > 0 else None),
			"checked": entry["checked"],
		})

	return items, {"worksheets": len(ws_list), "items": len(items)}


def load_cache() -> Dict[str, Any]:
	"""캐시 파일을 읽어 반환한다. 없으면 빈 구조 반환."""
	try:
		with open(CACHE_FILE, "r", encoding="utf-8") as f:
			data = json.load(f)
			if not isinstance(data, dict):
				return {"updated_at": None, "items": []}
			data.setdefault("updated_at", None)
			data.setdefault("items", [])
			return data
	except FileNotFoundError:
		return {"updated_at": None, "items": []}
	except Exception:
		# 손상 시 초기화
		return {"updated_at": None, "items": []}


def save_cache(items: List[Dict[str, Any]]) -> Dict[str, Any]:
	updated_at = datetime.now(timezone.utc).astimezone().isoformat()
	data = {"updated_at": updated_at, "items": items}
	with open(CACHE_FILE, "w", encoding="utf-8") as f:
		json.dump(data, f, ensure_ascii=False)
	return data


def refresh_cache() -> Dict[str, Any]:
	items, stats = fetch_internal_items()
	data = save_cache(items)
	data["stats"] = stats
	return data


def fetch_workload_schedule(company: str = None, business_name: str = None) -> Dict[str, Any]:
	"""최근 3주간의 작업량 스케줄을 주차별로 반환 (캐시 우선)
	
	Args:
		company: 회사명 필터 (제이투랩, 일류기획)
		business_name: 상호명 필터 (특정 업체만 조회)
		
	Returns:
		{
			"weeks": [
				{
					"start_date": "09/12",
					"end_date": "09/18",
					"bizname": "업체명",
					"items": [
						{"name": "일류 저장", "workload": "300"},
						{"name": "일류 영수증B", "workload": "10"}
					]
				}
			],
			"from_cache": bool
		}
	"""
	import logging
	logger = logging.getLogger(__name__)
	
	# 캐시에서 먼저 조회
	try:
		from workload_cache import WorkloadCache
		cache = WorkloadCache()
		
		if cache.is_cache_valid():
			# 업체별 조회
			if business_name:
				cached_data = cache.get_business_workload(company, business_name)
				if cached_data:
					logger.info(f"✅ Using cached workload data for business: {business_name}")
					result = cached_data.copy()
					result["from_cache"] = True
					return result
				else:
					logger.warning(f"⚠️ No cached data for business: {business_name}")
			# 회사 전체 조회
			else:
				cached_data = cache.get_company_workload(company)
				if cached_data:
					logger.info(f"✅ Using cached workload data for company: {company}")
					result = cached_data.copy()
					result["from_cache"] = True
					return result
		else:
			logger.info(f"Cache invalid or expired")
	except Exception as e:
		logger.warning(f"Failed to load from cache: {e}")
	
	# 캐시 없으면 직접 조회
	logger.info(f"Fetching workload directly for {company}/{business_name or 'all'}")
	result = fetch_workload_schedule_direct(company, business_name)
	result["from_cache"] = False
	return result


def fetch_workload_schedule_direct(company: str = None, business_name: str = None) -> Dict[str, Any]:
	"""최근 3주간의 작업량 스케줄을 주차별로 반환 (직접 조회)
	
	Args:
		company: 회사명 필터 (제이투랩, 일류기획)
		business_name: 상호명 필터 (특정 업체만 조회)
		
	Returns:
		{
			"weeks": [
				{
					"start_date": "09/12",
					"end_date": "09/18",
					"bizname": "업체명",
					"items": [
						{"name": "일류 저장", "workload": "300"},
						{"name": "일류 영수증B", "workload": "10"}
					]
				}
			]
		}
	"""
	from datetime import date, timedelta
	import logging
	logger = logging.getLogger(__name__)
	
	settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")
	
	# 회사 필터가 있으면 보장건 데이터에서 해당 회사의 상호명 리스트 가져오기
	company_business_names = None
	guarantee_data_map = {}  # 상호명 -> 보장건 데이터 매핑
	if company:
		try:
			from guarantee_manager import GuaranteeManager
			gm = GuaranteeManager()
			items = gm.get_items({"company": company})
			company_business_names = {item.get("business_name") for item in items if item.get("business_name")}
			# 상호명별 데이터 매핑 (작업 시작일 참조용)
			for item in items:
				biz = item.get("business_name")
				if biz:
					guarantee_data_map[biz] = item
			if company_business_names:
				logger.info(f"📋 {company} 보장건 상호명: {len(company_business_names)}개")
			else:
				logger.warning(f"⚠️ {company} 보장건 데이터가 없습니다 (전체 내부 진행건 조회)")
				company_business_names = None
		except Exception as e:
			logger.warning(f"보장건 데이터 로드 실패 (전체 내부 진행건 조회): {e}")
			company_business_names = None
	
	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)
	
	# 한국 시간 기준 (KST)
	kst = pytz.timezone('Asia/Seoul')
	today = datetime.now(kst).date()
	all_items = []
	
	# 디버깅 카운터
	total_rows = 0
	internal_rows = 0
	filtered_by_company = 0
	no_start_date = 0
	valid_items = 0
	
	ws_list = ss.worksheets()
	logger.info(f"📊 작업량 조회 시작 - 회사: {company}, 워크시트 수: {len(ws_list)}")
	
	for ws in ws_list:
		tab_title = (ws.title or "").strip()
		header_row, headers = _find_header_row(ws, settings)
		records = _build_records(ws, header_row, headers)
		
		for row in records:
			row_norm = { _normalize_key(k): v for k, v in row.items() }
			total_rows += 1
			
			# 내부 진행건 또는 후불 건 필터
			is_internal = _is_internal_or_postpaid(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			if not is_internal:
				continue
			
			internal_rows += 1
			
			# 기본 정보
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
			if not bizname:
				continue
			
			# 상호명 필터 (특정 업체만 조회)
			if business_name:
				if bizname != business_name:
					continue
			
			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			workload = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
			product = str(_get_value_flexible(row_norm, settings.product_col, "PRODUCT_COLUMN") or "").strip()
			product_name = str(_get_value_flexible(row_norm, settings.product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
			
			# 회사 필터 (상호명 기준으로 매칭) - 상호명 필터가 없을 때만 적용
			if not business_name:
				if company and company_business_names is not None:
					# 보장건 상호명 리스트에 있는 것만 포함
					if bizname not in company_business_names:
						filtered_by_company += 1
						continue
				elif company and company_business_names is None:
					# 보장건 로드 실패 시 대행사명으로 폴백
					if agency_raw != company:
						filtered_by_company += 1
						continue
			
			# 마감일 계산 (필수)
			if remain is None:
				continue
			end_date = today + timedelta(days=remain)
			
			# 작업 시작일 파싱 (여러 컬럼명 시도)
			start_date_str = None
			for possible_col in ["작업 시작일", "작업시작일", "시작일", "세팅일"]:
				start_val = _get_value_flexible(row_norm, possible_col, "")
				if start_val:
					start_date_str = str(start_val).strip()
					break
			
			# 작업 시작일 파싱
			start_date = None
			has_real_start_date = False
			if start_date_str:
				# 날짜 파싱 (YYYY-MM-DD 또는 YYYY.MM.DD 형식)
				try:
					import re
					# YYYY-MM-DD 또는 YYYY.MM.DD 형식
					match = re.match(r"(\d{4})[.-](\d{1,2})[.-](\d{1,2})", start_date_str)
					if match:
						year, month, day = match.groups()
						start_date = date(int(year), int(month), int(day))
						has_real_start_date = True
				except:
					pass
			
			# 작업 시작일이 없으면 보장건 데이터에서 가져오기
			if not has_real_start_date and bizname in guarantee_data_map:
				guarantee_item = guarantee_data_map[bizname]
				work_start = guarantee_item.get("work_start_date")
				if work_start:
					try:
						import re
						match = re.match(r"(\d{4})[.-](\d{1,2})[.-](\d{1,2})", work_start)
						if match:
							year, month, day = match.groups()
							start_date = date(int(year), int(month), int(day))
							has_real_start_date = True
							logger.debug(f"보장건 데이터에서 작업 시작일 가져옴: {bizname} -> {work_start}")
					except:
						pass
			
			# 그래도 작업 시작일이 없으면 역산 (마감일 - 14일)
			if not has_real_start_date:
				no_start_date += 1
				# 마감일에서 2주 전을 시작일로 추정
				start_date = end_date - timedelta(days=14)
			
			# 작업명 생성 ('영수증리뷰' 탭은 항목 컬럼 사용)
			is_review_tab = _collapse_spaces(tab_title) == _collapse_spaces("영수증리뷰")
			if is_review_tab:
				# 영수증리뷰 탭은 '항목' 컬럼 읽기
				item_col_value = None
				for possible_col in ["항목", "항목명"]:
					val = _get_value_flexible(row_norm, possible_col, "")
					if val:
						item_col_value = str(val).strip()
						break
				task_display = item_col_value if item_col_value else _build_task_display(tab_title, product, product_name)
			else:
				task_display = _build_task_display(tab_title, product, product_name)
			
			all_items.append({
				"agency": agency_raw or "내부 진행",
				"bizname": bizname,
				"task_display": task_display,
				"workload": workload,
				"start_date": start_date,
				"end_date": end_date,
				"has_real_start_date": has_real_start_date,
				"tab_title": tab_title,  # 업종 분류용
				"product": product,  # 업종 분류용
				"product_name": product_name  # 업종 분류용
			})
			valid_items += 1
	
	# 통계 로깅
	logger.info(f"📈 작업량 조회 통계:")
	logger.info(f"  - 전체 행: {total_rows}")
	logger.info(f"  - 내부 진행건: {internal_rows}")
	if company and company_business_names is not None:
		logger.info(f"  - {company} 보장건 매칭으로 제외: {filtered_by_company}")
	else:
		logger.info(f"  - 회사 필터로 제외: {filtered_by_company}")
	logger.info(f"  - 작업 시작일 없음 (역산 처리): {no_start_date}")
	logger.info(f"  - 유효한 작업: {valid_items}")
	
	# 최근 시작일 찾기
	if not all_items:
		logger.warning(f"⚠️ {company}의 작업량 데이터가 없습니다.")
		return {"weeks": []}
	
	# 실제 작업 시작일이 있는 항목들만으로 최신 날짜 계산
	items_with_real_start = [item for item in all_items if item.get("has_real_start_date")]
	
	# 업체별 조회시에는 오늘 기준 3주만 표시 (더 엄격)
	if business_name:
		three_weeks_ago = today - timedelta(days=21)
		logger.info(f"📅 업체별 조회({business_name}): 오늘({today})부터 3주 전({three_weeks_ago})")
		
		filtered_items = [
			item for item in all_items 
			if item["start_date"] >= three_weeks_ago
		]
		logger.info(f"📊 필터링 결과: {len(all_items)}개 → {len(filtered_items)}개 (최근 3주)")
	elif items_with_real_start:
		# 회사 전체 조회: 최신 시작일 기준 3주
		latest_start = max(item["start_date"] for item in items_with_real_start)
		three_weeks_ago = latest_start - timedelta(days=21)
		logger.info(f"📅 최신 작업 시작일: {latest_start}, 3주 전: {three_weeks_ago}")
		
		# 실제 시작일이 있는 항목은 3주 필터링, 없는 항목은 모두 포함
		filtered_items = [
			item for item in all_items 
			if (item.get("has_real_start_date") and item["start_date"] >= three_weeks_ago)
			or (not item.get("has_real_start_date"))  # 시작일이 없는 항목은 모두 포함
		]
	else:
		# 실제 시작일이 없는 경우: 모든 항목 포함
		logger.info(f"📅 실제 작업 시작일이 없어 전체 작업 포함")
		filtered_items = all_items
	
	# 기간별 그룹핑 (같은 시작일-종료일끼리 묶음)
	period_groups = {}
	for item in filtered_items:
		# 기간을 키로 그룹핑
		key = (item["start_date"], item["end_date"])
		if key not in period_groups:
			period_groups[key] = {}
		
		# 같은 작업명이면 작업량 합산
		task_name = item["task_display"]
		
		# 작업량 파싱
		try:
			wl_num = _parse_int_maybe(item["workload"]) or 0
		except:
			wl_num = 0
		
		if task_name in period_groups[key]:
			# 기존 작업량에 합산
			period_groups[key][task_name] += wl_num
		else:
			# 새 작업 추가
			period_groups[key][task_name] = wl_num
	
	# 정렬 및 포맷팅 (기간별로 정렬)
	weeks = []
	for (start_dt, end_dt), tasks in sorted(period_groups.items(), key=lambda x: x[0][0]):  # 시작일 기준 정렬
		items = []
		# 시트 순서 유지를 위해 정렬하지 않고 원래 순서대로 (삽입 순서 유지)
		for task_name, total_workload in tasks.items():
			items.append({
				"name": task_name,
				"workload": str(total_workload) if total_workload > 0 else "0"
			})
		
		weeks.append({
			"start_date": start_dt.strftime("%m/%d"),
			"end_date": end_dt.strftime("%m/%d"),
			"items": items
		})
	
	# 최종 통계
	total_workload_items = sum(len(items) for items in period_groups.values())
	logger.info(f"✅ {company} 작업량 조회 완료 - {len(weeks)}개 기간, 총 {total_workload_items}개 작업 (필터링 후: {len(filtered_items)}건)")
	
	return {"weeks": weeks}


