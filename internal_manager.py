import os
import json
from datetime import datetime, timezone
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


def _build_task_display(tab_title: str, product: str, product_name: str) -> str:
	base_task = (tab_title or "").strip()
	is_misc = _collapse_spaces(base_task) == _collapse_spaces("기타")
	if is_misc:
		return (product_name or base_task).strip() or base_task
	else:
		return (f"{base_task} {product}".strip() if product else base_task) or base_task


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
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
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


def fetch_workload_schedule(company: str = None) -> Dict[str, Any]:
	"""최근 3주간의 작업량 스케줄을 주차별로 반환 (캐시 우선)
	
	Args:
		company: 회사명 필터 (제이투랩, 일류기획)
		
	Returns:
		{
			"weeks": [
				{
					"start_date": "09/12",
					"end_date": "09/18",
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
			cached_data = cache.get_company_workload(company)
			if cached_data:
				logger.info(f"Using cached workload data for {company}")
				result = cached_data.copy()
				result["from_cache"] = True
				return result
		else:
			logger.info(f"Cache invalid or expired for {company}")
	except Exception as e:
		logger.warning(f"Failed to load from cache: {e}")
	
	# 캐시 없으면 직접 조회
	logger.info(f"Fetching workload directly for {company}")
	result = fetch_workload_schedule_direct(company)
	result["from_cache"] = False
	return result


def fetch_workload_schedule_direct(company: str = None) -> Dict[str, Any]:
	"""최근 3주간의 작업량 스케줄을 주차별로 반환 (직접 조회)
	
	Args:
		company: 회사명 필터 (제이투랩, 일류기획)
		
	Returns:
		{
			"weeks": [
				{
					"start_date": "09/12",
					"end_date": "09/18",
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
	if company:
		try:
			from guarantee_manager import GuaranteeManager
			gm = GuaranteeManager()
			items = gm.get_items({"company": company})
			company_business_names = {item.get("business_name") for item in items if item.get("business_name")}
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
	
	today = date.today()
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
			
			# 내부 진행건만 필터
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			if not is_internal:
				continue
			
			internal_rows += 1
			
			# 기본 정보
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
			if not bizname:
				continue
			
			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			workload = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
			product = str(_get_value_flexible(row_norm, settings.product_col, "PRODUCT_COLUMN") or "").strip()
			product_name = str(_get_value_flexible(row_norm, settings.product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
			
			# 회사 필터 (상호명 기준으로 매칭)
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
			
			# 작업 시작일이 없으면 마감일 기준으로 처리 (필터링에서 제외하지 않음)
			if not has_real_start_date:
				no_start_date += 1
				# 현재 날짜를 시작일로 사용 (실제 진행 중인 작업으로 간주)
				start_date = today
			
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
				"has_real_start_date": has_real_start_date
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
	
	if items_with_real_start:
		# 실제 시작일이 있는 경우: 최신 시작일 기준 3주
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
	
	# 업체(상호명) + 주차별 그룹핑
	week_groups = {}
	for item in filtered_items:
		# 상호명을 포함한 키로 그룹핑
		key = (item["bizname"], item["start_date"], item["end_date"])
		if key not in week_groups:
			week_groups[key] = []
		
		# 작업량 파싱
		try:
			wl_num = _parse_int_maybe(item["workload"]) or 0
		except:
			wl_num = 0
		
		workload_display = str(wl_num) if wl_num > 0 else item["workload"]
		
		week_groups[key].append({
			"name": item["task_display"],
			"workload": workload_display
		})
	
	# 정렬 및 포맷팅 (상호명별로 주차를 그룹화)
	weeks = []
	for (bizname, start_dt, end_dt), items in sorted(week_groups.items(), key=lambda x: (x[0][1], x[0][0])):  # 날짜 우선, 상호명 순
		# 같은 업체의 같은 주차 작업들을 하나로 묶음
		week_label = f"{start_dt.strftime('%m/%d')} ~ {end_dt.strftime('%m/%d')}"
		
		weeks.append({
			"start_date": start_dt.strftime("%m/%d"),
			"end_date": end_dt.strftime("%m/%d"),
			"bizname": bizname,
			"items": items
		})
	
	# 최종 통계
	total_workload_items = sum(len(items) for items in week_groups.values())
	logger.info(f"✅ {company} 작업량 조회 완료 - {len(weeks)}주차, 총 {total_workload_items}개 작업 (필터링 후: {len(filtered_items)}건)")
	
	return {"weeks": weeks}


