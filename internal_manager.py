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
	_parse_date_maybe,
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



# -----------------------------
# 내부 진행 주간 요약 (접수일 기준)
# -----------------------------
from datetime import date as _date, timedelta as _timedelta


def _fmt_mmdd(d: _date) -> str:
	return f"{d.month:02d}/{d.day:02d}"


def _guess_unit_for_task(task_display: str) -> str:
	"""작업명으로 출력 단위를 유추한다. 기본은 '건'."""
	s = (_collapse_spaces(task_display) or "")
	if "영수증" in s:
		return "건"
	for key in ["저장", "길찾기", "지도", "저트길", "클릭", "노출", "타"]:
		if _collapse_spaces(key) in s:
			return "타"
	return "건"


def _build_week_windows(base: _date, weeks: int) -> List[Dict[str, Any]]:
	"""base(기준일)로부터 과거로 이어지는 7일 구간을 weeks개 생성한다.
	예: base=10/02, weeks=3 => [09/26~10/02], [09/19~09/25], [09/12~09/18]
	"""
	windows: List[Dict[str, Any]] = []
	cur_end = base
	for _ in range(max(1, int(weeks))):
		start = cur_end - _timedelta(days=6)
		label = f"{_fmt_mmdd(start)} ~ {_fmt_mmdd(cur_end)}"
		windows.append({"start": start, "end": cur_end, "label": label})
		cur_end = start - _timedelta(days=1)
	return windows


def fetch_internal_weekly_summary(base: _date, weeks: int) -> List[Dict[str, Any]]:
	"""내부 진행건을 '작업 시작일' 기준으로 집계하되, 업체별로
	'마지막에 접수한 작업'의 시작일을 기준 앵커로 삼아 3주차에 해당하는 항목만 출력한다.

	규칙:
	- '작업 시작일' 값이 있으면 그 날짜 사용
	- 없으면 '접수일' 사용
	- 둘 다 없으면 탭 제목의 날짜 폴백
	- 업체별로 최신(가장 최근) 접수일을 가진 레코드를 찾고, 그 레코드의 시작일을 anchor로 함
	- anchor를 기준으로 과거로 1주=7일씩 끊어 1주차(anchor-6~anchor), 2주차, 3주차 범위를 만들고 3주차 항목만 포함

	출력: 업체별 1개 섹션(3주차), '작업명 : 일작업량숫자' 목록
	"""
	settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)
	windows = _build_week_windows(base, weeks)

	# (agency, biz) -> List[record]
	records_by_key: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

	def _parse_date_from_title_to_date(title: str) -> _date | None:
		try:
			import re as _re
			m = _re.search(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", title or "")
			if not m:
				return None
			y = int(m.group(1)); mth = int(m.group(2)); d = int(m.group(3))
			return _date(y, mth, d)
		except Exception:
			return None

	for ws in ss.worksheets():
		tab_title = (ws.title or "").strip()
		header_row, headers = _find_header_row(ws, settings)
		records = _build_records(ws, header_row, headers)
		for row in records:
			row_norm = { _normalize_key(k): v for k, v in row.items() }
			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
			product = str(_get_value_flexible(row_norm, settings.product_col, "PRODUCT_COLUMN") or "").strip()
			product_name = str(_get_value_flexible(row_norm, settings.product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
			workload = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
			received_raw = _get_value_flexible(row_norm, settings.received_date_col, "RECEIVED_DATE_COLUMN")
			start_raw = _get_value_flexible(row_norm, settings.start_date_col, "START_DATE_COLUMN")
			remain_val = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))

			if not is_internal:
				continue
			if not bizname:
				continue

			# 시작일 우선 → 접수일 → 탭 제목 날짜 폴백
			parsed = _parse_date_maybe(start_raw) or _parse_date_maybe(received_raw)
			dt = None
			if parsed:
				try:
					dt = _date(parsed[0], parsed[1], parsed[2])
				except Exception:
					dt = None
			if dt is None:
				dt = _parse_date_from_title_to_date(tab_title)
			if dt is None:
				continue

			# 접수일 파싱 (앵커 판단용)
			rec_parsed = _parse_date_maybe(received_raw)
			received_dt = None
			if rec_parsed:
				try:
					received_dt = _date(rec_parsed[0], rec_parsed[1], rec_parsed[2])
				except Exception:
					received_dt = None

			week_idx = None
			for idx, win in enumerate(windows):
				if win["start"] <= dt <= win["end"]:
					week_idx = idx
					break
			if week_idx is None:
				continue

			# 작업 표시명 (특수 탭 '영수증리뷰'는 '항목' 우선 → 없으면 '구분(내부 소통용)' 변형)
			if _collapse_spaces(tab_title) == _collapse_spaces("영수증리뷰"):
				# 1순위: 항목
				item_val = _get_value_flexible(row_norm, "항목", "PRODUCT_NAME_COLUMN")
				candidates = [
					"구분(내부 소통용)",
					"구분 (내부 소통용)",
					"구분(내부소통용)",
					"구분\n(내부 소통용)",
					"구분",
				]
				cat = str(item_val or "").strip()
				for key in candidates:
					val = _get_value_flexible(row_norm, key, "PRODUCT_NAME_COLUMN")
					if cat:
						break
					if val is not None and str(val).strip() != "":
						cat = str(val).strip()
				if not cat:
					memo = _get_value_flexible(row_norm, "내부 소통용", "PRODUCT_NAME_COLUMN")
					cat = str(memo or "").strip()
				display_task = cat or (product_name if _collapse_spaces(tab_title)==_collapse_spaces("기타") else (f"{tab_title} {product}".strip() if product else tab_title))
			else:
				base_task = tab_title
				is_misc = _collapse_spaces(tab_title) == _collapse_spaces("기타")
				if is_misc:
					display_task = product_name if product_name else base_task
				else:
					display_task = f"{base_task} {product}".strip() if product else base_task

			# 레코드 적재
			try:
				wl_num = _parse_int_maybe(workload) or 0
			except Exception:
				wl_num = 0
			key = (agency_raw or "내부 진행", bizname)
			lst = records_by_key.setdefault(key, [])
			# 종료일 계산: remain_days는 base 기준 상대값
			end_dt = dt
			try:
				if remain_val is not None:
					end_dt = base + _timedelta(days=int(remain_val))
			except Exception:
				end_dt = dt
			lst.append({
				"dt": dt,  # 시작일
				"end": end_dt,  # 종료일
				"received_dt": received_dt or dt,
				"task": display_task,
				"wl": wl_num,
			})

	# 출력 구성: 업체별 3주차만 산출
	groups: List[Dict[str, Any]] = []
	for (agency, biz), recs in sorted(records_by_key.items(), key=lambda kv: (kv[0][0], kv[0][1])):
		if not recs:
			continue
		# 최신 접수일 레코드 찾기
		recv_sorted = sorted(recs, key=lambda r: (r.get("received_dt") or _date.min), reverse=True)
		anchor_dt = recv_sorted[0].get("dt")
		if not isinstance(anchor_dt, _date):
			continue
		# anchor 기준 3주차 범위 계산 (week3: anchor-20 ~ anchor-14)
		w3_start = anchor_dt - _timedelta(days=20)
		w3_end = anchor_dt - _timedelta(days=14)
		# 집계: 3주차 범위에 속하는 레코드만
		task_to_sum: Dict[str, int] = {}
		for r in recs:
			st = r.get("dt")
			ed = r.get("end") or st
			if not isinstance(st, _date) or not isinstance(ed, _date):
				continue
			# 기간 겹침: [st, ed] ∩ [w3_start, w3_end]
			if not (ed < w3_start or st > w3_end):
				task = r.get("task") or ""
				wl = int(r.get("wl") or 0)
				task_to_sum[task] = task_to_sum.get(task, 0) + wl
		# 섹션/텍스트 구성
		label = f"{_fmt_mmdd(w3_start)} ~ {_fmt_mmdd(w3_end)}"
		lines = [f"{t} : {int(v)}" for t, v in sorted(task_to_sum.items(), key=lambda kv: kv[0])]
		sections = [{"label": label, "lines": lines}]
		parts: List[str] = []
		for sec in sections:
			parts.append(sec["label"])
			parts.extend(sec["lines"]) 
			parts.append("")
		text = "\n".join(parts).rstrip()
		groups.append({
			"agency": agency,
			"bizname": biz,
			"sections": sections,
			"text": text,
		})

	return groups

