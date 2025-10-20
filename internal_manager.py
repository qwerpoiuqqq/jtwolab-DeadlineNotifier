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
	"""내부 진행건을 접수일 기준으로 최근 weeks개 주간 구간에 집계한다.

	반환: [ { agency, bizname, sections: [ { label, lines: [ "작업명 : 수량단위" ] } ], text } ]
	- text는 sections를 순서대로 1줄 공백으로 이어붙인 최종 문자열
	"""
	settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)
	windows = _build_week_windows(base, weeks)

	# (agency, biz) -> week_idx -> task_display -> sum(workload)
	aggr: Dict[Tuple[str, str], Dict[int, Dict[str, int]]] = {}

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

			if not is_internal:
				continue
			if not bizname:
				continue

			# 접수일 파싱 및 주간 범위 매핑 (없으면 탭 제목 날짜 폴백)
			parsed = _parse_date_maybe(received_raw)
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

			week_idx = None
			for idx, win in enumerate(windows):
				if win["start"] <= dt <= win["end"]:
					week_idx = idx
					break
			if week_idx is None:
				continue

			# 작업 표시명 (특수 탭 '영수증리뷰'는 구분/내부 소통용 우선)
			if _collapse_spaces(tab_title) == _collapse_spaces("영수증리뷰"):
				cat = str(_get_value_flexible(row_norm, "구분", "PRODUCT_NAME_COLUMN") or "").strip()
				memo = str(_get_value_flexible(row_norm, "내부 소통용", "PRODUCT_NAME_COLUMN") or "").strip()
				display_task = (cat if cat else memo) or (product_name if _collapse_spaces(tab_title)==_collapse_spaces("기타") else (f"{tab_title} {product}".strip() if product else tab_title))
			else:
				base_task = tab_title
				is_misc = _collapse_spaces(tab_title) == _collapse_spaces("기타")
				if is_misc:
					display_task = product_name if product_name else base_task
				else:
					display_task = f"{base_task} {product}".strip() if product else base_task

			# 수량 합산
			try:
				wl_num = _parse_int_maybe(workload) or 0
			except Exception:
				wl_num = 0
			key = (agency_raw or "내부 진행", bizname)
			by_week = aggr.setdefault(key, {})
			by_task = by_week.setdefault(week_idx, {})
			by_task[display_task] = int(by_task.get(display_task, 0)) + wl_num

	# 출력 구성
	groups: List[Dict[str, Any]] = []
	for (agency, biz), week_map in sorted(aggr.items(), key=lambda kv: (kv[0][0], kv[0][1])):
		sections: List[Dict[str, Any]] = []
		# 최신 주부터 오래된 주 순으로
		for idx in sorted(week_map.keys()):
			win = windows[idx]
			label = win["label"]
			lines: List[str] = []
			for task, total in sorted(week_map[idx].items(), key=lambda kv: kv[0]):
				unit = _guess_unit_for_task(task)
				val = f"{total}{unit}" if total > 0 else "0건"
				lines.append(f"{task} : {val}")
			sections.append({"label": label, "lines": lines})
		# 최종 텍스트
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

