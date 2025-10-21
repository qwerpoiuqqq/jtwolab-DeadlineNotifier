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


