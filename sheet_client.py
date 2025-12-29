import os
import json
import re
import time
import random
from typing import Dict, List, Set, Any, Tuple, Callable

import gspread
from google.oauth2.service_account import Credentials

# 환경변수 기본 키
DEFAULT_KEYS = {
	"AGENCY_COLUMN": "대행사 명",
	"INTERNAL_COLUMN": "내부 진행건",
	"REMAINING_DAYS_COLUMN": "마감 잔여일",
	"CHECKED_COLUMN": "마감 안내 체크",
	"BIZNAME_COLUMN": "상호명",
	"PRODUCT_COLUMN": "상품",
	"PRODUCT_NAME_COLUMN": "상품 명",
	"DAILY_WORKLOAD_COLUMN": "일작업량",
}

# header 동의어(공백 무시, 소문자 비교)
SYNONYMS: Dict[str, List[str]] = {
	"AGENCY_COLUMN": ["대행사", "대행사명", "광고대행사", "파트너", "파트너사"],
	"BIZNAME_COLUMN": ["상호", "업체명", "매장명", "브랜드명", "점포명", "상호명"],
	"INTERNAL_COLUMN": ["내부", "내부진행", "자체진행", "내부 진행건"],
	"REMAINING_DAYS_COLUMN": ["잔여일", "마감잔여일", "d-day", "dday", "남은일", "남은 일"],
	"CHECKED_COLUMN": ["마감안내", "안내체크", "공지여부", "발송완료", "완료체크", "안내 여부"],
	"PRODUCT_COLUMN": ["상품", "유형", "type", "종류"],
	"PRODUCT_NAME_COLUMN": ["상품 명", "상품명", "작업명", "작업 명"],
	"DAILY_WORKLOAD_COLUMN": ["일 작업량", "일작업량"],
}

TRUTHY_VALUES = {"true", "1", "yes", "y", "o", "ok", "checked", "done", "완료", "예", "y", "yy", "ㅇ", "ㅇㅇ", "o", "O", "✓", "✔"}


class Settings:
	def __init__(self, **kwargs: Any) -> None:
		self.spreadsheet_id: str = kwargs.get("SPREADSHEET_ID", "").strip()
		self.agency_col: str = kwargs.get("AGENCY_COLUMN", DEFAULT_KEYS["AGENCY_COLUMN"]).strip()
		self.internal_col: str = kwargs.get("INTERNAL_COLUMN", DEFAULT_KEYS["INTERNAL_COLUMN"]).strip()
		self.remaining_days_col: str = kwargs.get("REMAINING_DAYS_COLUMN", DEFAULT_KEYS["REMAINING_DAYS_COLUMN"]).strip()
		self.checked_col: str = kwargs.get("CHECKED_COLUMN", DEFAULT_KEYS["CHECKED_COLUMN"]).strip()
		self.bizname_col: str = kwargs.get("BIZNAME_COLUMN", DEFAULT_KEYS["BIZNAME_COLUMN"]).strip()
		self.product_col: str = kwargs.get("PRODUCT_COLUMN", DEFAULT_KEYS["PRODUCT_COLUMN"]).strip()
		self.product_name_col: str = kwargs.get("PRODUCT_NAME_COLUMN", DEFAULT_KEYS["PRODUCT_NAME_COLUMN"]).strip()
		self.daily_workload_col: str = kwargs.get("DAILY_WORKLOAD_COLUMN", DEFAULT_KEYS["DAILY_WORKLOAD_COLUMN"]).strip()

	def to_dict(self) -> Dict[str, str]:
		return {
			"SPREADSHEET_ID": self.spreadsheet_id,
			"AGENCY_COLUMN": self.agency_col,
			"INTERNAL_COLUMN": self.internal_col,
			"REMAINING_DAYS_COLUMN": self.remaining_days_col,
			"CHECKED_COLUMN": self.checked_col,
			"BIZNAME_COLUMN": self.bizname_col,
			"PRODUCT_COLUMN": self.product_col,
			"PRODUCT_NAME_COLUMN": self.product_name_col,
			"DAILY_WORKLOAD_COLUMN": self.daily_workload_col,
		}


def load_settings() -> Settings:
	return Settings(
		SPREADSHEET_ID=os.getenv("SPREADSHEET_ID", ""),
		AGENCY_COLUMN=os.getenv("AGENCY_COLUMN", DEFAULT_KEYS["AGENCY_COLUMN"]),
		INTERNAL_COLUMN=os.getenv("INTERNAL_COLUMN", DEFAULT_KEYS["INTERNAL_COLUMN"]),
		REMAINING_DAYS_COLUMN=os.getenv("REMAINING_DAYS_COLUMN", DEFAULT_KEYS["REMAINING_DAYS_COLUMN"]),
		CHECKED_COLUMN=os.getenv("CHECKED_COLUMN", DEFAULT_KEYS["CHECKED_COLUMN"]),
		BIZNAME_COLUMN=os.getenv("BIZNAME_COLUMN", DEFAULT_KEYS["BIZNAME_COLUMN"]),
		PRODUCT_COLUMN=os.getenv("PRODUCT_COLUMN", DEFAULT_KEYS["PRODUCT_COLUMN"]),
		PRODUCT_NAME_COLUMN=os.getenv("PRODUCT_NAME_COLUMN", DEFAULT_KEYS["PRODUCT_NAME_COLUMN"]),
		DAILY_WORKLOAD_COLUMN=os.getenv("DAILY_WORKLOAD_COLUMN", DEFAULT_KEYS["DAILY_WORKLOAD_COLUMN"]),
	)


def _build_credentials() -> Credentials:
	service_account_json_inline = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
	keyfile_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

	scopes = [
		"https://www.googleapis.com/auth/spreadsheets",
		"https://www.googleapis.com/auth/drive.readonly",
	]

	if service_account_json_inline:
		try:
			data = json.loads(service_account_json_inline)
		except json.JSONDecodeError as e:
			raise RuntimeError("SERVICE_ACCOUNT_JSON 환경변수가 유효한 JSON이 아닙니다.") from e
		return Credentials.from_service_account_info(data, scopes=scopes)

	if keyfile_path:
		if not os.path.exists(keyfile_path):
			raise FileNotFoundError(f"서비스 계정 키 파일을 찾을 수 없습니다: {keyfile_path}")
		return Credentials.from_service_account_file(keyfile_path, scopes=scopes)

	raise RuntimeError("서비스 계정 인증정보가 없습니다. SERVICE_ACCOUNT_JSON 또는 GOOGLE_APPLICATION_CREDENTIALS를 설정하세요.")


def _get_client() -> gspread.Client:
	creds = _build_credentials()
	return gspread.authorize(creds)


# -----------------------
# 읽기 요청 최소화를 위한 간단 캐시
# 환경변수 READ_CACHE_TTL_SECS (기본 120초)
# -----------------------
_WS_CACHE: Dict[int, Dict[str, Any]] = {}

def _now() -> float:
	return time.time()

def _get_cache_ttl_secs() -> int:
	try:
		return int(os.getenv("READ_CACHE_TTL_SECS", "120").strip())
	except Exception:
		return 120

def _with_retry(func: Callable, *args: Any, **kwargs: Any) -> Any:
	"""지수 백오프 재시도 (429/5xx 완화). 환경변수로 조정 가능.

	- RETRY_MAX_ATTEMPTS (기본 6)
	- RETRY_BASE_DELAY (초, 기본 0.8)
	"""
	try:
		max_attempts = int(os.getenv("RETRY_MAX_ATTEMPTS", "6").strip())
	except Exception:
		max_attempts = 6
	try:
		base = float(os.getenv("RETRY_BASE_DELAY", "0.8").strip())
	except Exception:
		base = 0.8
	for attempt in range(max_attempts):
		try:
			return func(*args, **kwargs)
		except Exception:
			if attempt == max_attempts - 1:
				raise
			delay = base * (2 ** attempt) + random.uniform(0, 0.4)
			time.sleep(delay)

def _get_all_values_full_cached(ws: gspread.Worksheet) -> List[List[str]]:
	"""워크시트 전체 값을 읽는다. 캐시를 우선 사용하고, 필요 시 배치 스캔으로 폴백.

	반환 형태는 get_all_values와 동일.
	"""
	try:
		ws_id = int(getattr(ws, 'id', 0) or 0)
	except Exception:
		ws_id = 0
	entry = _WS_CACHE.get(ws_id)
	if entry and (_now() - entry.get('ts', 0)) <= _get_cache_ttl_secs():
		values = entry.get('values')
		if isinstance(values, list):
			return values

	# 1) 단일 호출 우선
	try:
		values = _with_retry(ws.get_all_values)
		_WS_CACHE[ws_id] = {"values": values, "ts": _now()}
		return values
	except Exception:
		pass

	# 2) 폴백: row_count 기반 청크 스캔
	try:
		total = int(getattr(ws, "row_count", 0) or 0)
	except Exception:
		total = 0
	if total <= 0:
		_WS_CACHE[ws_id] = {"values": [], "ts": _now()}
		return []

	all_values: List[List[str]] = []
	chunk = 5000
	try:
		chunk_sleep = float(os.getenv("CHUNK_SLEEP_SECS", "0.25").strip())
	except Exception:
		chunk_sleep = 0.25
	last_non_empty_row = 0
	for start in range(1, total + 1, chunk):
		end = min(total, start + chunk - 1)
		try:
			part = _with_retry(ws.get_values, f"{start}:{end}")
		except Exception:
			part = []
		if part:
			all_values.extend(part)
			for i, r in enumerate(part, start=start):
				if any(str(c).strip() != "" for c in r):
					last_non_empty_row = i
		else:
			if start > max(1, last_non_empty_row) + chunk:
				break
		# 청크 간 대기 (RPM 완화)
		time.sleep(chunk_sleep)

	_WS_CACHE[ws_id] = {"values": all_values, "ts": _now()}
	return all_values


def _normalize_key(key: str) -> str:
	return (key or "").strip()


def _collapse_spaces(s: str) -> str:
	return re.sub(r"\s+", "", s or "").strip().lower()


def _parse_int_maybe(value: Any) -> int | None:
	if value is None:
		return None
	s = str(value).strip()
	if s == "":
		return None
	m = re.search(r"-?\d+", s)
	if not m:
		return None
	try:
		return int(m.group(0))
	except ValueError:
		return None


def _is_truthy(value: Any) -> bool:
	if value is None:
		return False
	s = str(value).strip().lower()
	return s in TRUTHY_VALUES


def _matches(header: str, preferred_key: str, key_id: str) -> bool:
	h = _collapse_spaces(header)
	p = _collapse_spaces(preferred_key)
	if h == p:
		return True
	for syn in SYNONYMS.get(key_id, []):
		if h == _collapse_spaces(syn):
			return True
	return False


def _get_value_flexible(row: Dict[str, Any], preferred_key: str, key_id: str) -> Any:
	# 직접 키
	if preferred_key in row:
		return row.get(preferred_key)
	# 공백/소문자 동치
	pref_norm = _collapse_spaces(preferred_key)
	for k in row.keys():
		if _collapse_spaces(k) == pref_norm:
			return row.get(k)
	# 동의어
	for syn in SYNONYMS.get(key_id, []):
		syn_norm = _collapse_spaces(syn)
		for k in row.keys():
			if _collapse_spaces(k) == syn_norm:
				return row.get(k)
	return None


def _find_header_row(ws: gspread.Worksheet, settings: Settings) -> Tuple[int, List[str]]:
	required_map = {
		"AGENCY_COLUMN": settings.agency_col,
		"INTERNAL_COLUMN": settings.internal_col,
		"REMAINING_DAYS_COLUMN": settings.remaining_days_col,
		"CHECKED_COLUMN": settings.checked_col,
		"BIZNAME_COLUMN": settings.bizname_col,
		"PRODUCT_COLUMN": settings.product_col,
		"PRODUCT_NAME_COLUMN": settings.product_name_col,
		"DAILY_WORKLOAD_COLUMN": settings.daily_workload_col,
	}
	try:
		candidates = _with_retry(ws.get_values, '1:100')  # 상단 100행 탐색 (재시도 적용)
	except Exception:
		candidates = []

	def score_headers(headers: List[str]) -> tuple[int, int]:
		has_remaining = any(_matches(h, settings.remaining_days_col, "REMAINING_DAYS_COLUMN") for h in headers)
		has_bizname = any(_matches(h, settings.bizname_col, "BIZNAME_COLUMN") for h in headers)
		essentials = int(has_remaining) + int(has_bizname)
		total = 0
		for header in headers:
			for key_id, pref in required_map.items():
				if _matches(header, pref, key_id):
					total += 1
					break
		return essentials, total

	best_idx = 1
	best_headers: List[str] = []
	best_tuple = (-1, -1)
	for idx, row in enumerate(candidates, start=1):
		headers = [str(h).strip() for h in row]
		s = score_headers(headers)
		if s > best_tuple:
			best_tuple = s
			best_idx = idx
			best_headers = headers
	if best_tuple == (-1, -1):
		try:
			best_headers = [h.strip() for h in _with_retry(ws.row_values, 1)]
		except Exception:
			best_headers = []
		best_idx = 1
	return best_idx, best_headers



def _build_records(ws: gspread.Worksheet, header_row: int, headers: List[str]) -> List[Dict[str, Any]]:
	try:
		values = _get_all_values_full_cached(ws)
	except Exception:
		return []
	if header_row - 1 >= len(values):
		return []
	data_rows = values[header_row:]
	records: List[Dict[str, Any]] = []
	for row in data_rows:
		if all((str(c).strip() == "" for c in row)):
			continue
		row_dict: Dict[str, Any] = {}
		for i, h in enumerate(headers):
			key = _normalize_key(h)
			val = row[i] if i < len(row) else ""
			row_dict[key] = val
		records.append(row_dict)
	return records


def fetch_grouped_messages(selected_days: List[int], settings: Settings | None = None) -> Dict[str, Dict[str, List[str]]]:
	if settings is None:
		settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	agency_to_task_to_names: Dict[str, Dict[str, List[str]]] = {}
	# 중복 상호명 병합을 위한 임시 집계: agency -> task -> bizname -> sum(workload)
	aggregator: Dict[str, Dict[str, Dict[str, int]]] = {}
	selected_set: Set[int] = set(selected_days)

	for ws in ss.worksheets():
		task_name = ws.title
		header_row, headers = _find_header_row(ws, settings)
		records = _build_records(ws, header_row, headers)
		for row in records:
			row_norm = { _normalize_key(k): v for k, v in row.items() }
			agency = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip() or "미지정 대행사"
			is_checked = _is_truthy(_get_value_flexible(row_norm, settings.checked_col, "CHECKED_COLUMN"))
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()
			workload_raw = str(_get_value_flexible(row_norm, settings.daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
			workload_num = _parse_int_maybe(workload_raw) or 0

			if is_checked:
				continue
			if is_internal:
				continue
			if remain is None or remain not in selected_set:
				continue
			if not bizname:
				continue

			by_task = aggregator.setdefault(agency, {}).setdefault(task_name, {})
			by_task[bizname] = by_task.get(bizname, 0) + workload_num
	# 집계 결과를 출력 포맷으로 변환
	for agency, tasks in aggregator.items():
		out_task_map: Dict[str, List[str]] = {}
		for task, name_to_wl in tasks.items():
			names: List[str] = []
			for name, wl_sum in sorted(name_to_wl.items(), key=lambda kv: kv[0]):
				names.append(f"{name} (일작업량 {wl_sum})" if wl_sum > 0 else name)
			out_task_map[task] = names
		agency_to_task_to_names[agency] = out_task_map

	return agency_to_task_to_names


def fetch_grouped_messages_by_date(selected_days: List[int], settings: Settings | None = None, filter_mode: str = "agency") -> Dict[str, Dict[int, Dict[str, List[str]]]]:
	"""대행사 -> 남은일수 -> 작업명(상품 포함 규칙) -> [상호명]
	- '기타' 탭: '상품 명' 열 값을 작업명으로 사용
	- 그 외 탭: 작업명(=탭명) 뒤에 '상품' 열 값이 있으면 공백으로 이어붙여 표시
	filter_mode: 'agency' | 'internal'
	"""
	if settings is None:
		settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	selected_set: Set[int] = set(selected_days)
	# 임시 집계: agency -> day -> task -> bizname -> sum(workload)
	aggregator: Dict[str, Dict[int, Dict[str, Dict[str, int]]]] = {}

	for ws in ss.worksheets():
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

			# 필터 모드 적용
			if filter_mode == "agency":
				if is_internal:
					continue
			elif filter_mode == "internal":
				if not is_internal:
					continue
			else:
				if is_internal:
					continue

			# 공통 필터
			if is_checked or remain is None or remain not in selected_set or not bizname:
				continue

			# 작업명 생성 규칙
			base_task = tab_title
			is_misc = _collapse_spaces(tab_title) == _collapse_spaces("기타")
			if is_misc:
				display_task = product_name if product_name else base_task
			else:
				display_task = f"{base_task} {product}".strip() if product else base_task

			agency_label = agency_raw if filter_mode == "agency" else (agency_raw or "내부 진행")
			# 집계 업데이트
			try:
				wl_num = _parse_int_maybe(workload) or 0
			except Exception:
				wl_num = 0
			dict_by_day = aggregator.setdefault(agency_label, {})
			dict_by_task = dict_by_day.setdefault(remain, {})
			by_name = dict_by_task.setdefault(display_task, {})
			by_name[bizname] = by_name.get(bizname, 0) + wl_num

	# 집계를 최종 출력 포맷으로 변환: List[str]
	result: Dict[str, Dict[int, Dict[str, List[str]]]] = {}
	for agency, by_day in aggregator.items():
		day_out: Dict[int, Dict[str, List[str]]] = {}
		for day_key, by_task in by_day.items():
			task_out: Dict[str, List[str]] = {}
			for task, name_to_wl in by_task.items():
				names: List[str] = []
				for name, wl_sum in sorted(name_to_wl.items(), key=lambda kv: kv[0]):
					names.append(f"{name} (일작업량 {wl_sum})" if wl_sum > 0 else name)
				task_out[task] = names
			day_out[day_key] = task_out
		result[agency] = day_out

	return result


def stream_grouped_messages_by_date(selected_days: List[int], settings: Settings | None = None, filter_mode: str = "agency"):
	"""워크시트 단위로 진행률을 스트리밍하기 위한 제너레이터.
	각 워크시트 처리 후 진행 이벤트를 yield 하고, 마지막에 최종 결과를 yield 한다.
	이 함수는 SSE 라우트에서 사용된다.
	"""
	if settings is None:
		settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)
	worksheets = ss.worksheets()
	total = len(worksheets)
	processed = 0

	selected_set: Set[int] = set(selected_days)
	# 임시 집계 저장
	aggregator: Dict[str, Dict[int, Dict[str, Dict[str, int]]]] = {}

	# 시작 이벤트
	yield {"type": "start", "total": total}

	for ws in worksheets:
		tab_title = (ws.title or "").strip()
		try:
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

				# 필터 모드 적용
				if filter_mode == "agency":
					if is_internal:
						continue
				elif filter_mode == "internal":
					if not is_internal:
						continue
				else:
					if is_internal:
						continue

				# 공통 필터
				if is_checked or remain is None or remain not in selected_set or not bizname:
					continue

				# 작업명 생성 규칙
				base_task = tab_title
				is_misc = _collapse_spaces(tab_title) == _collapse_spaces("기타")
				if is_misc:
					display_task = product_name if product_name else base_task
				else:
					display_task = f"{base_task} {product}".strip() if product else base_task

				agency_label = agency_raw if filter_mode == "agency" else (agency_raw or "내부 진행")
				dict_by_day = aggregator.setdefault(agency_label, {})
				dict_by_task = dict_by_day.setdefault(remain, {})
				by_name = dict_by_task.setdefault(display_task, {})
				wl_num = _parse_int_maybe(workload) or 0
				by_name[bizname] = by_name.get(bizname, 0) + wl_num
		except Exception as e:
			# 워크시트 처리 실패도 진행률로 보고
			yield {"type": "progress", "processed": processed, "total": total, "tab": tab_title, "error": str(e)}
		else:
			processed += 1
			yield {"type": "progress", "processed": processed, "total": total, "tab": tab_title}

	# 완료 시 집계를 최종 포맷으로 변환
	final_map: Dict[str, Dict[int, Dict[str, List[str]]]] = {}
	for agency, by_day in aggregator.items():
		day_out: Dict[int, Dict[str, List[str]]] = {}
		for day_key, by_task in by_day.items():
			task_out: Dict[str, List[str]] = {}
			for task, name_to_wl in by_task.items():
				names: List[str] = []
				for name, wl_sum in sorted(name_to_wl.items(), key=lambda kv: kv[0]):
					names.append(f"{name} (일작업량 {wl_sum})" if wl_sum > 0 else name)
				task_out[task] = names
			day_out[day_key] = task_out
		final_map[agency] = day_out

	# 완료 이벤트
	yield {"type": "result", "total": total, "grouped": final_map}


def _find_checked_col_index(headers: List[str], settings: Settings) -> int | None:
	"""헤더들에서 체크 컬럼의 1-based 인덱스를 찾는다."""
	for idx, h in enumerate(headers, start=1):
		if _matches(h, settings.checked_col, "CHECKED_COLUMN"):
			return idx
	return None


def mark_checked_for_agency(selected_days: List[int], agency_label: str, filter_mode: str = "agency", settings: Settings | None = None) -> Dict[str, Any]:
	"""선택한 일수/보기 모드에서 특정 카드(agency_label)에 포함되는 모든 행의
	'마감 안내 체크' 값을 TRUE로 업데이트한다.

	반환: { updated: int, details: [{worksheet: str, updated: int}] }
	"""
	if settings is None:
		settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	selected_set: Set[int] = set(selected_days)
	results: List[Dict[str, Any]] = []
	total_updated = 0

	for ws in ss.worksheets():
		# 헤더 및 컬럼 인덱스 파악
		header_row, headers = _find_header_row(ws, settings)
		checked_col = _find_checked_col_index(headers, settings)
		if checked_col is None:
			results.append({"worksheet": ws.title, "updated": 0, "reason": "no_checked_col"})
			continue

		# 전체 값 읽고 레코드 + 실제 행번호 생성
		try:
			values = _get_all_values_full_cached(ws)
		except Exception as e:
			results.append({"worksheet": ws.title, "updated": 0, "reason": f"read_failed:{e}"})
			continue
		if header_row - 1 >= len(values):
			results.append({"worksheet": ws.title, "updated": 0, "reason": "no_data"})
			continue
		data_rows = values[header_row:]

		update_targets: List[int] = []  # 실제 시트 행 번호(1-based)
		for idx, row in enumerate(data_rows):
			# dict 구성
			row_dict: Dict[str, Any] = {}
			for i, h in enumerate(headers):
				key = _normalize_key(h)
				val = row[i] if i < len(row) else ""
				row_dict[key] = val
			row_norm = { _normalize_key(k): v for k, v in row_dict.items() }

			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			is_checked = _is_truthy(_get_value_flexible(row_norm, settings.checked_col, "CHECKED_COLUMN"))
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()

			# 필터 모드 적용 (리스트 뷰와 동일 규칙)
			if filter_mode == "agency":
				if is_internal:
					continue
			elif filter_mode == "internal":
				if not is_internal:
					continue
			else:
				if is_internal:
					continue

			# 공통 필터 (리스트 뷰와 동일)
			if is_checked or remain is None or remain not in selected_set or not bizname:
				continue

			# 에이전시 라벨 계산 (뷰와 동일)
			computed_label = agency_raw if filter_mode == "agency" else (agency_raw or "내부 진행")
			if computed_label != agency_label:
				continue

			# 실제 시트 상의 행 번호 계산: header_row는 헤더가 있는 1-based 라인
			# data_rows는 header_row 바로 다음 줄부터 시작이므로 + (header_row + idx + 1)
			real_row_num = header_row + idx + 1
			update_targets.append(real_row_num)

		# 업데이트 수행 (개별 업데이트: 신뢰성 우선)
		updated_here = 0
		for r in update_targets:
			try:
				ws.update_cell(r, checked_col, "TRUE")
			except Exception:
				continue
			else:
				updated_here += 1

		results.append({"worksheet": ws.title, "updated": updated_here})
		total_updated += updated_here

	return {"updated": total_updated, "details": results}


def mark_checked_for_agencies(selected_days: List[int], agency_labels: List[str], filter_mode: str = "agency", settings: Settings | None = None) -> Dict[str, Any]:
	"""선택한 일수/보기 모드에서 여러 카드(agency_labels)에 포함되는 모든 행의
	'마감 안내 체크' 값을 TRUE로 일괄 업데이트한다.

	반환: { updated: int, details: [{worksheet: str, updated: int}], per_agency: {label: int} }
	"""
	if settings is None:
		settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	# 타겟 라벨 집합 정리
	target_labels: Set[str] = set([str(a or "").strip() for a in agency_labels if str(a or "").strip()])
	if not target_labels:
		return {"updated": 0, "details": [], "per_agency": {}}

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	selected_set: Set[int] = set(selected_days)
	results: List[Dict[str, Any]] = []
	total_updated = 0
	per_agency: Dict[str, int] = {label: 0 for label in target_labels}

	for ws in ss.worksheets():
		# 헤더 및 컬럼 인덱스 파악
		header_row, headers = _find_header_row(ws, settings)
		checked_col = _find_checked_col_index(headers, settings)
		if checked_col is None:
			results.append({"worksheet": ws.title, "updated": 0, "reason": "no_checked_col"})
			continue

		# 전체 값 읽고 레코드 + 실제 행번호 생성 (캐시 활용)
		try:
			values = _get_all_values_full_cached(ws)
		except Exception as e:
			results.append({"worksheet": ws.title, "updated": 0, "reason": f"read_failed:{e}"})
			continue
		if header_row - 1 >= len(values):
			results.append({"worksheet": ws.title, "updated": 0, "reason": "no_data"})
			continue
		data_rows = values[header_row:]

		# 업데이트 대상 수집
		update_targets: List[int] = []  # 실제 시트 행 번호(1-based)
		labels_for_row: List[str] = []  # 행별 라벨(통계용)
		for idx, row in enumerate(data_rows):
			row_dict: Dict[str, Any] = {}
			for i, h in enumerate(headers):
				key = _normalize_key(h)
				val = row[i] if i < len(row) else ""
				row_dict[key] = val
			row_norm = { _normalize_key(k): v for k, v in row_dict.items() }

			agency_raw = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			is_checked = _is_truthy(_get_value_flexible(row_norm, settings.checked_col, "CHECKED_COLUMN"))
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			remain = _parse_int_maybe(_get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"))
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()

			# 필터 모드 적용
			if filter_mode == "agency":
				if is_internal:
					continue
			elif filter_mode == "internal":
				if not is_internal:
					continue
			else:
				if is_internal:
					continue

			# 공통 필터
			if is_checked or remain is None or remain not in selected_set or not bizname:
				continue

			computed_label = agency_raw if filter_mode == "agency" else (agency_raw or "내부 진행")
			if computed_label not in target_labels:
				continue

			real_row_num = header_row + idx + 1
			update_targets.append(real_row_num)
			labels_for_row.append(computed_label)

		# 업데이트 수행 (개별 업데이트: 신뢰성 우선)
		updated_here = 0
		for r, label in zip(update_targets, labels_for_row):
			try:
				ws.update_cell(r, checked_col, "TRUE")
			except Exception:
				continue
			else:
				updated_here += 1
				per_agency[label] = per_agency.get(label, 0) + 1

		results.append({"worksheet": ws.title, "updated": updated_here})
		total_updated += updated_here

	return {"updated": total_updated, "details": results, "per_agency": per_agency}

def inspect_sheets(settings: Settings | None = None) -> List[Dict[str, Any]]:
	if settings is None:
		settings = load_settings()
	if not settings.spreadsheet_id:
		raise RuntimeError("SPREADSHEET_ID 환경변수를 설정하세요.")

	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	results: List[Dict[str, Any]] = []
	for ws in ss.worksheets():
		header_row, headers = _find_header_row(ws, settings)
		results.append({
			"title": ws.title,
			"header_row": header_row,
			"headers": headers,
			"has_agency": any(_matches(h, settings.agency_col, "AGENCY_COLUMN") for h in headers),
			"has_internal": any(_matches(h, settings.internal_col, "INTERNAL_COLUMN") for h in headers),
			"has_remaining": any(_matches(h, settings.remaining_days_col, "REMAINING_DAYS_COLUMN") for h in headers),
			"has_checked": any(_matches(h, settings.checked_col, "CHECKED_COLUMN") for h in headers),
			"has_bizname": any(_matches(h, settings.bizname_col, "BIZNAME_COLUMN") for h in headers),
		})
	return results


def diagnose_matches(selected_days: List[int], settings: Settings | None = None, limit: int = 50) -> Dict[str, Any]:
	"""탭별 매칭된 항목과 제외 사유 샘플, 사유별 카운트를 반환한다."""
	if settings is None:
		settings = load_settings()
	client = _get_client()
	ss = client.open_by_key(settings.spreadsheet_id)

	selected_set: Set[int] = set(selected_days)
	report: Dict[str, Any] = {}
	for ws in ss.worksheets():
		task_name = ws.title
		header_row, headers = _find_header_row(ws, settings)
		records = _build_records(ws, header_row, headers)
		matched: List[Dict[str, Any]] = []
		excluded: List[Dict[str, Any]] = []
		reason_counts: Dict[str, int] = {}
		for row in records:
			row_norm = { _normalize_key(k): v for k, v in row.items() }
			agency = str(_get_value_flexible(row_norm, settings.agency_col, "AGENCY_COLUMN") or "").strip()
			is_checked = _is_truthy(_get_value_flexible(row_norm, settings.checked_col, "CHECKED_COLUMN"))
			is_internal = _is_truthy(_get_value_flexible(row_norm, settings.internal_col, "INTERNAL_COLUMN"))
			remain_val_raw = _get_value_flexible(row_norm, settings.remaining_days_col, "REMAINING_DAYS_COLUMN")
			remain = _parse_int_maybe(remain_val_raw)
			bizname = str(_get_value_flexible(row_norm, settings.bizname_col, "BIZNAME_COLUMN") or "").strip()

			reason = None
			if is_checked:
				reason = "checked"
			elif is_internal:
				reason = "internal"
			elif remain is None:
				reason = f"remain_parse_failed:{remain_val_raw}"
			elif remain not in selected_set:
				reason = f"remain_not_selected:{remain}"
			elif not bizname:
				reason = "no_bizname"

			if reason is None:
				matched.append({"agency": agency or "", "bizname": bizname, "remain": remain})
			else:
				excluded.append({"agency": agency or "", "bizname": bizname, "remain_raw": remain_val_raw, "reason": reason})
				reason_counts[reason] = reason_counts.get(reason, 0) + 1

		report[task_name] = {
			"header_row": header_row,
			"matched_count": len(matched),
			"matched_sample": matched[:limit],
			"excluded_sample": excluded[:limit],
			"excluded_reason_counts": reason_counts,
		}
	return report


def list_sheet_tabs(spreadsheet_id: str | None = None) -> List[str]:
	"""스프레드시트의 워크시트 탭 제목 목록을 반환한다.
	spreadsheet_id를 명시하면 그것을 사용하고, 없으면 기본 설정의 SPREADSHEET_ID를 사용한다.
	"""
	if not spreadsheet_id:
		settings = load_settings()
		spreadsheet_id = settings.spreadsheet_id
	if not spreadsheet_id:
		raise RuntimeError("스프레드시트 ID가 비어 있습니다.")
	client = _get_client()
	ss = client.open_by_key(spreadsheet_id)
	return [str((ws.title or "").strip()) for ws in ss.worksheets()]


def inspect_sheets_by_id(spreadsheet_id: str) -> List[Dict[str, Any]]:
	"""지정된 스프레드시트 ID에 대해 탭별 헤더 정보를 반환한다."""
	if not spreadsheet_id:
		raise RuntimeError("스프레드시트 ID가 비어 있습니다.")
	client = _get_client()
	ss = client.open_by_key(spreadsheet_id)
	settings = load_settings()
	results: List[Dict[str, Any]] = []
	for ws in ss.worksheets():
		header_row, headers = _find_header_row(ws, settings)
		results.append({
			"title": ws.title,
			"header_row": header_row,
			"headers": headers,
		})
	return results


def _find_header_row_simple(values: List[List[str]]) -> int:
	"""상단 20행 내에서 '상호명/상품명/저장/트래픽' 중 하나 이상이 있는 첫 행을 헤더로 간주한다.
	반환: 1-based 행 번호(없으면 1)
	"""
	max_scan = min(len(values), 20)
	keys = {"상호명", "상품명", "저장", "트래픽"}
	for idx in range(max_scan):
		row = [str(c or "").strip() for c in values[idx]]
		row_set = set(row)
		if any(k in row_set for k in keys):
			return idx + 1
	return 1



def _authed_session():
	"""Google API Authorized session 생성"""
	from google.auth.transport.requests import AuthorizedSession
	creds = _build_credentials()
	return AuthorizedSession(creds)


def _fetch_background_colors(spreadsheet_id: str, sheet_title: str, max_rows: int) -> Dict[Tuple[int, int], Tuple[float, float, float]]:
	"""지정 탭의 A1:Z{max_rows} 배경색을 조회한다.
	반환: {(row0,col0): (r,g,b)} 0-based
	"""
	sess = _authed_session()
	url = f"https://sheets.googleapis.com/v4/spreadsheets/{spreadsheet_id}"
	params = {
		"includeGridData": "true",
		"ranges": f"{sheet_title}!A1:Z{max_rows}",
		"fields": "sheets(data(rowData(values(effectiveFormat(backgroundColor)))))",
	}
	try:
		resp = sess.get(url, params=params)
		resp.raise_for_status()
		data = resp.json()
	except Exception:
		return {}
	colors: Dict[Tuple[int, int], Tuple[float, float, float]] = {}
	try:
		row_data = data.get("sheets", [{}])[0].get("data", [{}])[0].get("rowData", [])
		for r_idx, row in enumerate(row_data):
			cells = row.get("values", [])
			for c_idx, cell in enumerate(cells):
				bg = cell.get("effectiveFormat", {}).get("backgroundColor", {})
				r, g, b = bg.get("red", 0.0), bg.get("green", 0.0), bg.get("blue", 0.0)
				colors[(r_idx, c_idx)] = (float(r or 0.0), float(g or 0.0), float(b or 0.0))
	except Exception:
		return {}
	return colors


def _is_yellow(rgb: Tuple[float, float, float]) -> bool:
    if not rgb or len(rgb) != 3:
        return False
    r, g, b = rgb
    return (abs(r - 1.0) <= 0.05 and abs(g - 1.0) <= 0.05 and abs(b - 0.0) <= 0.05)


def _is_manage_green(rgb: Tuple[float, float, float]) -> bool:
    """연녹색(관리형) 감지: #d9ead3 (R=217,G=234,B=211) ≈ (0.851,0.918,0.827)

    구글시트 API는 0~1 float을 반환하므로 근사치 범위로 판정한다.
    """
    if not rgb or len(rgb) != 3:
        return False
    r, g, b = rgb
    target = (217.0/255.0, 234.0/255.0, 211.0/255.0)
    tol = 0.06  # 허용 오차
    return (abs(r - target[0]) <= tol and abs(g - target[1]) <= tol and abs(b - target[2]) <= tol)


def compute_settlement_rows(spreadsheet_id: str, selected_tabs: List[str], pricebook: List[Dict[str, Any]]) -> Dict[str, Any]:
	"""결재선 시트에서 선택 탭의 행을 읽어 정산 행 + 집계를 생성한다.

	- 단가 레코드 예: { client, product, price, account, type? }  // type 미지정=공통
	- 자사건: 배경 노란색(#ffff00) → 지출만 산출
	- 대행사건: 지출 + 매출(금액[vAT제외]) 산출
	"""
	if not spreadsheet_id:
		raise RuntimeError("스프레드시트 ID가 비어 있습니다.")
	client = _get_client()
	ss = client.open_by_key(spreadsheet_id)
	wanted = set([str(t).strip() for t in (selected_tabs or []) if str(t).strip()])

	# 단가 조회: (client, product, type) → (client, product, 공통) → (product, type) → (product, 공통)
	def find_unit_price(client_name: str, product_name: str, qty_type: str) -> float:
		client_name = (client_name or "").strip()
		product_name = (product_name or "").strip()
		qty_type = (qty_type or "").strip()
		for it in pricebook:
			it_type = str(it.get("type") or "공통").strip()
			if str(it.get("client") or "").strip() == client_name and str(it.get("product") or "").strip() == product_name and it_type == qty_type:
				return float(it.get("price") or 0)
		for it in pricebook:
			it_type = str(it.get("type") or "공통").strip()
			if str(it.get("client") or "").strip() == client_name and str(it.get("product") or "").strip() == product_name and it_type == "공통":
				return float(it.get("price") or 0)
		for it in pricebook:
			it_type = str(it.get("type") or "공통").strip()
			if str(it.get("product") or "").strip() == product_name and it_type == qty_type:
				return float(it.get("price") or 0)
		for it in pricebook:
			it_type = str(it.get("type") or "공통").strip()
			if str(it.get("product") or "").strip() == product_name and it_type == "공통":
				return float(it.get("price") or 0)
		return 0.0

	rows_out: List[Dict[str, Any]] = []
	missing: Dict[Tuple[str, str, str], int] = {}
	by_client_expense: Dict[str, float] = {}
	by_client_income: Dict[str, float] = {}
	unpaid_by_agency: Dict[str, float] = {}
	grand_expense = 0.0
	grand_income = 0.0
	by_product: Dict[str, Dict[str, Dict[str, float]]] = {}
	by_agency: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

	for ws in ss.worksheets():
		tab = (ws.title or "").strip()
		if wanted and tab not in wanted:
			continue
		# 전체 값
		values = _get_all_values_full_cached(ws)
		if not values:
			continue
		header_row = _find_header_row_simple(values)
		headers = [str(c or "").strip() for c in (values[header_row-1] if header_row-1 < len(values) else [])]
		# 배경색 조회
		bg = _fetch_background_colors(spreadsheet_id, tab, max_rows=len(values))
		# 컬럼 인덱스 (유연한 매칭)
		def col_idx(target: str) -> int | None:
			target_norm = _collapse_spaces(target)
			for i, h in enumerate(headers):
				if _collapse_spaces(h) == target_norm:
					return i
			return None

		ci_agency = col_idx("상호명")
		ci_job = col_idx("상품명")
		ci_store = col_idx("저장")
		ci_traf = col_idx("트래픽")
		ci_store_actual = col_idx("저장 감은타수")
		ci_traf_actual = col_idx("트래픽 감은타수")
		
		# 금액 컬럼 찾기 (여러 후보)
		ci_amount = None
		for cand in ["금액(vat제외)", "금액(VAT제외)", "금액(vat별도)", "금액", "매출"]:
			ci = col_idx(cand)
			if ci is not None:
				ci_amount = ci
				break
		
		# 입금확인 컬럼 찾기 (여러 후보)
		ci_paid = None
		for cand in ["입금확인", "입금 확인", "입금여부", "입금 여부", "입금"]:
			ci = col_idx(cand)
			if ci is not None:
				ci_paid = ci
				break

		for row_idx, r in enumerate(values[header_row:]):
			agency = str((r[ci_agency] if (ci_agency is not None and ci_agency < len(r)) else "").strip()) if ci_agency is not None else ""
			job = str((r[ci_job] if (ci_job is not None and ci_job < len(r)) else "").strip()) if ci_job is not None else ""
			store_s = str((r[ci_store] if (ci_store is not None and ci_store < len(r)) else "").strip()) if ci_store is not None else ""
			traf_s = str((r[ci_traf] if (ci_traf is not None and ci_traf < len(r)) else "").strip()) if ci_traf is not None else ""
			store_actual_s = str((r[ci_store_actual] if (ci_store_actual is not None and ci_store_actual < len(r)) else "").strip()) if ci_store_actual is not None else ""
			traf_actual_s = str((r[ci_traf_actual] if (ci_traf_actual is not None and ci_traf_actual < len(r)) else "").strip()) if ci_traf_actual is not None else ""
			amount_note_s = str((r[ci_amount] if (ci_amount is not None and ci_amount < len(r)) else "").strip()) if ci_amount is not None else ""
			paid_s = str((r[ci_paid] if (ci_paid is not None and ci_paid < len(r)) else "").strip()) if ci_paid is not None else ""

			if not agency:
				continue
			# 수량 파싱
			def to_int(s: str) -> int:
				s_clean = (s or "").replace(",", "").strip()
				m = re.search(r"-?\d+", s_clean)
				return int(m.group(0)) if m else 0
			def to_float(s: str) -> float:
				s_clean = (s or "").replace(",", "").strip()
				m = re.search(r"-?[\d.]+", s_clean)
				return float(m.group(0)) if m else 0.0
			qty_store_raw = to_int(store_s)
			qty_traf_raw = to_int(traf_s)
			qty_store_act = to_int(store_actual_s)
			qty_traf_act = to_int(traf_actual_s)
			# 감은타수가 존재(>0)하면 그것을 우선 사용
			qty_store = qty_store_act if qty_store_act > 0 else qty_store_raw
			qty_traf = qty_traf_act if qty_traf_act > 0 else qty_traf_raw
			income_noted = to_float(amount_note_s)  # 대행사건에만 매출 반영
			if qty_store == 0 and qty_traf == 0:
				continue

			# 입금 여부 확인
			is_paid = _is_truthy(paid_s)

			# 자사건/관리형 판정: 상호명 셀 배경 노란색(자사) 또는 연녹색(관리형) → 매출 0 처리
			is_internal = False
			internal_type = None  # 'guarantee'(노란색) or 'manage'(연녹색)
			if ci_agency is not None:
				rgb = bg.get((header_row + row_idx, ci_agency))
				if _is_yellow(rgb):
					is_internal = True
					internal_type = "guarantee"
				elif _is_manage_green(rgb):
					is_internal = True
					internal_type = "manage"

			# 미수금 집계: 'O'가 없는 행의 '금액(vat제외)' 값을 상호명별로 합산 (자사 보장건 제외)
			is_paid = _is_truthy(paid_s)
			if not is_internal and income_noted > 0 and not is_paid:
				unpaid_by_agency[agency] = unpaid_by_agency.get(agency, 0.0) + income_noted

			# 저장 행
			if qty_store:
				unit = find_unit_price(agency, job, "저장")
				expense = float(qty_store) * unit
				income = 0.0 if is_internal else income_noted
				rows_out.append({"date": tab, "client": agency, "job": job, "type": "저장", "qty": qty_store, "unit_price": unit, "expense": expense, "income": income, "is_internal": is_internal, "internal_type": internal_type})
				by_client_expense[agency] = by_client_expense.get(agency, 0.0) + expense
				if not is_internal:
					by_client_income[agency] = by_client_income.get(agency, 0.0) + income
					# 미수금 집계는 라인 1082-1085에서 행 단위로 이미 처리됨 (중복 방지)

				grand_expense += expense
				grand_income += income
				if unit == 0:
					missing[(agency, job, "저장")] = missing.get((agency, job, "저장"), 0) + qty_store
				# 집계: 상품명 기준(type을 붙이지 않음)
				bp = by_product.setdefault(tab, {}).setdefault(job, {"qty": 0.0, "expense": 0.0, "income": 0.0})
				bp["qty"] += float(qty_store); bp["expense"] += expense; bp["income"] += income
				by_agency.setdefault(tab, {}).setdefault(agency, []).append({"product": job, "type": "저장", "qty": qty_store, "unit_price": unit, "expense": expense, "income": income, "internal_type": internal_type})
			# 트래픽 행
			if qty_traf:
				unit = find_unit_price(agency, job, "트래픽")
				expense = float(qty_traf) * unit
				income = 0.0 if is_internal else income_noted
				rows_out.append({"date": tab, "client": agency, "job": job, "type": "트래픽", "qty": qty_traf, "unit_price": unit, "expense": expense, "income": income, "is_internal": is_internal, "internal_type": internal_type})
				by_client_expense[agency] = by_client_expense.get(agency, 0.0) + expense
				if not is_internal:
					by_client_income[agency] = by_client_income.get(agency, 0.0) + income
					# 미수금 집계는 라인 1082-1085에서 행 단위로 이미 처리됨 (중복 방지)

				grand_expense += expense
				grand_income += income
				if unit == 0:
					missing[(agency, job, "트래픽")] = missing.get((agency, job, "트래픽"), 0) + qty_traf
				bp = by_product.setdefault(tab, {}).setdefault(job, {"qty": 0.0, "expense": 0.0, "income": 0.0})
				bp["qty"] += float(qty_traf); bp["expense"] += expense; bp["income"] += income
				by_agency.setdefault(tab, {}).setdefault(agency, []).append({"product": job, "type": "트래픽", "qty": qty_traf, "unit_price": unit, "expense": expense, "income": income, "internal_type": internal_type})

	# missing 리스트 가공
	missing_list = [{"client": k[0], "job": k[1], "type": k[2], "qty_sum": v} for k, v in missing.items()]
	return {
		"rows": rows_out,
		"totals": {
			"by_client_expense": by_client_expense,
			"by_client_income": by_client_income,
			"grand_expense": grand_expense,
			"grand_income": grand_income,
		},
		"aggregates": {"by_product": by_product, "by_agency": by_agency},
		"missing_prices": missing_list,
		"unpaid_receivables": unpaid_by_agency,
	}
