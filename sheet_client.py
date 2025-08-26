import os
import json
import re
from typing import Dict, List, Set, Any, Tuple

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
		# 확장 옵션
		self.header_scan_max_rows: int = int(str(kwargs.get("HEADER_SCAN_MAX_ROWS", "150")).strip() or "150")
		self.enable_batch_get: bool = str(kwargs.get("BATCH_GET_ENABLED", "true")).strip().lower() == "true"
		self.enable_batch_get_fallback: bool = str(kwargs.get("BATCH_GET_FALLBACK", "true")).strip().lower() == "true"

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
			"HEADER_SCAN_MAX_ROWS": str(self.header_scan_max_rows),
			"BATCH_GET_ENABLED": "true" if self.enable_batch_get else "false",
			"BATCH_GET_FALLBACK": "true" if self.enable_batch_get_fallback else "false",
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
		HEADER_SCAN_MAX_ROWS=os.getenv("HEADER_SCAN_MAX_ROWS", "150"),
		BATCH_GET_ENABLED=os.getenv("BATCH_GET_ENABLED", "true"),
		BATCH_GET_FALLBACK=os.getenv("BATCH_GET_FALLBACK", "true"),
	)


def _build_credentials() -> Credentials:
	service_account_json_inline = os.getenv("SERVICE_ACCOUNT_JSON", "").strip()
	keyfile_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "").strip()

	scopes = [
		"https://www.googleapis.com/auth/spreadsheets.readonly",
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
	}
	try:
		candidates = ws.get_values(f'1:{settings.header_scan_max_rows}')  # 상단 N행 탐색
	except Exception:
		candidates = []
	best_idx = 1
	best_headers: List[str] = []
	best_score = -1
	for idx, row in enumerate(candidates, start=1):
		headers = [str(h).strip() for h in row]
		score = 0
		for header in headers:
			for key_id, pref in required_map.items():
				if _matches(header, pref, key_id):
					score += 1
					break
		if score > best_score:
			best_idx = idx
			best_headers = headers
			best_score = score
	if best_score <= 0:
		try:
			best_headers = [h.strip() for h in ws.row_values(1)]
		except Exception:
			best_headers = []
		best_idx = 1
	return best_idx, best_headers


def _build_records(ws: gspread.Worksheet, header_row: int, headers: List[str]) -> List[Dict[str, Any]]:
	try:
		values = ws.get_all_values()
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

			if is_checked:
				continue
			if is_internal:
				continue
			if remain is None or remain not in selected_set:
				continue
			if not bizname:
				continue

			task_map = agency_to_task_to_names.setdefault(agency, {})
			name_list = task_map.setdefault(task_name, [])
			if bizname not in name_list:
				name_list.append(bizname)

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
	agency_map: Dict[str, Dict[int, Dict[str, List[str]]]] = {}

	# 내부 헬퍼: 헤더에서 필요한 열 인덱스 탐색 (0-based)
	def _resolve_positions(headers: List[str]) -> Dict[str, int | None]:
		positions: Dict[str, int | None] = {
			"AGENCY_COLUMN": None,
			"INTERNAL_COLUMN": None,
			"REMAINING_DAYS_COLUMN": None,
			"CHECKED_COLUMN": None,
			"BIZNAME_COLUMN": None,
			"PRODUCT_COLUMN": None,
			"PRODUCT_NAME_COLUMN": None,
		}
		for idx, h in enumerate(headers):
			if positions["AGENCY_COLUMN"] is None and _matches(h, settings.agency_col, "AGENCY_COLUMN"):
				positions["AGENCY_COLUMN"] = idx
			if positions["INTERNAL_COLUMN"] is None and _matches(h, settings.internal_col, "INTERNAL_COLUMN"):
				positions["INTERNAL_COLUMN"] = idx
			if positions["REMAINING_DAYS_COLUMN"] is None and _matches(h, settings.remaining_days_col, "REMAINING_DAYS_COLUMN"):
				positions["REMAINING_DAYS_COLUMN"] = idx
			if positions["CHECKED_COLUMN"] is None and _matches(h, settings.checked_col, "CHECKED_COLUMN"):
				positions["CHECKED_COLUMN"] = idx
			if positions["BIZNAME_COLUMN"] is None and _matches(h, settings.bizname_col, "BIZNAME_COLUMN"):
				positions["BIZNAME_COLUMN"] = idx
			if positions["PRODUCT_COLUMN"] is None and _matches(h, settings.product_col, "PRODUCT_COLUMN"):
				positions["PRODUCT_COLUMN"] = idx
			if positions["PRODUCT_NAME_COLUMN"] is None and _matches(h, settings.product_name_col, "PRODUCT_NAME_COLUMN"):
				positions["PRODUCT_NAME_COLUMN"] = idx
		return positions

	# 내부 헬퍼: 0-based 인덱스를 시트 컬럼명으로 변환
	def _col_letter(zero_based_index: int) -> str:
		n = zero_based_index + 1
		letters = []
		while n > 0:
			n, rem = divmod(n - 1, 26)
			letters.append(chr(65 + rem))
		return "".join(reversed(letters))

	for ws in ss.worksheets():
		tab_title = (ws.title or "").strip()
		header_row, headers = _find_header_row(ws, settings)
		positions = _resolve_positions(headers)

		# 전체 읽기 폴백 함수
		def _legacy_full_scan() -> None:
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
				dict_by_day = agency_map.setdefault(agency_label, {})
				dict_by_task = dict_by_day.setdefault(remain, {})
				name_list = dict_by_task.setdefault(display_task, [])
				if bizname not in name_list:
					name_list.append(bizname)

		# 배치 비활성화면 즉시 폴백 경로로
		if not settings.enable_batch_get:
			_legacy_full_scan()
			continue

		# 필요한 열이 한 개도 추론되지 않으면 폴백
		if all(positions[k] is None for k in positions.keys()):
			if settings.enable_batch_get_fallback:
				_legacy_full_scan()
			continue

		# 필요한 열 범위만 batch_get으로 가져오기 (헤더 다음 행부터 끝까지)
		ranges: List[str] = []
		order_keys = [
			"AGENCY_COLUMN",
			"INTERNAL_COLUMN",
			"REMAINING_DAYS_COLUMN",
			"CHECKED_COLUMN",
			"BIZNAME_COLUMN",
			"PRODUCT_COLUMN",
			"PRODUCT_NAME_COLUMN",
		]
		for key in order_keys:
			pos = positions[key]
			if pos is not None:
				col = _col_letter(pos)
				start = header_row + 1
				ranges.append(f"{col}{start}:{col}")

		columns_data = []
		try:
			columns_data = ws.batch_get(ranges, major_dimension='COLUMNS') if ranges else []
		except Exception:
			# 배치 실패 시 폴백
			if settings.enable_batch_get_fallback:
				_legacy_full_scan()
				continue

		# key별 컬럼 데이터 매핑
		key_to_values: Dict[str, List[str]] = {}
		idx_in_result = 0
		for key in order_keys:
			pos = positions[key]
			if pos is not None:
				col_vals = columns_data[idx_in_result][0] if columns_data and idx_in_result < len(columns_data) and len(columns_data[idx_in_result]) > 0 else []
				# 문자열로 정규화
				key_to_values[key] = [str(v).strip() for v in col_vals]
				idx_in_result += 1
			else:
				key_to_values[key] = []

		max_len = 0
		for vals in key_to_values.values():
			if len(vals) > max_len:
				max_len = len(vals)

		for i in range(max_len):
			agency_raw = (key_to_values["AGENCY_COLUMN"][i] if i < len(key_to_values["AGENCY_COLUMN"]) else "").strip()
			is_checked = _is_truthy(key_to_values["CHECKED_COLUMN"][i] if i < len(key_to_values["CHECKED_COLUMN"]) else "")
			is_internal = _is_truthy(key_to_values["INTERNAL_COLUMN"][i] if i < len(key_to_values["INTERNAL_COLUMN"]) else "")
			remain = _parse_int_maybe(key_to_values["REMAINING_DAYS_COLUMN"][i] if i < len(key_to_values["REMAINING_DAYS_COLUMN"]) else "")
			bizname = (key_to_values["BIZNAME_COLUMN"][i] if i < len(key_to_values["BIZNAME_COLUMN"]) else "").strip()
			product = (key_to_values["PRODUCT_COLUMN"][i] if i < len(key_to_values["PRODUCT_COLUMN"]) else "").strip()
			product_name = (key_to_values["PRODUCT_NAME_COLUMN"][i] if i < len(key_to_values["PRODUCT_NAME_COLUMN"]) else "").strip()

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
			dict_by_day = agency_map.setdefault(agency_label, {})
			dict_by_task = dict_by_day.setdefault(remain, {})
			name_list = dict_by_task.setdefault(display_task, [])
			if bizname not in name_list:
				name_list.append(bizname)

	return agency_map


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
	"""탭별 매칭된 항목과 제외 사유 샘플을 반환한다."""
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

		report[task_name] = {
			"header_row": header_row,
			"matched_count": len(matched),
			"matched_sample": matched[:limit],
			"excluded_sample": excluded[:limit],
		}
	return report
