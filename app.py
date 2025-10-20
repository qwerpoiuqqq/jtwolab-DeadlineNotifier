import os
import json
import argparse
from typing import Dict, List
from flask import Flask, render_template, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
from datetime import date, timedelta
import re

from sheet_client import fetch_grouped_messages, load_settings, inspect_sheets, diagnose_matches, fetch_grouped_messages_by_date, stream_grouped_messages_by_date, mark_checked_for_agency, mark_checked_for_agencies, _get_client, _find_header_row, _build_records, _get_value_flexible, _normalize_key, _collapse_spaces
from internal_manager import load_cache as internal_load_cache, refresh_cache as internal_refresh_cache
from internal_manager import fetch_internal_weekly_summary
import csv
from io import StringIO
from pathlib import Path
from datetime import datetime


# .env 로드
load_dotenv()


def _strip_parentheses(text: str) -> str:
	"""문자열에서 소괄호 내 내용을 제거한다. 예: '작업명(부가)' -> '작업명'"""
	if not text:
		return text
	return re.sub(r"\s*\([^)]*\)", "", text).strip()


def create_app() -> Flask:
	app = Flask(__name__)

	@app.route("/", methods=["GET"])  # 메인 페이지: 폼 + 결과
	def index():
		settings = load_settings()
		days_param = request.args.get("days", "").strip()
		base_date_str = request.args.get("base_date", "").strip()
		filter_mode = request.args.get("filter_mode", "agency").strip().lower()  # 'agency' | 'internal'
		did_fetch = request.args.get("submit", "") == "1"

		# 기준일 처리 (기본: 오늘)
		if base_date_str:
			try:
				base_dt = date.fromisoformat(base_date_str)
			except ValueError:
				base_dt = date.today()
		else:
			base_dt = date.today()

		ordered_days: List[int] = []
		day_to_date: Dict[int, str] = {}
		day_to_date_label: Dict[int, str] = {}
		grouped_by_date: Dict[str, Dict[int, Dict[str, List[str]]]] = {}
		agency_to_message: Dict[str, str] = {}
		agency_to_message_workload: Dict[str, str] = {}
		agency_to_date_line: Dict[str, str] = {}
		error = None
		suggested_prefix = ""

		if did_fetch:
			selected_days = _parse_days(days_param)
			ordered_days = sorted(selected_days)

			# 날짜 매핑 생성 (YYYY-MM-DD 및 요일 포함 라벨)
			weekday_kr = ["월", "화", "수", "목", "금", "토", "일"]
			for d in ordered_days:
				cur = base_dt + timedelta(days=d)
				day_to_date[d] = cur.isoformat()
				day_to_date_label[d] = f"{cur.isoformat()}({weekday_kr[cur.weekday()]})"

			try:
				# 날짜별 그룹핑 (필터 모드 적용)
				grouped_by_date = fetch_grouped_messages_by_date(selected_days=selected_days, settings=settings, filter_mode=filter_mode)
			except Exception as e:
				grouped_by_date = {}
				error = str(e)
			else:
				error = None

			# 복붙 포맷 2종 생성
			# 1) 기본: 날짜(요일) → <작업명> → 상호명
			# 2) 작업량 포함: 날짜(요일) → <작업명> → 상호명 : 일작업량
			name_wl_re = re.compile(r"^(.+?)\s*\(일작업량\s+(.*?)\)$")
			for agency, by_day in grouped_by_date.items():
				parts_base: List[str] = []
				parts_wl: List[str] = []
				for d in sorted(by_day.keys()):
					# 날짜 헤더
					date_label = day_to_date_label.get(d, f"+{d}")
					parts_base.append(date_label)
					parts_wl.append(date_label)
					# 작업명과 상호들
					for task, names in by_day[d].items():
						if not names:
							continue
						display_task = _strip_parentheses(task)
						parts_base.append(f"<{display_task}>")
						parts_wl.append(f"<{display_task}>")
						for name in names:
							name_str = str(name).strip()
							m = name_wl_re.match(name_str)
							if m:
								base_name = m.group(1).strip()
								workload_val = m.group(2).strip()
								parts_base.append(base_name)
								parts_wl.append(f"{base_name} : {workload_val}")
							else:
								# 작업량 정보가 없으면 동일하게 표기
								parts_base.append(name_str)
								parts_wl.append(name_str)
						# 작업 블록 사이: 1줄 공백
						parts_base.append("")
						parts_wl.append("")
					# 날짜 블록 사이: 추가로 1줄 더 공백(= 총 2줄)
					parts_base.append("")
					parts_wl.append("")
				agency_to_message[agency] = "\n".join(parts_base).rstrip()
				agency_to_message_workload[agency] = "\n".join(parts_wl).rstrip()

			# 대행사별 실제 존재하는 마감일 범위를 기준으로 날짜 문구(요일 포함) 생성
			weekday_kr = ["월", "화", "수", "목", "금", "토", "일"]
			def fmt_mmdd_w(dt: date) -> str:
				return f"{dt.month:02d}/{dt.day:02d}({weekday_kr[dt.weekday()]})"
			for agency, by_day in grouped_by_date.items():
				present_days = sorted(list(by_day.keys()))
				if not present_days:
					continue
				start_dt = base_dt + timedelta(days=present_days[0])
				end_dt = base_dt + timedelta(days=present_days[-1])
				if start_dt == end_dt:
					line = f"{fmt_mmdd_w(start_dt)} 마감건 안내드립니다."
				else:
					line = f"{fmt_mmdd_w(start_dt)} ~ {fmt_mmdd_w(end_dt)} 마감건 안내드립니다."
				agency_to_date_line[agency] = line

			# 선택된 날짜 범위 기반 추천 첫 멘트 생성 (인사 + 날짜 문구, MM/DD 포맷)
			if ordered_days:
				all_dates = [base_dt + timedelta(days=d) for d in ordered_days]
				start_dt = min(all_dates)
				end_dt = max(all_dates)
				greeting = "대표님 안녕하세요~"
				def mmdd(dt: date) -> str:
					return f"{dt.month:02d}/{dt.day:02d}"
				if start_dt == end_dt:
					line = f"{mmdd(start_dt)} 마감건 안내드립니다."
				else:
					line = f"{mmdd(start_dt)} ~ {mmdd(end_dt)} 마감건 안내드립니다."
				suggested_prefix = greeting + "\n" + line

		return render_template(
			"index.html",
			error=error,
			did_fetch=did_fetch,
			days_param=days_param,
			base_date_str=base_date_str or base_dt.isoformat(),
			day_to_date=day_to_date,
			day_to_date_label=day_to_date_label,
			ordered_days=ordered_days,
			grouped=grouped_by_date,
			agency_to_message=agency_to_message,
			agency_to_message_workload=agency_to_message_workload,
			agency_to_date_line=agency_to_date_line,
			settings=settings,
			filter_mode=filter_mode,
			suggested_prefix=suggested_prefix,
		)

	@app.route("/manage", methods=["GET"])  # 내부 보장건/관리 탭
	def manage():
		cache = internal_load_cache()
		updated_at = cache.get("updated_at")
		items = cache.get("items", [])
		return render_template(
			"manage.html",
			updated_at=updated_at,
			initial_items=items,
		)

	@app.route("/settlement", methods=["GET"])  # 마감 체크 전용 탭
	def settlement():
		return render_template("settlement.html")

	@app.route("/debug/headers")
	def debug_headers():
		try:
			info = inspect_sheets()
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(info)

	@app.route("/debug/matches")
	def debug_matches():
		days_param = request.args.get("days", "0").strip()
		selected_days = _parse_days(days_param)
		try:
			report = diagnose_matches(selected_days)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(report)

	@app.route("/api/fetch-stream")
	def fetch_stream():
		"""SSE: 진행률과 최종 결과를 스트리밍한다."""
		settings = load_settings()
		days_param = request.args.get("days", "").strip()
		filter_mode = request.args.get("filter_mode", "agency").strip().lower()
		selected_days = _parse_days(days_param)

		def event_stream():
			try:
				for evt in stream_grouped_messages_by_date(selected_days, settings, filter_mode):
					yield f"data: {json.dumps(evt, ensure_ascii=False)}\n\n"
			except Exception as e:
				payload = {"type": "error", "message": str(e)}
				yield f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"

		return Response(stream_with_context(event_stream()), mimetype="text/event-stream")

	@app.route("/api/internal/items", methods=["GET"])  # 캐시된 내부 진행건 목록 반환
	def api_internal_items():
		try:
			cache = internal_load_cache()
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({
			"updated_at": cache.get("updated_at"),
			"items": cache.get("items", []),
		}), 200

	@app.route("/api/internal/weekly", methods=["GET"])  # 내부 진행 주간 요약 (대행사>업체)
	def api_internal_weekly():
		try:
			weeks_str = request.args.get("weeks", "3").strip()
			weeks = int(weeks_str) if weeks_str else 3
		except Exception:
			weeks = 3
		base_date_str = request.args.get("base_date", "").strip()
		try:
			base_dt = date.fromisoformat(base_date_str) if base_date_str else date.today()
		except Exception:
			base_dt = date.today()
		try:
			groups = fetch_internal_weekly_summary(base_dt, weeks)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({
			"base_date": base_dt.isoformat(),
			"weeks": weeks,
			"groups": groups,
		}), 200

	@app.route("/api/internal/refresh", methods=["POST"])  # 수동 불러오기
	def api_internal_refresh():
		try:
			data = internal_refresh_cache()
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(data), 200

	# -----------------------------
	# Settlement(정산) API
	# -----------------------------
	_PRICEBOOK_FILE = os.getenv("PRICEBOOK_FILE", "settlement_pricebook.json")
	_EXTRAS_FILE = os.getenv("EXTRAS_FILE", "settlement_extras.json")

	def _read_json_file(path: str, default_value):
		try:
			with open(path, "r", encoding="utf-8") as f:
				data = json.load(f)
				return data
		except FileNotFoundError:
			return default_value
		except Exception:
			return default_value


	def _ensure_parent_dir(path: str) -> None:
		try:
			p = Path(path)
			if p.parent and not p.parent.exists():
				p.parent.mkdir(parents=True, exist_ok=True)
		except Exception:
			pass

	def _backup_existing_file(path: str) -> None:
		try:
			p = Path(path)
			if p.exists() and p.is_file():
				stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
				backup = p.with_name(p.stem + f".{stamp}.bak" + p.suffix)
				p.replace(backup)
		except Exception:
			pass

	def _write_json_file(path: str, data) -> None:
		_ensure_parent_dir(path)
		# 기존 파일 백업
		_backup_existing_file(path)
		with open(path, "w", encoding="utf-8") as f:
			json.dump(data, f, ensure_ascii=False)

	@app.route("/api/settlement/pricebook", methods=["GET"])
	def api_pricebook_get():
		data = _read_json_file(_PRICEBOOK_FILE, {"items": []})
		if not isinstance(data, dict):
			data = {"items": []}
		items = data.get("items", [])
		# meta 조회(파일 상태): /api/settlement/pricebook?meta=1 또는 debug=1
		q = (request.args.get("meta") or request.args.get("debug") or "").strip().lower()
		meta = None
		if q in ("1", "true", "yes", "y"):
			try:
				p = Path(_PRICEBOOK_FILE)
				meta = {
					"path": str(p),
					"exists": p.exists(),
					"size": (p.stat().st_size if p.exists() and p.is_file() else 0),
					"mtime": (datetime.fromtimestamp(p.stat().st_mtime).isoformat() if p.exists() and p.is_file() else None),
				}
			except Exception:
				meta = {"path": _PRICEBOOK_FILE, "error": "inspect_failed"}
		return jsonify({"items": items, "meta": meta})

	@app.route("/api/settlement/pricebook", methods=["POST"])
	def api_pricebook_save():
		try:
			payload = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		items = payload.get("items")
		if not isinstance(items, list):
			return jsonify({"error": "items_array_required"}), 400
		_write_json_file(_PRICEBOOK_FILE, {"items": items})
		return jsonify({"ok": True, "count": len(items)})

	@app.route("/api/settlement/pricebook/template", methods=["GET"])
	def api_pricebook_template():
		# 간단 CSV 템플릿 제공
		si = StringIO()
		w = csv.writer(si)
		w.writerow(["client", "product", "type", "price", "account", "bank", "holder"])
		csv_data = si.getvalue()
		resp = Response(csv_data, mimetype="text/csv; charset=utf-8")
		resp.headers["Content-Disposition"] = "attachment; filename=pricebook_template.csv"
		return resp

	@app.route("/api/settlement/pricebook/upload", methods=["POST"])
	def api_pricebook_upload():
		file = request.files.get("file")
		if not file:
			return jsonify({"error": "file_required"}), 400
		filename = file.filename or ""
		name_lower = filename.lower()
		# CSV만 지원 (간단 구현)
		if not name_lower.endswith(".csv"):
			return jsonify({"error": "csv_only_supported"}), 400
		try:
			text = file.stream.read().decode("utf-8", errors="replace")
			si = StringIO(text)
			r = csv.DictReader(si)
			items = []
			for row in r:
				client = (row.get("client") or "").strip()
				product = (row.get("product") or "").strip()
				typev = (row.get("type") or "공통").strip() or "공통"
				price = int(str(row.get("price") or "0").replace(",", "").strip() or "0")
				account = (row.get("account") or "").strip()
				bank = (row.get("bank") or "").strip()
				holder = (row.get("holder") or "").strip()
				if client and product:
					items.append({"client": client, "product": product, "type": typev, "price": price, "account": account, "bank": bank, "holder": holder})
		except Exception as e:
			return jsonify({"error": f"parse_failed:{e}"}), 400
		return jsonify({"items": items})

	@app.route("/api/settlement/extra", methods=["GET"])
	def api_extra_get():
		data = _read_json_file(_EXTRAS_FILE, {"items": []})
		if not isinstance(data, dict):
			data = {"items": []}
		items = data.get("items", [])
		q = (request.args.get("meta") or request.args.get("debug") or "").strip().lower()
		meta = None
		if q in ("1", "true", "yes", "y"):
			try:
				p = Path(_EXTRAS_FILE)
				meta = {
					"path": str(p),
					"exists": p.exists(),
					"size": (p.stat().st_size if p.exists() and p.is_file() else 0),
					"mtime": (datetime.fromtimestamp(p.stat().st_mtime).isoformat() if p.exists() and p.is_file() else None),
				}
			except Exception:
				meta = {"path": _EXTRAS_FILE, "error": "inspect_failed"}
		return jsonify({"items": items, "meta": meta})

	@app.route("/api/settlement/extra", methods=["POST"])
	def api_extra_save():
		try:
			payload = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		items = payload.get("items")
		if not isinstance(items, list):
			return jsonify({"error": "items_array_required"}), 400
		_write_json_file(_EXTRAS_FILE, {"items": items})
		return jsonify({"ok": True, "count": len(items)})

	@app.route("/api/settlement/debug/files", methods=["GET"])  # 파일 상태 헬스체크
	def api_settlement_debug_files():
		def inspect(path: str):
			try:
				p = Path(path)
				return {
					"path": str(p),
					"exists": p.exists(),
					"size": (p.stat().st_size if p.exists() and p.is_file() else 0),
					"mtime": (datetime.fromtimestamp(p.stat().st_mtime).isoformat() if p.exists() and p.is_file() else None),
				}
			except Exception as e:
				return {"path": path, "error": str(e)}
		return jsonify({
			"pricebook": inspect(_PRICEBOOK_FILE),
			"extras": inspect(_EXTRAS_FILE),
		}), 200

	# 별칭(일부 환경에서 'debug' 경로 필터링 시 대체)
	@app.route("/api/settlement/files", methods=["GET"])
	def api_settlement_files_alias():
		return api_settlement_debug_files()

	@app.route("/api/settlement/tabs", methods=["GET"])
	def api_settlement_tabs():
		try:
			settings = load_settings()
			client = _get_client()
			ss = client.open_by_key(settings.spreadsheet_id)
			ws = ss.worksheets()
			titles = [ (w.title or "").strip() for w in ws ]
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({"tabs": titles})

	def _parse_date_from_title(title: str) -> str:
		# YYYY-MM-DD 또는 YYYY.MM.DD 등 단순 포맷만 추출
		m = re.search(r"(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})", title or "")
		if not m:
			return title or ""
		y = int(m.group(1)); mth = int(m.group(2)); d = int(m.group(3))
		return f"{y:04d}-{mth:02d}-{d:02d}"

	def _display_task_for_tab(tab_title: str, product: str, product_name: str) -> str:
		base_task = (tab_title or "").strip()
		is_misc = _collapse_spaces(base_task) == _collapse_spaces("기타")
		if is_misc:
			return (product_name or base_task).strip() or base_task
		return (f"{base_task} {product}".strip() if product else base_task) or base_task

	def _derive_type(product: str) -> str:
		s = (product or "").strip()
		if "저장" in s:
			return "저장"
		if "트래픽" in s:
			return "트래픽"
		return "공통"

	def _lookup_unit_price(pricebook_items: List[Dict[str, any]], client: str, product: str, typev: str) -> int:
		# 정확 타입 우선, 없으면 공통
		for it in pricebook_items:
			if (it.get("client") or "").strip()==client and (it.get("product") or "").strip()==product and (it.get("type") or "공통").strip()==typev:
				try: return int(it.get("price") or 0)
				except Exception: return 0
		for it in pricebook_items:
			if (it.get("client") or "").strip()==client and (it.get("product") or "").strip()==product and (it.get("type") or "공통").strip()=="공통":
				try: return int(it.get("price") or 0)
				except Exception: return 0
		return 0

	@app.route("/api/settlement/compute", methods=["POST"])
	def api_settlement_compute():
		try:
			payload = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		tabs = payload.get("tabs") or []
		if not isinstance(tabs, list):
			return jsonify({"error": "tabs_array_required"}), 400
		try:
			settings = load_settings()
			client = _get_client()
			ss = client.open_by_key(settings.spreadsheet_id)
			worksheets = { (w.title or "").strip(): w for w in ss.worksheets() }
		except Exception as e:
			return jsonify({"error": str(e)}), 500

		pricebook = _read_json_file(_PRICEBOOK_FILE, {"items": []})
		price_items = pricebook.get("items", []) if isinstance(pricebook, dict) else []

		rows: List[Dict[str, any]] = []
		missing_prices: List[Dict[str, str]] = []
		# aggregates: by_product: {date: {product: {qty, expense, income}}}, by_agency: {agency: {product: {qty, expense, income}}}
		by_product: Dict[str, Dict[str, Dict[str, int]]] = {}
		by_agency: Dict[str, Dict[str, Dict[str, int]]] = {}

		for tab in tabs:
			ws = worksheets.get((tab or "").strip())
			if not ws:
				continue
			try:
				header_row, headers = _find_header_row(ws, load_settings())
				records = _build_records(ws, header_row, headers)
			except Exception:
				records = []
			date_str = _parse_date_from_title(ws.title or "")
			for row in records:
				row_norm = { _normalize_key(k): v for k, v in row.items() }
				agency = str(_get_value_flexible(row_norm, load_settings().agency_col, "AGENCY_COLUMN") or "").strip()
				bizname = str(_get_value_flexible(row_norm, load_settings().bizname_col, "BIZNAME_COLUMN") or "").strip()
				product = str(_get_value_flexible(row_norm, load_settings().product_col, "PRODUCT_COLUMN") or "").strip()
				product_name = str(_get_value_flexible(row_norm, load_settings().product_name_col, "PRODUCT_NAME_COLUMN") or "").strip()
				workload_raw = str(_get_value_flexible(row_norm, load_settings().daily_workload_col, "DAILY_WORKLOAD_COLUMN") or "").strip()
				qty = 0
				try:
					qty = int(re.search(r"-?\d+", workload_raw).group(0)) if workload_raw else 0
				except Exception:
					qty = 0
				if not bizname:
					continue
				job = _display_task_for_tab(ws.title or "", product, product_name)
				typev = _derive_type(product)
				product_key = job if typev=="공통" else f"{job} {typev}"
				unit_price = _lookup_unit_price(price_items, agency, product_key, typev)
				expense = qty * unit_price
				income = expense
				rows.append({
					"date": date_str,
					"client": agency,
					"job": job,
					"type": (None if typev=="공통" else typev),
					"qty": qty,
					"unit_price": unit_price,
					"expense": expense,
					"income": income,
				})
				# aggregates by date/product
				pmap = by_product.setdefault(date_str, {})
				acc = pmap.setdefault(product_key, {"qty": 0, "expense": 0, "income": 0})
				acc["qty"] += qty; acc["expense"] += expense; acc["income"] += income
				# aggregates by agency/product
				amap = by_agency.setdefault(agency or "기타", {})
				acc2 = amap.setdefault(product_key, {"qty": 0, "expense": 0, "income": 0})
				acc2["qty"] += qty; acc2["expense"] += expense; acc2["income"] += income
				if unit_price <= 0:
					missing_prices.append({"client": agency, "product": job, "type": (None if typev=="공통" else typev)})

		return jsonify({
			"rows": rows,
			"aggregates": {"by_product": by_product, "by_agency": by_agency},
			"missing_prices": missing_prices,
		}), 200

	@app.route("/api/mark-done", methods=["POST"])
	def mark_done():
		"""특정 대행사 카드의 모든 해당 행을 '마감 안내 체크'로 표시한다."""
		try:
			data = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400

		agency_label = str(data.get("agency") or "").strip()
		days_param = str(data.get("days") or "").strip()
		filter_mode = str(data.get("filter_mode") or "agency").strip().lower()
		if not agency_label:
			return jsonify({"error": "missing_agency"}), 400

		selected_days = _parse_days(days_param)
		try:
			result = mark_checked_for_agency(selected_days=selected_days, agency_label=agency_label, filter_mode=filter_mode)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(result)

	@app.route("/api/mark-done-bulk", methods=["POST"])
	def mark_done_bulk():
		"""여러 대행사 카드를 한 번에 체크 처리한다."""
		try:
			data = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400

		agency_labels = data.get("agencies") or []
		if not isinstance(agency_labels, list):
			return jsonify({"error": "invalid_agencies"}), 400
		agency_labels = [str(a or "").strip() for a in agency_labels if str(a or "").strip()]
		if not agency_labels:
			return jsonify({"error": "empty_agencies"}), 400

		days_param = str(data.get("days") or "").strip()
		filter_mode = str(data.get("filter_mode") or "agency").strip().lower()
		selected_days = _parse_days(days_param)
		try:
			result = mark_checked_for_agencies(selected_days=selected_days, agency_labels=agency_labels, filter_mode=filter_mode)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(result)

	return app

app = create_app()


def _parse_days(days_param: str) -> List[int]:
	if not days_param:
		return []
	parts = [p.strip() for p in days_param.split(",") if p.strip()]
	selected: List[int] = []
	for p in parts:
		try:
			selected.append(int(p))
		except ValueError:
			continue
	return selected


if __name__ == "__main__":
	parser = argparse.ArgumentParser()
	parser.add_argument("--prod", action="store_true", help="Run with waitress server")
	args = parser.parse_args()

	host = os.getenv("FLASK_HOST", "0.0.0.0")
	port = int(os.getenv("FLASK_PORT", "8080"))
	debug = os.getenv("FLASK_DEBUG", "false").lower() == "true"

	if args.prod:
		from waitress import serve

		serve(app, host=host, port=port)
	else:
		app.run(host=host, port=port, debug=debug)
