import os
import json
import argparse
from typing import Dict, List
from flask import Flask, render_template, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
from datetime import date, timedelta, datetime
import re
import pytz
from apscheduler.schedulers.background import BackgroundScheduler
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from sheet_client import fetch_grouped_messages, load_settings, inspect_sheets, diagnose_matches, fetch_grouped_messages_by_date, stream_grouped_messages_by_date, mark_checked_for_agency, mark_checked_for_agencies, list_sheet_tabs, inspect_sheets_by_id, compute_settlement_rows
from internal_manager import load_cache as internal_load_cache, refresh_cache as internal_refresh_cache, fetch_workload_schedule
from guarantee_manager import GuaranteeManager

try:
	from data_security import DataSecurity
	SECURITY_AVAILABLE = True
except ImportError:
	SECURITY_AVAILABLE = False


# .env 로드
load_dotenv()


def _strip_parentheses(text: str) -> str:
	"""문자열에서 소괄호 내 내용을 제거한다. 예: '작업명(부가)' -> '작업명'"""
	if not text:
		return text
	return re.sub(r"\s*\([^)]*\)", "", text).strip()


def create_app() -> Flask:
	app = Flask(__name__)
	
	# 스케줄러 초기화
	scheduler = BackgroundScheduler(timezone=pytz.timezone('Asia/Seoul'))
	scheduler.start()
	
	# 자동 동기화 태스크
	def sync_guarantee_data():
		"""보장건 데이터 자동 동기화"""
		try:
			logger.info(f"Starting automatic sync at {datetime.now(pytz.timezone('Asia/Seoul'))}")
			gm = GuaranteeManager()
			result = gm.sync_from_google_sheets()
			logger.info(f"Sync completed: {result}")
		except Exception as e:
			logger.error(f"Sync failed: {e}")
	
	# 작업량 캐시 자동 갱신 태스크
	def refresh_workload_cache():
		"""작업량 캐시 자동 갱신"""
		try:
			logger.info(f"Starting workload cache refresh at {datetime.now(pytz.timezone('Asia/Seoul'))}")
			from workload_cache import refresh_all_workload_cache
			result = refresh_all_workload_cache()
			logger.info(f"Workload cache refresh completed: {result['message']}")
		except Exception as e:
			logger.error(f"Workload cache refresh failed: {e}")
	
	# 매일 9시, 16시 스케줄 등록 (보장건 동기화)
	scheduler.add_job(func=sync_guarantee_data, trigger="cron", hour=9, minute=0, id="morning_sync")
	scheduler.add_job(func=sync_guarantee_data, trigger="cron", hour=16, minute=0, id="afternoon_sync")
	
	# 매일 11시 30분 스케줄 등록 (작업량 캐시 갱신)
	scheduler.add_job(func=refresh_workload_cache, trigger="cron", hour=11, minute=30, id="workload_cache_refresh")

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

	@app.route("/manage", methods=["GET"])  # 월보장 관리 대시보드
	def manage():
		return render_template("manage.html")

	@app.route("/settlement", methods=["GET"])  # 결재선 · 정산 페이지 (UI 스켈레톤)
	def settlement():
		from flask import send_file
		# 템플릿 엔진 경유 대신 파일을 직접 서빙하여, 템플릿 로더/캐시 이슈를 우회한다.
		return send_file(os.path.join(app.root_path, "templates", "settlement.html"), mimetype="text/html; charset=utf-8")

	# --- 결재선 보조 API들 ---
	@app.route("/api/settlement/tabs", methods=["GET"])  # 시트 탭 제목 목록 (결재선 전용 시트)
	def api_settlement_tabs():
		try:
			settlement_ssid = os.getenv("SETTLEMENT_SPREADSHEET_ID", "").strip()
			tabs = list_sheet_tabs(settlement_ssid or None)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({"tabs": tabs}), 200

	@app.route("/api/settlement/pricebook", methods=["GET", "POST"])  # 단가/계좌 저장소 - 파일 기반(로컬)
	def api_settlement_pricebook():
		storage_path = os.getenv("PRICEBOOK_PATH", os.path.join(os.getcwd(), "pricebook.json"))
		if request.method == "GET":
			try:
				if os.path.exists(storage_path):
					with open(storage_path, "r", encoding="utf-8") as f:
						data = json.load(f)
				else:
					data = []
			except Exception as e:
				return jsonify({"error": str(e)}), 500
			return jsonify({"items": data}), 200
		# POST: 저장
		try:
			payload = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		items = payload.get("items")
		if not isinstance(items, list):
			return jsonify({"error": "invalid_items"}), 400
		try:
			with open(storage_path, "w", encoding="utf-8") as f:
				json.dump(items, f, ensure_ascii=False, indent=2)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({"ok": True}), 200

	@app.route("/api/settlement/pricebook/upload", methods=["POST"])  # XLSX 업로드 → 항목 파싱 반환
	def api_settlement_pricebook_upload():
		from io import BytesIO
		from openpyxl import load_workbook
		f = request.files.get("file")
		if not f:
			return jsonify({"error": "missing_file"}), 400
		try:
			buf = BytesIO(f.read())
			wb = load_workbook(buf, data_only=True)
			ws = wb.active
		except Exception as e:
			return jsonify({"error": f"xlsx_load_failed: {e}"}), 400
		# 헤더 매핑: 거래처, 상품명, 유형, 단가, 계좌, 예금주
		items = []
		try:
			headers = []
			for cell in ws[1]:
				headers.append(str(cell.value or "").strip())
			def idx(name: str) -> int | None:
				try:
					return headers.index(name)
				except ValueError:
					return None
			i_client = idx("거래처"); i_product = idx("상품명"); i_type = idx("유형"); i_price = idx("단가"); i_account = idx("계좌"); i_bank = idx("은행"); i_holder = idx("예금주")
			if i_client is None or i_product is None or i_price is None:
				return jsonify({"error": "missing_required_headers"}), 400
			for r in ws.iter_rows(min_row=2):
				def get(i):
					if i is None: return ""
					v = r[i].value if i < len(r) else ""
					return str(v).strip() if v is not None else ""
				client = get(i_client); product = get(i_product)
				if not client and not product:
					continue
				type_s = get(i_type)
				type_s = "공통" if type_s not in ("저장", "트래픽") else type_s
				price_s = get(i_price)
				try:
					price = float(str(price_s).replace(",", "")) if price_s else 0.0
				except Exception:
					price = 0.0
				account = get(i_account); bank = get(i_bank); holder = get(i_holder)
				items.append({"client": client, "product": product, "type": type_s, "price": price, "account": account, "bank": bank, "holder": holder})
		except Exception as e:
			return jsonify({"error": f"parse_failed: {e}"}), 400
		return jsonify({"items": items, "count": len(items)}), 200

	@app.route("/api/settlement/pricebook/template", methods=["GET"])  # 대량등록 XLSX 템플릿 다운로드
	def api_settlement_pricebook_template():
		from io import BytesIO
		from openpyxl import Workbook
		wb = Workbook()
		ws = wb.active
		ws.title = "pricebook"
		ws.append(["거래처", "상품명", "유형", "단가", "계좌", "은행", "예금주"])
		ws.append(["일류기획", "호올스", "저장", 32, "123-45-67890", "국민", "류준호"])  # 샘플
		buf = BytesIO()
		wb.save(buf)
		buf.seek(0)
		return app.response_class(buf.getvalue(), mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition": "attachment; filename=pricebook_template.xlsx"})

	@app.route("/api/settlement/inspect", methods=["GET"])  # 결재선 시트 헤더 점검
	def api_settlement_inspect():
		try:
			ssid = os.getenv("SETTLEMENT_SPREADSHEET_ID", "").strip()
			info = inspect_sheets_by_id(ssid)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({"tabs": info}), 200

	@app.route("/api/settlement/compute", methods=["POST"])  # 결재선 집계 계산
	def api_settlement_compute():
		try:
			payload = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		ssid = os.getenv("SETTLEMENT_SPREADSHEET_ID", "").strip()
		selected_tabs = payload.get("tabs") or []
		if not isinstance(selected_tabs, list):
			return jsonify({"error": "invalid_tabs"}), 400
		# 단가 로드
		storage_path = os.getenv("PRICEBOOK_PATH", os.path.join(os.getcwd(), "pricebook.json"))
		try:
			if os.path.exists(storage_path):
				with open(storage_path, "r", encoding="utf-8") as f:
					pricebook = json.load(f)
			else:
				pricebook = []
		except Exception:
			pricebook = []
		try:
			result = compute_settlement_rows(ssid, selected_tabs, pricebook)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(result), 200

	@app.route("/api/settlement/extra", methods=["GET", "POST"])  # 수기 추가 지출 저장소
	def api_settlement_extra():
		storage_path = os.getenv("EXTRA_EXPENSES_PATH", os.path.join(os.getcwd(), "extra_expenses.json"))
		if request.method == "GET":
			try:
				if os.path.exists(storage_path):
					with open(storage_path, "r", encoding="utf-8") as f:
						data = json.load(f)
				else:
					data = []
			except Exception as e:
				return jsonify({"error": str(e)}), 500
			return jsonify({"items": data}), 200
		# POST 저장 (전체 치환 방식)
		try:
			payload = request.get_json(force=True, silent=False) or {}
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		items = payload.get("items")
		if not isinstance(items, list):
			return jsonify({"error": "invalid_items"}), 400
		try:
			with open(storage_path, "w", encoding="utf-8") as f:
				json.dump(items, f, ensure_ascii=False, indent=2)
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify({"ok": True}), 200

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

	@app.route("/api/internal/refresh", methods=["POST"])  # 수동 불러오기
	def api_internal_refresh():
		try:
			data = internal_refresh_cache()
		except Exception as e:
			return jsonify({"error": str(e)}), 500
		return jsonify(data), 200
	
	@app.route("/api/workload/schedule", methods=["GET"])  # 작업량 스케줄 조회
	def api_workload_schedule():
		"""최근 3주간 작업량 스케줄 조회 (캐시 우선)"""
		company = request.args.get("company")
		business_name = request.args.get("business_name")  # 업체 필터 추가
		
		try:
			schedule = fetch_workload_schedule(company, business_name)
			return jsonify(schedule), 200
		except Exception as e:
			logger.error(f"Workload schedule error: {e}")
			import traceback
			logger.error(traceback.format_exc())
			return jsonify({"error": str(e)}), 500
	
	@app.route("/api/workload/cache/refresh", methods=["POST"])  # 작업량 캐시 수동 갱신
	def api_workload_cache_refresh():
		"""작업량 캐시 수동 갱신"""
		try:
			from workload_cache import refresh_all_workload_cache
			result = refresh_all_workload_cache()
			return jsonify(result), 200
		except Exception as e:
			logger.error(f"Workload cache refresh error: {e}")
			import traceback
			logger.error(traceback.format_exc())
			return jsonify({"error": str(e)}), 500
	
	@app.route("/api/workload/cache/status", methods=["GET"])  # 작업량 캐시 상태 조회
	def api_workload_cache_status():
		"""작업량 캐시 상태 조회"""
		try:
			from workload_cache import WorkloadCache
			cache = WorkloadCache()
			status = cache.get_cache_status()
			return jsonify(status), 200
		except Exception as e:
			logger.error(f"Workload cache status error: {e}")
			return jsonify({"error": str(e)}), 500

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

	# --- 월보장 관리 API ---
	@app.route("/api/guarantee/items", methods=["GET", "POST"])
	def api_guarantee_items():
		"""보장건 목록 조회 및 생성"""
		gm = GuaranteeManager()
		
		if request.method == "GET":
			# 필터 파라미터
			filters = {}
			if request.args.get("company"):
				filters["company"] = request.args.get("company")
			if request.args.get("status"):
				filters["status"] = request.args.get("status")
			if request.args.get("product"):
				filters["product"] = request.args.get("product")
			if request.args.get("active_only"):
				filters["active_only"] = True
			
			logger.info(f"Getting items with filters: {filters}")
			items = gm.get_items(filters)
			logger.info(f"Found {len(items)} items")
			
			# 디버깅: 처음 몇 개 아이템 로그
			if items:
				logger.info(f"Sample item: {items[0] if items else 'No items'}")
			
			return jsonify({"items": items, "count": len(items)}), 200
		
		# POST: 새 보장건 생성
		try:
			data = request.get_json(force=True)
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		
		if not data.get("business_name"):
			return jsonify({"error": "business_name_required"}), 400
		
		item = gm.create_item(data)
		return jsonify(item), 201

	@app.route("/api/guarantee/items/<item_id>", methods=["GET", "PUT", "DELETE"])
	def api_guarantee_item(item_id):
		"""특정 보장건 조회/수정/삭제"""
		gm = GuaranteeManager()
		
		if request.method == "GET":
			item = gm.get_item(item_id)
			if not item:
				return jsonify({"error": "not_found"}), 404
			return jsonify(item), 200
		
		elif request.method == "PUT":
			try:
				data = request.get_json(force=True)
			except Exception:
				return jsonify({"error": "invalid_json"}), 400
			
			item = gm.update_item(item_id, data)
			if not item:
				return jsonify({"error": "not_found"}), 404
			return jsonify(item), 200
		
		elif request.method == "DELETE":
			if gm.delete_item(item_id):
				return jsonify({"ok": True}), 200
			return jsonify({"error": "not_found"}), 404

	@app.route("/api/guarantee/statistics", methods=["GET"])
	def api_guarantee_stats():
		"""통계 조회"""
		gm = GuaranteeManager()
		return jsonify(gm.get_statistics()), 200

	@app.route("/api/guarantee/search", methods=["GET"])
	def api_guarantee_search():
		"""검색"""
		query = request.args.get("q", "").strip()
		if not query:
			return jsonify({"items": []}), 200
		
		gm = GuaranteeManager()
		items = gm.search(query)
		return jsonify({"items": items, "count": len(items)}), 200

	@app.route("/api/guarantee/daily-rank", methods=["POST"])
	def api_guarantee_daily_rank():
		"""일차별 순위 업데이트"""
		try:
			data = request.get_json(force=True)
		except Exception:
			return jsonify({"error": "invalid_json"}), 400
		
		item_id = data.get("item_id")
		day = data.get("day")
		rank = data.get("rank")
		
		if not all([item_id, day is not None, rank is not None]):
			return jsonify({"error": "missing_params"}), 400
		
		gm = GuaranteeManager()
		item = gm.update_daily_rank(item_id, day, rank)
		if not item:
			return jsonify({"error": "not_found"}), 404
		return jsonify(item), 200

	@app.route("/api/guarantee/sync", methods=["POST"])
	def api_guarantee_sync():
		"""수동 동기화"""
		try:
			gm = GuaranteeManager()
			logger.info("Starting manual sync...")
			result = gm.sync_from_google_sheets()
			last_sync = gm.get_last_sync_time()
			
			# 현재 총 데이터 수 가져오기
			total_items = len(gm.get_items())
			
			# 동기화 결과 로그
			logger.info(f"Sync completed - Added: {result['added']}, Updated: {result['updated']}, Failed: {result['failed']}")
			logger.info(f"Total items in database: {total_items}")
			
			# 실패가 있는 경우 경고
			if result['failed'] > 0:
				message = f"동기화 부분 완료 - 추가: {result['added']}건, 수정: {result['updated']}건, 실패: {result['failed']}건 (총 {total_items}건)"
			elif result['added'] == 0 and result['updated'] == 0:
				message = f"변경사항 없음 (총 {total_items}건)"
			else:
				message = f"동기화 완료 - 추가: {result['added']}건, 수정: {result['updated']}건 (총 {total_items}건)"
			
			return jsonify({
				"ok": True,
				"result": result,
				"last_sync": last_sync,
				"total_items": total_items,
				"message": message
			}), 200
		except Exception as e:
			logger.error(f"Manual sync failed: {str(e)}")
			import traceback
			logger.error(f"Traceback: {traceback.format_exc()}")
			return jsonify({
				"error": str(e),
				"detail": "서버 로그를 확인하세요"
			}), 500

	@app.route("/api/guarantee/sync-status", methods=["GET"])
	def api_guarantee_sync_status():
		"""동기화 상태 확인"""
		gm = GuaranteeManager()
		last_sync = gm.get_last_sync_time()
		
		# 다음 동기화 시간 계산
		now = datetime.now(pytz.timezone('Asia/Seoul'))
		current_hour = now.hour
		
		if current_hour < 9:
			next_sync = now.replace(hour=9, minute=0, second=0, microsecond=0)
		elif current_hour < 16:
			next_sync = now.replace(hour=16, minute=0, second=0, microsecond=0)
		else:
			# 다음날 9시
			next_sync = (now + timedelta(days=1)).replace(hour=9, minute=0, second=0, microsecond=0)
		
		return jsonify({
			"last_sync": last_sync,
			"next_sync": next_sync.isoformat(),
			"next_sync_kst": next_sync.strftime("%Y-%m-%d %H:%M KST")
		}), 200

	@app.route("/api/guarantee/exposure-status", methods=["GET"])
	def api_exposure_status():
		"""실시간 노출 현황 조회"""
		company = request.args.get("company")  # 제이투랩, 일류기획
		
		try:
			gm = GuaranteeManager()
			status = gm.get_exposure_status(company)
			return jsonify(status), 200
		except Exception as e:
			logger.error(f"Exposure status error: {e}")
			return jsonify({"error": str(e)}), 500
	
	@app.route("/api/guarantee/security-status", methods=["GET"])
	def api_security_status():
		"""데이터 보안 상태 확인 (관리자용)"""
		status = {
			"encryption_enabled": SECURITY_AVAILABLE,
			"data_info": {}
		}
		
		if SECURITY_AVAILABLE:
			try:
				security = DataSecurity()
				status["data_info"] = security.get_data_info()
				status["encryption_key_source"] = "environment" if os.getenv("DATA_ENCRYPTION_KEY") else "file"
			except Exception as e:
				status["error"] = str(e)
		else:
			status["warning"] = "Encryption module not available"
		
		# 기본 데이터 파일 존재 여부
		status["plain_files"] = {
			"guarantee_data.json": os.path.exists("guarantee_data.json"),
			"pricebook.json": os.path.exists("pricebook.json"),
			"internal_cache.json": os.path.exists("internal_cache.json")
		}
		
		return jsonify(status), 200
	
	@app.route("/api/guarantee/export", methods=["GET"])
	def api_guarantee_export():
		"""보장건 데이터 내보내기 (JSON)"""
		try:
			gm = GuaranteeManager()
			items = gm.get_items()
			
			# 민감 정보 제거 옵션
			remove_sensitive = request.args.get("remove_sensitive", "false").lower() == "true"
			if remove_sensitive:
				for item in items:
					item.pop("place_account", None)
					item.pop("url", None)
			
			return jsonify({
				"exported_at": datetime.now().isoformat(),
				"count": len(items),
				"items": items
			}), 200
		except Exception as e:
			return jsonify({"error": str(e)}), 500

	# Shutdown scheduler when app closes
	import atexit
	atexit.register(lambda: scheduler.shutdown())

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
