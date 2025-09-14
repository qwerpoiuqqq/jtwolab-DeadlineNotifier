import os
import json
import argparse
from typing import Dict, List
from flask import Flask, render_template, request, jsonify, Response, stream_with_context
from dotenv import load_dotenv
from datetime import date, timedelta
import re

from sheet_client import fetch_grouped_messages, load_settings, inspect_sheets, diagnose_matches, fetch_grouped_messages_by_date, stream_grouped_messages_by_date, mark_checked_for_agency, mark_checked_for_agencies
from internal_manager import load_cache as internal_load_cache, refresh_cache as internal_refresh_cache


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
