import os
import argparse
from typing import Dict, List
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv
from datetime import date, timedelta
import re

from sheet_client import (
    fetch_grouped_messages_by_date,
    load_settings,
    inspect_sheets,
    diagnose_matches,
)

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
        error = None

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
                grouped_by_date = fetch_grouped_messages_by_date(
                    selected_days=selected_days, settings=settings, filter_mode=filter_mode
                )
            except Exception as e:
                grouped_by_date = {}
                error = str(e)
            else:
                error = None

            # 복붙 포맷: 날짜(요일) 한 줄 → <작업명> → 상호들 (불필요한 빈줄/공백 없음)
            for agency, by_day in grouped_by_date.items():
                parts: List[str] = []
                for d in sorted(by_day.keys()):
                    parts.append(day_to_date_label.get(d, f"+{d}"))
                    for task, names in by_day[d].items():
                        if not names:
                            continue
                        display_task = _strip_parentheses(task)
                        parts.append(f"<{display_task}>")
                        for name in names:
                            parts.append(str(name).strip())
                agency_to_message[agency] = "\n".join(parts).rstrip()

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
            settings=settings,
            filter_mode=filter_mode,
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

    return app


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


# Render용 전역 WSGI 객체
app = create_app()


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
