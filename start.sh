#!/usr/bin/env bash
# Render 시작 스크립트

set -e

echo "========================================="
echo "Starting DeadlineNotifier application..."
echo "========================================="

# 데이터 디렉토리 확인
mkdir -p data

# Playwright 브라우저 경로 설정 (기본 위치 사용)
export PLAYWRIGHT_BROWSERS_PATH=0

echo "🎭 Checking Playwright browser..."
# Playwright 브라우저 자동 설치 (누락된 경우)
playwright install chromium 2>/dev/null || {
    echo "⚠️  Playwright install skipped (may already be installed)"
}

echo ""
echo "✅ Ready to start"
echo "📍 Host: ${FLASK_HOST:-0.0.0.0}"
echo "📍 Port: ${FLASK_PORT:-8080}"
echo "🔄 Auto-sync: ${AUTO_SYNC_ON_START:-false}"
echo "🎭 Playwright: ${PLAYWRIGHT_BROWSERS_PATH}"
echo ""

# 프로덕션 모드로 실행
exec python app.py --prod

