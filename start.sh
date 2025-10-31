#!/usr/bin/env bash
# Render 시작 스크립트

set -e

echo "========================================="
echo "Starting DeadlineNotifier application..."
echo "========================================="

# 데이터 디렉토리 확인
mkdir -p data

echo "✅ Ready to start"
echo "📍 Host: ${FLASK_HOST:-0.0.0.0}"
echo "📍 Port: ${FLASK_PORT:-8080}"
echo "🔄 Auto-sync: ${AUTO_SYNC_ON_START:-false}"
echo ""

# 프로덕션 모드로 실행
exec python app.py --prod

