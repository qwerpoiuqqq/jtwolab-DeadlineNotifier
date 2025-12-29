#!/usr/bin/env bash
# Render 빌드 스크립트

set -e

echo "========================================="
echo "Starting build process..."
echo "========================================="

echo "📦 Installing Python dependencies..."
pip install --upgrade pip
pip install -r requirements.txt

echo ""
echo "🎭 Installing Playwright Chromium..."
# PLAYWRIGHT_BROWSERS_PATH=0을 설정하여 기본 위치 사용
export PLAYWRIGHT_BROWSERS_PATH=0
playwright install chromium

echo ""
echo "🔧 Installing system dependencies for Playwright..."
# Render 환경에서는 일부 의존성 설치가 실패할 수 있음 (권한 문제)
playwright install-deps chromium || {
    echo "⚠️  Warning: Some system dependencies might not be installed"
    echo "    This is normal on Render. Playwright should still work."
}

echo ""
echo "📍 Playwright installation verification..."
echo "   PLAYWRIGHT_BROWSERS_PATH=${PLAYWRIGHT_BROWSERS_PATH}"

echo ""
echo "✅ Build completed successfully!"
echo "========================================="

