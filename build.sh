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
playwright install chromium

echo ""
echo "🔧 Installing system dependencies for Playwright..."
# Render 환경에서는 일부 의존성 설치가 실패할 수 있음 (권한 문제)
playwright install-deps chromium || {
    echo "⚠️  Warning: Some system dependencies might not be installed"
    echo "    This is normal on Render. Playwright should still work."
}

echo ""
echo "✅ Build completed successfully!"
echo "========================================="

