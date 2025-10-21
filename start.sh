#!/bin/bash
# Render 시작 스크립트

# 환경 변수 확인
echo "Python version: $(python --version)"
echo "Starting Flask app with waitress..."

# Flask 앱 시작
python app.py --prod
