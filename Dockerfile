# Playwright 공식 이미지 사용 (모든 브라우저 의존성 포함)
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

# 작업 디렉토리 설정
WORKDIR /app

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Playwright Chromium 설치 (이미지에 포함되어 있지만 명시적으로 확인)
RUN playwright install chromium

# 애플리케이션 코드 복사
COPY . .

# 데이터 디렉토리 생성
RUN mkdir -p data

# 환경변수 설정
ENV FLASK_HOST=0.0.0.0
ENV FLASK_PORT=8080
ENV FLASK_DEBUG=false
ENV PLAYWRIGHT_BROWSERS_PATH=0

# 포트 노출
EXPOSE 8080

# 애플리케이션 실행
CMD ["python", "app.py", "--prod"]

