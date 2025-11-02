# Playwright 공식 이미지 사용 (모든 브라우저 의존성 포함)
FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy

# 작업 디렉토리 설정
WORKDIR /app

# 시스템 업데이트 및 필수 패키지 설치
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    ca-certificates \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libatk1.0-0 \
    libatspi2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgbm1 \
    libgtk-3-0 \
    libnspr4 \
    libnss3 \
    libwayland-client0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxkbcommon0 \
    libxrandr2 \
    xdg-utils \
    && rm -rf /var/lib/apt/lists/*

# Python 의존성 설치
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Playwright Chromium 및 시스템 의존성 설치
RUN playwright install chromium && \
    playwright install-deps chromium

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

