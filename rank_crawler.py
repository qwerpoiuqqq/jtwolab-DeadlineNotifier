"""
네이버 플레이스 순위 크롤링 모듈
애드로그 사이트에서 순위 데이터를 수집하여 DB에 저장
"""
import os
import sqlite3
import logging
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pytz
from playwright.sync_api import sync_playwright, Page, Browser

logger = logging.getLogger(__name__)

# 데이터베이스 설정
DB_PATH = os.getenv("RANK_DB_PATH", "rank_history.db")


class RankDatabase:
    """순위 데이터베이스 관리 클래스"""
    
    def __init__(self, db_path: str = None):
        self.db_path = db_path or DB_PATH
        self._init_db()
    
    def _init_db(self):
        """데이터베이스 초기화"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS rank_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                company TEXT NOT NULL,
                business_name TEXT NOT NULL,
                keyword TEXT NOT NULL,
                rank INTEGER,
                checked_at DATETIME NOT NULL,
                source TEXT DEFAULT 'adlog',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # 인덱스 생성 (조회 성능 향상)
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_business_name 
            ON rank_history(business_name)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_checked_at 
            ON rank_history(checked_at DESC)
        """)
        
        cursor.execute("""
            CREATE INDEX IF NOT EXISTS idx_company_business 
            ON rank_history(company, business_name, checked_at DESC)
        """)
        
        conn.commit()
        conn.close()
        logger.info(f"Database initialized: {self.db_path}")
    
    def save_rank(self, company: str, business_name: str, keyword: str, 
                  rank: Optional[int], checked_at: datetime = None) -> bool:
        """순위 데이터 저장"""
        if checked_at is None:
            kst = pytz.timezone('Asia/Seoul')
            checked_at = datetime.now(kst)
        
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            
            cursor.execute("""
                INSERT INTO rank_history 
                (company, business_name, keyword, rank, checked_at, source)
                VALUES (?, ?, ?, ?, ?, 'adlog')
            """, (company, business_name, keyword, rank, checked_at.isoformat()))
            
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Failed to save rank: {e}")
            return False
    
    def get_latest_ranks(self, company: str = None) -> List[Dict]:
        """최신 순위 데이터 조회 (각 업체의 가장 최근 데이터)"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if company:
            query = """
                SELECT business_name, keyword, rank, checked_at
                FROM rank_history
                WHERE company = ? 
                  AND id IN (
                    SELECT MAX(id)
                    FROM rank_history
                    WHERE company = ?
                    GROUP BY business_name
                  )
                ORDER BY business_name
            """
            cursor.execute(query, (company, company))
        else:
            query = """
                SELECT business_name, keyword, rank, checked_at, company
                FROM rank_history
                WHERE id IN (
                    SELECT MAX(id)
                    FROM rank_history
                    GROUP BY company, business_name
                )
                ORDER BY company, business_name
            """
            cursor.execute(query)
        
        rows = cursor.fetchall()
        conn.close()
        
        results = []
        for row in rows:
            if company:
                business_name, keyword, rank, checked_at = row
                comp = company
            else:
                business_name, keyword, rank, checked_at, comp = row
            
            results.append({
                "company": comp,
                "business_name": business_name,
                "keyword": keyword,
                "rank": rank,
                "checked_at": checked_at
            })
        
        return results
    
    def get_rank_history(self, business_name: str, limit: int = 30) -> List[Dict]:
        """업체별 순위 히스토리 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT keyword, rank, checked_at, company
            FROM rank_history
            WHERE business_name = ?
            ORDER BY checked_at DESC
            LIMIT ?
        """, (business_name, limit))
        
        rows = cursor.fetchall()
        conn.close()
        
        results = []
        for keyword, rank, checked_at, company in rows:
            results.append({
                "keyword": keyword,
                "rank": rank,
                "checked_at": checked_at,
                "company": company
            })
        
        return results


class AdlogCrawler:
    """애드로그 순위 크롤러"""
    
    def __init__(self):
        self.adlog_id = os.getenv("ADLOG_ID", "jtwolab")
        self.adlog_password = os.getenv("ADLOG_PASSWORD", "1234")
        self.adlog_url = os.getenv("ADLOG_URL", 
            "https://www.adlog.kr/adlog/naver_place_rank_check.php?sca=&sfl=api_memo&stx=%EC%9B%94%EB%B3%B4%EC%9E%A5&page_rows=100")
        self.db = RankDatabase()
    
    def crawl_ranks(self, company: str = None) -> Dict:
        """애드로그에서 순위 데이터 크롤링
        
        Args:
            company: 특정 회사만 필터링 (None이면 전체)
            
        Returns:
            {
                "success": bool,
                "crawled_count": int,
                "failed_count": int,
                "message": str,
                "data": List[Dict]
            }
        """
        logger.info(f"Starting rank crawling from Adlog for company: {company or 'ALL'}")
        
        crawled_data = []
        crawled_count = 0
        failed_count = 0
        
        try:
            # Playwright 브라우저 자동 설치 (없을 경우)
            try:
                import subprocess
                import os
                
                # 브라우저 설치 확인 및 자동 설치
                logger.info("Checking Playwright browser installation...")
                result = subprocess.run(
                    ['playwright', 'install', 'chromium'],
                    capture_output=True,
                    text=True,
                    timeout=120
                )
                if result.returncode == 0:
                    logger.info("Chromium browser ready")
                else:
                    logger.warning(f"Browser install warning: {result.stderr}")
            except Exception as install_error:
                logger.warning(f"Browser install check failed: {install_error}")
            
            with sync_playwright() as p:
                # 브라우저 실행 (headless mode, 메모리 최적화)
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',  # Render 메모리 제한 대응
                        '--disable-gpu',
                        '--disable-software-rasterizer',
                        '--disable-extensions',
                        '--single-process',  # 단일 프로세스 모드 (메모리 절약)
                        '--no-zygote'  # Zygote 프로세스 비활성화
                    ]
                )
                context = browser.new_context(
                    viewport={'width': 1280, 'height': 720},  # 해상도 낮춤 (메모리 절약)
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = context.new_page()
                
                logger.info("Browser launched")
                
                # 로그인 페이지로 이동
                login_url = "https://www.adlog.kr/bbs/login.php?url=%2Fadlog%2Fnaver_place_rank_check.php%3Fsca%3D%26sfl%3Dapi_memo%26stx%3D%25EC%259B%2594%25EB%25B3%25B4%25EC%259E%25A5%26page_rows%3D100"
                page.goto(login_url, wait_until="networkidle", timeout=30000)
                logger.info(f"Navigated to login page: {login_url}")
                
                # 로그인 처리
                page.fill('input[name="mb_id"]', self.adlog_id)
                page.fill('input[name="mb_password"]', self.adlog_password)
                logger.info("Credentials filled")
                
                # 로그인 버튼 클릭
                page.click('button[type="submit"], input[type="submit"]')
                logger.info("Login button clicked")
                
                # 페이지 로드 대기
                page.wait_for_load_state("networkidle", timeout=30000)
                logger.info("Login completed, page loaded")
                
                # 테이블 데이터 추출
                # 애드로그 페이지 구조에 맞게 셀렉터 조정 필요
                rows = page.locator('table tbody tr').all()
                logger.info(f"Found {len(rows)} rows in table")
                
                kst = pytz.timezone('Asia/Seoul')
                checked_at = datetime.now(kst)
                
                # 보장건 데이터 로드 (회사 필터링용)
                from guarantee_manager import GuaranteeManager
                gm = GuaranteeManager()
                guarantee_items = gm.get_items()
                if company:
                    guarantee_items = [item for item in guarantee_items 
                                     if item.get("company") == company]
                
                # 상호명 -> 메인 키워드 매핑
                business_to_keyword = {}
                for item in guarantee_items:
                    biz_name = item.get("business_name")
                    main_kw = item.get("main_keyword")
                    if biz_name and main_kw:
                        business_to_keyword[biz_name] = main_kw
                
                logger.info(f"Mapped {len(business_to_keyword)} businesses with keywords")
                
                # 각 행에서 데이터 추출
                for idx, row in enumerate(rows):
                    try:
                        cells = row.locator('td').all()
                        if len(cells) < 3:
                            continue
                        
                        # 셀 데이터 추출 (구조에 맞게 조정 필요)
                        # 일반적으로: [상호명, 키워드, 순위, ...]
                        business_name = cells[0].inner_text().strip()
                        keyword = cells[1].inner_text().strip()
                        rank_text = cells[2].inner_text().strip()
                        
                        # 순위 파싱
                        rank = None
                        if rank_text and rank_text.replace('위', '').isdigit():
                            rank = int(rank_text.replace('위', '').strip())
                        
                        # 회사 매칭 (보장건 데이터 기준)
                        matched_company = None
                        for item in guarantee_items:
                            if item.get("business_name") == business_name:
                                matched_company = item.get("company")
                                break
                        
                        if not matched_company:
                            # 보장건에 없는 업체는 건너뛰기
                            continue
                        
                        # 회사 필터 적용
                        if company and matched_company != company:
                            continue
                        
                        # DB 저장
                        success = self.db.save_rank(
                            matched_company, 
                            business_name, 
                            keyword, 
                            rank, 
                            checked_at
                        )
                        
                        if success:
                            crawled_data.append({
                                "company": matched_company,
                                "business_name": business_name,
                                "keyword": keyword,
                                "rank": rank
                            })
                            crawled_count += 1
                            
                            if idx < 5:  # 처음 5개만 로그
                                logger.info(f"  [{idx+1}] {business_name} / {keyword} → {rank}위")
                        else:
                            failed_count += 1
                            
                    except Exception as e:
                        logger.warning(f"Failed to process row {idx}: {e}")
                        failed_count += 1
                        continue
                
                # 브라우저 종료
                browser.close()
                logger.info("Browser closed")
        
        except Exception as e:
            logger.error(f"Crawling failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            return {
                "success": False,
                "crawled_count": 0,
                "failed_count": 0,
                "message": f"크롤링 실패: {str(e)}",
                "data": []
            }
        
        message = f"순위 크롤링 완료 - 성공: {crawled_count}건, 실패: {failed_count}건"
        logger.info(message)
        
        return {
            "success": True,
            "crawled_count": crawled_count,
            "failed_count": failed_count,
            "message": message,
            "data": crawled_data,
            "checked_at": checked_at.isoformat()
        }


def crawl_ranks_for_company(company: str = None) -> Dict:
    """특정 회사의 순위 크롤링 (외부 호출용)"""
    crawler = AdlogCrawler()
    return crawler.crawl_ranks(company)


def get_latest_ranks(company: str = None) -> List[Dict]:
    """최신 순위 데이터 조회"""
    db = RankDatabase()
    return db.get_latest_ranks(company)


def get_rank_history(business_name: str, limit: int = 30) -> List[Dict]:
    """업체별 순위 히스토리 조회"""
    db = RankDatabase()
    return db.get_rank_history(business_name, limit)


def get_current_rank_for_business(company: str, business_name: str) -> Optional[Dict]:
    """특정 업체의 현재 순위 조회
    
    Returns:
        {"rank": int, "keyword": str, "checked_at": str} or None
    """
    db = RankDatabase()
    latest_ranks = db.get_latest_ranks(company)
    
    for rank_data in latest_ranks:
        if rank_data["business_name"] == business_name:
            return {
                "rank": rank_data["rank"],
                "keyword": rank_data["keyword"],
                "checked_at": rank_data["checked_at"]
            }
    
    return None

