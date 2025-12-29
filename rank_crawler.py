"""
네이버 플레이스 순위 크롤링 모듈
애드로그 사이트에서 순위/저장/리뷰/N2 데이터를 수집하여 Google Sheets 원장에 저장

핵심 설계:
1. row_text에 여러 날짜 데이터가 있으므로 '최신 날짜 블록'만 파싱
2. date는 화면에서 파싱한 날짜, collected_at는 실제 수집 시각
3. 크롤링 전 '리뷰수보기/업체점수보기' 체크박스 상태 확인
4. ADLOG_ID/PASSWORD 기본값 제거, env 없으면 에러
5. Render 중복 실행 방지: cron token endpoint 방식 권장
"""
import os
import re
import sqlite3
import logging
import time
from datetime import datetime
from typing import List, Dict, Optional, Tuple
import pytz
from playwright.sync_api import sync_playwright, Page, Browser

logger = logging.getLogger(__name__)

# 데이터베이스 설정 (레거시 호환용)
DB_PATH = os.getenv("RANK_DB_PATH", "rank_history.db")

# 한국 시간대
KST = pytz.timezone('Asia/Seoul')


# =============================================================================
# 환경변수 필수 체크
# =============================================================================

def get_adlog_credentials() -> Tuple[str, str]:
    """애드로그 로그인 정보 가져오기 (기본값 없음, env 필수)
    
    Returns:
        (adlog_id, adlog_password)
        
    Raises:
        ValueError: 환경변수가 설정되지 않은 경우
    """
    adlog_id = os.getenv("ADLOG_ID")
    adlog_password = os.getenv("ADLOG_PASSWORD")
    
    if not adlog_id:
        raise ValueError("ADLOG_ID 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
    if not adlog_password:
        raise ValueError("ADLOG_PASSWORD 환경변수가 설정되지 않았습니다. .env 파일을 확인하세요.")
    
    return adlog_id, adlog_password


# =============================================================================
# 날짜 블록 파싱 유틸리티
# =============================================================================

def parse_date_marker(text: str) -> Optional[str]:
    """날짜 마커 파싱 (예: '12-28(토)' -> '2025-12-28')
    
    Args:
        text: 날짜 마커 텍스트
        
    Returns:
        YYYY-MM-DD 형식 날짜 또는 None
    """
    # 패턴: MM-DD(요일) 또는 MM-DD 또는 M-DD
    match = re.search(r'(\d{1,2})-(\d{1,2})(?:\([^)]+\))?', text)
    if match:
        month = int(match.group(1))
        day = int(match.group(2))
        
        # 유효성 검사: 월은 1-12, 일은 1-31
        if not (1 <= month <= 12 and 1 <= day <= 31):
            return None
        
        # 현재 연도 기준 (12월 데이터가 1월에 조회될 수 있으므로 처리)
        now = datetime.now(KST)
        year = now.year
        
        # 현재 1-2월인데 파싱한 월이 11-12월이면 작년
        if now.month <= 2 and month >= 11:
            year -= 1
        # 현재 11-12월인데 파싱한 월이 1-2월이면 내년
        elif now.month >= 11 and month <= 2:
            year += 1
        
        return f"{year}-{month:02d}-{day:02d}"
    
    return None


def find_date_blocks(row_text: str) -> List[Tuple[str, int, int]]:
    """행 텍스트에서 날짜 블록 위치 찾기
    
    Args:
        row_text: 전체 행 텍스트
        
    Returns:
        [(날짜, 시작위치, 끝위치), ...] - 최신 날짜순
    """
    # 날짜 마커 패턴: MM-DD(요일) 형식
    pattern = r'(\d{1,2}-\d{1,2})\s*(?:\([^)]+\))?'
    
    blocks = []
    for match in re.finditer(pattern, row_text):
        date_str = parse_date_marker(match.group(0))
        if date_str:
            blocks.append((date_str, match.start(), match.end()))
    
    # 가장 최신 날짜가 먼저 오도록 정렬 (날짜 역순)
    blocks.sort(key=lambda x: x[0], reverse=True)
    
    return blocks


def extract_latest_date_block(row_text: str) -> Tuple[Optional[str], str]:
    """최신 날짜 블록만 추출
    
    행 텍스트에서 여러 날짜의 rank/N2가 반복되므로,
    '첫 날짜 마커 ~ 두 번째 날짜 마커 전' 영역만 추출
    
    Args:
        row_text: 전체 행 텍스트
        
    Returns:
        (파싱된 날짜, 최신 블록 텍스트)
    """
    # 날짜 마커 패턴: MM-DD(요일) 형식
    pattern = r'\d{1,2}-\d{1,2}\s*(?:\([^)]+\))?'
    
    matches = list(re.finditer(pattern, row_text))
    
    if not matches:
        # 날짜 마커가 없으면 전체 텍스트 반환
        return None, row_text
    
    # 첫 번째 날짜 마커
    first_match = matches[0]
    first_date = parse_date_marker(first_match.group(0))
    
    if len(matches) >= 2:
        # 두 번째 날짜 마커 이전까지만 추출
        second_match = matches[1]
        block_text = row_text[first_match.start():second_match.start()]
    else:
        # 날짜 마커가 하나면 그 이후 전체
        block_text = row_text[first_match.start():]
    
    return first_date, block_text


def extract_data_from_block(block_text: str) -> Dict:
    """날짜 블록에서 순위/블/방/N2 추출
    
    Args:
        block_text: 날짜 블록 텍스트
        
    Returns:
        {rank, saves, blog_reviews, visitor_reviews, n2_score}
    """
    data = {
        "rank": None,
        "saves": None,
        "blog_reviews": None,
        "visitor_reviews": None,
        "n2_score": None,
    }
    
    # N2 추출: "N2 0.439588" 패턴 (주황색 텍스트)
    n2_match = re.search(r'N2\s*([0-9.]+)', block_text, re.IGNORECASE)
    if n2_match:
        try:
            data["n2_score"] = float(n2_match.group(1))
        except ValueError:
            pass
    
    # 순위 추출: "3위" 또는 단독 숫자
    # 블록 앞부분에서 첫 번째 순위 추출
    rank_match = re.search(r'(\d+)\s*위', block_text)
    if rank_match:
        data["rank"] = int(rank_match.group(1))
    
    # 저장 수: "저 X,XXX" 패턴
    saves_match = re.search(r'저\s*([0-9,]+)', block_text)
    if saves_match:
        try:
            data["saves"] = int(saves_match.group(1).replace(',', ''))
        except ValueError:
            pass
    
    # 블로그 리뷰: "블 XXX" 패턴
    blog_match = re.search(r'블\s*([0-9,]+)', block_text)
    if blog_match:
        try:
            data["blog_reviews"] = int(blog_match.group(1).replace(',', ''))
        except ValueError:
            pass
    
    # 방문자 리뷰: "방 XXX" 패턴
    visitor_match = re.search(r'방\s*([0-9,]+)', block_text)
    if visitor_match:
        try:
            data["visitor_reviews"] = int(visitor_match.group(1).replace(',', ''))
        except ValueError:
            pass
    
    return data


# =============================================================================
# RankDatabase (레거시 호환 - SQLite)
# =============================================================================

class RankDatabase:
    """순위 데이터베이스 관리 클래스 (레거시 호환용)"""
    
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
        
        # 인덱스 생성
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
            checked_at = datetime.now(KST)
        
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
        """최신 순위 데이터 조회"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        if company:
            query = """
                SELECT business_name, keyword, rank, checked_at
                FROM rank_history
                WHERE company = ? 
                  AND id IN (
                    SELECT MAX(id) FROM rank_history
                    WHERE company = ? GROUP BY business_name
                  )
                ORDER BY business_name
            """
            cursor.execute(query, (company, company))
        else:
            query = """
                SELECT business_name, keyword, rank, checked_at, company
                FROM rank_history
                WHERE id IN (
                    SELECT MAX(id) FROM rank_history
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
        
        return [
            {"keyword": kw, "rank": r, "checked_at": ca, "company": co}
            for kw, r, ca, co in rows
        ]


# =============================================================================
# AdlogCrawler - N2 추출 + Google Sheets 저장
# =============================================================================

class AdlogCrawler:
    """애드로그 순위 크롤러 (N2 추출 + Google Sheets 저장)
    
    핵심 기능:
    - 체크박스 상태 맞춤 (리뷰수보기/업체점수보기)
    - 최신 날짜 블록만 파싱
    - Google Sheets 원장 저장
    """
    
    def __init__(self):
        # 환경변수 필수 체크 (기본값 없음)
        self.adlog_id, self.adlog_password = get_adlog_credentials()
        
        self.adlog_url = os.getenv("ADLOG_URL", 
            "https://www.adlog.kr/adlog/naver_place_rank_check.php?sca=&sfl=api_memo&stx=%EC%9B%94%EB%B3%B4%EC%9E%A5&page_rows=100")
        
        # Headless 모드 설정
        self.headless = os.getenv("HEADLESS", "true").lower() == "true"
        
        # 스크린샷 저장 설정
        self.save_screenshots = os.getenv("SAVE_SCREENSHOTS", "false").lower() == "true"
        self.screenshot_path = os.getenv("SCREENSHOT_PATH", "/tmp/rank_crawler_screenshots")
        
        # 저장소 선택
        self.storage_mode = os.getenv("RANK_STORAGE_MODE", "sheets").lower()
        
        # 레거시 SQLite
        self.db = RankDatabase()
        
        # Google Sheets 매니저 (lazy init)
        self._snapshot_manager = None
    
    @property
    def snapshot_manager(self):
        """RankSnapshotManager lazy initialization"""
        if self._snapshot_manager is None:
            try:
                from rank_snapshot_manager import RankSnapshotManager
                self._snapshot_manager = RankSnapshotManager()
            except ImportError as e:
                logger.warning(f"RankSnapshotManager not available: {e}")
                self._snapshot_manager = None
        return self._snapshot_manager
    
    def _get_monitoring_targets(self, company: str = None) -> List[Dict]:
        """모니터링 대상 목록 가져오기 (진행중/후불 상태만)"""
        from guarantee_manager import GuaranteeManager
        gm = GuaranteeManager()
        
        # 전체 아이템 가져오기
        items = gm.get_items()
        
        # 진행중/후불 필터
        active_statuses = ['진행중', '후불']
        items = [item for item in items 
                 if item.get("status") in active_statuses]
        
        # 회사 필터
        if company:
            items = [item for item in items 
                     if item.get("company") == company]
        
        # 필수 필드가 있는 항목만 반환
        targets = []
        for item in items:
            if item.get("business_name") and item.get("main_keyword"):
                targets.append({
                    "business_name": item.get("business_name"),
                    "keyword": item.get("main_keyword"),
                    "place_url": item.get("url", ""),
                    "company": item.get("company", ""),
                    "agency": item.get("agency", ""),
                })
        
        return targets
    
    def _ensure_checkboxes(self, page: Page) -> None:
        """체크박스 상태 확인 및 설정
        
        '리뷰수 보기', '업체점수 보기' 체크박스를 원하는 상태로 맞춤
        """
        try:
            # 체크박스 셀렉터 (애드로그 페이지 구조에 맞게 조정 필요)
            checkboxes = {
                "리뷰수보": "input[name='show_review']",  # 예시
                "업체점수보": "input[name='show_score']",  # 예시
            }
            
            # 더 일반적인 접근: 체크박스 레이블로 찾기
            # 실제 셀렉터는 페이지 구조에 따라 조정 필요
            
            # PC리뷰보 체크박스
            pc_review = page.locator('text=PC리뷰보').first
            if pc_review.count() > 0:
                checkbox = pc_review.locator('xpath=preceding-sibling::input[1]')
                if checkbox.count() > 0 and not checkbox.is_checked():
                    checkbox.check()
                    logger.info("Checked 'PC리뷰보' checkbox")
            
            # 리뷰수보 체크박스  
            review_count = page.locator('text=리뷰수보').first
            if review_count.count() > 0:
                checkbox = review_count.locator('xpath=preceding-sibling::input[1]')
                if checkbox.count() > 0 and not checkbox.is_checked():
                    checkbox.check()
                    logger.info("Checked '리뷰수보' checkbox")
            
            # 업체점수보 체크박스
            score = page.locator('text=업체점수보').first
            if score.count() > 0:
                checkbox = score.locator('xpath=preceding-sibling::input[1]')
                if checkbox.count() > 0 and not checkbox.is_checked():
                    checkbox.check()
                    logger.info("Checked '업체점수보' checkbox")
            
            # 페이지 새로고침 (체크박스 변경 적용)
            page.wait_for_timeout(500)
            
        except Exception as e:
            logger.warning(f"Checkbox handling warning: {e}")
    
    def _parse_row(self, row_element, targets_map: Dict, url_map: Dict = None) -> Optional[Dict]:
        """행 데이터 파싱 (최신 날짜 블록만)
        
        애드로그 테이블 구조:
        - [0] 체크박스
        - [1] 그룹명
        - [2] 검색 키워드 ID (숫자)
        - [3] 플레이스 URL/상호명 (링크 텍스트가 상호명)
        - [4~] 날짜별 순위 데이터
        
        Args:
            row_element: Playwright row element
            targets_map: {상호명: target_info} 매핑
            url_map: {place_id: target_info} 매핑 (추가 매칭용)
            
        Returns:
            파싱된 데이터 딕셔너리 또는 None
        """
        try:
            # 행 전체 텍스트
            row_text = row_element.inner_text()
            
            # 셀 추출
            cells = row_element.locator('td').all()
            if len(cells) < 5:
                return None
            
            # === 상호명 추출 ===
            # 방법 1: 플레이스 URL 셀(3번째)에서 링크 텍스트 찾기
            business_name = ""
            place_url_from_page = ""
            keyword_from_page = ""
            
            # 셀 3 또는 4에서 place.naver.com 링크 찾기
            for cell_idx in [3, 4, 2]:
                if cell_idx >= len(cells):
                    continue
                try:
                    cell = cells[cell_idx]
                    links = cell.locator('a').all()
                    for link in links:
                        href = link.get_attribute('href') or ""
                        if 'place.naver.com' in href or 'm.place.naver.com' in href:
                            place_url_from_page = href
                            # 링크 텍스트가 상호명 (단, URL이 아닌 경우)
                            link_text = link.inner_text().strip()
                            if link_text and 'place.naver.com' not in link_text and 'http' not in link_text:
                                business_name = link_text
                            break
                except:
                    continue
                if place_url_from_page:
                    break
            
            # 방법 2: 상호명이 없으면 행 텍스트에서 추출 시도
            if not business_name:
                # 행 텍스트에서 상호명 패턴 찾기
                # 보통 "상호명" 형태로 나타남
                import re
                # place URL 앞의 텍스트 추출
                if '제이투랩' in row_text or '일류기획' in row_text:
                    # 그룹명 이후, URL 이전 텍스트
                    parts = row_text.split('\n')
                    for part in parts:
                        part = part.strip()
                        # URL이나 날짜가 아닌 2글자 이상 텍스트
                        if len(part) >= 2 and 'place' not in part and 'http' not in part:
                            if not re.match(r'^[\d\-\.\s\(\)]+$', part):  # 숫자/날짜가 아님
                                if '등록자' not in part and '메모' not in part:
                                    business_name = part
                                    break
            
            # === 키워드 추출 ===
            # 셀 내용에서 키워드 힌트 찾기
            try:
                # 검색 키워드 셀 (보통 2번째 또는 그 근처)
                for cell_idx in [2, 1]:
                    if cell_idx >= len(cells):
                        continue
                    cell_text = cells[cell_idx].inner_text().strip()
                    # 숫자만 있는 경우 무시
                    if cell_text and not cell_text.isdigit():
                        keyword_from_page = cell_text
                        break
            except:
                pass
            
            # === place_id 추출 ===
            place_id = ""
            if place_url_from_page:
                import re
                match = re.search(r'/(\d{5,})', place_url_from_page)
                if match:
                    place_id = match.group(1)
            
            # === 타겟 매칭 ===
            target = None
            matched_by = ""
            
            # 1단계: place_id로 URL 매칭 (가장 정확)
            if url_map and place_id:
                if place_id in url_map:
                    target = url_map[place_id]
                    matched_by = f"place_id:{place_id}"
            
            # 2단계: 상호명 정확 매칭
            if not target and business_name:
                target = targets_map.get(business_name)
                if target:
                    matched_by = f"exact:{business_name}"
            
            # 3단계: 상호명 부분 매칭
            if not target and business_name:
                for target_name, target_info in targets_map.items():
                    # 애드로그 상호명이 타겟에 포함
                    if business_name in target_name:
                        target = target_info
                        matched_by = f"partial:{business_name} in {target_name}"
                        break
                    # 타겟이 애드로그 상호명에 포함
                    if target_name in business_name:
                        target = target_info
                        matched_by = f"partial:{target_name} in {business_name}"
                        break
            
            # 4단계: 키워드 매칭 (마지막 수단)
            if not target and keyword_from_page:
                for target_name, target_info in targets_map.items():
                    if target_info.get("keyword") == keyword_from_page:
                        target = target_info
                        matched_by = f"keyword:{keyword_from_page}"
                        break
            
            if not target:
                return None
            
            # === 최신 날짜 셀에서 데이터 추출 ===
            # 애드로그 테이블 구조: [체크박스][그룹][키워드ID][URL][날짜1][날짜2]...
            # 날짜 셀은 인덱스 4부터 시작 (첫 번째 날짜가 가장 최근)
            
            parsed_date = None
            block_data = {"rank": None, "saves": None, "blog_reviews": None, "visitor_reviews": None, "n2_score": None}
            
            # 날짜 셀 탐색 (인덱스 4부터)
            for cell_idx in range(4, min(len(cells), 20)):  # 최대 20개 셀까지
                try:
                    cell = cells[cell_idx]
                    cell_text = cell.inner_text().strip()
                    
                    if not cell_text:
                        continue
                    
                    # 날짜 마커 확인 (예: "12-28(토)")
                    date_match = re.search(r'(\d{1,2})-(\d{1,2})\s*(?:\([^)]+\))?', cell_text)
                    if date_match:
                        month = int(date_match.group(1))
                        day = int(date_match.group(2))
                        
                        # 유효한 날짜인지 확인
                        if 1 <= month <= 12 and 1 <= day <= 31:
                            # 연도 계산
                            now = datetime.now(KST)
                            year = now.year
                            if now.month <= 2 and month >= 11:
                                year -= 1
                            elif now.month >= 11 and month <= 2:
                                year += 1
                            parsed_date = f"{year}-{month:02d}-{day:02d}"
                            
                            # 이 셀에서 순위 데이터 추출
                            # 순위: "3위" 패턴
                            rank_match = re.search(r'(\d+)\s*위', cell_text)
                            if rank_match:
                                block_data["rank"] = int(rank_match.group(1))
                            
                            # N2 점수: "N2 0.439" 패턴
                            n2_match = re.search(r'N2\s*([0-9.]+)', cell_text, re.IGNORECASE)
                            if n2_match:
                                try:
                                    block_data["n2_score"] = float(n2_match.group(1))
                                except ValueError:
                                    pass
                            
                            # 저장 수: "저 2,419" 또는 "저장 2,419" 패턴
                            saves_match = re.search(r'저[장]?\s*([0-9,]+)', cell_text)
                            if saves_match:
                                try:
                                    block_data["saves"] = int(saves_match.group(1).replace(',', ''))
                                except ValueError:
                                    pass
                            
                            # 블로그 리뷰: "블 243" 패턴
                            blog_match = re.search(r'블\s*([0-9,]+)', cell_text)
                            if blog_match:
                                try:
                                    block_data["blog_reviews"] = int(blog_match.group(1).replace(',', ''))
                                except ValueError:
                                    pass
                            
                            # 방문자 리뷰: "방 1,189" 패턴
                            visitor_match = re.search(r'방\s*([0-9,]+)', cell_text)
                            if visitor_match:
                                try:
                                    block_data["visitor_reviews"] = int(visitor_match.group(1).replace(',', ''))
                                except ValueError:
                                    pass
                            
                            # 첫 번째 유효한 날짜 셀을 찾으면 종료
                            break
                            
                except Exception as cell_error:
                    logger.debug(f"Cell {cell_idx} parse error: {cell_error}")
                    continue
            
            # 날짜가 없으면 오늘로 설정
            if not parsed_date:
                parsed_date = datetime.now(KST).strftime("%Y-%m-%d")
            
            # 디버그 로그
            if matched_by:
                logger.debug(f"Matched by {matched_by}: date={parsed_date}, rank={block_data.get('rank')}, n2={block_data.get('n2_score')}")
            
            # 결과 조합 (상호명은 타겟에서 가져오기)
            return {
                "date": parsed_date,
                "client_name": target.get("business_name") or business_name,  # 타겟 상호명 우선
                "keyword": target.get("keyword", "") or keyword_from_page,
                "place_url": target.get("place_url", "") or place_url_from_page,
                "group": target.get("company", ""),
                "rank": block_data.get("rank"),
                "saves": block_data.get("saves"),
                "blog_reviews": block_data.get("blog_reviews"),
                "visitor_reviews": block_data.get("visitor_reviews"),
                "n2_score": block_data.get("n2_score"),
            }
            
        except Exception as e:
            logger.warning(f"Row parsing error: {e}")
            import traceback
            logger.debug(traceback.format_exc())
            return None
    
    def crawl_ranks(self, company: str = None) -> Dict:
        """애드로그에서 순위 데이터 크롤링
        
        Args:
            company: 특정 회사만 필터링 (None이면 전체)
            
        Returns:
            크롤링 결과 딕셔너리
        """
        start_time = time.time()
        logger.info(f"Starting rank crawling from Adlog for company: {company or 'ALL'}")
        logger.info(f"Storage mode: {self.storage_mode}, Headless: {self.headless}")
        
        crawled_data = []
        failed_details = []
        crawled_count = 0
        failed_count = 0
        
        # 현재 시간대
        now = datetime.now(KST)
        time_slot = "09:00" if now.hour < 12 else "15:00"
        collected_at = now.isoformat()
        
        try:
            # Playwright 브라우저 설치 확인
            try:
                import subprocess
                logger.info("Checking Playwright browser installation...")
                result = subprocess.run(
                    ['playwright', 'install', 'chromium'],
                    capture_output=True, text=True, timeout=120
                )
                if result.returncode == 0:
                    logger.info("Chromium browser ready")
            except Exception as install_error:
                logger.warning(f"Browser install check failed: {install_error}")
            
            with sync_playwright() as p:
                logger.info("Launching Chromium browser...")
                
                browser = p.chromium.launch(
                    headless=self.headless,
                    args=[
                        '--no-sandbox',
                        '--disable-setuid-sandbox',
                        '--disable-dev-shm-usage',
                        '--disable-gpu',
                        '--disable-software-rasterizer',
                        '--disable-extensions',
                        '--single-process',
                        '--no-zygote'
                    ]
                )
                logger.info("✅ Browser launched")
                
                context = browser.new_context(
                    viewport={'width': 1280, 'height': 720},
                    user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                )
                page = context.new_page()
                
                # 로그인
                login_url = "https://www.adlog.kr/bbs/login.php?url=%2Fadlog%2Fnaver_place_rank_check.php%3Fsca%3D%26sfl%3Dapi_memo%26stx%3D%25EC%259B%2594%25EB%25B3%25B4%25EC%259E%25A5%26page_rows%3D100"
                logger.info("Navigating to login page...")
                
                page.goto(login_url, wait_until="domcontentloaded", timeout=60000)
                logger.info("✅ Login page loaded")
                
                # 로그인 정보 입력
                page.fill('input[name="mb_id"]', self.adlog_id)
                page.fill('input[name="mb_password"]', self.adlog_password)
                logger.info("✅ Credentials filled")
                
                # 로그인 버튼 클릭
                page.click('button[type="submit"], input[type="submit"]')
                logger.info("✅ Login button clicked")
                
                # 페이지 로드 대기
                page.wait_for_load_state("domcontentloaded", timeout=60000)
                page.wait_for_timeout(2000)
                logger.info("✅ Login completed")
                
                # 체크박스 상태 확인/설정
                self._ensure_checkboxes(page)
                
                # 테이블 데이터 추출
                logger.info("Looking for table data...")
                
                # 모든 행 가져오기
                all_rows = page.locator('table tbody tr').all()
                logger.info(f"✅ Found {len(all_rows)} total rows")
                
                # 모니터링 대상 로드
                targets = self._get_monitoring_targets(company)
                logger.info(f"Loaded {len(targets)} monitoring targets")
                
                # 디버그: 처음 5개 타겟 출력
                for i, t in enumerate(targets[:5]):
                    logger.info(f"  Target {i+1}: {t.get('business_name')} / {t.get('keyword')}")
                
                # 타겟 → 상호명 맵 (빠른 매칭용)
                targets_map = {t["business_name"]: t for t in targets}
                
                # URL 맵 (place_url 기반 매칭용)
                url_map = {}
                for t in targets:
                    url = t.get("place_url", "")
                    if url:
                        import re
                        match = re.search(r'/(\d{5,})', url)
                        if match:
                            url_map[match.group(1)] = t
                
                logger.info(f"Created {len(targets_map)} name mappings, {len(url_map)} URL mappings")
                
                # 스냅샷 저장용 레코드
                snapshot_records = []
                
                # 기본정보 행 + 순위 행(tr.tr2)을 쌍으로 처리
                # 기본정보 행에서 상호명/URL 추출, 순위 행에서 순위/N2 추출
                current_info = None  # 현재 기본정보 행 데이터
                processed_count = 0
                
                for idx, row in enumerate(all_rows):
                    if idx % 20 == 0:
                        logger.info(f"Processing row {idx+1}/{len(all_rows)}...")
                    
                    try:
                        # 현재 행이 순위 행(tr.tr2)인지 확인
                        row_class = row.get_attribute('class') or ""
                        is_rank_row = 'tr2' in row_class
                        
                        if not is_rank_row:
                            # === 기본정보 행: 상호명/URL 추출 ===
                            business_name = ""
                            place_url = ""
                            place_id = ""
                            
                            # 링크에서 place.naver.com URL과 상호명 찾기
                            links = row.locator('a').all()
                            for link in links:
                                href = link.get_attribute('href') or ""
                                if 'place.naver.com' in href:
                                    place_url = href
                                    link_text = link.inner_text().strip()
                                    if link_text and 'http' not in link_text and len(link_text) > 1:
                                        business_name = link_text
                                    # place_id 추출
                                    id_match = re.search(r'/(\d{5,})', href)
                                    if id_match:
                                        place_id = id_match.group(1)
                                    break
                            
                            # 현재 기본정보 저장 (다음 순위 행에서 사용)
                            current_info = {
                                "business_name": business_name,
                                "place_url": place_url,
                                "place_id": place_id,
                            }
                            
                            # 디버그 (처음 3개)
                            if processed_count < 3 and business_name:
                                logger.info(f"  Info row: business_name='{business_name}', place_id='{place_id}'")
                            
                        else:
                            # === 순위 행(tr.tr2): 순위/N2 추출 ===
                            if not current_info:
                                continue
                            
                            # stat_div 찾기
                            stat_divs = row.locator('div.stat_div').all()
                            if not stat_divs:
                                stat_divs = row.locator('[class*="stat"]').all()
                            
                            if not stat_divs:
                                continue
                            
                            # 첫 번째 stat_div (최신 날짜)
                            first_stat_div = stat_divs[0]
                            stat_text = first_stat_div.inner_text().strip()
                            
                            # 디버그: 처음 3개 stat_text 출력
                            if processed_count < 3:
                                logger.info(f"  Rank row stat_text: {stat_text[:80]}...")
                            
                            # === 순위 데이터 파싱 ===
                            # 날짜 추출: "12-28(일)" 형식
                            parsed_date = None
                            date_match = re.search(r'(\d{1,2})-(\d{1,2})', stat_text)
                            if date_match:
                                month, day = int(date_match.group(1)), int(date_match.group(2))
                                if 1 <= month <= 12 and 1 <= day <= 31:
                                    year = now.year
                                    if now.month <= 2 and month >= 11:
                                        year -= 1
                                    elif now.month >= 11 and month <= 2:
                                        year += 1
                                    parsed_date = f"{year}-{month:02d}-{day:02d}"
                            
                            # 순위 추출: "12위" 형식
                            rank_val = None
                            rank_match = re.search(r'(\d+)\s*위', stat_text)
                            if rank_match:
                                rank_val = int(rank_match.group(1))
                            
                            # 저장수 추출: fc_grn 클래스 또는 텍스트에서
                            saves_val = None
                            try:
                                saves_elem = first_stat_div.locator('b.fc_grn').first
                                if saves_elem.count() > 0:
                                    saves_text = saves_elem.inner_text().strip()
                                    saves_match = re.search(r'([0-9,]+)', saves_text)
                                    if saves_match:
                                        saves_val = int(saves_match.group(1).replace(',', ''))
                            except:
                                pass
                            
                            # 블로그 리뷰: "블 393개" 형식
                            blog_val = None
                            blog_match = re.search(r'블\s*([0-9,]+)\s*개?', stat_text)
                            if blog_match:
                                blog_val = int(blog_match.group(1).replace(',', ''))
                            
                            # 방문자 리뷰: "방 287개" 형식
                            visitor_val = None
                            visitor_match = re.search(r'방\s*([0-9,]+)\s*개?', stat_text)
                            if visitor_match:
                                visitor_val = int(visitor_match.group(1).replace(',', ''))
                            
                            # N2 점수: "N2 0.439588" 형식
                            n2_val = None
                            n2_match = re.search(r'N2\s*([0-9.]+)', stat_text, re.IGNORECASE)
                            if n2_match:
                                try:
                                    n2_val = float(n2_match.group(1))
                                except ValueError:
                                    pass
                            
                            # === 타겟 매칭 ===
                            business_name = current_info["business_name"]
                            place_url = current_info["place_url"]
                            place_id = current_info["place_id"]
                            
                            target = None
                            matched_by = ""
                            
                            # 1단계: place_id로 URL 매칭 (가장 정확)
                            if url_map and place_id:
                                if place_id in url_map:
                                    target = url_map[place_id]
                                    matched_by = f"place_id:{place_id}"
                            
                            # 2단계: 상호명 정확 매칭
                            if not target and business_name:
                                target = targets_map.get(business_name)
                                if target:
                                    matched_by = f"exact:{business_name}"
                            
                            # 3단계: 상호명 부분 매칭
                            if not target and business_name:
                                for target_name, target_info in targets_map.items():
                                    if business_name in target_name or target_name in business_name:
                                        target = target_info
                                        matched_by = f"partial:{business_name}"
                                        break
                            
                            if not target:
                                if processed_count < 3:
                                    logger.warning(f"  No match: business='{business_name}', place_id='{place_id}'")
                                current_info = None  # 리셋
                                continue
                            
                            # 날짜가 없으면 오늘
                            if not parsed_date:
                                parsed_date = now.strftime("%Y-%m-%d")
                            
                            # 결과 생성
                            parsed = {
                                "date": parsed_date,
                                "time_slot": time_slot,
                                "collected_at": collected_at,
                                "agency": target.get("agency", ""),  # 대행사명
                                "client_name": target.get("business_name") or business_name,
                                "keyword": target.get("keyword", ""),
                                "place_url": target.get("place_url", "") or place_url,
                                "group": target.get("company", ""),
                                "rank": rank_val,
                                "saves": saves_val,
                                "blog_reviews": blog_val,
                                "visitor_reviews": visitor_val,
                                "n2_score": n2_val,
                            }
                            
                            # 디버그 로그 (처음 5개)
                            if processed_count < 5:
                                logger.info(
                                    f"  ✅ [{processed_count+1}] {parsed['client_name'][:15]} / {parsed['keyword']} → "
                                    f"순위:{rank_val}, N2:{n2_val}, 저장:{saves_val}"
                                )
                            
                            # Google Sheets 저장용
                            if self.storage_mode in ["sheets", "both"] and self.snapshot_manager:
                                snapshot_records.append(parsed)
                            
                            # SQLite 저장 (레거시)
                            if self.storage_mode in ["sqlite", "both"]:
                                self.db.save_rank(
                                    parsed.get("group", ""),
                                    parsed.get("client_name", ""),
                                    parsed.get("keyword", ""),
                                    parsed.get("rank"),
                                    now
                                )
                            
                            crawled_data.append(parsed)
                            crawled_count += 1
                            processed_count += 1
                            
                            # 현재 정보 리셋 (다음 기본정보 행까지)
                            current_info = None
                            
                    except Exception as e:
                        logger.warning(f"Row {idx} error: {e}")
                        failed_details.append({"row": idx, "reason": str(e)})
                        failed_count += 1
                
                # Google Sheets 배치 저장
                if snapshot_records and self.snapshot_manager:
                    logger.info(f"Saving {len(snapshot_records)} records to Google Sheets...")
                    try:
                        upsert_result = self.snapshot_manager.upsert_bulk(snapshot_records)
                        logger.info(f"✅ Sheets save result: {upsert_result}")
                    except Exception as sheets_error:
                        logger.error(f"Sheets save error: {sheets_error}")
                
                browser.close()
                logger.info("✅ Browser closed")
        
        except ValueError as ve:
            # 환경변수 오류 (ADLOG_ID/PASSWORD 미설정)
            logger.error(f"❌ Configuration error: {ve}")
            return {
                "success": False,
                "crawled_count": 0,
                "failed_count": 0,
                "message": str(ve),
                "data": [],
                "failed_details": [{"error": str(ve)}],
                "elapsed_seconds": 0
            }
        
        except Exception as e:
            logger.error(f"❌ Crawling failed: {e}")
            import traceback
            logger.error(traceback.format_exc())
            
            elapsed = time.time() - start_time
            
            if self.snapshot_manager:
                try:
                    self.snapshot_manager.log_execution({
                        "success_count": 0,
                        "failed_count": 1,
                        "elapsed_seconds": round(elapsed, 2),
                        "message": f"크롤링 실패: {str(e)}",
                        "failed_details": [{"error": str(e)}]
                    })
                except:
                    pass
            
            return {
                "success": False,
                "crawled_count": 0,
                "failed_count": 0,
                "message": f"크롤링 실패: {str(e)}",
                "data": [],
                "failed_details": [{"error": str(e)}],
                "elapsed_seconds": round(time.time() - start_time, 2)
            }
        
        elapsed = time.time() - start_time
        message = f"순위 크롤링 완료 - 성공: {crawled_count}건, 실패: {failed_count}건 ({elapsed:.1f}초)"
        logger.info(f"🎉 {message}")
        
        # 실행 로그 기록
        if self.snapshot_manager:
            try:
                self.snapshot_manager.log_execution({
                    "success_count": crawled_count,
                    "failed_count": failed_count,
                    "elapsed_seconds": round(elapsed, 2),
                    "message": message,
                    "failed_details": failed_details[:10]
                })
            except Exception as log_error:
                logger.warning(f"Log error: {log_error}")
        
        return {
            "success": True,
            "crawled_count": crawled_count,
            "failed_count": failed_count,
            "message": message,
            "data": crawled_data,
            "failed_details": failed_details[:20],
            "elapsed_seconds": round(elapsed, 2),
            "checked_at": collected_at
        }
    
    def _save_screenshot(self, page: Page, name: str) -> None:
        """에러 스크린샷 저장"""
        try:
            os.makedirs(self.screenshot_path, exist_ok=True)
            timestamp = datetime.now(KST).strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join(self.screenshot_path, f"{name}_{timestamp}.png")
            page.screenshot(path=filepath)
            logger.info(f"Screenshot saved: {filepath}")
        except Exception as e:
            logger.warning(f"Screenshot error: {e}")


# =============================================================================
# 외부 호출용 함수
# =============================================================================

def crawl_ranks_for_company(company: str = None) -> Dict:
    """특정 회사의 순위 크롤링 (외부 호출용)"""
    crawler = AdlogCrawler()
    return crawler.crawl_ranks(company)


def get_latest_ranks(company: str = None) -> List[Dict]:
    """최신 순위 데이터 조회 (SQLite 기반)"""
    db = RankDatabase()
    return db.get_latest_ranks(company)


def get_latest_ranks_from_sheets(company: str = None) -> List[Dict]:
    """최신 순위 데이터 조회 (Google Sheets 기반)"""
    try:
        from rank_snapshot_manager import RankSnapshotManager
        manager = RankSnapshotManager()
        history = manager.get_history(days=1)
        
        if company:
            history = [h for h in history if h.get("group") == company]
        
        return history
    except Exception as e:
        logger.error(f"Sheets rank error: {e}")
        return []


def get_rank_history(business_name: str, limit: int = 30) -> List[Dict]:
    """업체별 순위 히스토리 조회"""
    db = RankDatabase()
    return db.get_rank_history(business_name, limit)


def get_current_rank_for_business(company: str, business_name: str) -> Optional[Dict]:
    """특정 업체의 현재 순위 조회"""
    # Sheets에서 조회 시도
    try:
        from rank_snapshot_manager import RankSnapshotManager
        manager = RankSnapshotManager()
        history = manager.get_history(days=1)
        
        for record in history:
            if record.get("client_name") == business_name:
                return {
                    "rank": int(record.get("rank")) if record.get("rank") else None,
                    "keyword": record.get("keyword"),
                    "checked_at": record.get("collected_at"),
                    "n2_score": float(record.get("n2_score")) if record.get("n2_score") else None,
                    "saves": int(record.get("saves")) if record.get("saves") else None,
                }
    except Exception as e:
        logger.warning(f"Sheets lookup failed: {e}")
    
    # SQLite 폴백
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


# =============================================================================
# Cron Token Endpoint용 함수 (Render 중복 실행 방지)
# =============================================================================

def crawl_ranks_with_token(token: str, company: str = None) -> Dict:
    """토큰 인증 후 크롤링 실행 (외부 cron 서비스용)
    
    Args:
        token: CRON_TOKEN 환경변수와 비교할 토큰
        company: 회사 필터
        
    Returns:
        크롤링 결과 또는 인증 오류
    """
    expected_token = os.getenv("CRON_TOKEN")
    
    if not expected_token:
        return {
            "success": False,
            "message": "CRON_TOKEN이 설정되지 않았습니다."
        }
    
    if token != expected_token:
        return {
            "success": False,
            "message": "Invalid token"
        }
    
    return crawl_ranks_for_company(company)
