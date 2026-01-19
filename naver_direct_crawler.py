"""
네이버 플레이스 직접 API 크롤러
- Playwright 없이 httpx만 사용 (메모리 효율적)
- Decodo 프록시 자동 할당
- gdid에서 Popularity(인기도) 점수 추출 (N2 점수와 유사)
"""
import httpx
import logging
import os
import random
import time
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


@dataclass
class ProxyConfig:
    """프록시 설정"""
    host: str
    port: int
    username: str = ""
    password: str = ""

    @property
    def url(self) -> str:
        if self.username and self.password:
            return f"http://{self.username}:{self.password}@{self.host}:{self.port}"
        return f"http://{self.host}:{self.port}"


class DecodoProxyPool:
    """Decodo 프록시 풀 관리 (라운드로빈/랜덤 할당)"""

    def __init__(self):
        # 환경변수에서 Decodo 설정 로드
        self.username = os.getenv("DECODO_USERNAME", "")
        self.password = os.getenv("DECODO_PASSWORD", "")
        self.host = os.getenv("DECODO_HOST", "kr.decodo.com")
        self.port_start = int(os.getenv("DECODO_PORT_START", "10001"))
        self.port_count = int(os.getenv("DECODO_ENDPOINT_COUNT", "50"))
        self._current_index = 0

        if not self.username or not self.password:
            logger.warning("Decodo 자격 증명이 설정되지 않았습니다. 프록시 없이 실행됩니다.")

    def get_proxy(self) -> Optional[ProxyConfig]:
        """라운드로빈 방식으로 프록시 반환"""
        if not self.username or not self.password:
            return None

        port = self.port_start + (self._current_index % self.port_count)
        self._current_index += 1
        return ProxyConfig(
            host=self.host,
            port=port,
            username=self.username,
            password=self.password
        )

    def get_random_proxy(self) -> Optional[ProxyConfig]:
        """랜덤 프록시 반환"""
        if not self.username or not self.password:
            return None

        port = self.port_start + random.randint(0, self.port_count - 1)
        return ProxyConfig(
            host=self.host,
            port=port,
            username=self.username,
            password=self.password
        )


class NaverDirectCrawler:
    """네이버 플레이스 직접 API 크롤러 (Playwright 불필요, Decodo 프록시 지원)"""

    GRAPHQL_URL = "https://pcmap-api.place.naver.com/graphql"

    def __init__(self, use_proxy: bool = True, retry_count: int = 3):
        self.use_proxy = use_proxy
        self.retry_count = retry_count
        self.proxy_pool = DecodoProxyPool() if use_proxy else None

    def _get_stealth_headers(self) -> Dict[str, str]:
        """네이버 429 우회를 위한 스텔스 헤더"""
        return {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
            "Accept-Encoding": "gzip, deflate",  # br(Brotli) 제거 - 디코딩 문제 방지
            "Content-Type": "application/json",
            "Origin": "https://m.search.naver.com",
            "Referer": "https://m.search.naver.com/",
            "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-site",
            "Connection": "keep-alive",
        }

    def _make_request(self, query: str, variables: Dict) -> Optional[Dict]:
        """GraphQL 요청 실행 (429 시 재시도 + 프록시 로테이션)"""
        headers = self._get_stealth_headers()

        for attempt in range(self.retry_count):
            proxy_url = None
            if self.use_proxy and self.proxy_pool:
                proxy = self.proxy_pool.get_proxy()
                if proxy:
                    proxy_url = proxy.url

            try:
                with httpx.Client(proxy=proxy_url, timeout=30, follow_redirects=True) as client:
                    resp = client.post(
                        self.GRAPHQL_URL,
                        headers=headers,
                        json={'query': query, 'variables': variables}
                    )

                    if resp.status_code == 429:
                        wait = 2 ** attempt  # 지수 백오프
                        logger.warning(f"429 Rate Limit - {wait}초 후 재시도 (시도 {attempt + 1}/{self.retry_count})")
                        time.sleep(wait)
                        continue

                    if resp.status_code == 200:
                        # 명시적 UTF-8 디코딩
                        return resp.json()
                    elif resp.status_code in (500, 502, 503, 504):
                        # 서버 오류는 재시도
                        wait = 2 ** attempt
                        logger.warning(f"서버 오류 {resp.status_code} - {wait}초 후 재시도 (시도 {attempt + 1}/{self.retry_count})")
                        time.sleep(wait)
                        continue
                    else:
                        logger.warning(f"API 응답 오류: {resp.status_code}")
                        return None

            except Exception as e:
                logger.error(f"요청 실패: {e}")
                if attempt < self.retry_count - 1:
                    time.sleep(1)
                    continue
                return None

        return None

    def parse_gdid_scores(self, gdid: str) -> Dict[str, float]:
        """
        gdid에서 점수 파싱

        gdid 형식: "2062825643,2:0.4203:0.55685:0.540253"
        - parts[0]: Place ID
        - parts[1]: Version (2)
        - parts[2]: Relevance (적합도)
        - parts[3]: Popularity (인기도) ≈ N2 점수
        - parts[4]: Trust (신뢰도)
        """
        result = {
            "relevance": None,
            "popularity": None,  # N2 점수와 유사
            "trust": None,
        }

        if not gdid or ":" not in gdid:
            return result

        try:
            # 쉼표 뒤의 점수 부분 파싱
            if "," in gdid:
                score_part = gdid.split(",")[1]
            else:
                score_part = gdid

            parts = score_part.split(":")
            if len(parts) >= 4:
                result["relevance"] = float(parts[1])
                result["popularity"] = float(parts[2])  # N2 점수와 유사
                result["trust"] = float(parts[3])
        except (ValueError, IndexError) as e:
            logger.debug(f"gdid 파싱 실패: {gdid}, 에러: {e}")

        return result

    def search_places(self, keyword: str, limit: int = 50) -> List[Dict]:
        """
        키워드로 플레이스 검색 + 순위/점수 정보

        Args:
            keyword: 검색 키워드 (예: "강남 피부과")
            limit: 최대 결과 수 (기본 50)

        Returns:
            순위, 점수, 리뷰 수 등이 포함된 결과 리스트
        """
        # 검증된 최소 필드로 시작
        query = '''
        query getPlaces($input: PlacesInput) {
            businesses: nxPlaces(input: $input) {
                total
                items {
                    id
                    name
                    gdid
                    category
                    address
                    roadAddress
                    phone
                    x
                    y
                    visitorReviewCount
                    blogCafeReviewCount
                    imageCount
                }
            }
        }
        '''

        variables = {
            'input': {
                'query': keyword,
                'display': limit,
                'x': '127.0276',  # 서울 기본 좌표
                'y': '37.4979'
            }
        }

        result = self._make_request(query, variables)

        if not result:
            logger.error(f"검색 실패: {keyword}")
            return []

        if 'errors' in result:
            logger.error(f"GraphQL 에러: {result['errors']}")
            return []

        items = result.get('data', {}).get('businesses', {}).get('items', [])
        if not items:
            logger.info(f"검색 결과 없음: {keyword}")
            return []

        # 순위 및 점수 파싱
        results = []
        for rank, item in enumerate(items, 1):
            scores = self.parse_gdid_scores(item.get('gdid', ''))
            results.append({
                "rank": rank,
                "id": item.get("id"),
                "name": item.get("name"),
                "category": item.get("category"),
                "address": item.get("address"),
                "road_address": item.get("roadAddress"),
                "phone": item.get("phone"),
                "x": item.get("x"),
                "y": item.get("y"),
                # 점수 (N2 점수와 유사)
                "popularity": scores.get("popularity"),
                "relevance": scores.get("relevance"),
                "trust": scores.get("trust"),
                # 리뷰/콘텐츠 수
                "visitor_review_count": item.get("visitorReviewCount"),
                "blog_review_count": item.get("blogCafeReviewCount"),
                "image_count": item.get("imageCount"),
            })

        logger.info(f"검색 완료: {keyword}, 결과 {len(results)}개")
        return results

    def find_place_rank(self, keyword: str, place_id: str, limit: int = 50) -> Optional[Dict]:
        """
        특정 키워드에서 특정 업체의 순위 찾기

        Args:
            keyword: 검색 키워드
            place_id: 찾을 업체의 Place ID
            limit: 최대 검색 범위

        Returns:
            순위/점수 정보 또는 None (순위권 외)
        """
        results = self.search_places(keyword, limit)

        for item in results:
            if str(item.get("id")) == str(place_id):
                return item

        # 순위권 외
        logger.info(f"순위권 외: {keyword}, place_id={place_id}")
        return None

    def crawl_ranks_batch(self, targets: List[Dict]) -> List[Dict]:
        """
        여러 업체의 순위를 배치로 크롤링

        Args:
            targets: [{"keyword": "강남 피부과", "place_id": "12345"}, ...]

        Returns:
            순위 정보 리스트
        """
        results = []
        keywords_cache = {}  # 동일 키워드 결과 캐시

        for target in targets:
            keyword = target.get("keyword")
            place_id = target.get("place_id")
            business_name = target.get("business_name", "")

            if not keyword or not place_id:
                continue

            # 캐시 확인
            if keyword not in keywords_cache:
                keywords_cache[keyword] = self.search_places(keyword, limit=50)
                # Rate Limit 방지
                time.sleep(0.3)

            search_results = keywords_cache[keyword]

            # 순위 찾기
            found = None
            for item in search_results:
                if str(item.get("id")) == str(place_id):
                    found = item
                    break

            if found:
                results.append({
                    "keyword": keyword,
                    "place_id": place_id,
                    "business_name": business_name,
                    "rank": found["rank"],
                    "popularity": found["popularity"],  # N2 점수와 유사
                    "trust": found["trust"],
                    "visitor_review_count": found["visitor_review_count"],
                    "blog_review_count": found["blog_review_count"],
                    "image_count": found["image_count"],
                    "found": True,
                })
            else:
                results.append({
                    "keyword": keyword,
                    "place_id": place_id,
                    "business_name": business_name,
                    "rank": None,
                    "popularity": None,
                    "trust": None,
                    "visitor_review_count": None,
                    "blog_review_count": None,
                    "image_count": None,
                    "found": False,
                })

        return results


# 테스트용
if __name__ == "__main__":
    import sys

    # 테스트 시 환경변수 직접 설정
    os.environ.setdefault("DECODO_USERNAME", "spsofyi82b")
    os.environ.setdefault("DECODO_PASSWORD", "R6B1igmt9lyp1Et~cM")

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    crawler = NaverDirectCrawler(use_proxy=True)

    # 단일 검색 테스트
    print("=" * 60)
    print("단일 검색 테스트: 강남 피부과")
    print("=" * 60)
    results = crawler.search_places("강남 피부과", limit=5)
    for item in results:
        print(f"[{item['rank']}] {item['name']}")
        print(f"    ID: {item['id']}")
        print(f"    Popularity (N2): {item['popularity']}")
        print(f"    Trust: {item['trust']}")
        print(f"    방문자리뷰: {item['visitor_review_count']}")
        print()
