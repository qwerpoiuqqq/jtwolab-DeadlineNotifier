"""
최적 레시피 분석 모듈
- N2 점수 변화 분석 (작업 시작일 기준 3일째)
- 트래픽 일 작업량별 그룹 분석
- 리뷰 급감 감지
"""
import logging
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Any
import pytz

logger = logging.getLogger(__name__)
KST = pytz.timezone("Asia/Seoul")


def is_restaurant_keyword(place_url: str) -> bool:
    """맛집 키워드 여부 판별 (URL 기반)"""
    if not place_url:
        return False
    return "restaurant" in place_url.lower()


def get_n2_at_day(snapshots: List[Dict], start_date: date, target_day: int) -> Optional[Dict]:
    """작업 시작일 기준 N일째 스냅샷 데이터 가져오기
    
    Args:
        snapshots: 해당 업체의 스냅샷 리스트 (date 포함)
        start_date: 작업 시작일
        target_day: 목표 일차 (1=시작일, 3=3일째)
    
    Returns:
        해당 일자의 스냅샷 데이터 또는 None
    """
    # 시작일을 1일로 카운트
    target_date = start_date + timedelta(days=target_day - 1)
    target_date_str = target_date.isoformat()
    
    for snapshot in snapshots:
        if snapshot.get("date") == target_date_str:
            return snapshot
    
    return None


def analyze_n2_change(snapshots: List[Dict], start_date: date) -> Optional[Dict]:
    """N2 점수 변화 분석 (3일째 기준, 없으면 4->2->5일 순)
    
    Returns:
        {
            "day_used": 3,  # 어느 일차 데이터 사용
            "start_n2": 0.4512,
            "end_n2": 0.4823,
            "delta": 0.0311,
            "trend": "상승" | "하락" | "유지"
        }
    """
    # 우선순위: 3일째 > 4일째 > 2일째 > 5일째
    priority_days = [3, 4, 2, 5]
    
    # 시작일(1일째) 데이터
    start_snapshot = get_n2_at_day(snapshots, start_date, 1)
    if not start_snapshot or start_snapshot.get("n2_score") is None:
        return None
    
    start_n2 = start_snapshot.get("n2_score")
    
    # 목표 일차 데이터 찾기
    for day in priority_days:
        end_snapshot = get_n2_at_day(snapshots, start_date, day)
        if end_snapshot and end_snapshot.get("n2_score") is not None:
            end_n2 = end_snapshot.get("n2_score")
            delta = end_n2 - start_n2
            
            if delta > 0.001:
                trend = "상승"
            elif delta < -0.001:
                trend = "하락"
            else:
                trend = "유지"
            
            return {
                "day_used": day,
                "start_n2": start_n2,
                "end_n2": end_n2,
                "delta": round(delta, 4),
                "trend": trend
            }
    
    return None


def analyze_review_change(snapshots: List[Dict], start_date: date) -> Optional[Dict]:
    """리뷰 수 변화 분석
    
    Returns:
        {
            "start_reviews": 741,
            "end_reviews": 756,
            "delta": 15,
            "change_rate": 2.02,
            "warning": None | "주의" | "경고"
        }
    """
    start_snapshot = get_n2_at_day(snapshots, start_date, 1)
    
    # 가장 최근 스냅샷 찾기
    if not snapshots:
        return None
    
    sorted_snapshots = sorted(snapshots, key=lambda x: x.get("date", ""), reverse=True)
    end_snapshot = sorted_snapshots[0] if sorted_snapshots else None
    
    if not start_snapshot or not end_snapshot:
        return None
    
    # 블로그 + 방문자 리뷰 합산
    start_blog = start_snapshot.get("blog_reviews") or 0
    start_visitor = start_snapshot.get("visitor_reviews") or 0
    start_total = start_blog + start_visitor
    
    end_blog = end_snapshot.get("blog_reviews") or 0
    end_visitor = end_snapshot.get("visitor_reviews") or 0
    end_total = end_blog + end_visitor
    
    if start_total == 0:
        return None
    
    delta = end_total - start_total
    change_rate = (delta / start_total) * 100
    
    # 경고 레벨 결정
    warning = None
    if change_rate < -10:
        warning = "경고"
    elif change_rate < -5:
        warning = "주의"
    
    return {
        "start_reviews": start_total,
        "end_reviews": end_total,
        "delta": delta,
        "change_rate": round(change_rate, 2),
        "warning": warning
    }


def categorize_traffic_group(daily_traffic: int) -> str:
    """트래픽 일 작업량 그룹 분류"""
    if daily_traffic < 100:
        return "A (0~99)"
    elif daily_traffic < 200:
        return "B (100~199)"
    elif daily_traffic < 300:
        return "C (200~299)"
    elif daily_traffic < 400:
        return "D (300~399)"
    else:
        return "E (400+)"


class RecipeAnalyzer:
    """최적 레시피 분석기"""
    
    def __init__(self):
        self.analysis_cache = {}
    
    def analyze_all(self, weeks: int = 3) -> Dict[str, Any]:
        """전체 분석 실행
        
        Returns:
            {
                "general_keywords": {
                    "avg_n2_change": 0.032,
                    "count": 18,
                    "traffic_groups": {...}
                },
                "restaurant_keywords": {
                    "avg_n2_change": 0.048,
                    "count": 9,
                    "traffic_groups": {...}
                },
                "businesses": [
                    {
                        "name": "상호명",
                        "keyword": "키워드",
                        "is_restaurant": False,
                        "n2_analysis": {...},
                        "review_analysis": {...},
                        "workload": {...}
                    }
                ],
                "alerts": [...]
            }
        """
        from rank_snapshot_manager import RankSnapshotManager
        from workload_cache import WorkloadCache
        from guarantee_manager import GuaranteeManager
        
        logger.info(f"Starting recipe analysis for {weeks} weeks...")
        
        # 데이터 로드
        snapshot_mgr = RankSnapshotManager()
        workload_cache = WorkloadCache()
        guarantee_mgr = GuaranteeManager()
        
        # 기간 설정
        today = date.today()
        date_from = (today - timedelta(weeks=weeks)).isoformat()
        date_to = today.isoformat()
        
        # 스냅샷 데이터 가져오기
        try:
            all_snapshots = snapshot_mgr.get_history(date_from=date_from, date_to=date_to)
        except Exception as e:
            logger.error(f"Failed to get snapshots: {e}")
            all_snapshots = []
        
        # 보장건 데이터 (순위 업데이트와 동일한 조건)
        VALID_STATUSES = ["진행중", "후불", "반불"]
        all_items = guarantee_mgr.get_items()
        guarantee_items = [i for i in all_items if i.get("status") in VALID_STATUSES]
        
        # 크롤링 순위 데이터 가져오기
        try:
            from rank_crawler import get_latest_ranks
            latest_ranks = get_latest_ranks(None)  # 전체
            
            # 상호명 -> 순위 매핑
            crawled_rank_map = {}
            for rank_data in latest_ranks:
                biz_name = rank_data.get("business_name")
                if biz_name:
                    crawled_rank_map[biz_name] = {
                        "rank": rank_data.get("rank"),
                        "keyword": rank_data.get("keyword"),
                        "checked_at": rank_data.get("checked_at")
                    }
        except Exception as e:
            logger.warning(f"Failed to get crawled ranks: {e}")
            crawled_rank_map = {}
        
        # 업체별 분석
        businesses = []
        general_n2_changes = []
        restaurant_n2_changes = []
        alerts = []
        
        for item in guarantee_items:
            business_name = item.get("business_name")
            keyword = item.get("main_keyword")
            place_url = item.get("place_url", "")
            contract_date_str = item.get("contract_date")
            status = item.get("status", "")
            
            if not business_name:
                continue
            
            # 키워드 분류
            is_restaurant = is_restaurant_keyword(place_url)
            
            # 작업 시작일 파싱 (없어도 진행)
            start_date = None
            if contract_date_str:
                try:
                    start_date = datetime.strptime(contract_date_str, "%Y-%m-%d").date()
                except:
                    try:
                        start_date = datetime.strptime(contract_date_str, "%Y.%m.%d").date()
                    except:
                        pass
            
            # 해당 업체 스냅샷 필터링
            business_snapshots = [
                s for s in all_snapshots
                if s.get("client_name") == business_name or s.get("keyword") == keyword
            ]
            
            # N2 분석 (start_date 있을 때만)
            n2_analysis = None
            if start_date:
                n2_analysis = analyze_n2_change(business_snapshots, start_date)
            
            # 리뷰 분석
            review_analysis = None
            if start_date:
                review_analysis = analyze_review_change(business_snapshots, start_date)
            
            # 작업량 데이터
            company = item.get("company", "제이투랩")
            workload_data = workload_cache.get_business_workload(company, business_name)
            
            # 노출 일수 계산
            daily_ranks = item.get("daily_ranks", {})
            exposure_days = len([d for d in daily_ranks.values() if d])
            
            # 현재 순위: 시트 데이터 또는 크롤링 데이터
            current_rank = None
            rank_source = None
            
            # 1. 시트의 daily_ranks에서 가장 최근 순위
            if daily_ranks:
                sorted_days = sorted(daily_ranks.keys(), reverse=True)
                for day_key in sorted_days:
                    rank_data = daily_ranks.get(day_key)
                    if isinstance(rank_data, dict):
                        current_rank = rank_data.get("rank")
                        rank_source = "sheet"
                    elif rank_data:
                        current_rank = rank_data
                        rank_source = "sheet"
                    if current_rank:
                        break
            
            # 2. 시트에 없으면 크롤링 데이터 사용
            if not current_rank and business_name in crawled_rank_map:
                current_rank = crawled_rank_map[business_name].get("rank")
                rank_source = "crawled"
            
            business_result = {
                "name": business_name,
                "keyword": keyword,
                "place_url": place_url,
                "company": company,
                "status": status,
                "is_restaurant": is_restaurant,
                "contract_date": contract_date_str,
                "n2_analysis": n2_analysis,
                "review_analysis": review_analysis,
                "workload": workload_data,
                "exposure_days": exposure_days,
                "guarantee_rank": item.get("guarantee_rank"),
                "current_rank": current_rank,
                "rank_source": rank_source,
                "agency": item.get("agency")
            }
            
            businesses.append(business_result)
            
            # N2 변화 집계
            if n2_analysis and n2_analysis.get("delta") is not None:
                if is_restaurant:
                    restaurant_n2_changes.append(n2_analysis["delta"])
                else:
                    general_n2_changes.append(n2_analysis["delta"])
            
            # 리뷰 경고 수집
            if review_analysis and review_analysis.get("warning"):
                alerts.append({
                    "type": "review_drop",
                    "level": review_analysis["warning"],
                    "business_name": business_name,
                    "message": f"리뷰 {review_analysis['change_rate']}% 감소"
                })
        
        # 평균 계산
        general_avg = sum(general_n2_changes) / len(general_n2_changes) if general_n2_changes else 0
        restaurant_avg = sum(restaurant_n2_changes) / len(restaurant_n2_changes) if restaurant_n2_changes else 0
        
        result = {
            "analyzed_at": datetime.now(KST).isoformat(),
            "weeks": weeks,
            "general_keywords": {
                "avg_n2_change": round(general_avg, 4),
                "count": len(general_n2_changes)
            },
            "restaurant_keywords": {
                "avg_n2_change": round(restaurant_avg, 4),
                "count": len(restaurant_n2_changes)
            },
            "businesses": businesses,
            "alerts": alerts,
            "total_analyzed": len(businesses)
        }
        
        logger.info(f"Analysis complete: {len(businesses)} businesses, {len(alerts)} alerts")
        return result
    
    def get_business_dashboard(self, business_name: str) -> Optional[Dict]:
        """특정 업체 대시보드 데이터"""
        analysis = self.analyze_all()
        
        for biz in analysis.get("businesses", []):
            if biz.get("name") == business_name:
                return biz
        
        return None


# 전역 인스턴스
_analyzer = None

def get_analyzer() -> RecipeAnalyzer:
    global _analyzer
    if _analyzer is None:
        _analyzer = RecipeAnalyzer()
    return _analyzer
