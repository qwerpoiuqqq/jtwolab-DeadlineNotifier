"""
학습용 데이터셋 빌더 모듈
rank_snapshots와 worklog_cache를 조인하여 학습용 데이터셋 생성

핵심 기능:
- business_name + date 기반 조인
- N2 delta 계산 (작업 시작일 기준 3일째, fallback: 4/2/5)
- 레시피 통계 생성 (단일/조합별)
"""
import os
import json
import hashlib
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Optional, Any, Tuple
from collections import defaultdict
import pytz

logger = logging.getLogger(__name__)

# 기본 설정
KST = pytz.timezone('Asia/Seoul')

# Render Disk 경로 우선 사용
DISK_PATH = "/var/data"
if os.path.isdir(DISK_PATH):
    DEFAULT_TRAINING_PATH = os.path.join(DISK_PATH, "training_rows.json")
    DEFAULT_RECIPE_PATH = os.path.join(DISK_PATH, "recipe_stats.json")
else:
    DEFAULT_TRAINING_PATH = os.path.join(os.getcwd(), "training_rows.json")
    DEFAULT_RECIPE_PATH = os.path.join(os.getcwd(), "recipe_stats.json")


def generate_tasks_hash(tasks: List[str]) -> str:
    """작업 목록의 canonical 해시 생성"""
    sorted_tasks = sorted(set(tasks))
    canonical = "|".join(sorted_tasks)
    return hashlib.sha1(canonical.encode()).hexdigest()[:12]


def get_n2_at_day(snapshots: List[Dict], start_date: date, target_day: int) -> Optional[Dict]:
    """작업 시작일 기준 N일째 스냅샷 데이터 가져오기
    
    Args:
        snapshots: 해당 업체의 스냅샷 리스트
        start_date: 작업 시작일
        target_day: 목표 일차 (1=시작일, 3=3일째)
        
    Returns:
        해당 일자의 스냅샷 데이터 또는 None
    """
    target_date = start_date + timedelta(days=target_day - 1)
    target_str = target_date.isoformat()
    
    for snap in snapshots:
        if snap.get("date") == target_str:
            return snap
    
    return None


def calculate_n2_delta(
    snapshots: List[Dict], 
    start_date: date
) -> Tuple[Optional[float], int, Optional[float], Optional[float]]:
    """N2 delta 계산 (3일째 기준, fallback: 4/2/5)
    
    Returns:
        (delta, day_used, start_n2, end_n2)
    """
    # 시작일의 N2
    start_snap = get_n2_at_day(snapshots, start_date, 1)
    if not start_snap:
        return None, 0, None, None
    
    start_n2 = start_snap.get("n2_score")
    if start_n2 is None:
        return None, 0, None, None
    
    # 3일째 우선, 없으면 4/2/5 순
    for target_day in [3, 4, 2, 5]:
        end_snap = get_n2_at_day(snapshots, start_date, target_day)
        if end_snap and end_snap.get("n2_score") is not None:
            end_n2 = end_snap.get("n2_score")
            delta = round(end_n2 - start_n2, 6)
            return delta, target_day, start_n2, end_n2
    
    return None, 0, start_n2, None


class TrainingDatasetBuilder:
    """학습용 데이터셋 빌더 클래스"""
    
    def __init__(self):
        self.training_rows = []
        self.recipe_stats = {}
    
    def build_training_rows(self, weeks: int = 3) -> List[Dict]:
        """training_rows 생성
        
        Args:
            weeks: 분석 기간 (주)
            
        Returns:
            training_rows 리스트
        """
        logger.info(f"🔄 Training rows 생성 시작 (최근 {weeks}주)")
        
        try:
            from worklog_cache import get_worklog_cache
            from rank_snapshot_manager import RankSnapshotManager
            from guarantee_manager import GuaranteeManager
            
            # 캐시 로드
            worklog_cache = get_worklog_cache()
            if not worklog_cache.is_cache_valid():
                logger.warning("Worklog 캐시가 유효하지 않음 - 갱신 필요")
            
            # 스냅샷 히스토리 로드
            snapshot_manager = RankSnapshotManager()
            days = weeks * 7
            all_snapshots = snapshot_manager.get_history(days=days)
            logger.info(f"📊 스냅샷 {len(all_snapshots)}개 로드")
            
            # 보장건 데이터 로드 (작업 시작일 조회용)
            gm = GuaranteeManager()
            guarantee_items = gm.get_items()
            guarantee_map = {item.get("business_name"): item for item in guarantee_items}
            
            # 업체별 스냅샷 그룹핑
            snapshots_by_biz = defaultdict(list)
            for snap in all_snapshots:
                biz = snap.get("client_name") or snap.get("business_name")
                if biz:
                    snapshots_by_biz[biz].append(snap)
            
            # 각 스냅샷에 대해 training row 생성
            training_rows = []
            processed_keys = set()
            
            for biz_name, biz_snapshots in snapshots_by_biz.items():
                # 보장건에서 작업 시작일 조회
                guarantee = guarantee_map.get(biz_name, {})
                start_date_str = guarantee.get("start_date")
                
                if start_date_str:
                    try:
                        start_date = date.fromisoformat(start_date_str)
                    except:
                        start_date = None
                else:
                    start_date = None
                
                # 스냅샷을 날짜순 정렬
                biz_snapshots.sort(key=lambda x: x.get("date", ""))
                
                for snap in biz_snapshots:
                    snap_date_str = snap.get("date")
                    if not snap_date_str:
                        continue
                    
                    try:
                        snap_date = date.fromisoformat(snap_date_str)
                    except:
                        continue
                    
                    # 중복 키 체크
                    unique_key = f"{biz_name}|{snap_date_str}"
                    if unique_key in processed_keys:
                        continue
                    processed_keys.add(unique_key)
                    
                    # 해당 날짜의 활성 작업 조회
                    active_tasks = worklog_cache.get_active_tasks_on_date(biz_name, snap_date)
                    task_names = [t.get("task_name", "") for t in active_tasks if t.get("task_name")]
                    task_totals = worklog_cache.get_task_totals_on_date(biz_name, snap_date)
                    
                    # N2 delta 계산
                    n2_delta, day_used, start_n2, end_n2 = None, 0, None, None
                    if start_date:
                        n2_delta, day_used, start_n2, end_n2 = calculate_n2_delta(
                            biz_snapshots, start_date
                        )
                    
                    # Training row 생성
                    row = {
                        "date": snap_date_str,
                        "time_slot": snap.get("time_slot", ""),
                        "business_name": biz_name,
                        "keyword": snap.get("keyword", ""),
                        "place_url": snap.get("place_url", ""),
                        "company": snap.get("group") or guarantee.get("company", ""),
                        "n2_score": snap.get("n2_score"),
                        "n2_delta_3d": n2_delta,
                        "delta_day_used": day_used,
                        "start_n2": start_n2,
                        "rank": snap.get("rank"),
                        "saves": snap.get("saves"),
                        "blog_reviews": snap.get("blog_reviews"),
                        "visitor_reviews": snap.get("visitor_reviews"),
                        "tasks_active": task_names,
                        "tasks_hash": generate_tasks_hash(task_names) if task_names else "",
                        "task_totals": task_totals,
                        "tasks_count": len(task_names),
                    }
                    training_rows.append(row)
            
            self.training_rows = training_rows
            logger.info(f"✅ Training rows {len(training_rows)}개 생성 완료")
            return training_rows
            
        except Exception as e:
            logger.error(f"❌ Training rows 생성 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return []
    
    def build_recipe_stats(self, training_rows: List[Dict] = None) -> Dict:
        """레시피 통계 생성
        
        Args:
            training_rows: training rows (None이면 self.training_rows 사용)
            
        Returns:
            레시피 통계
        """
        if training_rows is None:
            training_rows = self.training_rows
        
        if not training_rows:
            return {}
        
        logger.info(f"🔄 레시피 통계 생성 ({len(training_rows)}개 행)")
        
        # 단일 작업별 통계
        single_task_stats = defaultdict(lambda: {
            "deltas": [],
            "count": 0,
            "up": 0,
            "down": 0,
            "stable": 0
        })
        
        # 조합별 통계
        combo_stats = defaultdict(lambda: {
            "deltas": [],
            "count": 0,
            "up": 0,
            "down": 0,
            "stable": 0
        })
        
        for row in training_rows:
            delta = row.get("n2_delta_3d")
            tasks = row.get("tasks_active", [])
            
            if delta is None or not tasks:
                continue
            
            # 트렌드 판정
            if delta > 0.005:
                trend = "up"
            elif delta < -0.005:
                trend = "down"
            else:
                trend = "stable"
            
            # 단일 작업 통계
            for task in tasks:
                single_task_stats[task]["deltas"].append(delta)
                single_task_stats[task]["count"] += 1
                single_task_stats[task][trend] += 1
            
            # 조합 통계 (2~3개 조합)
            tasks_sorted = sorted(set(tasks))
            if len(tasks_sorted) >= 2:
                # 2개 조합
                for i in range(len(tasks_sorted)):
                    for j in range(i + 1, len(tasks_sorted)):
                        combo = f"{tasks_sorted[i]}+{tasks_sorted[j]}"
                        combo_stats[combo]["deltas"].append(delta)
                        combo_stats[combo]["count"] += 1
                        combo_stats[combo][trend] += 1
            
            # 전체 조합 (tasks_hash 기준)
            if tasks_sorted:
                full_combo = "+".join(tasks_sorted)
                combo_stats[full_combo]["deltas"].append(delta)
                combo_stats[full_combo]["count"] += 1
                combo_stats[full_combo][trend] += 1
        
        # 통계 계산
        def calc_stats(stat_dict: Dict) -> List[Dict]:
            results = []
            for name, data in stat_dict.items():
                deltas = data["deltas"]
                if not deltas:
                    continue
                
                avg_delta = sum(deltas) / len(deltas)
                count = data["count"]
                up_rate = data["up"] / count if count > 0 else 0
                
                results.append({
                    "name": name,
                    "avg_delta": round(avg_delta, 6),
                    "count": count,
                    "up_count": data["up"],
                    "down_count": data["down"],
                    "stable_count": data["stable"],
                    "up_rate": round(up_rate, 4),
                })
            
            # avg_delta 내림차순 정렬
            results.sort(key=lambda x: x["avg_delta"], reverse=True)
            return results
        
        single_results = calc_stats(single_task_stats)
        combo_results = calc_stats(combo_stats)
        
        # 상위 20개 레시피
        top_recipes = combo_results[:20] if len(combo_results) >= 20 else combo_results
        
        self.recipe_stats = {
            "generated_at": datetime.now(KST).isoformat(),
            "training_rows_count": len(training_rows),
            "single_task_stats": single_results,
            "combo_stats": combo_results[:50],  # 상위 50개만
            "top_recipes": top_recipes,
            "summary": {
                "total_single_tasks": len(single_results),
                "total_combos": len(combo_results),
                "avg_delta_all": round(
                    sum(r["avg_delta"] for r in single_results) / len(single_results), 6
                ) if single_results else 0
            }
        }
        
        logger.info(f"✅ 레시피 통계 생성 완료 - 단일:{len(single_results)}, 조합:{len(combo_results)}")
        return self.recipe_stats
    
    def save_results(
        self, 
        training_path: str = None,
        recipe_path: str = None,
        save_to_sheets: bool = True
    ) -> Dict:
        """결과 저장 (JSON + Google Sheets)
        
        Args:
            save_to_sheets: True면 Google Sheets에도 백업
            
        Returns:
            저장 결과 {"success": bool, ...}
        """
        training_path = training_path or os.getenv("TRAINING_ROWS_FILE", DEFAULT_TRAINING_PATH)
        recipe_path = recipe_path or os.getenv("RECIPE_STATS_FILE", DEFAULT_RECIPE_PATH)
        
        result = {
            "success": True,
            "training_rows_count": len(self.training_rows),
            "json_saved": False,
            "sheets_saved": False
        }
        
        # 1. JSON 파일 저장
        try:
            for path in [training_path, recipe_path]:
                dir_path = os.path.dirname(path)
                if dir_path and not os.path.exists(dir_path):
                    os.makedirs(dir_path, exist_ok=True)
            
            with open(training_path, "w", encoding="utf-8") as f:
                json.dump({
                    "generated_at": datetime.now(KST).isoformat(),
                    "count": len(self.training_rows),
                    "rows": self.training_rows
                }, f, ensure_ascii=False, indent=2)
            
            with open(recipe_path, "w", encoding="utf-8") as f:
                json.dump(self.recipe_stats, f, ensure_ascii=False, indent=2)
            
            result["json_saved"] = True
            result["training_rows_path"] = training_path
            result["recipe_stats_path"] = recipe_path
            logger.info(f"✅ JSON 저장 완료 - {training_path}")
            
        except Exception as e:
            logger.error(f"❌ JSON 저장 실패: {e}")
            result["json_error"] = str(e)
        
        # 2. Google Sheets 백업
        if save_to_sheets and self.training_rows:
            try:
                sheets_result = self._save_to_google_sheets()
                result["sheets_saved"] = sheets_result.get("success", False)
                result["sheets_result"] = sheets_result
            except Exception as e:
                logger.error(f"❌ Sheets 백업 실패: {e}")
                result["sheets_error"] = str(e)
        
        return result
    
    def _save_to_google_sheets(self) -> Dict:
        """Google Sheets에 학습 데이터 백업
        
        월보장 순위 DB 시트에 training_rows와 recipe_stats 탭 생성/갱신
        """
        import gspread
        from google.oauth2.service_account import Credentials
        import os
        import json as json_module
        
        logger.info("📊 Google Sheets 백업 시작...")
        
        # 인증 설정
        scopes = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        
        creds = None
        json_str = os.getenv("SERVICE_ACCOUNT_JSON", "")
        if json_str:
            import io
            creds = Credentials.from_service_account_info(
                json_module.loads(json_str), scopes=scopes
            )
        else:
            creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "service_account.json")
            if os.path.exists(creds_path):
                creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
        
        if not creds:
            return {"success": False, "error": "인증 정보 없음"}
        
        client = gspread.authorize(creds)
        
        # 스프레드시트 열기 (RANK_SHEET_ID 또는 JTWOLAB_SHEET_ID)
        sheet_id = os.getenv("RANK_SHEET_ID") or os.getenv(
            "JTWOLAB_SHEET_ID", "1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y"
        )
        ss = client.open_by_key(sheet_id)
        
        result = {"success": True, "training_rows": 0, "recipe_stats": 0}
        
        # === training_rows 탭 저장 ===
        try:
            tab_name = "training_rows"
            try:
                ws = ss.worksheet(tab_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = ss.add_worksheet(title=tab_name, rows=1000, cols=20)
            
            # 헤더
            headers = [
                "date", "time_slot", "business_name", "keyword", "company",
                "n2_score", "n2_delta_3d", "delta_day_used", "start_n2",
                "rank", "saves", "blog_reviews", "visitor_reviews",
                "tasks_active", "tasks_hash", "task_totals", "tasks_count"
            ]
            
            # 데이터 행 생성
            rows = [headers]
            for row in self.training_rows[:500]:  # 최대 500행
                rows.append([
                    row.get("date", ""),
                    row.get("time_slot", ""),
                    row.get("business_name", ""),
                    row.get("keyword", ""),
                    row.get("company", ""),
                    row.get("n2_score") or "",
                    row.get("n2_delta_3d") or "",
                    row.get("delta_day_used") or "",
                    row.get("start_n2") or "",
                    row.get("rank") or "",
                    row.get("saves") or "",
                    row.get("blog_reviews") or "",
                    row.get("visitor_reviews") or "",
                    "|".join(row.get("tasks_active", [])),
                    row.get("tasks_hash", ""),
                    json_module.dumps(row.get("task_totals", {}), ensure_ascii=False),
                    row.get("tasks_count") or 0
                ])
            
            ws.update(rows, value_input_option="USER_ENTERED")
            result["training_rows"] = len(rows) - 1
            logger.info(f"  ✅ training_rows 탭: {len(rows)-1}행 저장")
            
        except Exception as e:
            logger.error(f"  ❌ training_rows 저장 실패: {e}")
            result["training_rows_error"] = str(e)
        
        # === recipe_stats 탭 저장 ===
        try:
            tab_name = "recipe_stats"
            try:
                ws = ss.worksheet(tab_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = ss.add_worksheet(title=tab_name, rows=200, cols=10)
            
            # 헤더
            headers = ["recipe_name", "avg_delta", "count", "up_count", "down_count", "stable_count", "up_rate"]
            
            # top_recipes 저장
            rows = [headers]
            top_recipes = self.recipe_stats.get("top_recipes", [])
            for recipe in top_recipes:
                rows.append([
                    recipe.get("name", ""),
                    recipe.get("avg_delta", 0),
                    recipe.get("count", 0),
                    recipe.get("up_count", 0),
                    recipe.get("down_count", 0),
                    recipe.get("stable_count", 0),
                    recipe.get("up_rate", 0)
                ])
            
            ws.update(rows, value_input_option="USER_ENTERED")
            result["recipe_stats"] = len(rows) - 1
            logger.info(f"  ✅ recipe_stats 탭: {len(rows)-1}행 저장")
            
        except Exception as e:
            logger.error(f"  ❌ recipe_stats 저장 실패: {e}")
            result["recipe_stats_error"] = str(e)
        
        logger.info(f"✅ Google Sheets 백업 완료 - training:{result['training_rows']}, recipe:{result['recipe_stats']}")
        return result


def build_and_save(weeks: int = 3) -> Dict:
    """학습 데이터셋 빌드 및 저장 (외부 호출용)
    
    Returns:
        빌드 결과
    """
    logger.info(f"🚀 학습 데이터셋 빌드 시작 (weeks={weeks})")
    
    try:
        builder = TrainingDatasetBuilder()
        
        # Training rows 생성
        training_rows = builder.build_training_rows(weeks=weeks)
        
        if not training_rows:
            return {
                "success": False,
                "message": "Training rows 생성 실패 - 데이터 없음"
            }
        
        # Recipe stats 생성
        recipe_stats = builder.build_recipe_stats(training_rows)
        
        # 저장
        save_result = builder.save_results()
        
        return {
            "success": True,
            "training_rows_count": len(training_rows),
            "recipe_stats": recipe_stats.get("summary", {}),
            "top_recipes": recipe_stats.get("top_recipes", [])[:10],
            "save_result": save_result
        }
        
    except Exception as e:
        logger.error(f"❌ 빌드 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return {
            "success": False,
            "error": str(e)
        }


def get_top_recipes(weeks: int = 3) -> List[Dict]:
    """상위 레시피 조회 (외부 호출용)
    
    캐시된 파일에서 읽거나, 없으면 새로 빌드
    """
    recipe_path = os.getenv("RECIPE_STATS_FILE", DEFAULT_RECIPE_PATH)
    
    # 캐시된 파일 확인
    if os.path.exists(recipe_path):
        try:
            with open(recipe_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # 24시간 이내면 캐시 사용
            generated_at = data.get("generated_at")
            if generated_at:
                gen_dt = datetime.fromisoformat(generated_at)
                if gen_dt.tzinfo is None:
                    gen_dt = KST.localize(gen_dt)
                
                if datetime.now(KST) - gen_dt < timedelta(hours=24):
                    return data.get("top_recipes", [])
        except Exception as e:
            logger.warning(f"캐시 읽기 실패: {e}")
    
    # 새로 빌드
    result = build_and_save(weeks=weeks)
    return result.get("top_recipes", [])
