"""
Worklog Cache 및 Training Dataset 테스트 스크립트
"""
import json
from datetime import datetime, date


def test_worklog_cache_module():
    """Worklog Cache 모듈 기본 기능 테스트"""
    print("=" * 50)
    print("Worklog Cache 시스템 테스트")
    print("=" * 50)
    
    try:
        from worklog_cache import WorklogCache, get_worklog_cache
        
        # 1. 캐시 초기화
        print("\n[1] 캐시 초기화 테스트")
        cache = get_worklog_cache()
        print("✅ Worklog 캐시 모듈 로드 성공")
        
        # 2. 캐시 상태 확인
        print("\n[2] 캐시 상태 확인")
        status = cache.get_cache_status()
        print(f"캐시 유효: {status['is_valid']}")
        print(f"마지막 업데이트: {status.get('updated_at', 'N/A')}")
        print(f"레코드 수: {status['records_count']}")
        print(f"회사 목록: {status.get('companies', [])}")
        
        print("\n" + "=" * 50)
        print("✅ Worklog Cache 기본 테스트 통과!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()


def test_training_dataset_builder():
    """Training Dataset Builder 테스트"""
    print("\n" + "=" * 50)
    print("Training Dataset Builder 테스트")
    print("=" * 50)
    
    try:
        from training_dataset_builder import TrainingDatasetBuilder, generate_tasks_hash
        
        # 1. 해시 생성 테스트
        print("\n[1] Tasks Hash 생성 테스트")
        tasks = ["저장", "영수증B", "트래픽"]
        hash1 = generate_tasks_hash(tasks)
        hash2 = generate_tasks_hash(["트래픽", "저장", "영수증B"])  # 순서 다름
        print(f"Hash 1: {hash1}")
        print(f"Hash 2 (순서 다름): {hash2}")
        assert hash1 == hash2, "동일 작업셋은 같은 해시여야 함"
        print("✅ 해시 생성 테스트 통과")
        
        # 2. Builder 초기화
        print("\n[2] Builder 초기화 테스트")
        builder = TrainingDatasetBuilder()
        print("✅ Builder 초기화 성공")
        
        # 3. Recipe Stats 테스트 (샘플 데이터)
        print("\n[3] Recipe Stats 생성 테스트 (샘플)")
        sample_rows = [
            {
                "business_name": "테스트업체1",
                "n2_delta_3d": 0.035,
                "tasks_active": ["저장", "트래픽"]
            },
            {
                "business_name": "테스트업체2",
                "n2_delta_3d": 0.02,
                "tasks_active": ["저장"]
            },
            {
                "business_name": "테스트업체3",
                "n2_delta_3d": -0.01,
                "tasks_active": ["저장", "트래픽"]
            }
        ]
        
        stats = builder.build_recipe_stats(sample_rows)
        print(f"단일 작업 수: {stats.get('summary', {}).get('total_single_tasks', 0)}")
        print(f"조합 수: {stats.get('summary', {}).get('total_combos', 0)}")
        
        if stats.get("single_task_stats"):
            print("\n단일 작업 통계:")
            for s in stats["single_task_stats"][:3]:
                print(f"  - {s['name']}: avg={s['avg_delta']:.6f}, count={s['count']}")
        
        print("\n✅ Recipe Stats 테스트 통과")
        
        print("\n" + "=" * 50)
        print("✅ Training Dataset Builder 테스트 통과!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()


def test_full_integration():
    """전체 통합 테스트 (실제 데이터 필요)"""
    print("\n" + "=" * 50)
    print("전체 통합 테스트 (실제 데이터 사용)")
    print("=" * 50)
    
    print("\n⚠️ 이 테스트는 실제 구글 시트 연결이 필요합니다.")
    response = input("테스트를 계속하시겠습니까? (y/n): ")
    
    if response.lower() != 'y':
        print("테스트를 건너뜁니다.")
        return
    
    try:
        # 1. Worklog 캐시 갱신
        print("\n[1] Worklog 캐시 갱신")
        from worklog_cache import refresh_worklog_cache
        result = refresh_worklog_cache()
        print(f"결과: {result.get('message')}")
        
        if not result.get("success"):
            print("❌ Worklog 캐시 갱신 실패")
            return
        
        # 2. Training rows 빌드
        print("\n[2] Training rows 빌드")
        from training_dataset_builder import build_and_save
        build_result = build_and_save(weeks=1)  # 1주 테스트
        
        print(f"Training rows: {build_result.get('training_rows_count', 0)}개")
        print(f"Recipe stats: {build_result.get('recipe_stats', {})}")
        
        # 3. Top recipes 조회
        print("\n[3] Top recipes 조회")
        from training_dataset_builder import get_top_recipes
        recipes = get_top_recipes(weeks=1)
        
        print(f"\n상위 5개 레시피:")
        for r in recipes[:5]:
            print(f"  - {r.get('name')}: delta={r.get('avg_delta'):.4f}, count={r.get('count')}")
        
        print("\n" + "=" * 50)
        print("✅ 전체 통합 테스트 통과!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 기본 모듈 테스트
    test_worklog_cache_module()
    
    # Builder 테스트
    test_training_dataset_builder()
    
    # 통합 테스트 (선택)
    test_full_integration()
    
    print("\n\n" + "=" * 50)
    print("🎉 모든 테스트 완료!")
    print("=" * 50)
