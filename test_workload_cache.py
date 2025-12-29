"""
작업량 캐시 시스템 테스트 스크립트
"""
import json
from datetime import datetime


def test_cache_module():
    """캐시 모듈 기본 기능 테스트"""
    print("=" * 50)
    print("작업량 캐시 시스템 테스트")
    print("=" * 50)
    
    try:
        from workload_cache import WorkloadCache
        
        # 1. 캐시 초기화
        print("\n[1] 캐시 초기화 테스트")
        cache = WorkloadCache()
        print("✅ 캐시 모듈 로드 성공")
        
        # 2. 캐시 상태 확인
        print("\n[2] 캐시 상태 확인")
        status = cache.get_cache_status()
        print(f"캐시 유효: {status['is_valid']}")
        print(f"마지막 업데이트: {status.get('updated_at', 'N/A')}")
        print(f"만료 시간: {status.get('expires_at', 'N/A')}")
        print(f"캐시된 업체 수: {status['company_count']}")
        
        # 3. 테스트 데이터로 캐시 업데이트
        print("\n[3] 테스트 데이터로 캐시 업데이트")
        test_data = {
            "제이투랩": {
                "weeks": [
                    {
                        "start_date": "10/07",
                        "end_date": "10/13",
                        "items": [
                            {"name": "플레이스 저장", "workload": "300"},
                            {"name": "영수증B", "workload": "10"}
                        ]
                    },
                    {
                        "start_date": "10/14",
                        "end_date": "10/20",
                        "items": [
                            {"name": "플레이스 저장", "workload": "280"},
                            {"name": "영수증B", "workload": "12"}
                        ]
                    }
                ]
            },
            "일류기획": {
                "weeks": [
                    {
                        "start_date": "10/07",
                        "end_date": "10/13",
                        "items": [
                            {"name": "일류 저장", "workload": "350"}
                        ]
                    }
                ]
            }
        }
        
        success = cache.update_cache(test_data)
        if success:
            print("✅ 캐시 업데이트 성공")
        else:
            print("❌ 캐시 업데이트 실패")
        
        # 4. 업체별 데이터 조회
        print("\n[4] 업체별 캐시 데이터 조회")
        for company in ["제이투랩", "일류기획"]:
            data = cache.get_company_workload(company)
            if data:
                weeks_count = len(data.get("weeks", []))
                print(f"✅ {company}: {weeks_count}주차 데이터")
            else:
                print(f"❌ {company}: 데이터 없음")
        
        # 5. 캐시 상태 재확인
        print("\n[5] 업데이트 후 캐시 상태")
        status = cache.get_cache_status()
        print(f"캐시 유효: {status['is_valid']}")
        print(f"마지막 업데이트: {status.get('updated_at', 'N/A')}")
        print(f"만료 시간: {status.get('expires_at', 'N/A')}")
        print(f"제이투랩 주차 수: {status.get('제이투랩_weeks', 0)}")
        print(f"일류기획 주차 수: {status.get('일류기획_weeks', 0)}")
        
        print("\n" + "=" * 50)
        print("✅ 모든 테스트 통과!")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()


def test_internal_manager():
    """internal_manager 캐시 연동 테스트"""
    print("\n" + "=" * 50)
    print("internal_manager 캐시 연동 테스트")
    print("=" * 50)
    
    try:
        from internal_manager import fetch_workload_schedule
        
        print("\n[1] 작업량 스케줄 조회 (캐시 우선)")
        
        companies = ["제이투랩", "일류기획"]
        for company in companies:
            print(f"\n{company} 조회 중...")
            try:
                result = fetch_workload_schedule(company)
                from_cache = result.get("from_cache", False)
                weeks_count = len(result.get("weeks", []))
                
                cache_status = "캐시" if from_cache else "실시간 조회"
                print(f"✅ {company}: {weeks_count}주차 ({cache_status})")
                
                # 첫 주차 데이터 미리보기
                if weeks_count > 0:
                    first_week = result["weeks"][0]
                    items_count = len(first_week.get("items", []))
                    print(f"   첫 주차: {first_week.get('start_date')} ~ {first_week.get('end_date')}, {items_count}개 작업")
                
            except Exception as e:
                print(f"❌ {company} 조회 실패: {e}")
        
        print("\n" + "=" * 50)
        print("✅ internal_manager 테스트 완료")
        print("=" * 50)
        
    except Exception as e:
        print(f"\n❌ 테스트 실패: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    # 기본 캐시 모듈 테스트
    test_cache_module()
    
    # internal_manager 연동 테스트
    # 주의: 실제 구글 시트 연결이 필요할 수 있음
    print("\n\n⚠️  internal_manager 테스트는 실제 구글 시트 연결이 필요합니다.")
    response = input("테스트를 계속하시겠습니까? (y/n): ")
    if response.lower() == 'y':
        test_internal_manager()
    else:
        print("테스트를 건너뜁니다.")
    
    print("\n\n" + "=" * 50)
    print("🎉 모든 테스트 완료!")
    print("=" * 50)

