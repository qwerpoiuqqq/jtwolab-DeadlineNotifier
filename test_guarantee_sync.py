"""
보장건 동기화 테스트 스크립트
단계별로 데이터 불러오기 → 파싱 → 저장까지 진행 상태를 점검합니다.
"""
import os
import sys
from dotenv import load_dotenv
import logging

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# .env 파일 로드
load_dotenv()

def check_env_variables():
    """환경변수 체크"""
    logger.info("=" * 60)
    logger.info("STEP 1: 환경변수 체크")
    logger.info("=" * 60)
    
    required_vars = {
        "구글 인증": ["GOOGLE_APPLICATION_CREDENTIALS", "SERVICE_ACCOUNT_JSON"],
        "시트 ID": ["JTWOLAB_SHEET_ID", "ILRYU_SHEET_ID"]
    }
    
    all_ok = True
    
    for category, vars_list in required_vars.items():
        logger.info(f"\n[{category}]")
        has_one = False
        for var in vars_list:
            value = os.getenv(var)
            if value:
                if "JSON" in var or "CREDENTIALS" in var:
                    # 민감정보는 일부만 표시
                    display = value[:50] + "..." if len(value) > 50 else value
                else:
                    display = value
                logger.info(f"  ✅ {var}: {display}")
                has_one = True
            else:
                logger.info(f"  ❌ {var}: 없음")
        
        if category == "구글 인증" and not has_one:
            logger.error(f"  ⚠️ {category} 중 하나는 반드시 설정되어야 합니다!")
            all_ok = False
    
    return all_ok


def test_google_auth():
    """구글 인증 테스트"""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 2: 구글 시트 인증 테스트")
    logger.info("=" * 60)
    
    try:
        import gspread
        from google.oauth2.service_account import Credentials
        import json
        
        scope = ['https://spreadsheets.google.com/feeds', 
                 'https://www.googleapis.com/auth/drive']
        
        creds_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
        service_account_json = os.getenv("SERVICE_ACCOUNT_JSON")
        
        if creds_path and os.path.exists(creds_path):
            logger.info(f"✅ 인증 파일 사용: {creds_path}")
            creds = Credentials.from_service_account_file(creds_path, scopes=scope)
        elif service_account_json:
            logger.info("✅ 환경변수 JSON 사용")
            service_account_info = json.loads(service_account_json)
            creds = Credentials.from_service_account_info(service_account_info, scopes=scope)
            logger.info(f"  서비스 계정: {service_account_info.get('client_email', 'N/A')}")
        else:
            logger.error("❌ 구글 인증 정보가 없습니다!")
            return None
        
        client = gspread.authorize(creds)
        logger.info("✅ gspread 클라이언트 생성 성공")
        return client
        
    except Exception as e:
        logger.error(f"❌ 인증 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return None


def test_sheet_access(client):
    """시트 접근 테스트"""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 3: 구글 시트 접근 테스트")
    logger.info("=" * 60)
    
    sheets_config = {
        "제이투랩": os.getenv("JTWOLAB_SHEET_ID", "1zRgtvTZ6SZF-bWiMO8qmnIhhrNVsrDxbIj3HvE8Tv3Y"),
        "일류기획": os.getenv("ILRYU_SHEET_ID", "1gInZEFprmb_p43SPKUZT6zjpG36AQPW9OpcsljgNtxM")
    }
    
    results = {}
    
    for company, sheet_id in sheets_config.items():
        logger.info(f"\n[{company}]")
        logger.info(f"  시트 ID: {sheet_id}")
        
        try:
            # 시트 열기
            spreadsheet = client.open_by_key(sheet_id)
            logger.info(f"  ✅ 시트 열기 성공: {spreadsheet.title}")
            
            # 워크시트 목록
            worksheets = spreadsheet.worksheets()
            worksheet_names = [ws.title for ws in worksheets]
            logger.info(f"  📋 워크시트 목록 ({len(worksheet_names)}개):")
            for idx, name in enumerate(worksheet_names, 1):
                logger.info(f"      {idx}. {name}")
            
            # '보장건' 탭 찾기
            target_sheet = None
            for ws in worksheets:
                if ws.title == "보장건":
                    target_sheet = ws
                    break
            
            if target_sheet:
                logger.info(f"  ✅ '보장건' 탭 발견!")
                
                # 데이터 미리보기
                try:
                    rows = target_sheet.get_all_values()
                    logger.info(f"  📊 총 행 수: {len(rows)}")
                    if rows:
                        logger.info(f"  📋 헤더 (첫 번째 행): {rows[0][:10]}")  # 처음 10개 컬럼만
                        logger.info(f"  📋 데이터 행 수: {len(rows) - 1}")
                    
                    results[company] = {
                        "success": True,
                        "sheet": spreadsheet,
                        "worksheet": target_sheet,
                        "rows": rows
                    }
                except Exception as e:
                    logger.error(f"  ❌ 데이터 읽기 실패: {e}")
                    results[company] = {"success": False, "error": str(e)}
            else:
                logger.error(f"  ❌ '보장건' 탭을 찾을 수 없습니다!")
                logger.error(f"  사용 가능한 탭: {', '.join(worksheet_names)}")
                results[company] = {"success": False, "error": "보장건 탭 없음"}
                
        except Exception as e:
            logger.error(f"  ❌ 시트 접근 실패: {e}")
            import traceback
            logger.error(traceback.format_exc())
            results[company] = {"success": False, "error": str(e)}
    
    return results


def test_data_parsing(sheet_results):
    """데이터 파싱 테스트"""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 4: 데이터 파싱 테스트")
    logger.info("=" * 60)
    
    from guarantee_manager import GuaranteeManager
    
    for company, result in sheet_results.items():
        if not result.get("success"):
            logger.warning(f"[{company}] 건너뜀 (시트 접근 실패)")
            continue
        
        logger.info(f"\n[{company}]")
        rows = result.get("rows", [])
        
        if len(rows) < 2:
            logger.warning(f"  ⚠️ 데이터가 충분하지 않습니다 (행 수: {len(rows)})")
            continue
        
        headers = rows[0]
        data_rows = rows[1:]
        
        logger.info(f"  헤더: {headers}")
        logger.info(f"  데이터 행 수: {len(data_rows)}")
        
        # 필수 컬럼 확인
        required_columns = ["상호", "계약일", "대행사", "작업"]
        found_columns = {}
        
        for req in required_columns:
            for idx, header in enumerate(headers):
                if req in header:
                    found_columns[req] = idx
                    logger.info(f"  ✅ '{req}' 컬럼 발견: {header} (인덱스 {idx})")
                    break
        
        missing = [col for col in required_columns if col not in found_columns]
        if missing:
            logger.warning(f"  ⚠️ 누락된 컬럼: {missing}")
        
        # 샘플 데이터 파싱
        logger.info(f"\n  📋 샘플 데이터 (처음 3개):")
        for idx, row in enumerate(data_rows[:3], 1):
            sample = {}
            for col_name, col_idx in found_columns.items():
                if col_idx < len(row):
                    sample[col_name] = row[col_idx]
            logger.info(f"      {idx}. {sample}")


def test_guarantee_manager_sync():
    """GuaranteeManager 동기화 테스트"""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 5: GuaranteeManager 동기화 테스트")
    logger.info("=" * 60)
    
    try:
        from guarantee_manager import GuaranteeManager
        
        gm = GuaranteeManager()
        logger.info("✅ GuaranteeManager 인스턴스 생성 성공")
        
        # 기존 데이터 확인
        existing_items = gm.get_items()
        logger.info(f"📊 기존 데이터: {len(existing_items)}건")
        
        # 동기화 실행
        logger.info("\n🔄 동기화 시작...")
        result = gm.sync_from_google_sheets()
        
        logger.info(f"\n✅ 동기화 완료!")
        logger.info(f"  추가: {result['added']}건")
        logger.info(f"  수정: {result['updated']}건")
        logger.info(f"  실패: {result['failed']}건")
        
        # 동기화 후 데이터 확인
        all_items = gm.get_items()
        logger.info(f"\n📊 동기화 후 총 데이터: {len(all_items)}건")
        
        if all_items:
            logger.info(f"\n📋 샘플 데이터 (처음 3개):")
            for idx, item in enumerate(all_items[:3], 1):
                logger.info(f"  {idx}. {item.get('business_name')} - {item.get('company')} - {item.get('contract_date')}")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ 동기화 실패: {e}")
        import traceback
        logger.error(traceback.format_exc())
        return False


def test_data_storage():
    """데이터 저장 확인"""
    logger.info("\n" + "=" * 60)
    logger.info("STEP 6: 데이터 저장 파일 확인")
    logger.info("=" * 60)
    
    files_to_check = [
        "guarantee_data.json",
        "secure_data/guarantee_data.enc"
    ]
    
    for filepath in files_to_check:
        if os.path.exists(filepath):
            size = os.path.getsize(filepath)
            logger.info(f"  ✅ {filepath} ({size:,} bytes)")
        else:
            logger.info(f"  ❌ {filepath} (없음)")


def main():
    """메인 테스트 실행"""
    logger.info("\n" + "=" * 70)
    logger.info("보장건 동기화 플로우 테스트 시작")
    logger.info("=" * 70)
    
    # 1. 환경변수 체크
    if not check_env_variables():
        logger.error("\n❌ 환경변수 설정이 올바르지 않습니다. .env 파일을 확인하세요.")
        return
    
    # 2. 구글 인증
    client = test_google_auth()
    if not client:
        logger.error("\n❌ 구글 인증에 실패했습니다.")
        return
    
    # 3. 시트 접근
    sheet_results = test_sheet_access(client)
    if not any(r.get("success") for r in sheet_results.values()):
        logger.error("\n❌ 모든 시트 접근에 실패했습니다.")
        return
    
    # 4. 데이터 파싱
    test_data_parsing(sheet_results)
    
    # 5. GuaranteeManager 동기화
    sync_success = test_guarantee_manager_sync()
    
    # 6. 저장 파일 확인
    test_data_storage()
    
    # 최종 결과
    logger.info("\n" + "=" * 70)
    if sync_success:
        logger.info("✅ 전체 테스트 성공! 동기화가 정상적으로 작동합니다.")
    else:
        logger.info("⚠️ 일부 테스트 실패. 위 로그를 확인하세요.")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()

