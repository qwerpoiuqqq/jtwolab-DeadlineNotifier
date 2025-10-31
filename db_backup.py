"""
데이터베이스 백업/복원 유틸리티
Render의 임시 파일시스템 문제를 해결하기 위해 Google Cloud Storage에 백업
"""
import os
import sqlite3
import logging
from datetime import datetime
import pytz

logger = logging.getLogger(__name__)


def backup_to_google_drive():
    """SQLite DB를 Google Drive에 백업 (선택사항)"""
    # TODO: Google Drive API 사용하여 백업 구현
    pass


def restore_from_google_drive():
    """Google Drive에서 SQLite DB 복원 (선택사항)"""
    # TODO: Google Drive API 사용하여 복원 구현
    pass


def export_rank_history_to_json(db_path: str = "rank_history.db") -> dict:
    """순위 히스토리를 JSON으로 내보내기"""
    if not os.path.exists(db_path):
        return {"ranks": [], "exported_at": None}
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT company, business_name, keyword, rank, checked_at, source
            FROM rank_history
            ORDER BY checked_at DESC
        """)
        
        rows = cursor.fetchall()
        conn.close()
        
        ranks = []
        for company, business_name, keyword, rank, checked_at, source in rows:
            ranks.append({
                "company": company,
                "business_name": business_name,
                "keyword": keyword,
                "rank": rank,
                "checked_at": checked_at,
                "source": source
            })
        
        kst = pytz.timezone('Asia/Seoul')
        return {
            "ranks": ranks,
            "exported_at": datetime.now(kst).isoformat(),
            "count": len(ranks)
        }
    except Exception as e:
        logger.error(f"Failed to export rank history: {e}")
        return {"ranks": [], "exported_at": None, "error": str(e)}


def import_rank_history_from_json(data: dict, db_path: str = "rank_history.db") -> bool:
    """JSON에서 순위 히스토리 가져오기"""
    try:
        # DB 초기화
        from rank_crawler import RankDatabase
        db = RankDatabase(db_path)
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 기존 데이터 삭제
        cursor.execute("DELETE FROM rank_history")
        
        # 데이터 삽입
        ranks = data.get("ranks", [])
        for rank_item in ranks:
            cursor.execute("""
                INSERT INTO rank_history 
                (company, business_name, keyword, rank, checked_at, source)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                rank_item.get("company"),
                rank_item.get("business_name"),
                rank_item.get("keyword"),
                rank_item.get("rank"),
                rank_item.get("checked_at"),
                rank_item.get("source", "adlog")
            ))
        
        conn.commit()
        conn.close()
        
        logger.info(f"Imported {len(ranks)} rank records")
        return True
    except Exception as e:
        logger.error(f"Failed to import rank history: {e}")
        return False

