"""
데이터 보안 및 백업 모듈
"""
import os
import json
import hashlib
import base64
from datetime import datetime
from pathlib import Path
from cryptography.fernet import Fernet
import logging
import shutil

logger = logging.getLogger(__name__)


class DataSecurity:
    """데이터 암호화 및 백업 관리"""
    
    def __init__(self, data_dir: str = None):
        """초기화
        Args:
            data_dir: 데이터 저장 디렉토리 (기본: ./secure_data)
        """
        if data_dir is None:
            data_dir = os.path.join(os.getcwd(), "secure_data")
        
        self.data_dir = Path(data_dir)
        self.backup_dir = self.data_dir / "backups"
        
        # 디렉토리 생성
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.backup_dir.mkdir(parents=True, exist_ok=True)
        
        # 암호화 키 설정
        self.cipher = self._get_or_create_cipher()
        
        # 파일 권한 설정 (Unix/Linux 환경)
        if os.name != 'nt':  # Windows가 아닌 경우
            os.chmod(self.data_dir, 0o700)  # 소유자만 읽기/쓰기/실행
            os.chmod(self.backup_dir, 0o700)
    
    def _get_or_create_cipher(self) -> Fernet:
        """암호화 키 가져오기 또는 생성"""
        key_file = self.data_dir / ".encryption_key"
        
        # 환경변수에서 키 가져오기 (우선순위 1)
        env_key = os.getenv("DATA_ENCRYPTION_KEY")
        if env_key:
            try:
                # Fernet 키 형식 검증
                key_bytes = env_key.encode() if isinstance(env_key, str) else env_key
                cipher = Fernet(key_bytes)
                logger.info("✅ Using encryption key from environment variable")
                return cipher
            except Exception as e:
                logger.error(f"❌ Invalid encryption key format in DATA_ENCRYPTION_KEY: {e}")
                logger.error("💡 Please generate a valid Fernet key using: python generate_key.py")
                raise ValueError(f"Invalid DATA_ENCRYPTION_KEY format. Must be a valid Fernet key (44 chars, base64). Error: {e}")
        
        # 파일에서 키 가져오기 (우선순위 2)
        if key_file.exists():
            with open(key_file, 'rb') as f:
                key = f.read()
            logger.info("Using encryption key from file")
            return Fernet(key)
        
        # 새 키 생성
        key = Fernet.generate_key()
        with open(key_file, 'wb') as f:
            f.write(key)
        
        # 키 파일 권한 설정
        if os.name != 'nt':
            os.chmod(key_file, 0o600)  # 소유자만 읽기/쓰기
        
        logger.info("New encryption key generated")
        return Fernet(key)
    
    def encrypt_data(self, data: dict) -> bytes:
        """데이터 암호화"""
        json_str = json.dumps(data, ensure_ascii=False)
        json_bytes = json_str.encode('utf-8')
        encrypted = self.cipher.encrypt(json_bytes)
        return encrypted
    
    def decrypt_data(self, encrypted_data: bytes) -> dict:
        """데이터 복호화"""
        try:
            decrypted_bytes = self.cipher.decrypt(encrypted_data)
            json_str = decrypted_bytes.decode('utf-8')
            return json.loads(json_str)
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise
    
    def save_encrypted(self, data: dict, filename: str) -> bool:
        """암호화하여 저장"""
        try:
            filepath = self.data_dir / filename
            encrypted = self.encrypt_data(data)
            
            # 원본 파일 백업
            if filepath.exists():
                self.create_backup(filename)
            
            # 암호화된 데이터 저장
            with open(filepath, 'wb') as f:
                f.write(encrypted)
            
            # 파일 권한 설정
            if os.name != 'nt':
                os.chmod(filepath, 0o600)
            
            logger.info(f"Data saved securely: {filename}")
            return True
        except Exception as e:
            logger.error(f"Failed to save encrypted data: {e}")
            return False
    
    def load_encrypted(self, filename: str) -> dict:
        """암호화된 데이터 로드"""
        filepath = self.data_dir / filename
        
        if not filepath.exists():
            logger.warning(f"File not found: {filename}")
            return {}
        
        try:
            with open(filepath, 'rb') as f:
                encrypted = f.read()
            
            return self.decrypt_data(encrypted)
        except Exception as e:
            logger.error(f"Failed to load encrypted data: {e}")
            # 백업에서 복구 시도
            return self.restore_from_backup(filename)
    
    def create_backup(self, filename: str) -> bool:
        """백업 생성"""
        try:
            source = self.data_dir / filename
            if not source.exists():
                return False
            
            # 타임스탬프 포함 백업 파일명
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_name = f"{filename}.{timestamp}.bak"
            destination = self.backup_dir / backup_name
            
            shutil.copy2(source, destination)
            
            # 파일 권한 설정
            if os.name != 'nt':
                os.chmod(destination, 0o600)
            
            # 오래된 백업 정리 (최근 10개만 유지)
            self._cleanup_old_backups(filename, keep=10)
            
            logger.info(f"Backup created: {backup_name}")
            return True
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            return False
    
    def restore_from_backup(self, filename: str, backup_index: int = 0) -> dict:
        """백업에서 복구
        Args:
            filename: 원본 파일명
            backup_index: 백업 인덱스 (0 = 가장 최근)
        """
        try:
            # 백업 파일 찾기
            backup_files = sorted(
                self.backup_dir.glob(f"{filename}.*.bak"),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            if not backup_files:
                logger.warning(f"No backups found for {filename}")
                return {}
            
            if backup_index >= len(backup_files):
                logger.warning(f"Backup index {backup_index} out of range")
                return {}
            
            backup_file = backup_files[backup_index]
            
            with open(backup_file, 'rb') as f:
                encrypted = f.read()
            
            data = self.decrypt_data(encrypted)
            logger.info(f"Data restored from backup: {backup_file.name}")
            return data
        except Exception as e:
            logger.error(f"Restore from backup failed: {e}")
            return {}
    
    def _cleanup_old_backups(self, filename: str, keep: int = 10):
        """오래된 백업 정리"""
        try:
            backup_files = sorted(
                self.backup_dir.glob(f"{filename}.*.bak"),
                key=lambda x: x.stat().st_mtime,
                reverse=True
            )
            
            # keep 개수 초과 백업 삭제
            for old_backup in backup_files[keep:]:
                old_backup.unlink()
                logger.info(f"Old backup removed: {old_backup.name}")
        except Exception as e:
            logger.error(f"Cleanup failed: {e}")
    
    def export_decrypted(self, filename: str, export_path: str) -> bool:
        """복호화된 데이터 내보내기 (디버깅용)"""
        try:
            data = self.load_encrypted(filename)
            if not data:
                return False
            
            with open(export_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Data exported to: {export_path}")
            return True
        except Exception as e:
            logger.error(f"Export failed: {e}")
            return False
    
    def get_data_info(self) -> dict:
        """데이터 파일 정보 조회"""
        info = {
            "data_files": [],
            "backup_count": 0,
            "total_size": 0
        }
        
        # 데이터 파일 정보
        for file in self.data_dir.glob("*.enc"):
            stat = file.stat()
            info["data_files"].append({
                "name": file.name,
                "size": stat.st_size,
                "modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            })
            info["total_size"] += stat.st_size
        
        # 백업 파일 개수
        info["backup_count"] = len(list(self.backup_dir.glob("*.bak")))
        
        return info

