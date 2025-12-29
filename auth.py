"""
인증 및 사용자 관리 모듈
- 메인 관리자 계정: 서버 내 별도 저장
- 하청 계정: 관리자가 생성/관리
- 역할 기반 접근 제어: admin, manager, user
"""
import os
import json
import hashlib
import secrets
from datetime import datetime
from typing import Dict, List, Optional
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

# 역할 정의
ROLES = {
    "admin": {
        "level": 100,
        "name": "관리자",
        "description": "모든 기능 접근 가능, 계정 관리 권한"
    },
    "manager": {
        "level": 50,
        "name": "매니저",
        "description": "대부분 기능 접근 가능, 계정 생성 불가"
    },
    "user": {
        "level": 10, 
        "name": "일반 사용자",
        "description": "기본 조회 기능만 접근 가능"
    }
}


class AuthManager:
    """사용자 인증 및 관리 클래스"""
    
    def __init__(self, storage_path: str = None):
        """초기화
        Args:
            storage_path: 사용자 데이터 저장 경로
        """
        # Render Disk 경로 우선 사용
        disk_path = "/var/data"
        if storage_path is None:
            if os.path.isdir(disk_path):
                storage_path = os.path.join(disk_path, "users.json")
            else:
                storage_path = os.path.join(os.getcwd(), "users.json")
        
        self.storage_path = storage_path
        self.users = self._load_users()
        
        # 초기 관리자 계정이 없으면 생성
        if not self._has_admin():
            self._create_initial_admin()
    
    def _load_users(self) -> Dict:
        """저장된 사용자 데이터 로드"""
        if os.path.exists(self.storage_path):
            try:
                with open(self.storage_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    logger.info(f"Loaded {len(data.get('users', []))} users")
                    return data
            except Exception as e:
                logger.error(f"Failed to load users: {e}")
        
        return {
            "users": [],
            "updated_at": None
        }
    
    def _save_users(self) -> bool:
        """사용자 데이터 저장"""
        try:
            self.users["updated_at"] = datetime.now().isoformat()
            
            # 디렉토리 생성
            Path(self.storage_path).parent.mkdir(parents=True, exist_ok=True)
            
            with open(self.storage_path, 'w', encoding='utf-8') as f:
                json.dump(self.users, f, ensure_ascii=False, indent=2)
            
            logger.info(f"Saved {len(self.users.get('users', []))} users")
            return True
        except Exception as e:
            logger.error(f"Failed to save users: {e}")
            return False
    
    def _hash_password(self, password: str, salt: str = None) -> tuple:
        """비밀번호 해싱 (SHA-256 + salt)"""
        if salt is None:
            salt = secrets.token_hex(16)
        
        hashed = hashlib.sha256((password + salt).encode()).hexdigest()
        return hashed, salt
    
    def _verify_password(self, password: str, hashed: str, salt: str) -> bool:
        """비밀번호 검증"""
        check_hash, _ = self._hash_password(password, salt)
        return check_hash == hashed
    
    def _has_admin(self) -> bool:
        """관리자 계정 존재 여부 확인"""
        for user in self.users.get("users", []):
            if user.get("role") == "admin":
                return True
        return False
    
    def _create_initial_admin(self):
        """초기 관리자 계정 생성"""
        # 환경변수에서 초기 관리자 정보 가져오기 (없으면 기본값)
        admin_username = os.getenv("ADMIN_USERNAME", "admin")
        admin_password = os.getenv("ADMIN_PASSWORD", "admin1234")
        
        hashed, salt = self._hash_password(admin_password)
        
        admin_user = {
            "id": "admin_001",
            "username": admin_username,
            "password_hash": hashed,
            "password_salt": salt,
            "role": "admin",
            "name": "시스템 관리자",
            "created_at": datetime.now().isoformat(),
            "created_by": "system",
            "is_active": True
        }
        
        self.users["users"].append(admin_user)
        self._save_users()
        
        logger.info(f"Created initial admin account: {admin_username}")
        logger.warning("⚠️ Please change the default admin password!")
    
    def authenticate(self, username: str, password: str) -> Optional[Dict]:
        """사용자 인증
        Returns:
            인증 성공 시 사용자 정보, 실패 시 None
        """
        for user in self.users.get("users", []):
            if user.get("username") == username and user.get("is_active", True):
                if self._verify_password(
                    password, 
                    user.get("password_hash", ""),
                    user.get("password_salt", "")
                ):
                    # 마지막 로그인 시간 업데이트
                    user["last_login"] = datetime.now().isoformat()
                    self._save_users()
                    
                    # 민감 정보 제외하고 반환
                    return {
                        "id": user["id"],
                        "username": user["username"],
                        "role": user["role"],
                        "name": user.get("name", ""),
                        "last_login": user.get("last_login")
                    }
        return None
    
    def get_user(self, user_id: str) -> Optional[Dict]:
        """사용자 정보 조회"""
        for user in self.users.get("users", []):
            if user.get("id") == user_id:
                return {
                    "id": user["id"],
                    "username": user["username"],
                    "role": user["role"],
                    "name": user.get("name", ""),
                    "created_at": user.get("created_at"),
                    "is_active": user.get("is_active", True)
                }
        return None
    
    def get_all_users(self, include_inactive: bool = False) -> List[Dict]:
        """모든 사용자 목록 조회 (관리자용)"""
        users = []
        for user in self.users.get("users", []):
            if not include_inactive and not user.get("is_active", True):
                continue
            users.append({
                "id": user["id"],
                "username": user["username"],
                "role": user["role"],
                "name": user.get("name", ""),
                "created_at": user.get("created_at"),
                "created_by": user.get("created_by"),
                "last_login": user.get("last_login"),
                "is_active": user.get("is_active", True)
            })
        return users
    
    def create_user(self, username: str, password: str, role: str, name: str, created_by: str) -> Optional[Dict]:
        """새 사용자 생성 (관리자만 가능)
        Args:
            username: 사용자 아이디
            password: 비밀번호
            role: 역할 (admin, manager, user)
            name: 사용자 이름
            created_by: 생성자 ID
        """
        # 중복 체크
        for user in self.users.get("users", []):
            if user.get("username") == username:
                logger.warning(f"Username already exists: {username}")
                return None
        
        # 역할 유효성 체크
        if role not in ROLES:
            logger.warning(f"Invalid role: {role}")
            return None
        
        hashed, salt = self._hash_password(password)
        
        # 새 ID 생성
        user_count = len(self.users.get("users", []))
        user_id = f"user_{user_count + 1:03d}"
        
        new_user = {
            "id": user_id,
            "username": username,
            "password_hash": hashed,
            "password_salt": salt,
            "role": role,
            "name": name,
            "created_at": datetime.now().isoformat(),
            "created_by": created_by,
            "is_active": True
        }
        
        self.users["users"].append(new_user)
        self._save_users()
        
        logger.info(f"Created new user: {username} (role: {role}) by {created_by}")
        
        return {
            "id": user_id,
            "username": username,
            "role": role,
            "name": name
        }
    
    def update_user(self, user_id: str, updates: Dict, updated_by: str) -> Optional[Dict]:
        """사용자 정보 수정"""
        for idx, user in enumerate(self.users.get("users", [])):
            if user.get("id") == user_id:
                # 수정 가능한 필드만 업데이트
                allowed_fields = ["name", "role", "is_active"]
                for field in allowed_fields:
                    if field in updates:
                        user[field] = updates[field]
                
                user["updated_at"] = datetime.now().isoformat()
                user["updated_by"] = updated_by
                
                self.users["users"][idx] = user
                self._save_users()
                
                logger.info(f"Updated user: {user_id} by {updated_by}")
                return self.get_user(user_id)
        
        return None
    
    def change_password(self, user_id: str, new_password: str) -> bool:
        """비밀번호 변경"""
        for idx, user in enumerate(self.users.get("users", [])):
            if user.get("id") == user_id:
                hashed, salt = self._hash_password(new_password)
                user["password_hash"] = hashed
                user["password_salt"] = salt
                user["password_changed_at"] = datetime.now().isoformat()
                
                self.users["users"][idx] = user
                self._save_users()
                
                logger.info(f"Password changed for user: {user_id}")
                return True
        
        return False
    
    def delete_user(self, user_id: str, deleted_by: str) -> bool:
        """사용자 삭제 (소프트 삭제)"""
        for idx, user in enumerate(self.users.get("users", [])):
            if user.get("id") == user_id:
                # 관리자는 삭제 불가 (최소 1명 유지)
                if user.get("role") == "admin":
                    admin_count = sum(1 for u in self.users["users"] if u.get("role") == "admin" and u.get("is_active", True))
                    if admin_count <= 1:
                        logger.warning("Cannot delete the last admin")
                        return False
                
                user["is_active"] = False
                user["deleted_at"] = datetime.now().isoformat()
                user["deleted_by"] = deleted_by
                
                self.users["users"][idx] = user
                self._save_users()
                
                logger.info(f"Deleted user: {user_id} by {deleted_by}")
                return True
        
        return False
    
    def has_permission(self, user_role: str, required_level: int) -> bool:
        """권한 확인"""
        user_level = ROLES.get(user_role, {}).get("level", 0)
        return user_level >= required_level
    
    def get_role_info(self, role: str = None) -> Dict:
        """역할 정보 조회"""
        if role:
            return ROLES.get(role, {})
        return ROLES
