import time
import uuid
from datetime import datetime, timedelta
from jose import jwt, JWTError

# Users with roles
USERS = {
    "Gayathri": {"password": "password1", "role": "customer"},
    "Sarthak": {"password": "password2", "role": "producer"},
    "Sushma": {"password": "password3", "role": "customer"},
}

# JWT Configuration
SECRET_KEY = "super_secret_key"
ALGORITHM = "HS256"
SESSION_TTL = 60 * 60  # 1 hour

# System token for internal LLM server
SYSTEM_TOKEN = str(uuid.uuid4())

def get_system_token():
    """Return system token."""
    return SYSTEM_TOKEN

def create_jwt_token(username, role):
    """Create JWT token with expiry."""
    expire = datetime.utcnow() + timedelta(seconds=SESSION_TTL)
    payload = {
        "username": username,
        "role": role,
        "exp": expire
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)
    return token

def decode_jwt_token(token):
    """Decode JWT and verify validity."""
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return True, payload["username"], payload["role"]
    except JWTError:
        return False, None, None

def authenticate(username, password):
    """Authenticate user and return (success, token, role)."""
    user = USERS.get(username)
    if user and user["password"] == password:
        token = create_jwt_token(username, user["role"])
        return True, token, user["role"]
    return False, None, None

def validate_token(token):
    """Validate token and return (valid, username, role)."""
    # Allow system token for internal LLM server
    if token == SYSTEM_TOKEN:
        return True, "system", "producer"

    valid, username, role = decode_jwt_token(token)
    return valid, username, role

def logout(token):
    """Simulate logout — JWTs are stateless, so we just acknowledge."""
    # You could implement a blacklist if needed, but for demo, just return True
    return True
