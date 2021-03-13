from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi import Depends
import base64

security = HTTPBasic()


def get_auth_header(credentials: HTTPBasicCredentials = Depends(security)):
    token_b64 = base64.b64encode(f"{credentials.username}:{credentials.password}".encode())
    token = token_b64.decode()
    return {"Authorization": f"Basic {token}"}