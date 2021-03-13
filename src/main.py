from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from api import router as es_router
from elasticsearch.exceptions import AuthenticationException, AuthorizationException

app = FastAPI()
app.include_router(es_router)


@app.exception_handler(AuthenticationException)
def handle_authentication_exception(request: Request, exc: AuthenticationException):
    return JSONResponse(
        status_code=401,
        content={"message": "Unauthorized"}
    )


@app.exception_handler(AuthorizationException)
def handle_authorization_exception(request: Request, exc: AuthorizationException):
    return JSONResponse(
        status_code=403,
        content={"message": "Unauthorized"}
    )
