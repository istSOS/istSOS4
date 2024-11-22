from app.oauth import authenticate_user, create_access_token
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from fastapi.security import OAuth2PasswordRequestForm

v1 = APIRouter()


@v1.api_route("/Login", methods=["POST"], tags=["Login"])
async def login_for_access_token(
    form_data: OAuth2PasswordRequestForm = Depends(),
):
    user = await authenticate_user(form_data.username)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token, expire = create_access_token(data={"sub": user["username"]})
    return JSONResponse(
        status_code=status.HTTP_200_OK,
        content={
            "access_token": access_token,
            "token_type": "bearer",
            "expires_in": expire,
        },
    )
