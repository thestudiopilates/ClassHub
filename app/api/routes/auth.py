from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from app.services.momence.client import MomenceClient
from app.services.momence.token_store import clear_tokens, load_tokens

router = APIRouter()

STATE_COOKIE = "momence_oauth_state"


@router.get("/momence/login")
async def momence_login() -> RedirectResponse:
    client = MomenceClient()
    if not client.client_id or not client.client_secret:
        raise HTTPException(status_code=500, detail="Missing MOMENCE_CLIENT_ID or MOMENCE_CLIENT_SECRET.")
    state = client.generate_oauth_state()
    response = RedirectResponse(client.get_authorization_url(state), status_code=302)
    response.set_cookie(
        STATE_COOKIE,
        state,
        httponly=True,
        secure=False,
        samesite="lax",
        max_age=600,
    )
    return response


@router.get("/momence/callback")
async def momence_callback(
    request: Request,
    code: Optional[str] = Query(default=None),
    state: Optional[str] = Query(default=None),
    error: Optional[str] = Query(default=None),
) -> HTMLResponse:
    if error:
        raise HTTPException(status_code=400, detail=f"Momence authorization failed: {error}")

    cookie_state = request.cookies.get(STATE_COOKIE)
    if not code or not state or not cookie_state or cookie_state != state:
        raise HTTPException(status_code=400, detail="Invalid or expired OAuth state.")

    client = MomenceClient()
    tokens = await client.exchange_authorization_code(code)
    access_token = tokens.get("access_token")
    refresh_token = tokens.get("refresh_token")
    expires_at = tokens.get("expires_at", "unknown")

    body = f"""
    <html>
      <head><title>Momence Connected</title></head>
      <body style="font-family: sans-serif; padding: 32px;">
        <h1>Momence authorization complete</h1>
        <p>The Host API tokens are now stored locally for this app.</p>
        <p><strong>Access token:</strong> {"stored" if access_token else "missing"}</p>
        <p><strong>Refresh token:</strong> {"stored" if refresh_token else "missing"}</p>
        <p><strong>Expires at:</strong> {expires_at}</p>
        <p><a href="/v1/demo">Open the demo</a></p>
      </body>
    </html>
    """
    response = HTMLResponse(body)
    response.delete_cookie(STATE_COOKIE)
    return response


@router.get("/momence/status")
def momence_status() -> dict:
    tokens = load_tokens()
    return {
        "connected": bool(tokens and tokens.get("access_token")),
        "has_refresh_token": bool(tokens and tokens.get("refresh_token")),
        "expires_at": tokens.get("expires_at") if tokens else None,
    }


@router.post("/momence/logout")
def momence_logout() -> dict[str, bool]:
    clear_tokens()
    return {"cleared": True}
