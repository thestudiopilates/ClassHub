from fastapi import APIRouter

from app.api.routes import admin, auth, bookings, clients, ops, ui

api_router = APIRouter()
api_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_router.include_router(ops.router, prefix="/ops", tags=["ops"])
api_router.include_router(bookings.router, prefix="/bookings", tags=["bookings"])
api_router.include_router(clients.router, prefix="/clients", tags=["clients"])
api_router.include_router(admin.router, prefix="/admin", tags=["admin"])
api_router.include_router(ui.router, tags=["ui"])
