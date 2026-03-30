from collections.abc import Generator

import psycopg
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker

from app.core.config import settings


def _normalized_database_url(raw_url: str) -> str:
    if raw_url.startswith("postgresql://"):
        return raw_url.replace("postgresql://", "postgresql+psycopg://", 1)
    if raw_url.startswith("postgres://"):
        return raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    return raw_url


def _build_engine():
    # If individual DB params are set, use them directly.
    # This bypasses URL parsing entirely — necessary for Supabase pooler where
    # psycopg3's URL parser misroutes dotted usernames (postgres.project_ref).
    if settings.db_host and settings.db_user:
        def _creator():
            return psycopg.connect(
                host=settings.db_host,
                port=settings.db_port,
                dbname=settings.db_name,
                user=settings.db_user,
                password=settings.db_password,
                sslmode="require",
            )
        return create_engine(
            "postgresql+psycopg://",
            creator=_creator,
            future=True,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
    return create_engine(_normalized_database_url(settings.database_url), future=True)


engine = _build_engine()
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, class_=Session)


def get_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
