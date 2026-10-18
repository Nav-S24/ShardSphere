from typing import AsyncGenerator
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.config import settings
from app.models.metadata import Base

# Database engine initialization
database_url = settings.DATABASE_URL

# For SQLite, enable check_same_thread=False
connect_args = {}
if "sqlite" in database_url:
    connect_args = {"check_same_thread": False}

async_engine = create_async_engine(
    database_url,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True,
)

AsyncSessionLocal = async_sessionmaker(
    bind=async_engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
)


async def init_db() -> None:
    """Creates database tables if they do not already exist."""
    async with async_engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """Dependency that yields a database session per request."""
    async with AsyncSessionLocal() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
