from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from storage.config import CONFIG

engine = create_engine(
    CONFIG["SQLALCHEMY_DATABASE_URI"],
    echo=False,
    pool_pre_ping=True,
    future=True
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
    future=True
)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
