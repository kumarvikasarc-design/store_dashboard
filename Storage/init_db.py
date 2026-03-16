from storage.storage_sql import engine
from storage.models import Base

def init_db():
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Database Ready!")
