from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class Store(Base):
    __tablename__ = "stores"

    id = Column(Integer, primary_key=True, index=True)
    store_id = Column(Integer, unique=True, nullable=False)
    outlet_name = Column(String(255), nullable=False)
    region = Column(String(50), nullable=False)
    city = Column(String(100), nullable=False)
    type = Column(String(100), nullable=False)
    area_manager = Column(String(100), nullable=False)
    opening_date = Column(Date, nullable=False)
    status = Column(String(20), nullable=False)
