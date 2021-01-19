from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, create_engine, ForeignKey, Table, REAL
from sqlalchemy.orm import relationship
from configs.base import Base
from configs.settings import DB_ENGINE

class Pattern(Base):
    __tablename__ = 'patterns'
    start_date = Column(DateTime, primary_key = True)
    end_date = Column(DateTime, nullable = True, index=True)
    name = Column(String(100), primary_key = True)
    sid = Column(String(10), primary_key = True)
    created_date = Column(DateTime, nullable = True)
    updated_date = Column(DateTime, nullable = True)

    def __init__(self, start_date, end_date, name, sid, created_date, updated_date):
        self.start_date = start_date
        self.end_date = end_date
        self.name = name
        self.sid = sid
        self.created_date = created_date
        self.updated_date = updated_date

    def dict(self):
        return {'start_date': self.start_date,
                'end_date': self.end_date,
                'name': self.name,
                'sid': self.sid,
                'created_date': self.created_date,
                'updated_date': self.updated_date
               }

engine = create_engine(DB_ENGINE)
Base.metadata.create_all(engine)