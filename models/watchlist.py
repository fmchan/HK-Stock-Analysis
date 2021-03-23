from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, create_engine, ForeignKey, Table, REAL
from sqlalchemy.orm import relationship
from configs.base import Base
from configs.settings import DB_ENGINE

class Watchlist(Base):
    __tablename__ = 'watchlist'
    id = Column(Integer, primary_key = True)
    sid = Column(String(10), nullable = False, index=True)
    pattern = Column(String(10), nullable = False, index=True)
    status = Column(String(4), nullable = False, index=True)
    start_date = Column(DateTime, nullable = True)
    end_date = Column(DateTime, nullable = True)
    created_date = Column(DateTime, nullable = True)
    updated_date = Column(DateTime, nullable = True)

    def __init__(self, sid, pattern, status, start_date):
        self.sid = sid
        self.pattern = pattern
        self.status = status
        self.start_date = start_date

    def dict(self):
        return {'id': self.id,
                'sid': self.sid,
                'pattern': self.pattern,
                'status': self.status,
                'start_date': self.start_date,
                'end_date': self.end_date,
                'created_date': self.created_date,
                'updated_date': self.updated_date
               }

engine = create_engine(DB_ENGINE)
Base.metadata.create_all(engine)