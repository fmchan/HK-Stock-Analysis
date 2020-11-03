from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, create_engine, ForeignKey, Table, REAL
from sqlalchemy.orm import relationship
from configs.base import Base
from configs.settings import DB_PATH

class Stock(Base):
    __tablename__ = 'stocks'
    id = Column(Integer, primary_key = True)
    provider = Column(String(100), nullable = False, index=True)
    market = Column(String(10), nullable = False)
    sid = Column(String(10), nullable = False, index=True)
    date = Column(DateTime, nullable = False, index=True)
    open = Column(REAL, nullable = False)
    high = Column(REAL, nullable = False)
    low = Column(REAL, nullable = False)
    close = Column(REAL, nullable = False)
    volume = Column(Integer, nullable = False)
    adj_close = Column(REAL, nullable = True)
    created_date = Column(DateTime, nullable = True)
    updated_date = Column(DateTime, nullable = True)

    def __init__(self, provider, market, sid, date, open, high, low, close, volume, adj_close, created_date, updated_date):
        self.provider = provider
        self.market = market
        self.sid = sid
        self.date = date
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.adj_close = adj_close
        self.created_date = created_date
        self.updated_date = updated_date

    def dict(self):
        return {'id': self.id,
                'provider': self.provider,
                'market': self.market,
                'sid': self.sid,
                'date': self.date,
                'open': self.open,
                'high': self.high,
                'low': self.low,
                'close': self.close,
                'volume': self.volume,
                'adj_close': self.adj_close,
                'created_date': self.created_date,
                'updated_date': self.updated_date
               }

engine = create_engine(DB_PATH)
Base.metadata.create_all(engine)