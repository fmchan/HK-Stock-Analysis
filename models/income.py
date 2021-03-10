from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, DateTime, create_engine, ForeignKey, Table, REAL
from sqlalchemy.orm import relationship
from configs.base import Base
from configs.settings import DB_ENGINE

# Total Revenue
# Gross Profit
# Net Income
# Basic EPS
# Normalized EBITDA

class Income(Base):
    __tablename__ = 'incomes'
    id = Column(Integer, primary_key = True)
    source = Column(String(20), nullable = False, index=True)
    sid = Column(String(10), nullable = False, index=True)
    period = Column(String(10), nullable = False, index=True)
    year = Column(String(4), nullable = False, index=True)
    frequency = Column(String(10), nullable = False, index=True)
    currency = Column(String(10), nullable = True)
    total_revenue = Column(String(20), nullable = True)
    gross_profit = Column(String(20), nullable = True)
    net_income = Column(String(20), nullable = True)
    basic_eps = Column(String(20), nullable = True)
    norm_ebitda = Column(String(20), nullable = True)
    net_profit = Column(String(20), nullable = True)
    net_profit_growth = Column(String(20), nullable = True)
    eps_growth = Column(String(20), nullable = True)
    dividend_per_share = Column(String(20), nullable = True)
    created_date = Column(DateTime, nullable = True)
    updated_date = Column(DateTime, nullable = True)

    def __init__(self, source, sid, period, year, frequency, currency):
        self.source = source
        self.sid = sid
        self.period = period
        self.year = year
        self.frequency = frequency
        self.currency = currency

    # income = Income()
    # setattr(income, "total_revenue", "100")

    def dict(self):
        return {'id': self.id,
                'source': self.source,
                'sid': self.sid,
                'period': self.period,
                'year': self.year,
                'frequency': self.frequency,
                'currency': self.currency,
                'created_date': self.created_date,
                'updated_date': self.updated_date
               }

engine = create_engine(DB_ENGINE)
Base.metadata.create_all(engine)