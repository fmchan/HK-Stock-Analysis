from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from configs.settings import DB_ENGINE

engine = create_engine(DB_ENGINE)
Session = sessionmaker(bind=engine)

Base = declarative_base()