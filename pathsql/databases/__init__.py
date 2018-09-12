from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_engine('postgresql://localhost/patarapolw')
Base = declarative_base()
Session = sessionmaker(bind=engine)
session = Session()


if __name__ == '__main__':
    Base.metadata.create_all(engine)
