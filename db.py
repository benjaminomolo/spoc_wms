from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from models import Base
from configs import db_username, db_password, db_hostname, db_name


# Create SQLAlchemy engine with pool_recycle to avoid timeout issues
# After upgrading to Web Dev plan
engine = create_engine(
    f'mysql+mysqldb://{db_username}:{db_password}@{db_hostname}/{db_name}',
    pool_recycle=28000,
    pool_pre_ping=True,
    pool_size=10,          # Comfortable base
    max_overflow=20,       # Handle peaks
    pool_timeout=30,
)

Session = sessionmaker(bind=engine, expire_on_commit=False)

Base.metadata.create_all(engine)
