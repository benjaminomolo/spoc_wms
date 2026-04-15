from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool  # ADD THIS LINE
from models import Base

# SQLite database - no connection limits, perfect for demo
# The database file will be created in your spoc_wms directory
engine = create_engine(
    'sqlite:///netbooks3.db',
    echo=False,  # Set to True if you want to see SQL logs
    connect_args={'check_same_thread': False},  # Required for Flask
    poolclass=NullPool,  # CHANGE THIS LINE (remove pool_size and max_overflow)
)



Session = sessionmaker(bind=engine, expire_on_commit=False)

# Create all tables automatically
Base.metadata.create_all(engine)




