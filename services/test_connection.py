# test_connection.py
#!/usr/bin/env python3
import logging
from db import Session
from services.migrations import debug_database_state

logging.basicConfig(level=logging.INFO)

def test_connection():
    db_session = Session()
    try:
        print("Testing database connection...")
        debug_database_state(db_session)
    finally:
        db_session.close()

if __name__ == "__main__":
    test_connection()
