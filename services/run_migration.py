# run_migration.py
#!/usr/bin/env python3
import logging
from db import Session
from services.migrations import migrate_transactions_to_journals, verify_migration

logging.basicConfig(level=logging.INFO)


def main():
    db_session = Session()
    try:

        migrate_transactions_to_journals(db_session)
        verify_migration(db_session)
    finally:
        db_session.close()


if __name__ == "__main__":
    main()
