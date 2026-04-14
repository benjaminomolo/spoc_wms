# app/services/migrations.py - UPDATED FOR SQLITE
from sqlalchemy import select, func, case, text
from datetime import datetime
import logging

logger = logging.getLogger(__name__)

def debug_database_state(db_session):
    """Debug function to check what's in the database - SQLite version"""
    logger.info("=== DATABASE DEBUG INFORMATION ===")

    # Check if transactions table exists and has data (SQLite version)
    try:
        # SQLite doesn't have information_schema, use sqlite_master instead
        result = db_session.execute(text("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name='transactions'
        """)).scalar()
        logger.info(f"Transactions table exists: {result > 0}")

        if result > 0:
            # Check row count
            count = db_session.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
            logger.info(f"Transactions table row count: {count}")

            # Check sample data
            sample = db_session.execute(text("SELECT * FROM transactions LIMIT 5")).fetchall()
            if sample:
                logger.info("Sample transactions:")
                for row in sample:
                    logger.info(f"  {row}")
            else:
                logger.info("No sample data found in transactions table")
        else:
            logger.info("Transactions table does not exist")

    except Exception as e:
        logger.error(f"Error checking transactions table: {e}")

    # Check if journals table exists (SQLite version)
    try:
        result = db_session.execute(text("""
            SELECT COUNT(*) FROM sqlite_master 
            WHERE type='table' AND name='journals'
        """)).scalar()
        logger.info(f"Journals table exists: {result > 0}")

        if result > 0:
            count = db_session.execute(text("SELECT COUNT(*) FROM journals")).scalar()
            logger.info(f"Journals table row count: {count}")

    except Exception as e:
        logger.error(f"Error checking journals table: {e}")

    # List all tables to see what's actually in the database
    try:
        tables = db_session.execute(text("""
            SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
        """)).scalars().all()
        logger.info(f"All tables in database: {tables}")
    except Exception as e:
        logger.error(f"Error listing tables: {e}")

    logger.info("=== END DEBUG ===")

def migrate_transactions_to_journals(db_session):
    """
    Migrate data from Transaction table to Journal/JournalEntry tables
    """
    try:
        logger.info("Starting migration from Transaction to Journal/JournalEntry...")

        # First, debug the current database state
        debug_database_state(db_session)

        # Import models here to avoid circular imports
        from app.models.transaction import Transaction
        from app.models.journal import Journal, JournalEntry

        # Check if we can query the Transaction model directly
        transaction_count = db_session.execute(select(func.count(Transaction.id))).scalar()
        logger.info(f"Transaction model query count: {transaction_count}")

        if transaction_count == 0:
            logger.warning("No transactions found using ORM model. Checking raw SQL...")
            raw_count = db_session.execute(text("SELECT COUNT(*) FROM transactions")).scalar()
            logger.info(f"Raw SQL transaction count: {raw_count}")

            if raw_count > 0:
                logger.error("ORM model query returns 0 but raw SQL shows data!")
                logger.error("This suggests a model configuration issue")
                return 0, 0

        # Get all distinct journal numbers from transactions
        journal_numbers = db_session.execute(
            select(Transaction.journal_number).distinct()
        ).scalars().all()

        total_journals = len(journal_numbers)
        logger.info(f"Found {total_journals} journals to migrate")

        # If no journals found, try raw SQL approach
        if total_journals == 0:
            logger.info("Trying raw SQL approach to get journal numbers...")
            journal_numbers = db_session.execute(
                text("SELECT DISTINCT journal_number FROM transactions")
            ).scalars().all()
            total_journals = len(journal_numbers)
            logger.info(f"Raw SQL found {total_journals} journals")

        migrated_count = 0
        error_count = 0

        for i, journal_number in enumerate(journal_numbers, 1):
            try:
                # Check if journal already exists (idempotent migration)
                existing_journal = db_session.execute(
                    select(Journal).where(Journal.journal_number == journal_number)
                ).scalar_one_or_none()

                if existing_journal:
                    logger.info(f"Journal {journal_number} already exists, skipping...")
                    continue

                # Get all transactions for this journal number
                transactions = db_session.execute(
                    select(Transaction).where(Transaction.journal_number == journal_number)
                ).scalars().all()

                if not transactions:
                    continue

                # Use the first transaction to create journal header info
                first_tx = transactions[0]

                # Calculate totals
                total_debit = 0
                total_credit = 0

                for tx in transactions:
                    if tx.dr_cr == 'D':
                        total_debit += tx.amount
                    else:
                        total_credit += tx.amount

                # Create journal header
                journal = Journal(
                    journal_number=journal_number,
                    date=first_tx.date,
                    date_added=first_tx.date_added,
                    payment_mode_id=first_tx.payment_mode_id,
                    project_id=first_tx.project_id,
                    vendor_id=first_tx.vendor_id,
                    currency_id=first_tx.currency,
                    created_by=first_tx.created_by,
                    updated_by=first_tx.updated_by,
                    app_id=first_tx.app_id,
                    source_type=first_tx.source_type,
                    source_id=first_tx.source_id,
                    exchange_rate_id=first_tx.exchange_rate_id,
                    reconciled=first_tx.reconciled,
                    reconciliation_date=first_tx.reconciliation_date,
                    adjustment_date=first_tx.adjustment_date,
                    adjustment_reason=first_tx.adjustment_reason,
                    total_debit=total_debit,
                    total_credit=total_credit,
                    balance=total_debit - total_credit,
                    status='Posted'
                )

                db_session.add(journal)
                db_session.flush()  # Get the journal ID

                # Create journal entries
                for line_number, tx in enumerate(transactions, 1):
                    entry = JournalEntry(
                        journal_id=journal.id,
                        line_number=line_number,
                        date=tx.date,
                        subcategory_id=tx.subcategory_id,
                        amount=tx.amount,
                        dr_cr=tx.dr_cr[0] if tx.dr_cr else 'D',
                        description=tx.description,
                        source_type=tx.source_type,
                        source_id=tx.source_id,
                        date_added=tx.date_added,
                        reconciled=tx.reconciled,
                        reconciliation_date=tx.reconciliation_date,
                        # Denormalized fields
                        journal_number=journal.journal_number,
                        app_id=journal.app_id
                    )
                    db_session.add(entry)

                migrated_count += 1

                # Commit in batches to avoid huge transactions
                if i % 100 == 0:
                    db_session.commit()
                    logger.info(f"Migrated {i}/{total_journals} journals...")

            except Exception as e:
                error_count += 1
                db_session.rollback()
                logger.error(f"Error migrating journal {journal_number}: {str(e)}")
                # Continue with next journal

        # Final commit
        db_session.commit()

        logger.info(f"Migration completed! Success: {migrated_count}, Errors: {error_count}")

        return migrated_count, error_count

    except Exception as e:
        db_session.rollback()
        logger.error(f"Migration failed: {str(e)}", exc_info=True)
        raise
