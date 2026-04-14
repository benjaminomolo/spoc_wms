import datetime

from sqlalchemy import func
from sqlalchemy.orm import Session
from app import LoanApplication, LoanHistory, LoanStatus, Session, create_notification, PayrollPayment, \
    DeductionPayment, NotificationType, PayrollTransaction, Deduction
from send_mail import notify_admin_and_user


# Define function to calculate daily interest amount
def calculate_daily_interest(principal, total_interest_rate, num_days, remaining_balance):
    # Calculate total interest
    total_interest = (principal * total_interest_rate) / 100

    # Calculate daily interest rate
    daily_interest_rate = total_interest / num_days

    # Calculate daily interest amount based on remaining balance
    daily_interest_amount = (daily_interest_rate * remaining_balance) / principal

    return daily_interest_amount


def check_overdue_loans_and_apply_fines():
    with Session() as db_session:  # Use the `with` statement to manage the session
        current_date = datetime.datetime.now().date()

        # Query for all loans where the payment date is passed and loan status is not 'paid'
        overdue_loans = db_session.query(LoanApplication).filter(
            LoanApplication.date_of_payment < current_date,
            LoanApplication.loan_status != LoanStatus.REPAID
        ).all()

        for loan in overdue_loans:
            overdue_days = (current_date - loan.date_of_payment).days
            fine_applied = 0
            loan_cycle_restarted = False

            # Check if loan is overdue for more than 200 days and not paid
            if overdue_days > 200 and loan.loan_status != LoanStatus.REPAID:
                loan.loan_status = LoanStatus.DISCONTINUED
                # Notify the admin and user
                notify_admin_and_user(loan, overdue_days, "discontinued")

            # Query LoanHistory to get the total paid amount and fines
            loan_history_entries = db_session.query(LoanHistory).filter_by(loan_id=loan.id).all()

            # Calculate the total paid amount and total fines (penalty + interest)
            total_paid = sum(entry.amount_paid for entry in loan_history_entries)
            total_penalty = sum(entry.penalty_amount for entry in loan_history_entries)
            total_interest = sum(entry.new_interest_amount for entry in loan_history_entries)
            total_fine = total_penalty + total_interest  # Total fine = Penalty + Interest
            total_discount_amount = sum(entry.discount_amount for entry in loan_history_entries)

            total_principal_and_interest = loan.principal_amount + loan.interest_amount

            # Calculate the total remaining amount (Principal + Interest + Fines - Total Paid)
            total_remaining_amount = total_principal_and_interest + total_fine - total_paid - total_discount_amount

            # Check if a fine has already been applied for the overdue period
            last_loan_history = db_session.query(LoanHistory).filter_by(loan_id=loan.id).order_by(
                LoanHistory.penalty_date.desc()).first()

            # Skip applying fine if a fine has already been applied
            if last_loan_history and last_loan_history.penalty_amount > 0:
                pass

            else:
                fine_applied = total_remaining_amount * 0.05
                total_repayment_amount = total_remaining_amount + fine_applied
                total_fine += fine_applied
                if loan.loan_status not in {LoanStatus.DISCONTINUED, LoanStatus.CANCELLED, LoanStatus.REPAID, LoanStatus.DEFAULTED}:
                    loan.loan_status = LoanStatus.EXTENDED  # Extend the loan

                    notify_admin_and_user(loan=loan, overdue_days=overdue_days, notification_type="fine_applied", fine_amount=fine_applied, total_repayment_amount=total_repayment_amount)

                    # Create an entry in loan history if fine has not been applied
                    loan_history = LoanHistory(
                        loan_id=loan.id,
                        repayment_amount=total_repayment_amount,  # Total repayment (principal + fines + interest)
                        penalty_amount=fine_applied,  # Fine applied
                        amount_paid=0,  # No payment yet
                        new_interest_amount=0,
                        deadline_date=loan.date_of_payment,
                        penalty_date=current_date,  # Date fine was applied,
                        total_fine_amount=total_fine,
                        restart_date=None,  # Restart date if the loan cycle was restarted
                        app_id=1,
                        new_due_date=current_date + datetime.timedelta(days=14) if not loan_cycle_restarted else None,
                        notes=f"Loan overdue by {overdue_days} days, Fine applied: {fine_applied} UGX."
                    )
                    db_session.add(loan_history)

            print(f'Here now with ver due days {overdue_days}')
            # Apply fines based on overdue days
            if overdue_days > 14:
                loan_cycle_restarted = True  # Restart the loan cycle
                fine_applied = 0  # Default to 0 in case fine was already applied
                loan.loan_status = LoanStatus.RESTARTED  # Mark as overdue again

                # Calculate new interest amount
                total_initial_interest_rate = (loan.interest_amount / loan.principal_amount) * 100
                initial_principal_amount = loan.principal_amount
                number_of_days = (loan.date_of_payment - loan.date_of_request).days

                daily_interest_amount = calculate_daily_interest(
                    initial_principal_amount, total_initial_interest_rate, number_of_days, total_remaining_amount
                )

                # Check if a fine has already been applied
                if not last_loan_history or last_loan_history.penalty_amount == 0:
                    fine_applied = total_remaining_amount * 0.05  # Apply fine on remaining balance
                    loan_cycle_restarted = True  # Restart the loan cycle
                    loan.loan_status = LoanStatus.OVERDUE  # Mark as overdue again

                total_fine = total_penalty + total_interest + fine_applied + daily_interest_amount  # Update total_fine to include new interest
                # Calculate the total repayment amount (including fines and new interest)
                total_repayment_amount = total_remaining_amount + fine_applied + daily_interest_amount
                total_fine += fine_applied

                # Notify admin and user with daily interest and new repayment total
                notify_admin_and_user(loan, overdue_days, "loan_restarted", daily_interest_amount, total_repayment_amount)

                # Create a loan history entry for the update
                loan_history = LoanHistory(
                    loan_id=loan.id,
                    repayment_amount=total_repayment_amount,  # Total repayment (principal + fines + interest)
                    penalty_amount=fine_applied,  # Fine applied (if any)
                    amount_paid=0,  # No payment yet
                    new_interest_amount=daily_interest_amount,  # Added daily interest
                    deadline_date=loan.date_of_payment,
                    penalty_date=current_date,  # Date fine was applied
                    total_fine_amount=total_fine,  # Total accumulated fines
                    restart_date=current_date if loan_cycle_restarted else None,  # Restart date if loan cycle restarted
                    new_due_date=current_date + datetime.timedelta(days=1) if loan_cycle_restarted else None,
                    app_id=1,
                    notes=f"Loan overdue by {overdue_days} days. Fine applied: {fine_applied} UGX. Daily interest: {daily_interest_amount} UGX. Loan cycle restarted: {loan_cycle_restarted}."
                )

                db_session.add(loan_history)
                db_session.commit()

                # Log for debugging
                print(
                    f"Loan ID {loan.id} updated: Status={loan.loan_status}, Fine Applied={fine_applied}, Daily Interest={daily_interest_amount}, Loan Restarted={loan_cycle_restarted}")

            # Commit changes for loan application and history
            db_session.commit()

            # Output for logging or debugging purposes
            print(
                f"Loan ID {loan.id} updated: Status={loan.loan_status}, Fine Applied={fine_applied}, Loan restarted={loan_cycle_restarted}")


def check_and_store_unposted_ledger_entries():
    """
    Checks for unposted payroll transactions and deduction entries per app_id, then creates notifications with URLs.
    """
    db_session = Session()

    try:
        # Define URLs for payroll and deductions
        payroll_url = "/payroll/pay_runs"
        deductions_url = "/payroll/employee_deductions"

        # Get all distinct app_ids with unposted payroll transactions or deduction entries
        app_ids = db_session.query(PayrollTransaction.app_id)\
            .filter(PayrollTransaction.is_posted_to_ledger == False)\
            .distinct().union(
                db_session.query(Deduction.app_id)
                .filter(Deduction.is_posted_to_ledger == False)
                .distinct()
            ).all()

        # Convert app_ids from list of tuples to a flat list
        app_ids = [a[0] for a in app_ids if a[0] is not None]

        for app_id in app_ids:
            # Count unposted payroll transactions for this app_id
            payroll_unposted_count = db_session.query(func.count(PayrollTransaction.id))\
                .filter(PayrollTransaction.is_posted_to_ledger == False,
                        PayrollTransaction.app_id == app_id)\
                .scalar()

            # Count unposted deduction entries for this app_id
            deduction_unposted_count = db_session.query(func.count(Deduction.id))\
                .filter(Deduction.is_posted_to_ledger == False,
                        Deduction.app_id == app_id)\
                .scalar()

            # Create notifications for unposted payroll transactions
            if payroll_unposted_count > 0:
                message = f"There are {payroll_unposted_count} payroll pay runs not posted to the ledger."
                create_notification(
                    db_session=db_session,
                    company_id=app_id,  # Store notification for the specific app_id
                    message=message,
                    type=NotificationType.warning,
                    is_popup=True,
                    url=payroll_url
                )

            # Create notifications for unposted deductions
            if deduction_unposted_count > 0:
                message = f"There are {deduction_unposted_count} employee deductions not posted to the ledger."
                create_notification(
                    db_session=db_session,
                    company_id=app_id,  # Store notification for the specific app_id
                    message=message,
                    type=NotificationType.warning,
                    is_popup=True,
                    url=deductions_url
                )

        # Commit all notifications
        db_session.commit()

    except Exception as e:
        # Rollback if there's any error
        db_session.rollback()
        print(f"Error occurred: {e}")

    finally:
        # Ensure the session is always closed after operation
        db_session.close()


# Call the function to check overdue loans and apply fines/restart
if __name__ == "__main__":
    check_overdue_loans_and_apply_fines()
    check_and_store_unposted_ledger_entries()
