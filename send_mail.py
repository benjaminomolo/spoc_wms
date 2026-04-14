import smtplib
import uuid

EMAIL_SOURCE = "creditrustcapitalsolutions@gmail.com"
GMAIL_SMTP = "smtp.gmail.com"
PASSWORD = "ycaz hlpr tmcr djqp"
ADMIN_EMAIL = ["benjaminandrewomolo@gmail.com", "creditrustcapitalsolutions@gmail.com"]

EMAIL_SOURCE_NETBOOKS = "netbookssuite.com"
PASSWORD_NB = "gywd lkcf hydk dwek"


def generate_reset_token():
    return str(uuid.uuid4())


def send_mail(to_adds, subject, body_message):
    if isinstance(to_adds, str):
        to_adds = [to_adds]  # Convert single email to list

    try:
        with smtplib.SMTP(GMAIL_SMTP, 587) as connection:
            connection.starttls()
            connection.login(EMAIL_SOURCE, PASSWORD)

            for recipient in to_adds:
                print(f"Sending email to: {recipient} with the subject {subject}")
                connection.sendmail(
                    from_addr=EMAIL_SOURCE,
                    to_addrs=recipient,  # Send email to one recipient at a time
                    msg=f"Subject: {subject}\n\n{body_message}"
                )
        return True  # Add this line to indicate success
    except Exception as e:
        print(f"Error sending email: {e}")  # Print the error for debugging
        return False


def nb_send_mail(to_adds, subject, body_message):
    if isinstance(to_adds, str):
        to_adds = [to_adds]  # Convert single email to list

    try:
        with smtplib.SMTP(GMAIL_SMTP, 587) as connection:
            connection.starttls()
            connection.login(EMAIL_SOURCE_NETBOOKS, PASSWORD_NB)

            for recipient in to_adds:
                print(f"Sending email to: {recipient} with the subject {subject}")
                connection.sendmail(
                    from_addr=EMAIL_SOURCE_NETBOOKS,
                    to_addrs=recipient,  # Send email to one recipient at a time
                    msg=f"Subject: {subject}\n\n{body_message}"
                )
        return True  # Add this line to indicate success
    except Exception as e:
        print(f"Error sending email: {e}")  # Print the error for debugging
        return False


def send_reset_email(to_add, token):
    subject = "Password Reset Request"
    reset_link = f"http://127.0.0.1:5000/reset-password?token={token}"  # Replace with your actual URL
    body_message = f"Click the following link to reset your password: {reset_link}"

    send_mail(to_add, subject, body_message)


def notify_applicant(user_email, user_name, loan_amount, loan_term_days, repayment_sum):
    """
    Sends an email notification to the applicant upon loan request submission.

    Parameters:
    user_email (str): The user's email address.
    user_name (str): The user's name.
    loan_amount (float): The amount of the loan requested.
    loan_term_days (int): The loan duration in days.
    repayment_sum (float): The total repayment amount.

    Returns:
    bool: True if the email was sent successfully, False otherwise.
    """
    try:
        subject_user = "Loan Request Submitted"
        body_user = (
            f"Dear {user_name},\n\n"
            f"Your loan request for UGX {loan_amount:,.0f} has been successfully submitted.\n"
            f"Loan Term: {loan_term_days} days\n"
            f"Total Repayment Amount: UGX {repayment_sum:,.0f}\n\n"
            "We will review your application and notify you of the decision shortly.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )

        send_mail(user_email, subject_user, body_user)
        return True

    except Exception as e:
        print(f"Error sending email to applicant: {e}")  # Replace this with flash() if needed
        return False


def notify_admin(user_email, user_name, loan_amount, loan_term_days, repayment_sum):
    """
    Sends an email notification to the admin/loan officer about the new loan application.

    Parameters:
    user_email (str): The user's email address.
    user_name (str): The user's name.
    loan_amount (float): The amount of the loan requested.
    loan_term_days (int): The loan duration in days.
    repayment_sum (float): The total repayment amount.
    admin_email (str): The admin's email address.

    Returns:
    bool: True if the email was sent successfully, False otherwise.
    """
    try:
        subject_admin = "New Loan Application Logged"
        body_admin = (
            f"Hello,\n\n"
            f"A new loan application has been submitted with the following details:\n\n"
            f"Name: {user_name}\n"
            f"Email: {user_email}\n"
            f"Loan Amount: UGX {loan_amount:,.0f}\n"
            f"Loan Term: {loan_term_days} days\n"
            f"Total Repayment: {repayment_sum:,.0f}\n\n"
            "Please review the application and take the necessary actions.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions System"
        )

        send_mail(ADMIN_EMAIL, subject_admin, body_admin)
        return True

    except Exception as e:
        print(f"Error sending email to admin: {e}")  # Replace this with flash() if needed
        return False


def notify_loan_approval(user_email, user_name, loan_amount, payment_number, payment_name):
    """
    Sends an email notification to the user upon loan approval, including payment details.

    Parameters:
    user_email (str): The user's email address.
    user_name (str): The user's name.
    loan_amount (float): The approved loan amount.
    payment_number (str): The payment number for loan repayment.
    payment_name (str): The account name for loan repayment.

    Returns:
    None
    """
    subject = "Loan Request Approved"
    body = (
        f"Dear {user_name},\n\n"
        f"Your loan request for UGX {loan_amount:,.0f} has been approved. "
        f"Our team will contact you with further details.\n\n"
        f"To make a payment, please use the following details:\n"
        f"Payment Number: {payment_number}\n"
        f"Account Name: {payment_name}\n\n"
        "If you have any questions, feel free to reach out.\n\n"
        "Best regards,\n"
        "Creditrust Capital Solutions"
    )

    send_mail(user_email, subject, body)


def notify_admin_and_user(loan, overdue_days, notification_type,
                          daily_interest_amount=None, total_repayment_amount=None, fine_amount=None):
    """
    Generic function to notify the admin and user about loan events.

    Parameters:
    loan (LoanApplication): The loan object.
    overdue_days (int): Number of days the loan is overdue.
    notification_type (str): The type of notification, e.g., "discontinued", "fine_applied", "loan_restarted".
    daily_interest_amount (float, optional): The calculated daily interest amount.
    total_repayment_amount (float, optional): The new total repayment amount.
    fine_amount (float, optional): The fine applied.

    Returns:
    None
    """
    user_email = loan.loan_applicant.email
    user_name = loan.loan_applicant.name
    loan_id = loan.id

    daily_interest_text = ""
    repayment_text = ""
    fine_text = ""

    if daily_interest_amount is not None:
        daily_interest_text = f"\nYour new daily interest rate is: UGX {daily_interest_amount:,.2f}."

    if fine_amount is not None:
        fine_text = f"\nA fine of UGX {fine_amount:,.2f} has been applied to your loan."

    if total_repayment_amount is not None:
        repayment_text = f"\nYour new total repayment amount is: UGX {total_repayment_amount:,.2f}."


    if notification_type == "discontinued":
        subject_user = "Loan Discontinued"
        body_user = (
            f"Dear {user_name},\n\n"
            f"Your loan (ID: {loan_id}) has been discontinued as it is overdue by {overdue_days} days. "
            f"Please contact us for further assistance.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )

        subject_admin = "Loan Discontinued"
        body_admin = (
            f"Hello,\n\n"
            f"Loan ID {loan_id} has been discontinued as it is overdue by {overdue_days} days. "
            f"Applicant: {user_name} ({user_email}).\n\n"
            "Please take necessary action.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )

    elif notification_type == "fine_applied":
        subject_user = "Late Payment Fine Applied"
        body_user = (
            f"Dear {user_name},\n\n"
            f"A late payment fine has been applied to your loan (ID: {loan_id}) due to an overdue period of {overdue_days} days. "
            f"{fine_text}.\n\n"
            f"{repayment_text}.\n\n"
            f"Please check your updated loan statement and make the necessary payments.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )

        subject_admin = "Late Payment Fine Applied"
        body_admin = (
            f"Hello,\n\n"
            f"A fine has been applied to Loan ID {loan_id}, which is overdue by {overdue_days} days. "
            f"Applicant: {user_name} ({user_email}).\n\n"
            "Please monitor the payment status.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )


    elif notification_type == "loan_restarted":
        subject_user = "Loan Cycle Restarted"
        body_user = (
            f"Dear {user_name},\n\n"
            f"Your loan (ID: {loan_id}) has exceeded the grace period of 14 days without full repayment. "
            f"As a result, the loan cycle has restarted with the applicable interest rate and fines. "
            f"Please check your updated repayment schedule.{daily_interest_text}{repayment_text}\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )

        subject_admin = "Loan Cycle Restarted"
        body_admin = (
            f"Hello,\n\n"
            f"Loan ID {loan_id} has restarted its cycle after exceeding the 14-day grace period. "
            f"Applicant: {user_name} ({user_email}).\n"
            f"New daily interest rate: UGX {daily_interest_amount:,.2f}.\n"
            f"New total repayment amount: UGX {total_repayment_amount:,.2f}.\n\n"
            "Please take the necessary actions.\n\n"
            "Best regards,\n"
            "Creditrust Capital Solutions"
        )

    else:
        print("Invalid notification type.")
        return

    # Send notifications
    send_mail(user_email, subject_user, body_user)  # Notify the user
    send_mail(ADMIN_EMAIL, subject_admin, body_admin)  # Notify the admin
