import datetime
import logging
import os
import pandas as pd
import traceback
from collections import defaultdict
from decimal import Decimal
from functools import partial
from io import BytesIO
from PIL import Image as PILImage
from flask import Blueprint, send_file, request, jsonify
from flask_login import login_required, current_user

from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT, TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from sqlalchemy import func, or_, case, and_, literal, desc
from sqlalchemy.orm import joinedload

from ai import get_base_currency

from db import Session

from models import PayrollPeriod, Company, PayrollTransaction, Deduction, Benefit, AdvanceRepayment, Currency, \
    ChartOfAccounts, Category, SalesTransaction, DirectSalesTransaction, OrderStatus, PaymentAllocation, \
    SalesPaymentStatus, Project, SalesInvoice, PurchaseTransaction, DirectPurchaseTransaction, PurchaseOrder, Vendor, \
    SalesInvoiceItem, PurchasePaymentStatus, Quotation, QuotationItem, SalesOrder, SalesOrderItem, DirectSaleItem, \
    PaymentReceipt, PaymentMode, PurchaseOrderItem, DirectPurchaseItem, DeliveryNote, DeliveryNoteItem, InventoryEntry, \
    InventoryItemVariationLink, InventoryItem, InventoryLocation, InventoryTransactionDetail, InventoryEntryLineItem, \
    ExchangeRate, JournalEntry, Journal, InvoiceStatus, PurchasePaymentAllocation, BulkPayment
from report_routes import calculate_account_summary, get_bank_transactions, get_latest_exchange_rate, \
    get_historical_exchange_rate, calculate_net_income, convert_amount
from services.chart_of_accounts_helpers import get_retained_earnings_account_id
from services.inventory_helpers import get_user_accessible_locations
from utils import get_cash_balances, is_cash_related, get_cash_balances_with_base, get_monetary_accounts, \
    calculate_fx_revaluation, get_item_details, get_converted_cost
from utils_and_helpers.amounts_utils import format_amount
from utils_and_helpers.date_time_utils import get_date_range_from_filter

download_route = Blueprint('download_route', __name__)

logger = logging.getLogger(__name__)


def add_footer(canvas, doc, company):
    from app import UPLOAD_FOLDER_FOOTER
    canvas.saveState()

    if company.footer_type == 'text' and company.footer:
        footer_style = ParagraphStyle(
            name='FooterStyle',
            fontName='Roboto',
            fontSize=8,
            alignment=TA_CENTER,
            textColor=colors.grey,
        )
        footer_text = Paragraph(company.footer, footer_style)
        footer_text.wrapOn(canvas, doc.width, doc.bottomMargin)
        footer_text.drawOn(canvas, doc.leftMargin, 20)


    elif company.footer_type == 'template' and company.footer:
        try:
            image_path = os.path.join(UPLOAD_FOLDER_FOOTER, company.footer)

            # Load image with PIL to get dimensions
            pil_img = PILImage.open(image_path)
            original_width, original_height = pil_img.size

            max_width = doc.width
            max_height = 100  # Adjust footer height as needed

            # Calculate aspect ratio
            aspect_ratio = original_width / original_height

            # Scale to fit within max dimensions
            if original_width > max_width:
                display_width = max_width
                display_height = max_width / aspect_ratio
            else:
                display_width = original_width
                display_height = original_height

            if display_height > max_height:
                display_height = max_height
                display_width = max_height * aspect_ratio

            # Draw image
            footer_img = Image(image_path, width=display_width, height=display_height)
            footer_img.drawOn(canvas, doc.leftMargin, 20)

        except Exception as e:
            logger.error(f"Failed to draw footer image: {e}")

    # Add page number
    page_num = canvas.getPageNumber()
    canvas.setFont("Roboto", 9)
    canvas.setFillColor(colors.grey)
    canvas.drawCentredString(doc.width / 2 + doc.leftMargin, 10, f"Page {page_num}")

    canvas.restoreState()


def add_pages(canvas, doc):
    """
    Adds a footer with a thank-you message and page numbers to every page of the PDF.
    """
    # Save the current state of the canvas
    canvas.saveState()

    # Define the footer text and style
    footer_style = ParagraphStyle(
        name='FooterStyle',
        fontName='Roboto',  # Use a regular font
        fontSize=8,
        alignment=TA_CENTER,  # Center-align the footer
        textColor=colors.grey,  # Grey color for the footer
        spaceBefore=10,  # Add some space above the footer
    )

    # Add the page number
    page_num = canvas.getPageNumber()  # Get the current page number
    page_num_text = f"Page {page_num}"  # Format the page number text
    canvas.setFont("Roboto", 9)  # Set font for the page number
    canvas.setFillColor(colors.grey)  # Set text color for the page number
    canvas.drawCentredString(doc.width / 2 + doc.leftMargin, 10, page_num_text)  # Position the page number

    # Restore the canvas state
    canvas.restoreState()


@download_route.route('/download_payrun/<int:payroll_period_id>', methods=['GET'])
@login_required
def download_payrun(payroll_period_id):
    from app import UPLOAD_FOLDER_LOGOS  # import here inside the route function to avoid circular import
    try:
        with Session() as db_session:
            # Fetch the payroll period and related data
            payroll_period = db_session.query(PayrollPeriod).filter_by(id=payroll_period_id).first()
            if not payroll_period:
                return "Payroll period not found", 404

            company = db_session.query(Company).filter_by(id=payroll_period.app_id).first()

            # Get all payroll transactions for this period
            transactions = db_session.query(PayrollTransaction).filter_by(
                payroll_period_id=payroll_period_id, app_id=current_user.app_id
            ).all()

            # Organize employees by currency
            employees_by_currency = {}
            for transaction in transactions:
                employee = transaction.employees
                currency = transaction.currency

                if currency.id not in employees_by_currency:
                    employees_by_currency[currency.id] = {
                        'code': currency.user_currency,
                        'employees': []
                    }

                # Get all deductions for this transaction
                deductions = db_session.query(Deduction).filter_by(
                    payroll_transaction_id=transaction.id
                ).all()
                total_deductions = sum(
                    float(d.amount) if d.amount is not None else 0.0 for d in deductions) if deductions else 0

                # Get all benefits for this transaction
                benefits = db_session.query(Benefit).filter_by(
                    payroll_transaction_id=transaction.id
                ).all()
                total_benefits = sum(
                    float(b.amount) if b.amount is not None else 0.0 for b in benefits) if benefits else 0

                # Get advance payments for this period
                advance_payments = db_session.query(AdvanceRepayment).filter_by(
                    payroll_id=payroll_period_id,
                    advance_payment_id=employee.advance_payments[0].id if employee.advance_payments else None
                ).all()

                total_advance = sum(a.payment_amount for a in advance_payments if
                                    a.payment_amount is not None) if advance_payments else 0

                # Safely calculate with explicit null checks
                gross = float(transaction.gross_salary) if transaction.gross_salary is not None else 0.0
                deductions = float(total_deductions) if total_deductions is not None else 0.0
                advance = float(total_advance) if total_advance is not None else 0.0
                benefits = float(total_benefits) if total_benefits is not None else 0.0

                # Perform calculations with consistent types
                overall_deductions = deductions + advance
                net_salary = gross - overall_deductions + benefits

                # Ensure net_salary is float before storing
                net_salary = float(net_salary)

                # Debug print employee data

                employees_by_currency[currency.id]['employees'].append({
                    'id': employee.employee_id or "-",
                    'name': f"{employee.first_name} {employee.last_name}",
                    'employee_id': employee.employee_id,
                    'gross_salary': gross,
                    'total_deductions': deductions,
                    'total_advance': advance,
                    'total_benefits': benefits,
                    'net_salary': float(net_salary),
                    'payment_status': transaction.payment_status.value,
                    'department': employee.department.department_name if employee.department else 'N/A'
                })

            # Prepare PDF layout in landscape
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=landscape(letter),
                                    rightMargin=36, leftMargin=36,
                                    topMargin=36, bottomMargin=36)

            # Define professional styles
            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(
                name='ReportTitle',
                fontName='Roboto-Bold',
                fontSize=18,
                alignment=TA_RIGHT,
                spaceAfter=12
            ))
            styles.add(ParagraphStyle(
                name='ReportTitle2',
                fontName='Roboto-Bold',
                fontSize=18,
                alignment=TA_LEFT,
                spaceAfter=12
            ))
            styles.add(ParagraphStyle(
                name='SectionHeader',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                spaceAfter=6,
                leftIndent=10,
            ))
            styles.add(ParagraphStyle(
                name='NormalBold',
                fontName='Roboto-Bold',
                fontSize=11,
                alignment=TA_CENTER
            ))
            styles.add(ParagraphStyle(
                name='RightAlign',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_RIGHT
            ))
            styles.add(ParagraphStyle(
                name='RightAlignBold',
                fontName='Roboto-Bold',
                fontSize=10,
                alignment=TA_RIGHT
            ))

            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define styles with reduced left margin
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=20  # Line height matches font size
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=0,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            headers_row = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            elements = []

            # Header with company logo and title
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        company.name, styles['ReportTitle2']),
                    Paragraph("PAYROLL REPORT", styles['ReportTitle'])
                ]
            ]
            header_table = Table(header_data, colWidths=[300, 400])
            header_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))
            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Payrun details in one row
            details_data = [[
                Paragraph(f"<b>Payroll Period:</b> {payroll_period.payroll_period_name}", headers_row),
                Paragraph(
                    f"<b>Period Dates:</b> {payroll_period.start_date.strftime('%d-%b-%Y')} to {payroll_period.end_date.strftime('%d-%b-%Y')}",
                    headers_row),
                Paragraph(f"<b>Status:</b> {payroll_period.payment_status.value}", headers_row),
                Paragraph(f"<b>Generated On:</b> {datetime.datetime.now().strftime('%d-%b-%Y %H:%M')}", headers_row)
            ]]

            details_table = Table(details_data, colWidths=[200, 240, 100, 180])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LEFTPADDING', (0, 0), (-1, -1), 0),
                ('RIGHTPADDING', (0, 0), (-1, -1), 0),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Process each currency group
            for currency_id, currency_data in employees_by_currency.items():
                employees = currency_data['employees']
                currency_code = currency_data['code']

                # Add currency header
                elements.append(Paragraph(f"Currency: {currency_code}", styles['SectionHeader']))
                elements.append(Spacer(1, 6))

                # Prepare employee data table with professional columns
                employee_data = [
                    [
                        Paragraph("ID", styles['NormalBold']),
                        Paragraph("Employee Name", styles['NormalBold']),
                        Paragraph("Department", styles['NormalBold']),
                        Paragraph(f"Gross Salary", styles['NormalBold']),
                        Paragraph(f"Deductions", styles['NormalBold']),
                        Paragraph(f"Advance<br/>Payments", styles['NormalBold']),
                        Paragraph(f"Benefits", styles['NormalBold']),
                        Paragraph(f"Net Salary", styles['NormalBold']),
                        Paragraph("Status", styles['NormalBold'])
                    ]
                ]

                for emp in employees:
                    employee_data.append([
                        Paragraph(emp['employee_id'] or "-", normal_style),
                        Paragraph(emp['name'], normal_style),
                        Paragraph(emp['department']),
                        Paragraph(f"{emp['gross_salary']:,.2f}", styles['RightAlign']),
                        Paragraph(f"{emp['total_deductions']:,.2f}", styles['RightAlign']),
                        Paragraph(f"{emp['total_advance']:,.2f}", styles['RightAlign']),
                        Paragraph(f"{emp['total_benefits']:,.2f}", styles['RightAlign']),
                        Paragraph(f"{emp['net_salary']:,.2f}", styles['RightAlign']),
                        Paragraph(emp['payment_status'], normal_style)
                    ])
                print(f'here how')
                # Calculate currency totals
                currency_gross = sum(emp['gross_salary'] for emp in employees)
                print(f'here ho2w')
                currency_deductions = sum(emp['total_deductions'] for emp in employees)
                currency_advance = sum(emp['total_advance'] for emp in employees)
                print(f'here hows {currency_advance}')
                currency_benefits = sum(emp['total_benefits'] for emp in employees)
                currency_net = sum(emp['net_salary'] for emp in employees)

                # Add totals row
                employee_data.append([
                    "", f"TOTAL ({currency_code})", "",
                    Paragraph(f"{currency_gross:,.2f}", styles['RightAlignBold']),
                    Paragraph(f"{currency_deductions:,.2f}", styles['RightAlignBold']),
                    Paragraph(f"{currency_advance:,.2f}", styles['RightAlignBold']),
                    Paragraph(f"{currency_benefits:,.2f}", styles['RightAlignBold']),
                    Paragraph(f"{currency_net:,.2f}", styles['RightAlignBold']),
                    ""
                ])

                # Create table with professional styling
                emp_table = Table(employee_data,
                                  colWidths=[53, 90, 75, 100, 80, 77, 70, 90, 50],
                                  repeatRows=1)

                emp_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#f5f5f5")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (3, 0), (7, -1), 'RIGHT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('FONTNAME', (0, 1), (-1, -2), 'Roboto'),
                    ('FONTSIZE', (0, 0), (-1, -1), 9),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 8),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('GRID', (0, 0), (-1, -2), 0.5, colors.lightgrey),
                    ('LINEBELOW', (0, -1), (-1, -1), 1, colors.black),
                    ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                    ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#f9f9f9")),
                    ('BOX', (0, 0), (-1, -1), 0.2, colors.black),  # Outer border

                    # Apply inner grid lines for internal borders (excluding outer borders)
                    ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),  # Internal grid lines
                ]))

                elements.append(emp_table)
                elements.append(Spacer(1, 12))

            # Professional signature section
            elements.append(Spacer(1, 24))

            signature_style = ParagraphStyle(
                name='Signature',
                fontName='Roboto',
                fontSize=10,
                leading=50,
                spaceAfter=20
            )

            signature_data = [
                [
                    Paragraph("Prepared by:", signature_style),
                    Paragraph("________________________", signature_style),
                    Paragraph("Date: ________________________", signature_style)
                ],
                [
                    Paragraph("Approved by:", signature_style),
                    Paragraph("________________________", signature_style),
                    Paragraph("Date: ________________________", signature_style)
                ]
            ]

            signature_table = Table(signature_data, colWidths=[80, 200, 200])
            signature_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'BOTTOM'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('LEADING', (0, 0), (-1, -1), 14),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
            ]))

            elements.append(signature_table)

            # Generate and save PDF
            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Payroll_Report_{payroll_period.payroll_period_name}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        print(f"Error generating payroll report: {e}")
        return f"An error occurred: {e}", 500


@download_route.route('/download_balance_sheet_report', methods=['POST'])
@login_required
def download_balance_sheet_report():
    db_session = Session()
    try:
        def format_currency(value):
            """Format currency values with parentheses for negatives"""
            if value < 0:
                return f"({abs(value):,.2f})"
            return f"{value:,.2f}"

        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        company_name = company.name if company else "Company"

        # Get filter parameters
        filters = request.json
        date_str = filters.get('endDate')
        hide_zero = filters.get('hideZero', False)

        if not date_str:
            return jsonify({'error': 'Date is required'}), 400

        as_of_date = datetime.datetime.strptime(date_str, '%Y-%m-%d').date()

        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({'error': 'Base currency not defined'}), 400

        base_currency_id = base_currency_info['base_currency_id']
        base_currency_code = base_currency_info['base_currency']
        base_currency_id_int = base_currency_id
        base_currency_code_str = base_currency_code

        # ===== EXACT COPY OF YOUR WORKING API =====
        # Load accounts with specific fields only
        accounts = db_session.query(
            ChartOfAccounts.id,
            ChartOfAccounts.parent_account_type,
            ChartOfAccounts.normal_balance,
            ChartOfAccounts.category,
            ChartOfAccounts.sub_category,
            ChartOfAccounts.is_cash,
            ChartOfAccounts.is_bank,
            ChartOfAccounts.is_receivable,
            ChartOfAccounts.is_payable,
            ChartOfAccounts.is_system_account
        ).filter_by(
            app_id=app_id,
            is_active=True
        ).all()

        # Create account info with optimized structure
        account_info = {}
        monetary_account_ids = set()

        # When loading accounts, determine is_monetary based on account flags
        for a in accounts:
            acc_id = a.id
            # Monetary if it's cash, bank, receivable, or payable
            is_monetary = a.is_cash or a.is_bank or a.is_receivable or a.is_payable

            account_info[acc_id] = {
                'type': a.parent_account_type,
                'parent': a.category,
                'name': a.sub_category,
                'is_monetary': is_monetary,  # Use the derived value
                'is_system_account': a.is_system_account,
                'normal_balance': a.normal_balance
            }
            if is_monetary:
                monetary_account_ids.add(acc_id)

        # Get exchange rates
        # Get all currency IDs
        currencies = db_session.query(Currency.id).filter(Currency.app_id == app_id).all()
        foreign_currency_ids = [c.id for c in currencies if c.id != base_currency_id]
        from report_routes import get_latest_exchange_rates_batch
        exchange_rates = {base_currency_id: 1.0}
        if foreign_currency_ids:
            batch_rates = get_latest_exchange_rates_batch(
                db_session, app_id, foreign_currency_ids, base_currency_id, as_of_date
            )
            exchange_rates.update(batch_rates)

        # Initialize balances with pre-computed fields
        balances = {
            acc_id: {
                'type': data['type'],
                'parent': data['parent'],
                'name': data['name'],
                'is_monetary': data['is_monetary'],
                'is_system_account': data['is_system_account'],
                'normal_balance': data['normal_balance'],
                'currency_balances': {base_currency_id_int: {'debit': 0.0, 'credit': 0.0}},
                'fx_gain_loss': 0.0,
                'net_balance': 0.0,
                'debit_total': 0.0,
                'credit_total': 0.0,
            }
            for acc_id, data in account_info.items()
        }

        # Single query for all journal data
        from sqlalchemy import case
        monetary_accounts_list = list(monetary_account_ids)

        combined_query = db_session.query(
            JournalEntry.subcategory_id,
            Journal.currency_id,
            JournalEntry.dr_cr,
            Journal.exchange_rate_id,
            func.sum(JournalEntry.amount).label('total_amount'),
            func.max(
                case(
                    (JournalEntry.subcategory_id.in_(monetary_accounts_list), 1),
                    else_=0
                )
            ).label('is_monetary_flag')
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).filter(
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.date <= as_of_date
        ).group_by(
            JournalEntry.subcategory_id,
            Journal.currency_id,
            JournalEntry.dr_cr,
            Journal.exchange_rate_id
        ).all()

        # Collect and pre-load ALL exchange_rate_ids in one pass
        all_exchange_rate_ids = set()
        exchange_rate_mapping = {}

        for row in combined_query:
            subcategory_id, currency_id, dr_cr, exchange_rate_id, total_amount, is_monetary_flag = row
            if currency_id != base_currency_id_int and exchange_rate_id:
                all_exchange_rate_ids.add(exchange_rate_id)
            exchange_rate_mapping[(subcategory_id, currency_id, dr_cr, exchange_rate_id)] = (
                total_amount, is_monetary_flag)

        # Load ALL exchange rates at once
        exchange_rate_cache = {}
        if all_exchange_rate_ids:
            rate_ids_list = list(all_exchange_rate_ids)
            rate_objs = db_session.query(ExchangeRate.id, ExchangeRate.rate).filter(
                ExchangeRate.id.in_(rate_ids_list)
            ).all()
            exchange_rate_cache.update({rate_id: float(rate) for rate_id, rate in rate_objs})

        # Process ALL results in one loop
        for (subcategory_id, currency_id, dr_cr, exchange_rate_id), (
                total_amount, is_monetary_flag) in exchange_rate_mapping.items():
            if subcategory_id not in balances:
                continue

            acc_data = balances[subcategory_id]
            is_monetary = bool(is_monetary_flag)
            total_amount_float = float(total_amount)

            # Convert to base currency with minimal branching
            if currency_id == base_currency_id_int:
                converted_amount = total_amount_float
            else:
                if is_monetary:
                    # Monetary: use latest rate
                    rate = exchange_rates.get(currency_id, 1.0)
                else:
                    # Non-monetary: use journal's historical rate from cache
                    rate = exchange_rate_cache.get(exchange_rate_id, exchange_rates.get(currency_id, 1.0))

                converted_amount = total_amount_float * rate

            # Add to appropriate balance with direct field access
            if dr_cr == 'D':
                acc_data['currency_balances'][base_currency_id_int]['debit'] += converted_amount
                acc_data['debit_total'] += converted_amount
            else:
                acc_data['currency_balances'][base_currency_id_int]['credit'] += converted_amount
                acc_data['credit_total'] += converted_amount

            # Calculate FX gain/loss for monetary foreign currency entries
            if is_monetary and currency_id != base_currency_id_int:
                current_rate = exchange_rates.get(currency_id, 1.0)
                historical_rate = exchange_rate_cache.get(exchange_rate_id, current_rate)

                if current_rate != historical_rate:
                    fx_diff = (current_rate - historical_rate) * total_amount_float
                    if dr_cr == 'C':
                        fx_diff *= -1
                    acc_data['fx_gain_loss'] += fx_diff

        # Compute net balances once for all accounts
        for acc_id, data in balances.items():
            account_type = data['type']

            if account_type == 'Asset':
                data['net_balance'] = data['debit_total'] - data['credit_total']
            elif account_type in ['Liability', 'Equity', 'Income']:
                data['net_balance'] = data['credit_total'] - data['debit_total']
            elif account_type == 'Expense':
                data['net_balance'] = data['debit_total'] - data['credit_total']

        # Initialize formatted structure
        formatted = defaultdict(list)
        summary = {'assets': 0.0, 'liabilities': 0.0, 'equity': 0.0,
                   'components': {'net_income': 0.0, 'fx_gain_loss': 0.0}}

        # Handle suspense account - move its balance to FX Gain/Loss
        for acc_id, data in balances.items():
            if 'Suspense' in data['name']:
                suspense_balance = data['net_balance']
                if abs(suspense_balance) > 0.01:
                    summary['components']['fx_gain_loss'] -= suspense_balance
                    data['net_balance'] = 0
                    data['debit_total'] = 0
                    data['credit_total'] = 0
                break

        category_totals = defaultdict(float)

        # Single-pass through all accounts
        system_retained_earnings_account = None
        accounts_by_type_category = defaultdict(lambda: defaultdict(list))

        for acc_id, data in balances.items():
            account_type = data['type']
            net_balance = data['net_balance']
            is_system = data['is_system_account']

            # Update summary totals
            if account_type == 'Asset':
                summary['assets'] += net_balance
            elif account_type == 'Liability':
                summary['liabilities'] += net_balance
            elif account_type == 'Equity':
                summary['equity'] += net_balance
            elif account_type == 'Income':
                summary['components']['net_income'] += net_balance
            elif account_type == 'Expense':
                summary['components']['net_income'] -= net_balance

            # Track FX gain/loss
            if data['is_monetary']:
                summary['components']['fx_gain_loss'] += data['fx_gain_loss']

            # Find ONLY SYSTEM retained earnings account
            if account_type == 'Equity' and 'retained earnings' in data['name'].lower() and is_system:
                system_retained_earnings_account = acc_id

            # Skip formatting for income/expense accounts
            if account_type in ['Income', 'Expense']:
                continue

            # Prepare account entry
            category = data['parent']
            account_entry = {
                'type': account_type,
                'category': category,
                'subcategory': data['name'],
                'balance': net_balance,
                'currency': base_currency_code_str,
                'is_monetary': data['is_monetary'],
                'is_system_account': is_system,
                'account_id': acc_id
            }

            accounts_by_type_category[account_type][category].append(account_entry)
            category_totals[category] += net_balance

        # Build formatted structure efficiently
        for account_type in ['Asset', 'Liability', 'Equity']:
            if account_type not in accounts_by_type_category:
                continue

            formatted_section = []
            type_categories = accounts_by_type_category[account_type]

            for category, accounts in type_categories.items():
                if accounts:
                    formatted_section.extend(accounts)
                    category_total = sum(acc['balance'] for acc in accounts)
                    formatted_section.append({
                        'type': account_type,
                        'category': category,
                        'subcategory': f'{category} Subtotal',
                        'balance': category_total,
                        'currency': base_currency_code_str,
                        'is_monetary': False,
                        'is_subtotal': True
                    })

            if formatted_section:
                section_total = sum(acc['balance'] for acc in formatted_section if not acc.get('is_subtotal', False))
                formatted_section.append({
                    'type': account_type,
                    'category': account_type,
                    'subcategory': f'{account_type} Total',
                    'balance': section_total,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_subtotal': True
                })

                formatted[account_type] = formatted_section

        # Handle equity adjustments
        net_income = summary['components']['net_income']
        total_fx_gain_loss = summary['components']['fx_gain_loss']

        # Handle net income - ONLY use SYSTEM retained earnings account
        if abs(net_income) > 0.01:
            if system_retained_earnings_account:
                for i, item in enumerate(formatted.get('Equity', [])):
                    if item.get('account_id') == system_retained_earnings_account:
                        formatted['Equity'][i]['balance'] += net_income
                        summary['equity'] += net_income
                        break
            else:
                # No system retained earnings exists - add directly
                retained_earnings_entry = {
                    'type': 'Equity',
                    'category': 'Retained Earnings',
                    'subcategory': 'Retained Earnings',
                    'balance': net_income,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_system_account': False,
                    'account_id': None
                }

                if 'Equity' not in formatted:
                    formatted['Equity'] = []

                insert_index = 0
                for i, item in enumerate(formatted['Equity']):
                    if item.get('is_subtotal', False):
                        insert_index = i
                        break
                formatted['Equity'].insert(insert_index, retained_earnings_entry)
                summary['equity'] += net_income

        # Add FX gain/loss
        if abs(total_fx_gain_loss) > 0.01:
            if 'Equity' in formatted:
                insert_index = 0
                for i, item in enumerate(formatted['Equity']):
                    if item.get('is_subtotal', False):
                        insert_index = i
                        break

                fx_entry = {
                    'type': 'Equity',
                    'category': 'System Equity',
                    'subcategory': 'Unrealized FX Gain/Loss',
                    'balance': total_fx_gain_loss,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_fx_component': True
                }

                formatted['Equity'].insert(insert_index, fx_entry)
                summary['equity'] += total_fx_gain_loss

        # Recalculate equity total after adjustments
        if 'Equity' in formatted:
            equity_items = [item for item in formatted['Equity'] if not item.get('is_subtotal', False)]
            if equity_items:
                equity_by_category = defaultdict(list)
                for item in equity_items:
                    equity_by_category[item['category']].append(item)

                new_equity_section = []
                equity_total = 0.0

                for category, accounts in equity_by_category.items():
                    new_equity_section.extend(accounts)
                    category_total = sum(acc['balance'] for acc in accounts)
                    new_equity_section.append({
                        'type': 'Equity',
                        'category': category,
                        'subcategory': f'{category} Subtotal',
                        'balance': category_total,
                        'currency': base_currency_code_str,
                        'is_monetary': False,
                        'is_subtotal': True
                    })
                    equity_total += category_total

                new_equity_section.append({
                    'type': 'Equity',
                    'category': 'Equity',
                    'subcategory': 'Equity Total',
                    'balance': equity_total,
                    'currency': base_currency_code_str,
                    'is_monetary': False,
                    'is_subtotal': True
                })
                formatted['Equity'] = new_equity_section

        # Extract data for PDF in the format your PDF generator expects
        assets_data = formatted.get('Asset', [])
        liabilities_data = formatted.get('Liability', [])
        equity_data = formatted.get('Equity', [])

        # After building formatted structure but before PDF generation
        # After building formatted structure but before PDF generation
        if hide_zero:
            for section in ['Asset', 'Liability', 'Equity']:
                if section in formatted:
                    filtered_section = []
                    category_balances = defaultdict(float)

                    # First pass: calculate category totals including only non-zero accounts
                    for item in formatted[section]:
                        if not item.get('is_subtotal', False):
                            if abs(item.get('balance', 0)) > 0.01:
                                category_balances[item.get('category', '')] += item.get('balance', 0)

                    # Second pass: build filtered section
                    current_category = None
                    for item in formatted[section]:
                        is_subtotal = item.get('is_subtotal', False)
                        category = item.get('category', '')

                        if is_subtotal:
                            # Check if this category subtotal should be shown
                            if abs(category_balances.get(category, 0)) > 0.01 or category == section:
                                filtered_section.append(item)
                        else:
                            # Only include non-zero accounts
                            if abs(item.get('balance', 0)) > 0.01:
                                filtered_section.append(item)

                    formatted[section] = filtered_section

        # Then extract data for PDF
        assets_data = formatted.get('Asset', [])
        liabilities_data = formatted.get('Liability', [])
        equity_data = formatted.get('Equity', [])

        # ===== YOUR PDF GENERATION CODE (keep exactly as you had it) =====
        # Prepare PDF layout
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=72)
        styles = getSampleStyleSheet()
        bold_style = styles['Heading2']
        italic_style = styles['Italic']

        report_period = f"as of {as_of_date.strftime('%Y-%m-%d')}"

        elements = [
            Paragraph(f"{company_name}", bold_style),
            Paragraph("Balance Sheet Report", bold_style),
            Paragraph(f"Period: {report_period}", styles['Normal']),
            Paragraph(f"Currency: {base_currency_code_str}", styles['Normal']),
            Spacer(1, 12),
            Paragraph("Assets", bold_style)
        ]

        # Helper function to create table with subtotals
        def create_section_table(section_data, section_name):
            right_align_style = ParagraphStyle(
                'RightAlign',
                parent=styles['Normal'],
                alignment=TA_RIGHT
            )

            table_data = [["Category", "Subcategory", f"Amount ({base_currency_code_str})"]]
            current_category = None

            # Filter out any duplicate or weird entries
            clean_data = []
            seen = set()
            for item in section_data:
                # Skip if it's a duplicate total or weird entry
                if item.get('is_subtotal') and 'Total' in item.get('subcategory', ''):
                    continue
                key = (item.get('category', ''), item.get('subcategory', ''))
                if key not in seen:
                    seen.add(key)
                    clean_data.append(item)

            clean_data.sort(key=lambda x: (x.get('category', ''), x.get('is_subtotal', False)))

            for item in clean_data:
                is_subtotal = item.get('is_subtotal', False)
                is_fx_component = item.get('is_fx_component', False)
                category = item.get('category', '')
                subcategory = item.get('subcategory', '')
                balance = item.get('balance', 0)

                if category != current_category and not is_subtotal and category:
                    current_category = category
                    table_data.append([Paragraph(f"<b>{current_category}</b>", styles['Normal']), "", ""])

                if is_subtotal:
                    table_data.append([
                        "",
                        Paragraph(f"<b><i>{subcategory}</i></b>", italic_style),
                        Paragraph(f"<b><i>{format_currency(balance)}</i></b>", right_align_style)
                    ])
                elif is_fx_component:
                    table_data.append(["", Paragraph(f"<i>{subcategory}</i>", italic_style),
                                       format_currency(balance)])
                else:
                    table_data.append(["", subcategory, format_currency(balance)])

            # Add section total at the END only once
            table_data.append([Paragraph(f"<b>Total {section_name}</b>", styles['Normal']), "",
                               format_currency(summary[section_name.lower()])])

            table = Table(table_data, colWidths=[150, 200, 100])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('LINEBELOW', (0, -1), (-1, -1), 1, colors.black),
                ('SPAN', (0, -1), (1, -1)),
                ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#F8F9FA")),
                ('BACKGROUND', (0, 1), (-1, -2), colors.white),
            ]))

            return table
        # Assets Section
        if assets_data:
            assets_table = create_section_table(assets_data, "Assets")
            elements.append(assets_table)
            elements.append(Spacer(1, 12))

        # Liabilities Section
        if liabilities_data:
            elements.append(Paragraph("Liabilities", bold_style))
            liabilities_table = create_section_table(liabilities_data, "Liabilities")
            elements.append(liabilities_table)
            elements.append(Spacer(1, 12))

        # Equity Section
        if equity_data:
            elements.append(Paragraph("Equity", bold_style))
            equity_table = create_section_table(equity_data, "Equity")
            elements.append(equity_table)
            elements.append(Spacer(1, 12))

        # Total Liabilities and Equity
        total_liabilities_and_equity = summary['liabilities'] + summary['equity']
        total_data = [["Total Liabilities and Equity", "", format_currency(total_liabilities_and_equity)]]
        total_table = Table(total_data, colWidths=[150, 200, 100])
        total_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
            ('SPAN', (0, 0), (1, 0)),
            ('BACKGROUND', (0, 0), (-1, 0), colors.white),
        ]))
        elements.append(total_table)

        # Generate and send PDF
        doc.build(elements)
        buffer.seek(0)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"Balance_Sheet_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
            mimetype='application/pdf'
        )

    except ValueError as e:
        logger.error(f'Value error: {e}')
        return jsonify({'error': 'Invalid date format'}), 400
    except Exception as e:
        logger.error(f'Error generating PDF: {e}\n{traceback.format_exc()}')
        db_session.rollback()
        return jsonify({'error': str(e)}), 500
    finally:
        db_session.close()



@download_route.route('/download_trial_balance_report', methods=['POST'])
@login_required
def download_trial_balance_report():
    with Session() as db_session:
        app_id = current_user.app_id
        company_name = db_session.query(Company.name).filter_by(id=app_id).first()[0]

        # Get filter parameters from the JSON body
        filters = request.json
        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        hide_zero = filters.get('hide_zero', False)

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        # Convert start_date and end_date from string to datetime if they are provided
        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        # Set default date range if start_date or end_date is missing
        if not start_date or not end_date:
            date_range = db_session.query(
                func.min(Journal.date),
                func.max(Journal.date)
            ).filter(Journal.app_id == app_id).one()

            if date_range[0] is None or date_range[1] is None:
                # No journals found, set a default or return an error
                start_date = end_date = datetime.date.today()
            else:
                if not start_date:
                    start_date = date_range[0]
                if not end_date:
                    end_date = date_range[1]

        report_period = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

        # SIMPLIFIED LOGIC - No normal_balance checks!
        # Query for debit amounts
        debit_query = db_session.query(
            Category.category,
            ChartOfAccounts.sub_category,
            func.sum(
                case(
                    (JournalEntry.dr_cr == 'D', JournalEntry.amount),
                    else_=0
                ) *
                case(
                    (Journal.currency_id == base_currency_id, 1.0),
                    (Journal.exchange_rate_id.isnot(None), func.coalesce(ExchangeRate.rate, 1.0)),
                    else_=1.0
                )
            ).label('debit_amount')
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).join(
            ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
        ).outerjoin(
            Category, Category.id == ChartOfAccounts.category_fk
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == Journal.exchange_rate_id
        ).filter(
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.date >= start_date,
            Journal.date <= end_date
        ).group_by(
            Category.category,
            ChartOfAccounts.sub_category
        )

        # Query for credit amounts
        credit_query = db_session.query(
            Category.category,
            ChartOfAccounts.sub_category,
            func.sum(
                case(
                    (JournalEntry.dr_cr == 'C', JournalEntry.amount),
                    else_=0
                ) *
                case(
                    (Journal.currency_id == base_currency_id, 1.0),
                    (Journal.exchange_rate_id.isnot(None), func.coalesce(ExchangeRate.rate, 1.0)),
                    else_=1.0
                )
            ).label('credit_amount')
        ).join(
            Journal, Journal.id == JournalEntry.journal_id
        ).join(
            ChartOfAccounts, ChartOfAccounts.id == JournalEntry.subcategory_id
        ).outerjoin(
            Category, Category.id == ChartOfAccounts.category_fk
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == Journal.exchange_rate_id
        ).filter(
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.date >= start_date,
            Journal.date <= end_date
        ).group_by(
            Category.category,
            ChartOfAccounts.sub_category
        )

        # Execute both queries
        debit_results = debit_query.all()
        credit_results = credit_query.all()

        # Combine results
        trial_balance_data = {}
        total_debit = Decimal('0')
        total_credit = Decimal('0')

        # Process debit results
        for category, subcategory, debit_amount in debit_results:
            if debit_amount is None or debit_amount == 0:
                continue

            key = (category or 'Uncategorized', subcategory or 'No Subcategory')
            if key not in trial_balance_data:
                trial_balance_data[key] = {
                    'debit': Decimal('0'),
                    'credit': Decimal('0')
                }

            trial_balance_data[key]['debit'] = Decimal(str(debit_amount))
            total_debit += Decimal(str(debit_amount))

        # Process credit results
        for category, subcategory, credit_amount in credit_results:
            if credit_amount is None or credit_amount == 0:
                continue

            key = (category or 'Uncategorized', subcategory or 'No Subcategory')
            if key not in trial_balance_data:
                trial_balance_data[key] = {
                    'debit': Decimal('0'),
                    'credit': Decimal('0')
                }

            trial_balance_data[key]['credit'] = Decimal(str(credit_amount))
            total_credit += Decimal(str(credit_amount))

        # Filter out zero balances if hide_zero is True
        if hide_zero:
            trial_balance_data = {
                k: v for k, v in trial_balance_data.items()
                if abs(v['debit']) > Decimal('0.01') or abs(v['credit']) > Decimal('0.01')
            }

        # Prepare PDF layout
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=72)
        styles = getSampleStyleSheet()
        bold_style = styles['Heading2']

        # Title and Report Details
        elements = [
            Paragraph(f"{company_name}", bold_style),
            Paragraph(f"Trial Balance Report", bold_style),
            Paragraph(f"Period: {report_period}", styles['Normal']),
            Paragraph(f"Currency: {base_currency}", styles['Normal']),
            Paragraph(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']),
            Spacer(1, 12)
        ]

        # Check if balanced
        balance_check = total_debit - total_credit
        is_balanced = abs(balance_check) < Decimal('0.01')

        # Add balance status
        balance_status = "BALANCED" if is_balanced else "UNBALANCED"
        balance_color = colors.green if is_balanced else colors.red
        elements.append(Paragraph(f"Status: <font color='{'#28a745' if is_balanced else '#dc3545'}'>{balance_status}</font>", styles['Normal']))

        if not is_balanced:
            elements.append(Paragraph(f"Difference: {float(balance_check):,.2f}", styles['Normal']))

        elements.append(Spacer(1, 12))

        # Trial Balance Table
        trial_balance_table_data = [
            ["Account", "Sub Account", f"Debit ({base_currency})", f"Credit ({base_currency})"]
        ]

        # Sort data
        sorted_trial_balance_items = sorted(
            trial_balance_data.items(),
            key=lambda x: (x[0][0], x[0][1])
        )

        current_category = None
        category_debit_subtotal = Decimal('0')
        category_credit_subtotal = Decimal('0')

        for (category, subcategory), values in sorted_trial_balance_items:
            # Skip if both debit and credit are zero (after filtering)
            if values['debit'] == 0 and values['credit'] == 0:
                continue

            if category != current_category:
                if current_category is not None:
                    # Add subtotal row for previous category
                    trial_balance_table_data.append([
                        "",
                        Paragraph("<i>Subtotal</i>", styles['Italic']),
                        f"{float(category_debit_subtotal):,.2f}",
                        f"{float(category_credit_subtotal):,.2f}"
                    ])

                # Start new category
                current_category = category
                category_debit_subtotal = Decimal('0')
                category_credit_subtotal = Decimal('0')

                # Add category header
                trial_balance_table_data.append([
                    Paragraph(f"<b>{category}</b>", styles['Normal']),
                    "",
                    "",
                    ""
                ])

            # Add account row
            category_debit_subtotal += values['debit']
            category_credit_subtotal += values['credit']

            trial_balance_table_data.append([
                "",
                subcategory,
                f"{float(values['debit']):,.2f}",
                f"{float(values['credit']):,.2f}"
            ])

        # Add final category subtotal
        if current_category is not None:
            trial_balance_table_data.append([
                "",
                Paragraph("<i>Subtotal</i>", styles['Italic']),
                f"{float(category_debit_subtotal):,.2f}",
                f"{float(category_credit_subtotal):,.2f}"
            ])

        # Add totals row
        trial_balance_table_data.append([
            Paragraph("<b>Total</b>", styles['Normal']),
            "",
            f"{float(total_debit):,.2f}",
            f"{float(total_credit):,.2f}"
        ])

        # Create table
        trial_balance_table = Table(
            trial_balance_table_data,
            colWidths=[120, 150, 100, 100]
        )

        # Table styling
        table_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
            ('ALIGN', (0, 0), (1, -1), 'LEFT'),
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTNAME', (0, 1), (-1, -2), 'Helvetica'),
            ('FONTNAME', (0, -1), (-1, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 1), (-1, -2), 'Helvetica'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
            ('LINEBELOW', (0, -1), (-1, -1), 1, colors.black),
            ('SPAN', (0, -1), (1, -1)),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#f9f9f9")]),
        ])

        # Highlight unbalanced total row
        if not is_balanced:
            table_style.add('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#ffe6e6"))

        trial_balance_table.setStyle(table_style)
        elements.append(trial_balance_table)

        # Add summary
        elements.append(Spacer(1, 20))
        elements.append(Paragraph(f"Summary:", styles['Heading3']))
        elements.append(Paragraph(f"Total Debit: {float(total_debit):,.2f} {base_currency}", styles['Normal']))
        elements.append(Paragraph(f"Total Credit: {float(total_credit):,.2f} {base_currency}", styles['Normal']))
        elements.append(Paragraph(f"Difference: {float(balance_check):,.2f} {base_currency}", styles['Normal']))
        elements.append(Paragraph(f"Number of Accounts: {len(trial_balance_data)}", styles['Normal']))

        # Generate and save PDF
        doc.build(elements)
        buffer.seek(0)

        # Create filename with date range
        filename = f"Trial_Balance_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        if start_date or end_date:
            filename = f"Trial_Balance_{start_date.strftime('%Y%m%d')}_to_{end_date.strftime('%Y%m%d')}.pdf"

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )

@download_route.route('/download_income_expense_report', methods=['GET', 'POST'])
@login_required
def download_income_expense_report():
    with Session() as db_session:
        app_id = current_user.app_id
        company_name = db_session.query(Company.name).filter_by(id=app_id).first()[0]
        currency_data = db_session.query(Currency).filter_by(app_id=app_id).all()

        # Get filter parameters from the JSON body
        filters = request.json
        currency_id = filters.get('currency')
        start_date = filters.get('start_date')  # Changed from startDate to start_date
        end_date = filters.get('end_date')      # Changed from endDate to end_date
        hide_zero = filters.get('hide_zero', False)  # Changed from hideZero to hide_zero
        company_filter = filters.get('company_name')  # You might not need this

        # Helper to show negatives in parentheses - ensure consistent Decimal handling
        def format_amount(value):
            # Convert to Decimal if it's not already
            if not isinstance(value, Decimal):
                value = Decimal(str(value))

            if value == 0:
                return "0.00"
            # Use quantize for consistent decimal places
            value = value.quantize(Decimal('0.01'))
            return f"({abs(value):,.2f})" if value < 0 else f"{value:,.2f}"

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        # Determine if we're using base currency
        use_base_currency = currency_id is None

        # Get the currency symbol to display
        if use_base_currency:
            user_currency = base_currency
        else:
            user_currency = next((c.user_currency for c in currency_data if c.id == currency_id), None)

        # Helper to convert amounts to base currency - ensure Decimal return
        def convert_to_base(journal, amount):
            amount_dec = Decimal(str(amount))
            if journal.currency_id != base_currency_id:
                exchange_rate = journal.exchange_rate.rate if journal.exchange_rate else None
                if exchange_rate:
                    return amount_dec * Decimal(str(exchange_rate))
            return amount_dec

        # Convert start_date and end_date from string to date
        if start_date:
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        # Default date range
        if not start_date or not end_date:
            date_range = db_session.query(
                func.min(Journal.date),
                func.max(Journal.date)
            ).filter(Journal.app_id == app_id).one()

            if date_range[0] is None or date_range[1] is None:
                start_date = end_date = datetime.date.today()
            else:
                if not start_date:
                    start_date = date_range[0]
                if not end_date:
                    end_date = date_range[1]

        report_period = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

        # Get ALL income and expense accounts
        income_accounts = db_session.query(ChartOfAccounts, Category.category) \
            .join(Category, ChartOfAccounts.category_fk == Category.id) \
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_active == True,
                ChartOfAccounts.parent_account_type == 'Income'
            ).all()

        expense_accounts = db_session.query(ChartOfAccounts, Category.category) \
            .join(Category, ChartOfAccounts.category_fk == Category.id) \
            .filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_active == True,
                ChartOfAccounts.parent_account_type == 'Expense'
            ).all()

        # EXPENSE TRANSACTIONS
        expense_q = db_session.query(JournalEntry, Journal, ChartOfAccounts, Category) \
            .join(Journal, JournalEntry.journal_id == Journal.id) \
            .join(ChartOfAccounts, JournalEntry.subcategory_id == ChartOfAccounts.id) \
            .join(Category, ChartOfAccounts.category_fk == Category.id) \
            .filter(
                JournalEntry.app_id == app_id,
                ChartOfAccounts.parent_account_type == 'Expense',
                Journal.status == 'Posted'
            )

        # Apply filters
        if currency_id:
            expense_q = expense_q.filter(Journal.currency_id == currency_id)
        if start_date:
            expense_q = expense_q.filter(Journal.date >= start_date)
        if end_date:
            expense_q = expense_q.filter(Journal.date <= end_date)

        expense_entries = expense_q.all()

        # Process expenses with Decimal consistently
        expense_details = {}
        for entry, journal, account, category in expense_entries:
            key = (category.category, account.sub_category)

            # Get amount as Decimal
            if currency_id:
                amount = Decimal(str(entry.amount))
            else:
                amount = convert_to_base(journal, entry.amount)

            # Handle expense accounts with normal balance
            if account.normal_balance == 'Debit':
                # Regular expense account
                signed_amount = amount if entry.dr_cr == 'D' else -amount
            else:
                # Expense contra account (Credit normal balance)
                signed_amount = amount if entry.dr_cr == 'C' else -amount

            # Use Decimal for accumulation
            expense_details[key] = expense_details.get(key, Decimal('0')) + signed_amount

        # Build expenses list with proper Decimal handling
        expenses = []
        for account, category in expense_accounts:
            key = (category, account.sub_category)
            amount = expense_details.get(key, Decimal('0'))

            # Filter out zero values if hide_zero is enabled
            if hide_zero and abs(amount) < Decimal('0.01'):
                continue

            # Round to 2 decimal places
            amount_rounded = amount.quantize(Decimal('0.01'))
            expenses.append((category, account.sub_category, amount_rounded))

        total_expenses = sum(x[2] for x in expenses).quantize(Decimal('0.01'))

        # INCOME TRANSACTIONS
        income_q = db_session.query(JournalEntry, Journal, ChartOfAccounts, Category) \
            .join(Journal, JournalEntry.journal_id == Journal.id) \
            .join(ChartOfAccounts, JournalEntry.subcategory_id == ChartOfAccounts.id) \
            .join(Category, ChartOfAccounts.category_fk == Category.id) \
            .filter(
                JournalEntry.app_id == app_id,
                ChartOfAccounts.parent_account_type == 'Income',
                Journal.status == 'Posted'
            )

        # Apply filters
        if currency_id:
            income_q = income_q.filter(Journal.currency_id == currency_id)
        if start_date:
            income_q = income_q.filter(Journal.date >= start_date)
        if end_date:
            income_q = income_q.filter(Journal.date <= end_date)

        income_entries = income_q.all()

        # Process income with Decimal consistently
        income_balances = {}
        for entry, journal, account, category in income_entries:
            # Get amount as Decimal
            if currency_id:
                amount = Decimal(str(entry.amount))
            else:
                amount = convert_to_base(journal, entry.amount)

            # Handle income accounts with normal balance
            if account.normal_balance == 'Credit':
                # Regular income account
                signed_amount = amount if entry.dr_cr == 'C' else -amount
            else:
                # Income contra account (Debit normal balance)
                signed_amount = amount if entry.dr_cr == 'D' else -amount

            key = (category.category, account.sub_category)
            # Use Decimal for accumulation
            income_balances[key] = income_balances.get(key, Decimal('0')) + signed_amount

        # Build income list with proper Decimal handling
        income = []
        for account, category in income_accounts:
            key = (category, account.sub_category)
            amount = income_balances.get(key, Decimal('0'))

            # Filter out zero values if hide_zero is enabled
            if hide_zero and abs(amount) < Decimal('0.01'):
                continue

            # Round to 2 decimal places
            amount_rounded = amount.quantize(Decimal('0.01'))
            income.append((category, account.sub_category, amount_rounded))

        total_income = sum(x[2] for x in income).quantize(Decimal('0.01'))
        net_income = (total_income - total_expenses).quantize(Decimal('0.01'))

        # PDF GENERATION (rest remains the same)
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=72)
        styles = getSampleStyleSheet()
        bold_style = styles['Heading2']

        elements = [
            Paragraph(f"{company_name}", bold_style),
            Paragraph("Income and Expense Report", bold_style),
            Paragraph(f"Period: {report_period}", styles['Normal']),
            Paragraph(f"Currency: {user_currency}", styles['Normal']),
            Spacer(1, 12),
            Paragraph("Income", bold_style)
        ]

        # Helper function to create consistent tables
        def create_income_expense_table(data, section_name, total_amount):
            table_data = [["Category", "Subcategory", f"Amount ({user_currency})"]]
            current_category = None
            category_subtotal = Decimal('0')
            has_category_data = False

            # Sort by category
            data.sort(key=lambda x: x[0])

            for item in data:
                if item[0] != current_category:
                    if current_category is not None and has_category_data:
                        # Add category subtotal
                        table_data.append([
                            "",
                            Paragraph(f"<b><i>{current_category} Subtotal</i></b>", styles['Italic']),
                            format_amount(category_subtotal)
                        ])
                    current_category = item[0]
                    category_subtotal = Decimal('0')
                    has_category_data = False
                    table_data.append([Paragraph(f"<b>{current_category}</b>", styles['Normal']), "", ""])

                category_subtotal += item[2]
                table_data.append(["", item[1], format_amount(item[2])])
                has_category_data = True

            # Add final category subtotal if there was data
            if current_category is not None and has_category_data:
                table_data.append([
                    "",
                    Paragraph(f"<b><i>{current_category} Subtotal</i></b>", styles['Italic']),
                    format_amount(category_subtotal)
                ])

            # Add section total
            table_data.append([
                Paragraph(f"<b>Total {section_name}</b>", styles['Normal']),
                "",
                format_amount(total_amount)
            ])

            table = Table(table_data, colWidths=[150, 200, 100])
            table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('LINEBELOW', (0, -1), (-1, -1), 1, colors.black),
                ('SPAN', (0, -1), (1, -1)),
                ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#F8F9FA")),
                ('BACKGROUND', (0, 1), (-1, -2), colors.white),
            ]))
            return table

        # INCOME TABLE
        income_table = create_income_expense_table(income, "Income", total_income)
        elements.append(income_table)
        elements.append(Spacer(1, 12))

        # EXPENSE TABLE
        elements.append(Paragraph("Expenses", bold_style))
        expense_table = create_income_expense_table(expenses, "Expenses", total_expenses)
        elements.append(expense_table)
        elements.append(Spacer(1, 12))

        # NET INCOME
        net_income_data = [["Net Income", "", format_amount(net_income)]]
        net_income_table = Table(net_income_data, colWidths=[150, 200, 100])
        net_income_table.setStyle(TableStyle([
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Roboto-Bold'),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#F8F9FA")),
            ('SPAN', (0, 0), (1, 0)),
        ]))
        elements.append(net_income_table)

        # build and send
        doc.build(elements)
        buffer.seek(0)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"Income_Expense_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
            mimetype='application/pdf'
        )


@download_route.route('/download_cash_flow_report', methods=['POST'])
@login_required
def download_cash_flow_report():
    with Session() as db_session:
        try:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            company_name = company.name

            # Get filter parameters
            filters = request.json
            currency_id = filters.get('currency')
            start_date_str = filters.get('startDate') or None
            end_date_str = filters.get('endDate') or None

            # Parse dates
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
            end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None

            if not end_date:
                end_date = datetime.date.today()

            # Get base currency info
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Determine if we're using base currency
            use_base_currency = currency_id is None

            # Get the currency symbol to display
            if use_base_currency:
                user_currency = base_currency
                display_currency = f'Base currency ({base_currency})'
            else:
                currency_record = db_session.query(Currency).filter_by(id=currency_id, app_id=app_id).first()
                if not currency_record:
                    return jsonify({"error": "Invalid currency"}), 400
                user_currency = currency_record.user_currency
                display_currency = user_currency

            # Helper function to convert amounts to base currency (returns Decimal)
            def convert_to_base(journal, amount):
                if journal.currency_id != base_currency_id:
                    exchange_rate = journal.exchange_rate.rate if journal.exchange_rate else None
                    if exchange_rate:
                        return Decimal(str(amount)) * Decimal(str(exchange_rate))
                return Decimal(str(amount))

            # Get cash balances (returns Decimal)
            def get_cash_balance(as_of_date=None):
                cash_balances, total_cash = get_cash_balances_with_base(
                    db_session=db_session,
                    app_id=app_id,
                    currency=currency_id if not use_base_currency else None,
                    end_date=as_of_date,
                    with_base_currency=use_base_currency,
                    base_currency=base_currency_id
                )
                return Decimal(str(total_cash)) if total_cash else Decimal('0')

            cash_at_beginning = get_cash_balance(start_date) if start_date else Decimal('0')
            cash_at_end = get_cash_balance(end_date) if end_date else get_cash_balance()

            # Get journals for the period
            query = db_session.query(Journal).filter(
                Journal.app_id == app_id,
                Journal.status == 'Posted',  # Only posted journals
                Journal.date > start_date if start_date else True,
                Journal.date <= end_date if end_date else True
            )

            # Apply currency filter if not using base currency
            if not use_base_currency:
                query = query.filter(Journal.currency_id == currency_id)

            # Identify cash accounts for THIS company
            cash_account_ids = {
                a.id for a in db_session.query(ChartOfAccounts.id)
                .filter(
                    ChartOfAccounts.app_id == app_id,
                    or_(ChartOfAccounts.is_cash == True,
                        ChartOfAccounts.is_bank == True)
                ).all()
            }

            # Get all journals in the period with their entries
            all_journals = query.options(joinedload(Journal.entries)).all()

            # Filter journals to only those involving cash (but not all cash accounts)
            cash_journals = []
            for journal in all_journals:
                # Check if journal has at least one cash entry but not all entries are cash
                has_cash = any(entry.subcategory_id in cash_account_ids for entry in journal.entries)
                all_cash = all(entry.subcategory_id in cash_account_ids for entry in journal.entries)

                # Include journal if it has cash entries but not all entries are cash
                if has_cash and not all_cash:
                    cash_journals.append(journal)

            # Get all journal entries from cash-related journals
            journal_entries = []
            for journal in cash_journals:
                for entry in journal.entries:
                    journal_entries.append((entry, journal))

            # Initialize sections with default operating categories
            sections = {
                'Operating Activities': {
                    'categories': {
                        'Customer Receipts': {'subcategories': {}, 'total': Decimal('0')},
                        'Operating Expenses': {'subcategories': {}, 'total': Decimal('0')}
                    },
                    'total': Decimal('0')
                },
                'Investing Activities': {'categories': {}, 'total': Decimal('0')},
                'Financing Activities': {'categories': {}, 'total': Decimal('0')}
            }

            skipped_entries = 0

            for entry, journal in journal_entries:
                try:
                    # Skip cash account entries themselves (we only care about non-cash accounts in cash flow)
                    if entry.subcategory_id in cash_account_ids:
                        continue

                    # Convert amount if needed (returns Decimal)
                    amount = Decimal(str(entry.amount)) if not use_base_currency else convert_to_base(journal,
                                                                                                      entry.amount)

                    # Get account information
                    account = entry.chart_of_accounts
                    if not account:
                        skipped_entries += 1
                        continue

                    # Determine cash flow direction based on account type and dr_cr
                    signed_amount = Decimal('0')

                    if account.parent_account_type == 'Income':
                        if account.normal_balance == 'Credit':
                            # Normal income: Credit entry = cash inflow (positive)
                            signed_amount = amount if entry.dr_cr == 'C' else -amount
                        else:
                            # Contra income: Debit entry = cash inflow (positive)
                            signed_amount = amount if entry.dr_cr == 'D' else -amount

                    elif account.parent_account_type == 'Expense':
                        if account.normal_balance == 'Debit':
                            # Normal expense: Debit entry = cash outflow (negative)
                            signed_amount = -amount if entry.dr_cr == 'D' else amount
                        else:
                            # Contra expense: Credit entry = cash outflow (negative)
                            signed_amount = -amount if entry.dr_cr == 'C' else amount

                    elif account.parent_account_type == 'Asset':
                        # Asset increase (debit) = cash outflow, decrease (credit) = cash inflow
                        signed_amount = -amount if entry.dr_cr == 'D' else amount

                    elif account.parent_account_type == 'Liability':
                        # Liability increase (credit) = cash inflow, decrease (debit) = cash outflow
                        signed_amount = amount if entry.dr_cr == 'C' else -amount

                    elif account.parent_account_type == 'Equity':
                        # Equity increase (credit) = cash inflow, decrease (debit) = cash outflow
                        signed_amount = amount if entry.dr_cr == 'C' else -amount

                    else:
                        # Skip unknown account types
                        skipped_entries += 1
                        continue

                    # Auto-classify OR use manual classification
                    if account.report_section:
                        # Use manual classification if report section exists
                        section_name = account.report_section.name
                        category = account.categories.category if account.categories else None
                        subcategory = account.sub_category or 'N/A'
                    else:
                        # Auto-classify based on account type
                        if account.parent_account_type == 'Income':
                            section_name = 'Operating Activities'
                            category = 'Customer Receipts'
                            subcategory = account.sub_category or 'Sales'
                        elif account.parent_account_type == 'Expense':
                            section_name = 'Operating Activities'
                            category = 'Operating Expenses'
                            subcategory = account.sub_category or 'General Expenses'
                        elif account.parent_account_type == 'Asset' and not account.is_cash and not account.is_bank:
                            # Non-cash assets typically go to Investing Activities
                            section_name = 'Investing Activities'
                            category = 'Asset Transactions'
                            subcategory = account.sub_category or 'Fixed Assets'
                        elif account.parent_account_type in ['Liability', 'Equity']:
                            # Liabilities and equity typically go to Financing Activities
                            section_name = 'Financing Activities'
                            category = f'{account.parent_account_type} Transactions'
                            subcategory = account.sub_category or 'General'
                        else:
                            skipped_entries += 1
                            continue

                    # Skip if section not found
                    if section_name not in sections:
                        skipped_entries += 1
                        continue

                    # Initialize category if needed
                    if category not in sections[section_name]['categories']:
                        sections[section_name]['categories'][category] = {
                            'subcategories': {},
                            'total': Decimal('0')
                        }

                    # Initialize subcategory if needed
                    if subcategory not in sections[section_name]['categories'][category]['subcategories']:
                        sections[section_name]['categories'][category]['subcategories'][subcategory] = {
                            'total': Decimal('0')
                        }

                    # Update totals with Decimal arithmetic
                    sections[section_name]['categories'][category]['subcategories'][subcategory][
                        'total'] += signed_amount
                    sections[section_name]['categories'][category]['total'] += signed_amount
                    sections[section_name]['total'] += signed_amount

                except Exception as e:
                    print(f"Error processing journal entry {entry.id}: {str(e)}")
                    skipped_entries += 1
                    continue

            # Calculate net cash flow (convert to float for PDF generation)
            net_cash_flow = float(sum(section['total'] for section in sections.values()))
            cash_at_beginning = float(cash_at_beginning)
            cash_at_end = float(cash_at_end)

            # Generate PDF
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter,
                                    rightMargin=72, leftMargin=72,
                                    topMargin=72, bottomMargin=72)
            styles = getSampleStyleSheet()
            bold_style = styles['Heading2']

            elements = [
                Paragraph(f"{company_name}", bold_style),
                Paragraph("Cash Flow Statement", bold_style),
                Paragraph(
                    f"Period: {start_date_str} to {end_date_str}"
                    if start_date and end_date
                    else f"As of {end_date_str}"
                    if end_date
                    else f"From {start_date_str}"
                    if start_date
                    else "Current Period",
                    styles['Normal']
                ),
                Paragraph(f"Currency: {display_currency}", styles['Normal']),
                Spacer(1, 12),
                Paragraph(f"Cash at Beginning of Period: {cash_at_beginning:,.2f} {user_currency}", styles['Normal']),
                Spacer(1, 12)
            ]

            # Helper functions for number formatting
            def format_amount(value):
                if value < 0:
                    return f"({abs(value):,.2f})"
                return f"{value:,.2f}"

            def format_subtotal(value):
                if value < 0:
                    return f"({abs(value):,.2f})"
                return f"{value:,.2f}"

            # Add each section
            for section_name, section_data in sections.items():
                elements.append(Paragraph(section_name, styles['Heading3']))

                section_data_table = [["Category", "Subcategory", f"Amount ({user_currency})"]]

                for category, cat_data in section_data['categories'].items():
                    # Convert category total to float for display
                    cat_total = float(cat_data['total'])

                    # Add category header
                    section_data_table.append([Paragraph(f"<b>{category}</b>", styles['Normal']), "", ""])

                    # Sort subcategories: inflows (positive) first, then outflows (negative)
                    sorted_subcategories = sorted(
                        cat_data['subcategories'].items(),
                        key=lambda x: (
                            0 if x[1]['total'] >= 0 else 1,  # inflows first
                            -abs(x[1]['total'])  # bigger amounts first
                        )
                    )

                    for subcategory, subcat_data in sorted_subcategories:
                        # Convert subcategory total to float for display
                        subcat_total = float(subcat_data['total'])
                        section_data_table.append([
                            "",
                            subcategory,
                            format_amount(subcat_total)
                        ])

                    # Add category subtotal
                    section_data_table.append([
                        "",
                        Paragraph(f"<i>Net Cash Provided by {category}</i>", styles['Italic']),
                        format_subtotal(cat_total)
                    ])

                # Add section total
                section_total = float(section_data['total'])
                section_clean = section_name.replace("Cash Flow – ", "")

                section_data_table.append([
                    Paragraph(f"<b>Net Cash Provided by {section_clean}</b>", styles['Normal']),
                    "",
                    format_amount(section_total)
                ])

                table = Table(section_data_table, colWidths=[150, 200, 100])
                table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                    ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                    ('FONTNAME', (1, 1), (-1, -2), 'Helvetica-Oblique'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                    ('LINEBELOW', (0, -1), (-1, -1), 1, colors.black),
                    ('SPAN', (0, -1), (1, -1)),
                    ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ]))
                elements.extend([table, Spacer(1, 12)])

            # Summary section
            elements.extend([
                Paragraph(f"Net Increase in Cash: {format_amount(net_cash_flow)} {user_currency}", styles['Heading3']),
                Spacer(1, 12)
            ])

            # Add FX Revaluation if company has multiple currencies
            if company.has_multiple_currencies and use_base_currency:
                try:
                    fx_result = calculate_fx_revaluation(
                        db_session=db_session,
                        app_id=app_id,
                        as_of_date=end_date,
                        start_date=start_date,
                        base_currency_id=base_currency_id,
                        base_currency_code=base_currency
                    )

                    if abs(fx_result['total_fx_gain_loss']) >= 0.01:
                        fx_label = "FX Revaluation Gain" if fx_result[
                                                                'total_fx_gain_loss'] >= 0 else "FX Revaluation Loss"
                        fx_amount = float(fx_result['total_fx_gain_loss'])
                        adjusted_net = net_cash_flow + fx_amount

                        # Create detailed FX breakdown table
                        fx_details = fx_result.get('details', [])

                        # Sort details by absolute FX impact (largest first)
                        fx_details_sorted = sorted(
                            fx_details,
                            key=lambda x: abs(x['fx_gain_loss']),
                            reverse=True
                        )

                        # Create table for FX details
                        fx_table_data = [
                            ["Account Name", "Account Type", "Currency", f"FX Gain/Loss ({user_currency})"]
                        ]

                        for detail in fx_details_sorted:
                            fx_table_data.append([
                                detail['account_name'],
                                detail['account_type'],
                                detail['currency'],
                                format_amount(float(detail['fx_gain_loss']))
                            ])

                        # Add totals row
                        fx_table_data.append([
                            "",
                            "",
                            "Total FX Impact",
                            format_amount(fx_amount)
                        ])

                        fx_table = Table(fx_table_data, colWidths=[150, 100, 80, 100])
                        fx_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#F5F5F5")),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                            ('ALIGN', (3, 0), (3, -1), 'RIGHT'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                            ('FONTNAME', (0, 1), (-1, -2), 'Roboto'),
                            ('FONTSIZE', (0, 0), (-1, -1), 9),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                            ('LINEABOVE', (0, -1), (-1, -1), 1, colors.black),
                            ('BACKGROUND', (0, 1), (-1, -2), colors.white),
                            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                            ('SPAN', (0, -1), (2, -1)),  # Span first 3 columns in totals row
                        ]))

                        # Create summary of FX impact
                        fx_summary_data = [
                            ["", f"Amount ({user_currency})"],
                            [fx_label, format_amount(fx_amount)],
                            ["Adjusted Net Increase in Cash", format_amount(adjusted_net)]
                        ]

                        fx_summary_table = Table(fx_summary_data, colWidths=[200, 100])
                        fx_summary_table.setStyle(TableStyle([
                            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#F5F5F5")),
                            ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                            ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                            ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                            ('FONTNAME', (0, 1), (-1, -1), 'Roboto'),
                            ('FONTSIZE', (0, 0), (-1, -1), 10),
                            ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                            ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                            ('LINEABOVE', (0, 2), (-1, 2), 1, colors.black),
                            ('SPAN', (0, 0), (0, 0)),
                            ('BACKGROUND', (0, 1), (-1, 1), colors.white),
                        ]))

                        elements.extend([
                            Spacer(1, 12),
                            Paragraph("<b>FX Revaluation Adjustment</b>", styles['Heading3']),
                            Spacer(1, 6),
                            fx_summary_table,
                            Spacer(1, 12),
                            Paragraph("<b>FX Revaluation Details</b>", styles['Heading4']),
                            Spacer(1, 6),
                            fx_table,
                            Spacer(1, 12)
                        ])
                except Exception as e:
                    logger.error(f"Error calculating FX revaluation: {str(e)}")
                    elements.append(Paragraph("FX Revaluation: Calculation Error", styles['Italic']))
                    elements.append(Spacer(1, 12))

            # Final cash balance
            elements.append(Paragraph(f"Cash at End of Period: {cash_at_end:,.2f} {user_currency}", styles['Heading3']))
            elements.append(Spacer(1, 12))

            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Cash_Flow_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error generating cash flow report: {str(e)}")
            return jsonify({"error": str(e)}), 500


@download_route.route('/download_reconciliation_report', methods=['POST'])
@login_required
def download_reconciliation_report():
    with Session() as db_session:
        try:
            app_id = current_user.app_id
            company_name = db_session.query(Company.name).filter_by(id=app_id).first()[0]

            # Get filter parameters
            filters = request.json
            currency_id = filters.get('currency_id')
            account_id = filters.get('account_id')
            status = filters.get('status', 'all')
            start_date_str = filters.get('start_date')
            end_date_str = filters.get('end_date')

            # Parse dates
            start_date = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').date() if start_date_str else None
            end_date = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').date() if end_date_str else None

            # Get base currency info
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency = base_currency_info["base_currency"]

            # Determine display currency
            if currency_id:
                currency_record = db_session.query(Currency).filter_by(id=currency_id, app_id=app_id).first()
                if not currency_record:
                    return jsonify({"error": "Invalid currency"}), 400
                display_currency = currency_record.user_currency
            else:
                display_currency = f'Base currency ({base_currency})'

            # Get account name if filtered
            account_name = "All Accounts"
            if account_id:
                account = db_session.query(ChartOfAccounts).filter_by(id=account_id, app_id=app_id).first()
                if account:
                    account_name = account.sub_category

            # Get summary data
            summary_data = []
            bank_accounts = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.is_bank == True,
                ChartOfAccounts.app_id == app_id
            )

            if account_id:
                bank_accounts = bank_accounts.filter(ChartOfAccounts.id == account_id)

            bank_accounts = bank_accounts.all()

            total_reconciled = Decimal('0')
            total_unreconciled = Decimal('0')

            for account in bank_accounts:
                # Get transactions for this account
                transactions = get_bank_transactions(
                    db=db_session,
                    app_id=app_id,
                    account_id=account.id,
                    currency_id=currency_id,
                    status=status,
                    start_date=start_date,
                    end_date=end_date
                )

                # Calculate opening balance (transactions before start date)
                opening_balance = Decimal('0')
                if start_date:
                    opening_transactions = get_bank_transactions(
                        db=db_session,
                        app_id=app_id,
                        account_id=account.id,
                        currency_id=currency_id,
                        status='all',
                        end_date=start_date - datetime.timedelta(days=1)
                    )

                    for tx in opening_transactions:
                        amount = Decimal(str(tx.amount))
                        opening_balance += amount if tx.dr_cr == 'D' else -amount

                # Calculate summary
                summary = calculate_account_summary(transactions, float(opening_balance))

                # Update reconciliation totals
                total_reconciled += Decimal(str(summary['reconciled_balance']))
                total_unreconciled += Decimal(str(summary['unreconciled_balance']))

                summary_data.append({
                    "account_name": account.sub_category,
                    "opening_balance": summary['opening_balance'],
                    "total_inflows": summary['total_inflows'],
                    "total_outflows": summary['total_outflows'],
                    "closing_balance": summary['closing_balance']
                })

            # Get transaction details
            transactions = get_bank_transactions(
                db=db_session,
                app_id=app_id,
                account_id=account_id,
                currency_id=currency_id,
                status=status,
                start_date=start_date,
                end_date=end_date
            )

            # Generate PDF
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter,
                                    rightMargin=72, leftMargin=72,
                                    topMargin=72, bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Heading2'].leftIndent = -25
            bold_style = styles['Heading2']

            # Create a style for wrapped text
            wrapped_style = ParagraphStyle(
                'Wrapped',
                parent=styles['Normal'],
                wordWrap='CJK',
                fontSize=8,  # Adjust this value as needed (default is usually 12)
            )

            indented_normal_style = ParagraphStyle(
                'IndentedNormal',
                parent=styles['Normal'],
                leftIndent=-25
            )

            # Style for amount content (right aligned)
            amount_content_style = ParagraphStyle(
                'AmountContent',
                parent=styles['Normal'],
                wordWrap='CJK',
                fontSize=8,
                leading=10,
                alignment=TA_RIGHT  # Right alignment for amounts
            )

            indented_h3_style = ParagraphStyle(
                'IndentedHeading3',
                parent=styles['Heading3'],
                leftIndent=-25,  # Adjust this value as needed (in points)
            )

            elements = [
                Paragraph(f"{company_name}", bold_style),
                Paragraph("Bank Reconciliation Report", bold_style),
                Paragraph(
                    f"Period: {start_date_str} to {end_date_str}"
                    if start_date and end_date
                    else f"As of {end_date_str}"
                    if end_date
                    else f"From {start_date_str}"
                    if start_date
                    else "Current Period",
                    indented_normal_style
                ),
                Paragraph(f"Bank Account: {account_name}", indented_normal_style),
                Paragraph(f"Currency: {display_currency}", indented_normal_style),
                Paragraph(
                    f"Reconciliation Status: {'All' if status == 'all' else 'Reconciled' if status == 'true' else 'Unreconciled'}",
                    indented_normal_style
                ),
                Spacer(1, 12),
                Paragraph("Account Summary", indented_h3_style)
            ]
            # Add summary section

            summary_table_data = [
                ["Account", "Opening Balance", "Inflows", "Outflows", "Closing Balance"]
            ]

            for account in summary_data:
                summary_table_data.append([
                    account['account_name'],
                    f"{account['opening_balance']:,.2f}",
                    f"{account['total_inflows']:,.2f}",
                    f"{account['total_outflows']:,.2f}",
                    f"{account['closing_balance']:,.2f}"
                ])

            summary_table = Table(summary_table_data, colWidths=[150, 90, 90, 90, 90])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 8)
            ]))
            elements.extend([summary_table, Spacer(1, 12)])

            # Add transaction details with text wrapping
            elements.append(Paragraph("Transaction Details", indented_h3_style))

            trans_table_data = [
                [
                    Paragraph("Date", wrapped_style),
                    Paragraph("Description", wrapped_style),
                    Paragraph("Ref", wrapped_style),
                    Paragraph("Inflow", amount_content_style),
                    Paragraph("Outflow", amount_content_style),
                    Paragraph("Status", wrapped_style)
                ]
            ]

            for tx in transactions:
                inflow = f"{tx.amount:,.2f}" if tx.dr_cr == 'D' else ""
                outflow = f"{tx.amount:,.2f}" if tx.dr_cr == 'C' else ""
                status = "Reconciled" if tx.reconciled else "Unreconciled"

                trans_table_data.append([
                    Paragraph(tx.date.strftime('%Y-%m-%d'), wrapped_style),
                    Paragraph(tx.description, wrapped_style),
                    Paragraph(tx.journal_number or "", wrapped_style),
                    Paragraph(inflow, wrapped_style),
                    Paragraph(outflow, wrapped_style),
                    Paragraph(status, wrapped_style)
                ])

            trans_table = Table(trans_table_data, colWidths=[60, 140, 90, 80, 80, 60])
            trans_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (3, 0), (4, -1), 'RIGHT'),
                ('ALIGN', (5, 0), (5, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
                ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor("#F0F0F0"))
            ]))
            elements.extend([trans_table, Spacer(1, 12)])

            # Add reconciliation summary at the bottom
            elements.extend([
                Paragraph("Reconciliation Summary", indented_h3_style),
                Paragraph(f"Total Reconciled Amount: {float(total_reconciled):,.2f} {display_currency}",
                          indented_normal_style),
                Paragraph(f"Total Unreconciled Amount: {float(total_unreconciled):,.2f} {display_currency}",
                          indented_normal_style),
                Spacer(1, 12)
            ])

            doc.build(elements, onFirstPage=add_pages, onLaterPages=add_pages)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Bank_Reconciliation_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

        except Exception as e:
            db_session.rollback()
            logger.error(f"Error generating bank reconciliation report: {str(e)}")
            return jsonify({"error": str(e)}), 500


@download_route.route('/download_expenses_report', methods=['POST'])
@login_required
def download_expenses_report():
    with Session() as db_session:
        # Get company and currency information
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return jsonify({"error": "Company not found"}), 404

        company_name = company.name
        currency_data = db_session.query(Currency).filter_by(app_id=app_id).all()
        has_multiple_currencies = len(currency_data) > 1

        # Get base currency information
        base_currency_info = get_base_currency(db_session, app_id)
        if not base_currency_info:
            return jsonify({"error": "Base currency not defined"}), 400

        base_currency_id = base_currency_info["base_currency_id"]
        base_currency = base_currency_info["base_currency"]

        # Parse request filters
        filters = request.json
        currency_id = filters.get('currency')
        start_date = filters.get('startDate')
        end_date = filters.get('endDate')
        time_filter = filters.get('time_filter')  # Added time filter
        category_id = filters.get('category_id')
        subcategory_id = filters.get('subcategory_id')
        payment_mode_id = filters.get('payment_mode_id')
        vendor_id = filters.get('vendor_id')
        project_id = filters.get('project_id')

        # Handle date filters with time_filter logic
        today = datetime.date.today()
        if time_filter:
            if time_filter == 'today':
                start_date = today
                end_date = today
            elif time_filter == 'week':
                start_date = today - datetime.timedelta(days=today.weekday())
                end_date = today
            elif time_filter == 'month':
                start_date = today.replace(day=1)
                end_date = today
            elif time_filter == 'quarter':
                quarter_month = ((today.month - 1) // 3) * 3 + 1
                start_date = today.replace(month=quarter_month, day=1)
                end_date = today
            elif time_filter == 'year':
                start_date = today.replace(month=1, day=1)
                end_date = today
        elif not start_date or not end_date:
            # Default to current month if no dates and no time filter
            start_date = today.replace(day=1)
            end_date = today
        # Convert string dates to date objects
        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        report_period = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

        # Build the base query using JournalEntry instead of Transaction
        query = db_session.query(JournalEntry).join(
            Journal, JournalEntry.journal_id == Journal.id
        ).join(
            ChartOfAccounts, JournalEntry.subcategory_id == ChartOfAccounts.id
        ).join(
            Category, ChartOfAccounts.category_fk == Category.id
        ).filter(
            Journal.app_id == app_id,
            Journal.status == 'Posted',
            Journal.date >= start_date,
            Journal.date <= end_date,
            Category.account_type == 'Expense'  # Filter for expense accounts
        )

        # Apply filters
        currency_filter_applied = False
        if currency_id and currency_id != "All":
            currency = db_session.query(Currency).filter_by(
                app_id=app_id,
                user_currency=currency_id
            ).first()
            if currency:
                query = query.filter(Journal.currency_id == currency.id)
                currency_filter_applied = True

        if category_id:
            query = query.filter(ChartOfAccounts.category_fk == category_id)
        if subcategory_id:
            query = query.filter(JournalEntry.subcategory_id == subcategory_id)
        if payment_mode_id:
            query = query.filter(Journal.payment_mode_id == payment_mode_id)
        if vendor_id:
            query = query.filter(Journal.vendor_id == vendor_id)
        if project_id:
            query = query.filter(Journal.project_id == project_id)

        # Execute query and organize data
        entries = query.order_by(Journal.date).all()

        # Data structures for reporting
        grouped_data = {}  # For main report grouping
        currency_totals = {}  # For currency breakdown
        total_amount = 0
        total_amount_base = 0

        for entry in entries:
            journal = entry.journal  # Get the journal header
            # Get transaction details
            month_year = journal.date.strftime('%Y-%m')
            category = entry.chart_of_accounts.categories.category if entry.chart_of_accounts and entry.chart_of_accounts.categories else "Uncategorized"
            subcategory = entry.chart_of_accounts.sub_category if entry.chart_of_accounts else "Uncategorized"
            tx_currency = journal.currency.user_currency if journal.currency else base_currency
            original_amount = float(entry.amount)
            rate = journal.exchange_rate.rate if journal.exchange_rate else None
            converted_amount = float(
                Decimal(
                    original_amount) * rate) if rate and journal.currency_id != base_currency_id else original_amount

            # Update currency totals
            if has_multiple_currencies and not currency_filter_applied:
                if tx_currency not in currency_totals:
                    currency_totals[tx_currency] = {'total': 0, 'total_base': 0}
                currency_totals[tx_currency]['total'] += original_amount
                currency_totals[tx_currency]['total_base'] += converted_amount

            # Group data for main report
            if month_year not in grouped_data:
                grouped_data[month_year] = {}
            if category not in grouped_data[month_year]:
                grouped_data[month_year][category] = {}
            if subcategory not in grouped_data[month_year][category]:
                grouped_data[month_year][category][subcategory] = {
                    'transactions': [],
                    'subtotal': 0,
                    'subtotal_base': 0
                }

            # Add transaction to group
            grouped_data[month_year][category][subcategory]['transactions'].append({
                'date': journal.date.strftime('%Y-%m-%d'),
                'description': entry.description or journal.narration,
                'vendor': journal.vendor.vendor_name if journal.vendor else None,
                'payment_mode': journal.payment_mode.payment_mode if journal.payment_mode else None,
                'project': journal.project.name if journal.project else None,
                'original_amount': original_amount,
                'original_currency': tx_currency,
                'converted_amount': converted_amount,
                'base_currency': base_currency,
                'journal_number': journal.journal_number,
                'line_number': entry.line_number
            })

            # Update group and global totals
            grouped_data[month_year][category][subcategory]['subtotal'] += original_amount
            grouped_data[month_year][category][subcategory]['subtotal_base'] += converted_amount
            total_amount += original_amount
            total_amount_base += converted_amount

        # Prepare PDF layout (remainder of the function remains the same)
        buffer = BytesIO()
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=36,
            leftMargin=36,
            topMargin=36,
            bottomMargin=36
        )

        # Register fonts
        pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
        pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))

        # Define styles
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name='Header',
            fontName='Roboto-Bold',
            fontSize=12,
            alignment=TA_CENTER,
            spaceAfter=12
        ))
        styles.add(ParagraphStyle(
            name='SectionHeader',
            fontName='Roboto-Bold',
            fontSize=10,
            textColor=colors.HexColor('#333333'),
            spaceAfter=6
        ))
        styles.add(ParagraphStyle(
            name='Category',
            fontName='Roboto-Bold',
            fontSize=10,
            textColor=colors.HexColor('#555555'),
            spaceAfter=4,
            leftIndent=10
        ))
        styles.add(ParagraphStyle(
            name='Subcategory',
            fontName='Roboto',
            fontSize=9,
            textColor=colors.HexColor('#666666'),
            spaceAfter=4,
            leftIndent=20
        ))
        styles.add(ParagraphStyle(
            name='TransactionText',
            fontName='Roboto',
            fontSize=8,
            leading=9,
            wordWrap='LTR'
        ))
        styles.add(ParagraphStyle(
            name='Total',
            fontName='Roboto-Bold',
            fontSize=10,
            textColor=colors.red,
            spaceBefore=10,
            alignment=TA_RIGHT
        ))

        elements = []

        # Report header
        elements.append(Paragraph("Expenses Report", styles['Title']))
        elements.append(Paragraph(company_name, styles['Header']))
        elements.append(Paragraph(f"Period: {report_period}", styles['Header']))

        if currency_filter_applied:
            elements.append(Paragraph(f"Currency: {currency_id}", styles['Header']))
        elif has_multiple_currencies:
            elements.append(Paragraph(
                f"Showing amounts in original currency and converted to {base_currency}",
                styles['Header']
            ))
        else:
            elements.append(Paragraph(f"All amounts in {base_currency}", styles['Header']))

        elements.append(Spacer(1, 12))

        # Main report content
        for month_year in sorted(grouped_data.keys()):
            elements.append(Paragraph(month_year, styles['SectionHeader']))

            for category in sorted(grouped_data[month_year].keys()):
                elements.append(Paragraph(category, styles['Category']))

                for subcategory in sorted(grouped_data[month_year][category].keys()):
                    sub_data = grouped_data[month_year][category][subcategory]

                    # Subcategory header with totals
                    if has_multiple_currencies and not currency_filter_applied:
                        subcategory_text = (
                            f"{subcategory} - {sub_data['subtotal']:,.2f} "
                            f"({base_currency} {sub_data['subtotal_base']:,.2f})"
                        )
                    else:
                        subcategory_text = f"{subcategory} - {sub_data['subtotal']:,.2f}"

                    elements.append(Paragraph(subcategory_text, styles['Subcategory']))

                    # Transactions table
                    table_data = [
                        [
                            Paragraph("<b>Date</b>", styles['TransactionText']),
                            Paragraph("<b>Journal #</b>", styles['TransactionText']),
                            Paragraph("<b>Description</b>", styles['TransactionText']),
                            Paragraph("<b>Payee</b>", styles['TransactionText']),
                            Paragraph("<b>Payment Mode</b>", styles['TransactionText']),
                            Paragraph("<b>Project</b>", styles['TransactionText']),
                            Paragraph("<b>Amount</b>", styles['TransactionText']),
                            Paragraph(f"<b>Amount ({base_currency})</b>", styles['TransactionText'])
                            if has_multiple_currencies and not currency_filter_applied
                            else ""
                        ]
                    ]

                    for tx in sub_data['transactions']:
                        row = [
                            Paragraph(tx['date'], styles['TransactionText']),
                            Paragraph(tx['journal_number'], styles['TransactionText']),
                            Paragraph(tx['description'] or "-", styles['TransactionText']),
                            Paragraph(tx['vendor'] or "-", styles['TransactionText']),
                            Paragraph(tx['payment_mode'] or "-", styles['TransactionText']),
                            Paragraph(tx['project'] or "-", styles['TransactionText']),
                            Paragraph(
                                f"{tx['original_amount']:,.2f} {tx['original_currency']}",
                                styles['TransactionText']
                            )
                        ]

                        if has_multiple_currencies and not currency_filter_applied:
                            row.append(Paragraph(
                                f"{tx['converted_amount']:,.2f}",
                                styles['TransactionText']
                            ))

                        table_data.append(row)

                    # Create and style the table
                    col_widths = [50, 60, 80, 60, 60, 60, 60]
                    if has_multiple_currencies and not currency_filter_applied:
                        col_widths.append(60)

                    table = Table(table_data, colWidths=col_widths, repeatRows=1)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#F0F0F0")),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                        ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),
                        ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                        ('FONTSIZE', (0, 0), (-1, -1), 8),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 4),
                        ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ]))

                    elements.append(table)
                    elements.append(Spacer(1, 6))

            elements.append(Spacer(1, 12))

        # Add totals section
        elements.append(Spacer(1, 12))

        if has_multiple_currencies and not currency_filter_applied:
            # Detailed currency breakdown
            elements.append(Paragraph("Expenses by Currency", styles['SectionHeader']))

            currency_table_data = [
                ["Currency", "Total Amount", f"Converted to {base_currency}"]
            ]

            for currency, amounts in sorted(currency_totals.items()):
                currency_table_data.append([
                    currency,
                    f"{amounts['total']:,.2f}",
                    f"{amounts['total_base']:,.2f}"
                ])

            # Add grand total row
            currency_table_data.append([
                Paragraph("<b>Grand Total</b>", styles['Normal']),
                f"{total_amount:,.2f}",
                f"{total_amount_base:,.2f}"
            ])

            currency_table = Table(currency_table_data, colWidths=[100, 100, 100])
            currency_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 8),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 4),
                ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                ('LINEBELOW', (0, -1), (-1, -1), 1, colors.black),
            ]))

            elements.append(currency_table)
        else:
            # Simple total display
            if currency_filter_applied:
                total_text = f"Total Expenses: {total_amount:,.2f} {currency_id}"
            else:
                total_text = f"Total Expenses: {total_amount:,.2f} {base_currency}"

            elements.append(Paragraph(total_text, styles['Total']))

        # Generate PDF
        doc.build(elements)
        buffer.seek(0)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"Expenses_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
            mimetype='application/pdf'
        )


@download_route.route('/download_general_ledger_report', methods=['POST'])
@login_required
def download_general_ledger_report():
    with Session() as db_session:
        app_id = current_user.app_id
        company = db_session.query(Company).filter_by(id=app_id).first()
        if not company:
            return jsonify({"error": "Company not found"}), 404

        company_name = company.name

        # Parse filters
        filters = request.json
        start_date = filters.get('startDate')
        end_date = filters.get('endDate')
        time_filter = filters.get('time_filter')
        reference_filter = filters.get('reference')

        today = datetime.date.today()
        if time_filter:
            if time_filter == 'today':
                start_date = end_date = today
            elif time_filter == 'week':
                start_date = today - datetime.timedelta(days=today.weekday())
                end_date = today
            elif time_filter == 'month':
                start_date = today.replace(day=1)
                end_date = today
            elif time_filter == 'quarter':
                q_month = ((today.month - 1) // 3) * 3 + 1
                start_date = today.replace(month=q_month, day=1)
                end_date = today
            elif time_filter == 'year':
                start_date = today.replace(month=1, day=1)
                end_date = today
        elif not start_date or not end_date:
            start_date = today.replace(day=1)
            end_date = today

        if isinstance(start_date, str):
            start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
        if isinstance(end_date, str):
            end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

        report_period = f"{start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"

        # Query transactions
        query = db_session.query(Transaction).join(
            ChartOfAccounts, Transaction.subcategory_id == ChartOfAccounts.id
        ).join(
            Category, ChartOfAccounts.category_fk == Category.id
        ).filter(
            Transaction.app_id == app_id,
            Transaction.date >= start_date,
            Transaction.date <= end_date
        )

        if reference_filter:
            query = query.filter(Transaction.reference_number.ilike(f"%{reference_filter}%"))

        transactions = query.order_by(Transaction.date).all()

        # Organize data by month and reference
        grouped_data = {}
        for tx in transactions:
            month_year = tx.date.strftime('%Y-%m')
            reference = tx.journal_number or "Unreferenced"
            currency = tx.currencies.user_currency if tx.currencies else ""

            if month_year not in grouped_data:
                grouped_data[month_year] = {}
            if reference not in grouped_data[month_year]:
                grouped_data[month_year][reference] = {
                    'transactions': [],
                    'currency': currency
                }

            grouped_data[month_year][reference]['transactions'].append({
                'date': tx.date.strftime('%Y-%m-%d'),
                'description': tx.description or '',
                'account': tx.chart_of_accounts.sub_category if tx.chart_of_accounts else "Unclassified",
                'debit': float(tx.amount) if tx.dr_cr == 'D' else 0,
                'credit': float(tx.amount) if tx.dr_cr == 'C' else 0,
                'currency': currency
            })

        # PDF setup
        buffer = BytesIO()

        doc = SimpleDocTemplate(buffer, pagesize=letter,
                                rightMargin=36, leftMargin=36,
                                topMargin=36, bottomMargin=36)

        # Register fonts
        pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
        pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))

        # Define styles with wrapping
        styles = getSampleStyleSheet()
        styles.add(ParagraphStyle(
            name='Header',
            fontName='Roboto-Bold',
            fontSize=12,
            alignment=TA_CENTER,
            spaceAfter=12
        ))

        styles.add(ParagraphStyle(
            name='SectionHeader',
            fontName='Roboto-Bold',
            fontSize=10,
            textColor=colors.HexColor('#333333'),
            spaceAfter=6
        ))
        styles.add(ParagraphStyle(
            name='ReferenceHeader',
            fontName='Roboto-Bold',
            fontSize=9,
            textColor=colors.HexColor('#444444'),
            spaceAfter=4,
            leftIndent=10
        ))
        styles.add(ParagraphStyle(
            name='WrappedText',
            fontName='Roboto',
            fontSize=8,
            leading=10,
            spaceAfter=2,
            wordWrap='LTR'
        ))
        styles.add(ParagraphStyle(name='TransactionText', fontName='Roboto', fontSize=8, spaceAfter=2, leftIndent=0,
                                  wordWrap='CJK'))

        elements = []

        # Header
        elements.append(Paragraph("General Ledger Report", styles['Title']))
        elements.append(Paragraph(company_name, styles['Header']))
        elements.append(Paragraph(f"Period: {report_period}", styles['Header']))
        elements.append(Spacer(1, 12))

        for month_year in sorted(grouped_data.keys()):
            elements.append(Paragraph(month_year, styles['SectionHeader']))

            for reference, ref_data in sorted(grouped_data[month_year].items()):
                currency = ref_data['currency']
                ref_text = f"Reference: {reference}"
                if currency:
                    ref_text += f" | Currency: {currency}"
                elements.append(Paragraph(ref_text, styles['ReferenceHeader']))

                table_data = [["Date", "Account", "Description", "Debit", "Credit"]]

                for tx in ref_data['transactions']:
                    desc = Paragraph(tx['description'], styles['TransactionText'])
                    row = [
                        tx['date'],
                        Paragraph(tx['account'], styles['TransactionText']),
                        desc,
                        f"{tx['debit']:,.2f}" if tx['debit'] else "",
                        f"{tx['credit']:,.2f}" if tx['credit'] else ""
                    ]
                    table_data.append(row)

                col_widths = [60, 100, 200, 60, 60]
                table = Table(table_data, colWidths=col_widths)
                table.setStyle(TableStyle([
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('FONTSIZE', (0, 1), (-1, -1), 7),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('ALIGN', (3, 0), (-1, -1), 'RIGHT'),
                ]))

                elements.append(table)
                elements.append(Spacer(1, 6))

            elements.append(Spacer(1, 12))

        # Build and return PDF
        doc.build(elements)
        buffer.seek(0)
        return send_file(
            buffer,
            as_attachment=True,
            download_name=f"General_Ledger_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
            mimetype='application/pdf'
        )


@download_route.route('/download_sales_report', methods=['POST'])
@login_required
def download_sales_report():
    with Session() as db_session:
        try:
            # Get company and currency information
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            if not company:
                return jsonify({"error": "Company not found"}), 404

            company_name = company.name
            has_multiple_currencies = company.has_multiple_currencies

            # Get base currency information
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Parse request filters
            filters = request.json
            if not filters:
                return jsonify({"error": "No filters provided"}), 400

            # Use the same parameter names as your frontend sends
            currency_id = filters.get('currency')
            start_date = filters.get('start_date') or filters.get('startDate')
            end_date = filters.get('end_date') or filters.get('endDate')
            time_filter = filters.get('time_filter')
            customer_id = filters.get('customer')
            payment_mode_id = filters.get('payment_mode')
            project_id = filters.get('project_id')
            sale_type = filters.get('transaction_type')
            invoice_number = filters.get('invoice_number')
            direct_sale_number = filters.get('direct_sale_number')
            status_filter = filters.get('status')
            payment_status = filters.get('payment_status')
            reference = filters.get('reference')
            filter_type = filters.get('filter_type', 'payment_date')
            transaction_type = filters.get('transaction_type')

            # Helper function to convert enum status to string
            def get_status_string(status_obj):
                """Convert enum status to string"""
                if not status_obj:
                    return None
                if hasattr(status_obj, 'value'):
                    return status_obj.value
                elif hasattr(status_obj, 'name'):
                    return status_obj.name
                return str(status_obj)

            # Handle date filters with filter_type logic - FIXED VERSION
            today = datetime.date.today()

            # Initialize dates to None
            start_date_obj = None
            end_date_obj = None

            # Handle time filter first
            # Handle date filters with filter_type logic - SIMPLIFIED VERSION
            start_date_obj, end_date_obj = get_date_range_from_filter(
                time_filter,
                custom_start=start_date,
                custom_end=end_date
            )

            report_period = f"{start_date_obj.strftime('%Y-%m-%d')} to {end_date_obj.strftime('%Y-%m-%d')}"

            # Define common columns for union - ADD EAGER LOADING COLUMNS
            invoice_columns = [
                SalesTransaction.id.label('id'),
                literal('invoice').label('type'),
                SalesInvoice.invoice_number.label('document_number'),
                Vendor.vendor_name.label('customer_name'),
                SalesTransaction.payment_date.label('payment_date'),
                SalesInvoice.invoice_date.label('invoice_date'),
                SalesTransaction.amount_paid.label('amount_paid'),
                Currency.user_currency.label('currency'),
                SalesTransaction.currency_id.label('currency_id'),  # ADDED for conversion
                SalesTransaction.reference_number.label('reference'),
                SalesTransaction.is_posted_to_ledger.label('is_posted_to_ledger'),
                SalesInvoice.status.label('invoice_status'),
                SalesTransaction.payment_status.label('payment_status'),
                SalesInvoice.total_amount.label('total_amount'),
                SalesInvoice.project_id.label('project_id'),
                literal(False).label('is_pos'),
                func.coalesce(func.sum(SalesTransaction.amount_paid).over(
                    partition_by=SalesTransaction.invoice_id
                ), 0).label('total_paid_to_date'),
                # Add payment allocation columns for exchange rates
                PaymentAllocation.id.label('payment_allocation_id')
            ]

            direct_sales_columns = [
                DirectSalesTransaction.id.label('id'),
                literal('direct').label('type'),
                DirectSalesTransaction.direct_sale_number.label('document_number'),
                Vendor.vendor_name.label('customer_name'),
                DirectSalesTransaction.payment_date.label('payment_date'),
                DirectSalesTransaction.created_at.label('invoice_date'),
                DirectSalesTransaction.amount_paid.label('amount_paid'),
                Currency.user_currency.label('currency'),
                DirectSalesTransaction.currency_id.label('currency_id'),  # ADDED for conversion
                DirectSalesTransaction.sale_reference.label('reference'),
                DirectSalesTransaction.is_posted_to_ledger.label('is_posted_to_ledger'),
                DirectSalesTransaction.status.label('invoice_status'),
                literal('full').label('payment_status'),
                DirectSalesTransaction.total_amount.label('total_amount'),
                DirectSalesTransaction.project_id.label('project_id'),
                DirectSalesTransaction.is_pos.label('is_pos'),
                func.coalesce(func.sum(DirectSalesTransaction.amount_paid).over(
                    partition_by=DirectSalesTransaction.direct_sale_number
                ), 0).label('total_paid_to_date'),
                # Add payment allocation columns for exchange rates
                PaymentAllocation.id.label('payment_allocation_id')
            ]

            # Base queries with joins - ADD PAYMENT ALLOCATION JOINS
            invoice_query = db_session.query(*invoice_columns).join(
                SalesInvoice, SalesTransaction.invoice_id == SalesInvoice.id
            ).join(
                Vendor, SalesTransaction.customer_id == Vendor.id
            ).join(
                Currency, SalesTransaction.currency_id == Currency.id
            ).outerjoin(
                PaymentAllocation, SalesTransaction.id == PaymentAllocation.payment_id
            ).filter(
                SalesTransaction.app_id == app_id,
                # EXCLUDE DRAFTS AND CANCELLED like in dashboard
                SalesTransaction.payment_status.notin_([SalesPaymentStatus.cancelled]),
                SalesTransaction.is_posted_to_ledger == True,  # ONLY POSTED TRANSACTIONS
                SalesInvoice.status.notin_([InvoiceStatus.draft, InvoiceStatus.canceled])  # EXCLUDE DRAFTS
            )

            direct_sales_query = db_session.query(*direct_sales_columns).join(
                Vendor, DirectSalesTransaction.customer_id == Vendor.id
            ).join(
                Currency, DirectSalesTransaction.currency_id == Currency.id
            ).outerjoin(
                PaymentAllocation, DirectSalesTransaction.id == PaymentAllocation.direct_sale_id
            ).filter(
                DirectSalesTransaction.app_id == app_id,
                # EXCLUDE DRAFTS AND CANCELLED like in dashboard
                DirectSalesTransaction.status.notin_([OrderStatus.canceled, OrderStatus.draft]),
                DirectSalesTransaction.is_posted_to_ledger == True  # ONLY POSTED TRANSACTIONS
            )

            # Apply transaction type filter
            if transaction_type:
                if transaction_type == 'invoice':
                    direct_sales_query = direct_sales_query.filter(False)
                elif transaction_type == 'direct':
                    invoice_query = invoice_query.filter(False)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_pos == False)
                elif transaction_type == 'pos':
                    invoice_query = invoice_query.filter(False)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_pos == True)

            # Apply date filters - USE THE VALIDATED DATE OBJECTS
            if start_date_obj:
                if filter_type == 'payment_date':
                    invoice_query = invoice_query.filter(SalesTransaction.payment_date >= start_date_obj)
                    direct_sales_query = direct_sales_query.filter(
                        DirectSalesTransaction.payment_date >= start_date_obj)
                elif filter_type == 'invoice_date':
                    invoice_query = invoice_query.filter(SalesInvoice.invoice_date >= start_date_obj)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.created_at >= start_date_obj)

            if end_date_obj:
                if filter_type == 'payment_date':
                    invoice_query = invoice_query.filter(SalesTransaction.payment_date <= end_date_obj)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.payment_date <= end_date_obj)
                elif filter_type == 'invoice_date':
                    invoice_query = invoice_query.filter(SalesInvoice.invoice_date <= end_date_obj)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.created_at <= end_date_obj)

            # Apply currency filter
            currency_filter_applied = False
            selected_currency = None
            if currency_id and currency_id != "All":
                currency = db_session.query(Currency).filter_by(
                    app_id=app_id,
                    user_currency=currency_id
                ).first()
                if currency:
                    invoice_query = invoice_query.filter(SalesTransaction.currency_id == currency.id)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.currency_id == currency.id)
                    currency_filter_applied = True
                    selected_currency = currency.user_currency

            # Apply other filters
            if customer_id:
                invoice_query = invoice_query.filter(SalesTransaction.customer_id == customer_id)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.customer_id == customer_id)

            if project_id:
                invoice_query = invoice_query.filter(SalesInvoice.project_id == project_id)
                direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.project_id == project_id)

            if invoice_number:
                invoice_query = invoice_query.filter(SalesInvoice.invoice_number == invoice_number)

            if direct_sale_number:
                direct_sales_query = direct_sales_query.filter(
                    DirectSalesTransaction.direct_sale_number == direct_sale_number)

            if status_filter:
                if status_filter == 'posted':
                    invoice_query = invoice_query.filter(SalesTransaction.is_posted_to_ledger == True)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_posted_to_ledger == True)
                elif status_filter == 'not_posted':
                    invoice_query = invoice_query.filter(SalesTransaction.is_posted_to_ledger == False)
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.is_posted_to_ledger == False)
                elif status_filter == 'draft':
                    invoice_query = invoice_query.filter(SalesInvoice.status == 'draft')
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'draft')
                elif status_filter == 'approved':
                    invoice_query = invoice_query.filter(SalesInvoice.status == 'approved')
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'approved')
                elif status_filter == 'cancelled':
                    invoice_query = invoice_query.filter(
                        (SalesInvoice.status == 'cancelled') |
                        (SalesTransaction.payment_status == 'cancelled')
                    )
                    direct_sales_query = direct_sales_query.filter(
                        (DirectSalesTransaction.status == 'cancelled') |
                        (DirectSalesTransaction.payment_status == 'cancelled')
                    )

            if payment_status:
                if payment_status == 'paid':
                    invoice_query = invoice_query.filter(SalesInvoice.status == 'paid')
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'paid')
                elif payment_status == 'inprogress':
                    invoice_query = invoice_query.filter(SalesInvoice.status == 'partially_paid')
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'partially_paid')
                elif payment_status == 'unpaid':
                    invoice_query = invoice_query.filter(SalesInvoice.status == 'unpaid')
                    direct_sales_query = direct_sales_query.filter(DirectSalesTransaction.status == 'unpaid')

            if reference:
                invoice_query = invoice_query.filter(SalesTransaction.reference_number.ilike(f'%{reference}%'))
                direct_sales_query = direct_sales_query.filter(
                    DirectSalesTransaction.sale_reference.ilike(f'%{reference}%'))

            # Determine order by clause
            if filter_type == 'invoice_date':
                order_by_clause = [desc('invoice_date'), desc('id')]
            else:
                order_by_clause = [desc('payment_date'), desc('id')]

            # Create union query and execute
            union_query = invoice_query.union_all(direct_sales_query).subquery()

            main_query = db_session.query(
                union_query.c.id,
                union_query.c.type,
                union_query.c.document_number,
                union_query.c.customer_name,
                union_query.c.payment_date,
                union_query.c.amount_paid,
                union_query.c.currency,
                union_query.c.currency_id,  # ADDED for conversion
                union_query.c.reference,
                union_query.c.is_posted_to_ledger,
                union_query.c.invoice_status,
                union_query.c.payment_status,
                union_query.c.total_amount,
                union_query.c.project_id,
                union_query.c.is_pos,
                union_query.c.total_paid_to_date,
                union_query.c.payment_allocation_id  # ADDED for conversion
            ).order_by(*order_by_clause)

            # Get all results
            all_results = main_query.all()

            # Helper function to get exchange rate
            def get_exchange_rate(payment_allocation_id):
                if not payment_allocation_id:
                    return None
                allocation = db_session.query(PaymentAllocation).filter_by(id=payment_allocation_id).first()
                if allocation and allocation.exchange_rate and allocation.exchange_rate.rate:
                    return allocation.exchange_rate.rate
                return None

            # Helper function to convert amount to base currency
            def convert_to_base_currency(amount, currency_id, payment_allocation_id):
                if currency_id == base_currency_id:
                    return float(amount or 0)

                rate = get_exchange_rate(payment_allocation_id)
                if rate:
                    return float(Decimal(amount or 0) * rate)
                return float(amount or 0)

            # Data structures for reporting
            project_data = {}
            currency_totals = {}
            original_currency_totals = {}  # NEW: Track totals by original currency
            total_revenue = 0
            total_amount_paid = 0
            total_balance = 0
            total_transactions = 0

            # Helper function to get project name
            def get_project_name(project_id):
                if not project_id:
                    return "No Project"
                project = db_session.query(Project).get(project_id)
                return project.name if project else f"Project {project_id}"

            # Process results
            for result in all_results:
                try:
                    project_id = result.project_id
                    project_name = get_project_name(project_id)
                    tx_currency = result.currency
                    tx_currency_id = result.currency_id

                    if project_id not in project_data:
                        project_data[project_id] = {
                            'project_name': project_name,
                            'transactions': [],
                            'totals': {'revenue': 0, 'paid': 0, 'balance': 0}
                        }

                    # Calculate remaining balance
                    remaining_balance = float(result.total_amount or 0) - float(result.total_paid_to_date or 0)
                    amount_paid = float(result.amount_paid or 0)

                    # Convert to base currency if no currency filter applied
                    if not currency_filter_applied and has_multiple_currencies:
                        total_amount_converted = convert_to_base_currency(
                            result.total_amount, tx_currency_id, result.payment_allocation_id
                        )
                        amount_paid_converted = convert_to_base_currency(
                            amount_paid, tx_currency_id, result.payment_allocation_id
                        )
                        balance_converted = convert_to_base_currency(
                            remaining_balance, tx_currency_id, result.payment_allocation_id
                        )
                    else:
                        total_amount_converted = float(result.total_amount or 0)
                        amount_paid_converted = amount_paid
                        balance_converted = remaining_balance

                    # Convert enum status to string
                    status_value = get_status_string(result.invoice_status)

                    transaction_data = {
                        'date': result.payment_date.strftime('%Y-%m-%d') if result.payment_date else None,
                        'type': "Invoice" if result.type == 'invoice' else
                        "POS" if result.type == 'direct' and result.is_pos else
                        "Direct Sale",
                        'number': result.document_number,
                        'customer': result.customer_name,
                        'total_amount': total_amount_converted if not currency_filter_applied else float(
                            result.total_amount or 0),
                        'amount_paid': amount_paid_converted if not currency_filter_applied else amount_paid,
                        'original_amount_paid': amount_paid,  # NEW: Store original amount for display
                        'balance': balance_converted if not currency_filter_applied else remaining_balance,
                        'currency': base_currency if not currency_filter_applied else tx_currency,
                        'status': status_value,
                        'reference': result.reference,
                        'posted_to_ledger': result.is_posted_to_ledger,
                        'is_pos': bool(result.is_pos) if result.type == 'direct' else False,
                        'original_currency': tx_currency  # Keep original currency for display
                    }

                    project_data[project_id]['transactions'].append(transaction_data)

                    # Use converted amounts for totals when no currency filter
                    if not currency_filter_applied and has_multiple_currencies:
                        project_data[project_id]['totals']['revenue'] += total_amount_converted
                        project_data[project_id]['totals']['paid'] += amount_paid_converted
                        project_data[project_id]['totals']['balance'] += balance_converted
                    else:
                        project_data[project_id]['totals']['revenue'] += float(result.total_amount or 0)
                        project_data[project_id]['totals']['paid'] += amount_paid
                        project_data[project_id]['totals']['balance'] += remaining_balance

                    # Update global totals
                    if not currency_filter_applied and has_multiple_currencies:
                        total_revenue += total_amount_converted
                        total_amount_paid += amount_paid_converted
                        total_balance += balance_converted
                    else:
                        total_revenue += float(result.total_amount or 0)
                        total_amount_paid += amount_paid
                        total_balance += remaining_balance

                    total_transactions += 1

                    # Update currency totals for display (when no currency filter)
                    if not currency_filter_applied and has_multiple_currencies:
                        if base_currency not in currency_totals:
                            currency_totals[base_currency] = {'revenue': 0, 'paid': 0, 'balance': 0}
                        currency_totals[base_currency]['revenue'] += total_amount_converted
                        currency_totals[base_currency]['paid'] += amount_paid_converted
                        currency_totals[base_currency]['balance'] += balance_converted

                        # NEW: Track totals by original currency for detailed breakdown
                        if tx_currency not in original_currency_totals:
                            original_currency_totals[tx_currency] = {
                                'revenue': 0,
                                'paid': 0,
                                'balance': 0,
                                'converted_revenue': 0,
                                'converted_paid': 0,
                                'converted_balance': 0
                            }
                        original_currency_totals[tx_currency]['revenue'] += float(result.total_amount or 0)
                        original_currency_totals[tx_currency]['paid'] += amount_paid
                        original_currency_totals[tx_currency]['balance'] += remaining_balance
                        original_currency_totals[tx_currency]['converted_revenue'] += total_amount_converted
                        original_currency_totals[tx_currency]['converted_paid'] += amount_paid_converted
                        original_currency_totals[tx_currency]['converted_balance'] += balance_converted
                    else:
                        if tx_currency not in currency_totals:
                            currency_totals[tx_currency] = {'revenue': 0, 'paid': 0, 'balance': 0}
                        currency_totals[tx_currency]['revenue'] += float(result.total_amount or 0)
                        currency_totals[tx_currency]['paid'] += amount_paid
                        currency_totals[tx_currency]['balance'] += remaining_balance

                except Exception as e:
                    logger.error(f"Error processing transaction {result.id}: {str(e)}")
                    continue

            # Determine display currency
            if currency_filter_applied:
                display_currency = selected_currency
            else:
                display_currency = base_currency

            # Prepare PDF layout
            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter,
                                    rightMargin=36, leftMargin=36,
                                    topMargin=36, bottomMargin=36)

            # Register fonts
            try:
                pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
                pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))
                font_name_regular = 'Roboto'
                font_name_bold = 'Roboto-Bold'
            except:
                font_name_regular = 'Helvetica'
                font_name_bold = 'Helvetica-Bold'

            # Define clean styles
            styles = getSampleStyleSheet()
            # Modify existing styles
            styles['Normal'].fontName = font_name_regular
            styles['Normal'].fontSize = 9
            styles['Normal'].leading = 12
            styles.add(ParagraphStyle(
                name='ReportTitle',
                fontName=font_name_bold,
                fontSize=16,
                alignment=TA_CENTER
            ))
            styles.add(ParagraphStyle(
                name='SubHeader',
                fontName=font_name_bold,
                fontSize=10,
                spaceAfter=6
            ))

            styles.add(ParagraphStyle(
                name='SmallText',
                fontName=font_name_regular,
                fontSize=7,
                textColor=colors.gray,
                leading=9
            ))
            styles.add(ParagraphStyle(
                name='TableCell',
                fontName=font_name_regular,
                fontSize=7,
                leading=9
            ))
            styles.add(ParagraphStyle(
                name='AmountCell',
                fontName=font_name_regular,
                fontSize=7,
                alignment=TA_RIGHT,
                leading=9
            ))

            elements = []

            # =============================
            # REPORT HEADER
            # =============================
            elements.append(Paragraph(company_name, styles['ReportTitle']))
            elements.append(Spacer(1, 4))
            elements.append(Paragraph("Sales Report", styles['SubHeader']))
            elements.append(Paragraph(f"Period: {report_period}", styles['Normal']))
            elements.append(
                Paragraph(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['SmallText']))
            elements.append(Spacer(1, 8))

            # Currency information
            if currency_filter_applied:
                elements.append(Paragraph(f"Currency: {selected_currency}", styles['Normal']))
                elements.append(Paragraph("All values shown in selected currency", styles['SmallText']))
            else:
                if has_multiple_currencies:
                    elements.append(Paragraph(f"Base Currency: {base_currency}", styles['Normal']))
                    elements.append(Paragraph("Values shown in base currency with original amounts in parentheses",
                                              styles['SmallText']))
                else:
                    elements.append(Paragraph(f"Currency: {base_currency}", styles['Normal']))
                    elements.append(Paragraph("All values shown in company currency", styles['SmallText']))

            elements.append(Spacer(1, 12))

            # =============================
            # SUMMARY SECTION
            # =============================
            elements.append(Paragraph("Summary", styles['SubHeader']))
            summary_data = [
                ["Total Revenue", f"{total_revenue:,.2f} {display_currency}"],
                ["Total Amount Paid", f"{total_amount_paid:,.2f} {display_currency}"],
                ["Outstanding Balance", f"{total_balance:,.2f} {display_currency}"],
                ["Total Transactions", f"{total_transactions:,}"]
            ]

            summary_table = Table(summary_data, colWidths=[200, 150])
            summary_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), font_name_regular),
                ('FONTNAME', (0, 0), (0, -1), font_name_bold),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f8f9fa')),
                ('LINEBELOW', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 16))

            # =============================
            # DETAILED SECTION
            # =============================
            for project_id, project_info in project_data.items():
                if not project_info['transactions']:
                    continue

                elements.append(Paragraph(f"Project: {project_info['project_name']}", styles['SubHeader']))

                # Create table headers
                table_data = [["Date", "Type", "Sale Reference", "Customer", "Amount Paid", "Status"]]

                # Set column widths based on whether we show conversions
                if currency_filter_applied or not has_multiple_currencies:
                    col_widths = [50, 60, 80, 100, 100, 50]  # Normal width
                else:
                    col_widths = [50, 60, 80, 120, 150, 50]  # Extra width for conversions

                # Add transaction rows
                for tx in project_info['transactions']:
                    # Clean data formatting
                    date_display = tx['date'] or "-"
                    type_display = tx['type']
                    number_display = tx['number'] or "-"

                    # Clean customer name - truncate if too long
                    customer_display = tx['customer'] or "-"
                    if len(customer_display) > 20:
                        customer_display = customer_display[:20] + "..."

                    # AMOUNT DISPLAY WITH CONVERSIONS IN PARENTHESES
                    if currency_filter_applied:
                        # Single currency - simple display
                        amount_display = f"{tx['amount_paid']:,.2f} {tx['currency']}"
                    else:
                        if has_multiple_currencies and tx['original_currency'] != base_currency:
                            # Multiple currencies - show conversion in parentheses
                            amount_display = f"{tx['amount_paid']:,.2f} {base_currency} ({tx['original_amount_paid']:,.2f} {tx['original_currency']})"
                        else:
                            # Single currency or same currency - simple display
                            amount_display = f"{tx['amount_paid']:,.2f} {base_currency}"

                    status_display = tx['status'] or "-"

                    table_data.append([
                        Paragraph(date_display, styles['TableCell']),
                        Paragraph(type_display, styles['TableCell']),
                        Paragraph(number_display, styles['TableCell']),
                        Paragraph(customer_display, styles['TableCell']),
                        Paragraph(amount_display, styles['AmountCell']),
                        Paragraph(status_display, styles['TableCell'])
                    ])

                # Add project totals row
                totals = project_info['totals']
                table_data.append([
                    "", "", "", "Project Total:",
                    Paragraph(f"{totals['paid']:,.2f} {display_currency}", styles['AmountCell']),
                    ""
                ])

                # Create table with proper styling
                project_table = Table(table_data, colWidths=col_widths)
                project_table.setStyle(TableStyle([
                    # Header row
                    ('FONTNAME', (0, 0), (-1, 0), font_name_bold),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#343a40')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, 0), 'LEFT'),

                    # Data rows
                    ('FONTNAME', (0, 1), (-1, -2), font_name_regular),
                    ('FONTSIZE', (0, 1), (-1, -2), 7),
                    ('GRID', (0, 0), (-1, -2), 0.5, colors.HexColor('#dee2e6')),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('TOPPADDING', (0, 0), (-1, -1), 3),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                    ('LEFTPADDING', (0, 0), (-1, -1), 4),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 4),

                    # Amount column alignment
                    ('ALIGN', (4, 1), (4, -2), 'RIGHT'),

                    # Totals row
                    ('FONTNAME', (0, -1), (-1, -1), font_name_bold),
                    ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e9ecef')),
                    ('LINEABOVE', (0, -1), (-1, -1), 1, colors.black),
                    ('ALIGN', (4, -1), (4, -1), 'RIGHT'),

                    # Zebra striping for readability
                    ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#f8f9fa')]),
                ]))

                elements.append(project_table)
                elements.append(Spacer(1, 16))

            # =============================
            # CURRENCY BREAKDOWN (only if multiple currencies and no currency filter)
            # =============================
            if not currency_filter_applied and has_multiple_currencies and original_currency_totals:
                elements.append(Paragraph("Currency Breakdown", styles['SubHeader']))
                elements.append(Paragraph("All amounts converted to base currency", styles['SmallText']))
                elements.append(Spacer(1, 8))

                # Count transactions per currency
                currency_counts = {}
                for result in all_results:
                    currency = result.currency
                    currency_counts[currency] = currency_counts.get(currency, 0) + 1

                # Currency breakdown table
                cur_table_data = [["Currency", "Transactions", "Original Amount", f"Converted ({base_currency})"]]

                for currency, vals in sorted(original_currency_totals.items()):
                    if vals['paid'] > 0:  # Only show currencies with transactions
                        count = currency_counts.get(currency, 0)
                        cur_table_data.append([
                            currency,
                            str(count),
                            f"{vals['paid']:,.2f} {currency}",
                            f"{vals['converted_paid']:,.2f}"
                        ])

                cur_table = Table(cur_table_data, colWidths=[60, 70, 150, 120])
                cur_table.setStyle(TableStyle([
                    ('FONTNAME', (0, 0), (-1, 0), font_name_bold),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#343a40')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
                    ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('TOPPADDING', (0, 0), (-1, -1), 5),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
                ]))
                elements.append(cur_table)

            # Build the PDF
            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Sales_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                mimetype='application/pdf'
            )

        except Exception as e:
            logger.error(f"Error generating sales report: {str(e)}\n{traceback.format_exc()}")
            return jsonify({"error": f"Failed to generate report: {str(e)}"}), 500


@download_route.route('/download_purchases_report', methods=['POST'])
@login_required
def download_purchases_report():
    with Session() as db_session:
        try:
            # Get company and currency information
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            if not company:
                return jsonify({"error": "Company not found"}), 404

            company_name = company.name
            has_multiple_currencies = company.has_multiple_currencies

            # Get base currency information
            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency = base_currency_info["base_currency"]

            # Parse request filters
            filters = request.json
            if not filters:
                return jsonify({"error": "No filters provided"}), 400

            # Use the same parameter names as your frontend sends
            currency_id = filters.get('currency')
            start_date = filters.get('start_date') or filters.get('startDate')
            end_date = filters.get('end_date') or filters.get('endDate')
            time_filter = filters.get('time_filter')
            vendor_id = filters.get('vendor')
            payment_mode_id = filters.get('payment_mode')
            project_id = filters.get('project')
            status_filter = filters.get('status')
            purchase_type = filters.get('transaction_type')
            po_number = filters.get('po_number')
            direct_purchase_number = filters.get('direct_purchase_number')
            reference = filters.get('reference')
            filter_type = filters.get('filter_type', 'payment_date')

            # Helper function to convert enum status to string
            def get_status_string(status_obj):
                """Convert enum status to string"""
                if not status_obj:
                    return None
                if hasattr(status_obj, 'value'):
                    return status_obj.value
                elif hasattr(status_obj, 'name'):
                    return status_obj.name
                return str(status_obj)

            # Handle date filters with filter_type logic
            today = datetime.date.today()

            # Initialize dates to None
            start_date_obj = None
            end_date_obj = None

            # Handle time filter first
            if time_filter:
                if time_filter == 'today':
                    start_date_obj = today
                    end_date_obj = today
                elif time_filter == 'week':
                    start_date_obj = today - datetime.timedelta(days=today.weekday())
                    end_date_obj = today
                elif time_filter == 'month':
                    start_date_obj = today.replace(day=1)
                    end_date_obj = today
                elif time_filter == 'quarter':
                    quarter_month = ((today.month - 1) // 3) * 3 + 1
                    start_date_obj = today.replace(month=quarter_month, day=1)
                    end_date_obj = today
                elif time_filter == 'year':
                    start_date_obj = today.replace(month=1, day=1)
                    end_date_obj = today

            # If no time filter or specific dates provided, use provided dates
            if not start_date_obj and start_date and start_date != '':
                try:
                    if isinstance(start_date, str):
                        start_date_obj = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
                    else:
                        start_date_obj = start_date
                except (ValueError, TypeError):
                    return jsonify({"error": "Invalid start date format. Use YYYY-MM-DD"}), 400

            if not end_date_obj and end_date and end_date != '':
                try:
                    if isinstance(end_date, str):
                        end_date_obj = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()
                    else:
                        end_date_obj = end_date
                except (ValueError, TypeError):
                    return jsonify({"error": "Invalid end date format. Use YYYY-MM-DD"}), 400

            # Default to a wider date range if no dates provided
            if not start_date_obj or not end_date_obj:
                start_date_obj = today - datetime.timedelta(days=30)
                end_date_obj = today

            # Ensure start_date is before end_date
            if start_date_obj > end_date_obj:
                start_date_obj, end_date_obj = end_date_obj, start_date_obj

            report_period = f"{start_date_obj.strftime('%Y-%m-%d')} to {end_date_obj.strftime('%Y-%m-%d')}"

            # Define common columns for union - SIMPLIFIED VERSION
            po_columns = [
                PurchaseTransaction.id.label('id'),
                literal('purchase_order').label('type'),
                PurchaseOrder.purchase_order_number.label('document_number'),
                Vendor.vendor_name.label('vendor_name'),
                PurchaseTransaction.payment_date.label('payment_date'),
                PurchaseOrder.purchase_order_date.label('order_date'),
                PurchaseTransaction.amount_paid.label('amount_paid'),
                Currency.user_currency.label('currency'),
                PurchaseTransaction.currency_id.label('currency_id'),
                PurchaseTransaction.reference_number.label('reference'),
                PurchaseTransaction.is_posted_to_ledger.label('is_posted_to_ledger'),
                PurchaseOrder.status.label('order_status'),
                PurchaseTransaction.payment_status.label('payment_status'),
                PurchaseOrder.total_amount.label('total_amount'),
                PurchaseOrder.project_id.label('project_id'),
                PurchaseOrder.id.label('purchase_order_id'),
                # Add payment allocation columns for exchange rates
                PurchasePaymentAllocation.id.label('payment_allocation_id')
            ]

            direct_purchase_columns = [
                DirectPurchaseTransaction.id.label('id'),
                literal('direct').label('type'),
                DirectPurchaseTransaction.direct_purchase_number.label('document_number'),
                Vendor.vendor_name.label('vendor_name'),
                DirectPurchaseTransaction.payment_date.label('payment_date'),
                DirectPurchaseTransaction.created_at.label('order_date'),
                DirectPurchaseTransaction.amount_paid.label('amount_paid'),
                Currency.user_currency.label('currency'),
                DirectPurchaseTransaction.currency_id.label('currency_id'),
                DirectPurchaseTransaction.purchase_reference.label('reference'),
                DirectPurchaseTransaction.is_posted_to_ledger.label('is_posted_to_ledger'),
                DirectPurchaseTransaction.status.label('order_status'),
                literal('full').label('payment_status'),
                DirectPurchaseTransaction.total_amount.label('total_amount'),
                DirectPurchaseTransaction.project_id.label('project_id'),
                DirectPurchaseTransaction.id.label('direct_purchase_id'),  # Use id instead of direct_purchase_number
                # Add payment allocation columns for exchange rates
                PurchasePaymentAllocation.id.label('payment_allocation_id')
            ]

            # Base queries with joins - EXCLUDE DRAFTS AND CANCELLED
            po_query = db_session.query(*po_columns).join(
                PurchaseOrder, PurchaseTransaction.purchase_order_id == PurchaseOrder.id
            ).join(
                Vendor, PurchaseTransaction.vendor_id == Vendor.id
            ).join(
                Currency, PurchaseTransaction.currency_id == Currency.id
            ).outerjoin(
                PurchasePaymentAllocation, PurchaseTransaction.id == PurchasePaymentAllocation.payment_id
            ).filter(
                PurchaseTransaction.app_id == app_id,
                # EXCLUDE DRAFTS AND CANCELLED like in dashboard
                PurchaseTransaction.payment_status.notin_([PurchasePaymentStatus.cancelled]),
                PurchaseTransaction.is_posted_to_ledger == True,  # ONLY POSTED TRANSACTIONS
                PurchaseOrder.status.notin_([OrderStatus.draft, OrderStatus.canceled])  # EXCLUDE DRAFTS
            )

            direct_purchase_query = db_session.query(*direct_purchase_columns).join(
                Vendor, DirectPurchaseTransaction.vendor_id == Vendor.id
            ).join(
                Currency, DirectPurchaseTransaction.currency_id == Currency.id
            ).outerjoin(
                PurchasePaymentAllocation, DirectPurchaseTransaction.id == PurchasePaymentAllocation.direct_purchase_id
            ).filter(
                DirectPurchaseTransaction.app_id == app_id,
                # EXCLUDE DRAFTS AND CANCELLED like in dashboard
                DirectPurchaseTransaction.status.notin_([OrderStatus.canceled, OrderStatus.draft]),
                DirectPurchaseTransaction.is_posted_to_ledger == True  # ONLY POSTED TRANSACTIONS
            )

            # Apply transaction type filter
            if purchase_type:
                if purchase_type == 'purchase_order':
                    direct_purchase_query = direct_purchase_query.filter(False)
                elif purchase_type == 'direct':
                    po_query = po_query.filter(False)

            # Apply date filters - USE THE VALIDATED DATE OBJECTS
            if start_date_obj:
                if filter_type == 'payment_date':
                    po_query = po_query.filter(PurchaseTransaction.payment_date >= start_date_obj)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.payment_date >= start_date_obj)
                elif filter_type == 'order_date':
                    po_query = po_query.filter(PurchaseOrder.purchase_order_date >= start_date_obj)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.created_at >= start_date_obj)

            if end_date_obj:
                if filter_type == 'payment_date':
                    po_query = po_query.filter(PurchaseTransaction.payment_date <= end_date_obj)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.payment_date <= end_date_obj)
                elif filter_type == 'order_date':
                    po_query = po_query.filter(PurchaseOrder.purchase_order_date <= end_date_obj)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.created_at <= end_date_obj)

            # Apply currency filter
            currency_filter_applied = False
            selected_currency = None
            if currency_id and currency_id != "All":
                currency = db_session.query(Currency).filter_by(
                    app_id=app_id,
                    user_currency=currency_id
                ).first()
                if currency:
                    po_query = po_query.filter(PurchaseTransaction.currency_id == currency.id)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.currency_id == currency.id)
                    currency_filter_applied = True
                    selected_currency = currency.user_currency

            # Apply other filters
            if vendor_id:
                po_query = po_query.filter(PurchaseTransaction.vendor_id == vendor_id)
                direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.vendor_id == vendor_id)

            if project_id:
                po_query = po_query.filter(PurchaseOrder.project_id == project_id)
                direct_purchase_query = direct_purchase_query.filter(DirectPurchaseTransaction.project_id == project_id)

            if po_number:
                po_query = po_query.filter(PurchaseOrder.purchase_order_number == po_number)

            if direct_purchase_number:
                direct_purchase_query = direct_purchase_query.filter(
                    DirectPurchaseTransaction.direct_purchase_number == direct_purchase_number)

            if status_filter:
                if status_filter == 'posted':
                    po_query = po_query.filter(PurchaseTransaction.is_posted_to_ledger == True)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.is_posted_to_ledger == True)
                elif status_filter == 'not_posted':
                    po_query = po_query.filter(PurchaseTransaction.is_posted_to_ledger == False)
                    direct_purchase_query = direct_purchase_query.filter(
                        DirectPurchaseTransaction.is_posted_to_ledger == False)

            if reference:
                po_query = po_query.filter(PurchaseTransaction.reference_number.ilike(f'%{reference}%'))
                direct_purchase_query = direct_purchase_query.filter(
                    DirectPurchaseTransaction.purchase_reference.ilike(f'%{reference}%'))

            # Determine order by clause
            if filter_type == 'order_date':
                order_by_clause = [desc('order_date'), desc('id')]
            else:
                order_by_clause = [desc('payment_date'), desc('id')]

            # Create union query and execute - SIMPLIFIED SELECTION
            union_query = po_query.union_all(direct_purchase_query).subquery()

            main_query = db_session.query(
                union_query.c.id,
                union_query.c.type,
                union_query.c.document_number,
                union_query.c.vendor_name,
                union_query.c.payment_date,
                union_query.c.amount_paid,
                union_query.c.currency,
                union_query.c.currency_id,
                union_query.c.reference,
                union_query.c.is_posted_to_ledger,
                union_query.c.order_status,
                union_query.c.payment_status,
                union_query.c.total_amount,
                union_query.c.project_id,
                union_query.c.payment_allocation_id
            ).order_by(*order_by_clause)

            # Get all results
            all_results = main_query.all()

            # Helper function to get exchange rate
            def get_exchange_rate(payment_allocation_id):
                if not payment_allocation_id:
                    return None
                allocation = db_session.query(PurchasePaymentAllocation).filter_by(id=payment_allocation_id).first()
                if allocation and allocation.exchange_rate and allocation.exchange_rate.rate:
                    return allocation.exchange_rate.rate
                return None

            # Helper function to convert amount to base currency
            def convert_to_base_currency(amount, currency_id, payment_allocation_id):
                if currency_id == base_currency_id:
                    return float(amount or 0)

                rate = get_exchange_rate(payment_allocation_id)
                if rate:
                    return float(Decimal(amount or 0) * rate)
                return float(amount or 0)

            # Data structures for reporting
            project_data = {}
            currency_totals = {}
            original_currency_totals = {}
            total_amount_paid = 0  # Total of all payments made
            total_transactions = 0

            # Helper function to get project name
            def get_project_name(project_id):
                if not project_id:
                    return "No Project"
                project = db_session.query(Project).get(project_id)
                return project.name if project else f"Project {project_id}"

            # Process results - SIMPLIFIED APPROACH
            for result in all_results:
                try:
                    project_id = result.project_id
                    project_name = get_project_name(project_id)
                    tx_currency = result.currency
                    tx_currency_id = result.currency_id

                    if project_id not in project_data:
                        project_data[project_id] = {
                            'project_name': project_name,
                            'transactions': [],
                            'totals': {'paid': 0}  # SIMPLIFIED: only track paid amounts
                        }

                    # Get the amount paid for this transaction
                    amount_paid = float(result.amount_paid or 0)

                    # Convert to base currency if no currency filter applied
                    if not currency_filter_applied and has_multiple_currencies:
                        amount_paid_converted = convert_to_base_currency(
                            amount_paid, tx_currency_id, result.payment_allocation_id
                        )
                    else:
                        amount_paid_converted = amount_paid

                    # Convert enum status to string
                    status_value = get_status_string(result.order_status)

                    transaction_data = {
                        'date': result.payment_date.strftime('%Y-%m-%d') if result.payment_date else None,
                        'type': "Purchase Order" if result.type == 'purchase_order' else "Direct Purchase",
                        'number': result.document_number,
                        'vendor': result.vendor_name,
                        'amount_paid': amount_paid_converted if not currency_filter_applied else amount_paid,
                        'original_amount_paid': amount_paid,  # Store original amount for display
                        'currency': base_currency if not currency_filter_applied else tx_currency,
                        'status': status_value,
                        'reference': result.reference,
                        'posted_to_ledger': result.is_posted_to_ledger,
                        'original_currency': tx_currency  # Keep original currency for display
                    }

                    project_data[project_id]['transactions'].append(transaction_data)

                    # Use converted amounts for totals when no currency filter
                    if not currency_filter_applied and has_multiple_currencies:
                        project_data[project_id]['totals']['paid'] += amount_paid_converted
                    else:
                        project_data[project_id]['totals']['paid'] += amount_paid

                    # Update global totals
                    if not currency_filter_applied and has_multiple_currencies:
                        total_amount_paid += amount_paid_converted
                    else:
                        total_amount_paid += amount_paid

                    total_transactions += 1

                    # Update currency totals for display (when no currency filter)
                    if not currency_filter_applied and has_multiple_currencies:
                        if base_currency not in currency_totals:
                            currency_totals[base_currency] = {'paid': 0}
                        currency_totals[base_currency]['paid'] += amount_paid_converted

                        # Track totals by original currency for detailed breakdown
                        if tx_currency not in original_currency_totals:
                            original_currency_totals[tx_currency] = {
                                'paid': 0,
                                'converted_paid': 0
                            }
                        original_currency_totals[tx_currency]['paid'] += amount_paid
                        original_currency_totals[tx_currency]['converted_paid'] += amount_paid_converted
                    else:
                        if tx_currency not in currency_totals:
                            currency_totals[tx_currency] = {'paid': 0}
                        currency_totals[tx_currency]['paid'] += amount_paid

                except Exception as e:
                    logger.error(f"Error processing transaction {result.id}: {str(e)}")
                    continue

            # Determine display currency
            if currency_filter_applied:
                display_currency = selected_currency
            else:
                display_currency = base_currency

            # FIXED: Remove emoji from log messages to prevent encoding errors
            logger.info(f"Total purchase transactions: {total_transactions}")
            logger.info(f"Total amount paid: {total_amount_paid:,.2f} {display_currency}")

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter,
                                    rightMargin=36, leftMargin=36,
                                    topMargin=36, bottomMargin=36)

            # Register fonts
            try:
                pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
                pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))
                font_name_regular = 'Roboto'
                font_name_bold = 'Roboto-Bold'
            except:
                font_name_regular = 'Helvetica'
                font_name_bold = 'Helvetica-Bold'

            # Define clean styles
            styles = getSampleStyleSheet()
            # Modify existing styles
            styles['Normal'].fontName = font_name_regular
            styles['Normal'].fontSize = 9
            styles['Normal'].leading = 12
            styles.add(ParagraphStyle(
                name='ReportTitle',
                fontName=font_name_bold,
                fontSize=16,
                alignment=TA_CENTER
            ))
            styles.add(ParagraphStyle(
                name='SubHeader',
                fontName=font_name_bold,
                fontSize=10,
                spaceAfter=6
            ))
            styles.add(ParagraphStyle(
                name='SmallText',
                fontName=font_name_regular,
                fontSize=7,
                textColor=colors.gray,
                leading=9
            ))
            styles.add(ParagraphStyle(
                name='TableCell',
                fontName=font_name_regular,
                fontSize=7,
                leading=9
            ))
            styles.add(ParagraphStyle(
                name='AmountCell',
                fontName=font_name_regular,
                fontSize=7,
                alignment=TA_RIGHT,
                leading=9
            ))

            elements = []

            # =============================
            # REPORT HEADER
            # =============================
            elements.append(Paragraph(company_name, styles['ReportTitle']))
            elements.append(Spacer(1, 4))
            elements.append(Paragraph("Purchases Report", styles['SubHeader']))
            elements.append(Paragraph(f"Period: {report_period}", styles['Normal']))
            elements.append(
                Paragraph(f"Generated: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['SmallText']))
            elements.append(Spacer(1, 8))

            # Currency information
            if currency_filter_applied:
                elements.append(Paragraph(f"Currency: {selected_currency}", styles['Normal']))
                elements.append(Paragraph("All values shown in selected currency", styles['SmallText']))
            else:
                if has_multiple_currencies:
                    elements.append(Paragraph(f"Base Currency: {base_currency}", styles['Normal']))
                    elements.append(Paragraph("Values shown in base currency with original amounts in parentheses",
                                              styles['SmallText']))
                else:
                    elements.append(Paragraph(f"Currency: {base_currency}", styles['Normal']))
                    elements.append(Paragraph("All values shown in company currency", styles['SmallText']))

            elements.append(Spacer(1, 12))

            # =============================
            # SUMMARY SECTION
            # =============================
            elements.append(Paragraph("Summary", styles['SubHeader']))
            # SIMPLIFIED: Only show amount paid
            summary_data = [
                ["Total Amount Paid", f"{total_amount_paid:,.2f} {display_currency}"],  # Sum of all payments made
                ["Total Transactions", f"{total_transactions:,}"]
            ]

            summary_table = Table(summary_data, colWidths=[200, 150])
            summary_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), font_name_regular),
                ('FONTNAME', (0, 0), (0, -1), font_name_bold),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                ('FONTSIZE', (0, 0), (-1, -1), 9),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('TOPPADDING', (0, 0), (-1, -1), 6),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#f8f9fa')),
                ('LINEBELOW', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 16))

            # =============================
            # DETAILED SECTION
            # =============================
            for project_id, project_info in project_data.items():
                if not project_info['transactions']:
                    continue

                elements.append(Paragraph(f"Project: {project_info['project_name']}", styles['SubHeader']))

                # Create table headers
                table_data = [["Date", "Type", "Purchase Reference", "Vendor", "Amount Paid", "Status"]]

                # Set column widths based on whether we show conversions
                if currency_filter_applied or not has_multiple_currencies:
                    col_widths = [50, 60, 80, 100, 100, 50]  # Normal width
                else:
                    col_widths = [50, 60, 80, 120, 150, 50]  # Extra width for conversions

                # Add transaction rows
                for tx in project_info['transactions']:
                    # Clean data formatting
                    date_display = tx['date'] or "-"
                    type_display = tx['type']
                    number_display = tx['number'] or "-"

                    # Clean vendor name - truncate if too long
                    vendor_display = tx['vendor'] or "-"
                    if len(vendor_display) > 20:
                        vendor_display = vendor_display[:20] + "..."

                    # AMOUNT DISPLAY WITH CONVERSIONS IN PARENTHESES
                    if currency_filter_applied:
                        # Single currency - simple display
                        amount_display = f"{tx['amount_paid']:,.2f} {tx['currency']}"
                    else:
                        if has_multiple_currencies and tx['original_currency'] != base_currency:
                            # Multiple currencies - show conversion in parentheses
                            amount_display = f"{tx['amount_paid']:,.2f} {base_currency} ({tx['original_amount_paid']:,.2f} {tx['original_currency']})"
                        else:
                            # Single currency or same currency - simple display
                            amount_display = f"{tx['amount_paid']:,.2f} {base_currency}"

                    status_display = tx['status'] or "-"

                    table_data.append([
                        Paragraph(date_display, styles['TableCell']),
                        Paragraph(type_display, styles['TableCell']),
                        Paragraph(number_display, styles['TableCell']),
                        Paragraph(vendor_display, styles['TableCell']),
                        Paragraph(amount_display, styles['AmountCell']),
                        Paragraph(status_display, styles['TableCell'])
                    ])

                # Add project totals row
                totals = project_info['totals']
                table_data.append([
                    "", "", "", "Project Total:",
                    Paragraph(f"{totals['paid']:,.2f} {display_currency}", styles['AmountCell']),
                    ""
                ])

                # Create table with proper styling
                project_table = Table(table_data, colWidths=col_widths)
                project_table.setStyle(TableStyle([
                    # Header row
                    ('FONTNAME', (0, 0), (-1, 0), font_name_bold),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#343a40')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('ALIGN', (0, 0), (-1, 0), 'LEFT'),

                    # Data rows
                    ('FONTNAME', (0, 1), (-1, -2), font_name_regular),
                    ('FONTSIZE', (0, 1), (-1, -2), 7),
                    ('GRID', (0, 0), (-1, -2), 0.5, colors.HexColor('#dee2e6')),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('TOPPADDING', (0, 0), (-1, -1), 3),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
                    ('LEFTPADDING', (0, 0), (-1, -1), 4),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 4),

                    # Amount column alignment
                    ('ALIGN', (4, 1), (4, -2), 'RIGHT'),

                    # Totals row
                    ('FONTNAME', (0, -1), (-1, -1), font_name_bold),
                    ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e9ecef')),
                    ('LINEABOVE', (0, -1), (-1, -1), 1, colors.black),
                    ('ALIGN', (4, -1), (4, -1), 'RIGHT'),

                    # Zebra striping for readability
                    ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#f8f9fa')]),
                ]))

                elements.append(project_table)
                elements.append(Spacer(1, 16))

            # =============================
            # CURRENCY BREAKDOWN (only if multiple currencies and no currency filter)
            # =============================
            if not currency_filter_applied and has_multiple_currencies and original_currency_totals:
                elements.append(Paragraph("Currency Breakdown", styles['SubHeader']))
                elements.append(Paragraph("All amounts converted to base currency", styles['SmallText']))
                elements.append(Spacer(1, 8))

                # Count transactions per currency
                currency_counts = {}
                for result in all_results:
                    currency = result.currency
                    currency_counts[currency] = currency_counts.get(currency, 0) + 1

                # Currency breakdown table
                cur_table_data = [["Currency", "Transactions", "Original Amount", f"Converted ({base_currency})"]]

                for currency, vals in sorted(original_currency_totals.items()):
                    if vals['paid'] > 0:  # Only show currencies with transactions
                        count = currency_counts.get(currency, 0)
                        cur_table_data.append([
                            currency,
                            str(count),
                            f"{vals['paid']:,.2f} {currency}",
                            f"{vals['converted_paid']:,.2f}"
                        ])

                cur_table = Table(cur_table_data, colWidths=[60, 70, 150, 120])
                cur_table.setStyle(TableStyle([
                    ('FONTNAME', (0, 0), (-1, 0), font_name_bold),
                    ('FONTSIZE', (0, 0), (-1, 0), 8),
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#343a40')),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
                    ('GRID', (0, 0), (-1, -1), 0.5, colors.HexColor('#dee2e6')),
                    ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                    ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                    ('TOPPADDING', (0, 0), (-1, -1), 5),
                    ('BOTTOMPADDING', (0, 0), (-1, -1), 5),
                    ('LEFTPADDING', (0, 0), (-1, -1), 6),
                    ('RIGHTPADDING', (0, 0), (-1, -1), 6),
                    ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor('#f8f9fa')]),
                ]))
                elements.append(cur_table)

            # Build the PDF
            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Purchases_Report_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                mimetype='application/pdf'
            )

        except Exception as e:
            logger.error(f"Error generating purchases report: {str(e)}\n{traceback.format_exc()}")
            return jsonify({"error": f"Failed to generate report: {str(e)}"}), 500


@download_route.route('/download_quotation/<int:quotation_id>', methods=['GET'])
@login_required
def download_quotation(quotation_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the quotation and related data
            quotation = db_session.query(Quotation).filter_by(id=quotation_id).first()
            if not quotation:
                return "Quotation not found", 404

            company = db_session.query(Company).filter_by(id=quotation.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=quotation.customer_id).first()
            items = db_session.query(QuotationItem).filter_by(quotation_id=quotation_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define styles with reduced left margin
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=20  # Line height matches font size
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            # Header Section with Logo and Quotation Details
            elements = []

            # Define a ParagraphStyle for the company name
            company_name_style = ParagraphStyle(
                name='CompanyNameStyle',
                fontName='Roboto-Bold',
                fontSize=32,
                leading=50  # Match the height of the logo (50 points)
            )

            # Create a table with two columns: Logo on the left, Title on the right
            header_data = [
                [
                    # Left Column: Logo (if present)
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else None,

                    # Right Column: Title
                    Paragraph("Quotation", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=32,
                        alignment=TA_RIGHT,  # Right-align the title
                        leading=50  # Match the height of the logo (50 points)
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])  # Adjust column widths as needed
            # Set styles for the table
            # Set styles for the table
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),  # Vertically center "Quotation" text in its cell
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),  # Right-align the title
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Quotation Details
            details_data = [
                [
                    # Left Column: Company Details
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    # Right Column: Quotation Details
                    [
                        Paragraph(f"Quotation # {quotation.quotation_number}", normal_style),
                        Paragraph(f"Date: {quotation.quotation_date.strftime('%Y-%m-%d')}", normal_style),
                        Paragraph(
                            f"Expiry Date: {quotation.expiry_date.strftime('%Y-%m-%d') if quotation.expiry_date else 'N/A'}",
                            normal_style),
                        Paragraph(f"Currency: {quotation.currencies.user_currency}", normal_style)
                    ]
                ]
            ]

            # Create the table with company and quotation details
            # Adjust column widths to push Quotation details further to the right
            details_table = Table(details_data, colWidths=[350, 150])  # Left column wider, right column narrower

            # Apply the table styles
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Align content to the top
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Align the left column content to the left
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),  # Align the right column content to the right
            ]))
            # Add the table to the elements list
            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Define a ParagraphStyle for wrapping text
            wrap_style = ParagraphStyle(
                name='WrapStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
            )

            # Define a ParagraphStyle for right-aligned numeric columns
            right_align_style = ParagraphStyle(
                name='RightAlignStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
                alignment=TA_RIGHT  # Right-align the text
            )

            # Quotation Items Table
            items_data = [["Item", "Description", "Quantity", "Unit Price", "Discount", "Tax(%)", "Total"]]
            for item in items:
                discount_text = f"{item.discount_amount:,.2f}" if item.discount_amount > 0 else "-"
                tax_text = f"({item.tax_rate}%)" if item.tax_amount > 0 else "-"

                item_name, description = get_item_details(item)

                items_data.append([
                    Paragraph(item_name, wrap_style),  # Wrap item name
                    Paragraph(description, wrap_style),  # Wrap description
                    Paragraph(f"{item.quantity} {item.unit_of_measurement.abbreviation}", right_align_style),
                    # Right-align quantity
                    Paragraph(f"{item.unit_price:,.2f}", right_align_style),  # Right-align unit price
                    Paragraph(discount_text, right_align_style),  # Right-align discount
                    Paragraph(tax_text, right_align_style),  # Right-align tax
                    Paragraph(f"{item.total_price:,.2f}", right_align_style)  # Right-align total
                ])

            items_table = Table(items_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),  # Light gray header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),  # Right-align numeric columns
                # If your totals column is the last one (index 6), align it to the far right as well
                ('ALIGN', (6, 0), (6, -1), 'RIGHT'),  # Align totals column (index 6) to the right

                # Adding wrap style to enable text wrapping (if you are using this style)
                ('WRAP', (0, 0), (-1, -1), True),  # Enable text wrapping for all columns

                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),  # Regular font for rows
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),  # Header underline
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),  # Set white background for rows
                # Adding borders for each column
                # Add outer border only (do not overlap with inner grid lines)
                ('BOX', (0, 0), (-1, -1), 0.2, colors.black),  # Outer border

                # Apply inner grid lines for internal borders (excluding outer borders)
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),  # Internal grid lines
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Totals Section
            totals_data = [
                [" ", "", "", "", "Subtotal", "", f"{quotation.total_line_subtotal:,.2f}"],
                ["", "", "", "", "Discount", "",
                 f"{quotation.calculated_discount_amount:,.2f}" if quotation.calculated_discount_amount > 0 else "-"],
                ["", "", "", "", "Tax(%)", "",
                 f"{quotation.quotation_tax_rate:,.2f}%" if quotation.quotation_tax_rate > 0 else "-"],
                ["", "", "", "", "Shipping Cost", "",
                 f"{quotation.shipping_cost:,.2f}" if quotation.shipping_cost > 0 else "-"],
                ["", "", "", "", "Handling Cost", "",
                 f"{quotation.handling_cost:,.2f}" if quotation.handling_cost > 0 else "-"],
                ["", "", "", "", "Grand Total", "", f"{quotation.total_amount:,.2f}"]
            ]

            totals_table = Table(totals_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),  # Right-align numeric columns
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),  # Bold for grand total
                ('LINEABOVE', (0, -1), (-1, -1), 0.2, colors.black),  # Line above grand total
            ]))
            elements.append(totals_table)
            elements.append(Spacer(1, 12))

            # Terms and Conditions
            terms_conditions = [
                Paragraph("Terms and Conditions:", bold_style2),
                Paragraph(quotation.terms_and_conditions or "N/A", normal_style2)
            ]
            elements.extend(terms_conditions)

            # Generate and save PDF
            doc.build(
                elements,
                onFirstPage=partial(add_footer, company=company),
                onLaterPages=partial(add_footer, company=company)
            )
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Quotation_{quotation.quotation_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating quotation PDF: {e}")  # Debugging
        return f"An error occurred: {e}", 500


@download_route.route('/download_sales_invoice/<int:invoice_id>', methods=['GET'])
@login_required
def download_sales_invoice(invoice_id):
    from app import UPLOAD_FOLDER_LOGOS  # import here inside the route function to avoid circular import
    try:
        with Session() as db_session:
            # Fetch the invoice and related data
            invoice = db_session.query(SalesInvoice).filter_by(id=invoice_id).first()
            if not invoice:
                return "Invoice not found", 404

            company = db_session.query(Company).filter_by(id=invoice.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=invoice.customer_id).first()
            items = db_session.query(SalesInvoiceItem).filter_by(invoice_id=invoice_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define custom styles
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=20,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            right_align_style = ParagraphStyle(
                name='RightAlignStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
                alignment=TA_RIGHT  # Right-align the text
            )

            # Header Section with Logo and Invoice Details
            elements = []

            # Company Name and Invoice Title
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"", ParagraphStyle(
                            name='CompanyNameStyle',
                            fontName='Roboto-Bold',
                            fontSize=32,
                            leading=50
                        )),
                    Paragraph("Invoice", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=32,
                        leading=50,
                        leftIndent=68,
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Invoice Details
            details_data = [
                [
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    [
                        Paragraph(f"Invoice #: {invoice.invoice_number}", normal_style3),
                        Paragraph(f"Date: {invoice.invoice_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(f"Due Date: {invoice.due_date.strftime('%Y-%m-%d') if invoice.due_date else 'N/A'}",
                                  normal_style3),
                        Paragraph(f"Currency: {invoice.currencies.user_currency}", normal_style3)
                    ]
                ]
            ]

            details_table = Table(details_data, colWidths=[350, 150])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Bill To / Customer Details:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Invoice Items Table
            items_data = [["Item", "Description", "Quantity", "Unit Price", "Discount", "Tax(%)", "Total"]]
            for item in items:
                discount_text = f"{item.discount_amount:,.2f}" if item.discount_amount > 0 else "-"
                tax_text = f"({item.tax_rate}%)" if item.tax_amount > 0 else "-"

                item_name, description = get_item_details(item)

                items_data.append([
                    Paragraph(item_name, normal_style),
                    Paragraph(description, normal_style),
                    Paragraph(f"{item.quantity} {item.unit_of_measurement.abbreviation}", normal_style),
                    Paragraph(f"{item.unit_price:,.2f}", right_align_style),
                    Paragraph(discount_text, right_align_style),
                    Paragraph(tax_text, right_align_style),
                    Paragraph(f"{item.total_price:,.2f}", right_align_style)
                ])

            items_table = Table(items_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('BOX', (0, 0), (-1, -1), 0.1, colors.black),
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Totals Section
            totals_data = [
                [" ", "", "", "", "Subtotal", "", f"{invoice.total_line_subtotal:,.2f}"],
                ["", "", "", "", "Discount", "",
                 f"{invoice.calculated_discount_amount:,.2f}" if invoice.calculated_discount_amount > 0 else "-"],
                ["", "", "", "", "Tax(%)", "",
                 f"{invoice.invoice_tax_rate:,.2f}" if invoice.invoice_tax_rate > 0 else "-"],
                ["", "", "", "", "Shipping Cost", "",
                 f"{invoice.shipping_cost:,.2f}" if invoice.shipping_cost > 0 else "-"],
                ["", "", "", "", "Handling Cost", "",
                 f"{invoice.handling_cost:,.2f}" if invoice.handling_cost > 0 else "-"],
                ["", "", "", "", "Grand Total", "", f"{invoice.total_amount:,.2f}"]
            ]

            totals_table = Table(totals_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                ('LINEABOVE', (0, -1), (-1, -1), 0.2, colors.black),
            ]))
            elements.append(totals_table)
            elements.append(Spacer(1, 12))

            # Terms and Conditions
            preformatted_style = ParagraphStyle(
                "Preformatted", parent=normal_style2, spaceAfter=6, leading=14
            )

            terms_conditions = [
                Paragraph("Payment Terms:", bold_style2),
                Paragraph(
                    invoice.terms_and_conditions.replace("\n", "<br/>") if invoice.terms_and_conditions else "N/A",
                    preformatted_style)
            ]

            elements.extend(terms_conditions)

            # Generate and save PDF
            doc.build(
                elements,
                onFirstPage=partial(add_footer, company=company),
                onLaterPages=partial(add_footer, company=company)
            )
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Invoice_{invoice.invoice_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f'Error occurred: {str(e)}\n{traceback.format_exc()}')
        return f"An error occurred with the download. Please try again or contact support", 500


@download_route.route('/download_sales_order/<int:sales_order_id>', methods=['GET'])
@login_required
def download_sales_order(sales_order_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the sales_order and related data
            sales_order = db_session.query(SalesOrder).filter_by(id=sales_order_id).first()
            if not sales_order:
                return "Sales Order not found", 404

            company = db_session.query(Company).filter_by(id=sales_order.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=sales_order.customer_id).first()
            items = db_session.query(SalesOrderItem).filter_by(sales_order_id=sales_order_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define styles with reduced left margin
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=20  # Line height matches font size
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=-6,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            # Header Section with Logo and sales_order Details
            elements = []

            # Define a ParagraphStyle for the company name
            company_name_style = ParagraphStyle(
                name='CompanyNameStyle',
                fontName='Roboto-Bold',
                fontSize=32,
                leading=50  # Match the height of the logo (50 points)
            )

            # Create a table with two columns: Logo on the left, Title on the right
            header_data = [
                [
                    # Left Column: Logo (if present)
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"", company_name_style),

                    # Right Column: Title
                    Paragraph("Sales Order", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=28,
                        alignment=TA_RIGHT,  # Right-align the title
                        leading=50  # Match the height of the logo (50 points)
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])  # Adjust column widths as needed
            # Set styles for the table
            # Set styles for the table
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),  # Vertically center "sales_order" text in its cell
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),  # Right-align the title
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and sales_order Details
            details_data = [
                [
                    # Left Column: Company Details
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    # Right Column: sales_order Details
                    [
                        Paragraph(f"Sales Order #: {sales_order.sales_order_number}", normal_style3),
                        Paragraph(f"Date: {sales_order.sales_order_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(
                            f"Due Date: {sales_order.expiry_date.strftime('%Y-%m-%d') if sales_order.expiry_date else 'N/A'}",
                            normal_style3),
                        Paragraph(f"Currency: {sales_order.currencies.user_currency}", normal_style3)
                    ]
                ]
            ]

            # Create the table with company and sales_order details
            # Adjust column widths to push sales_order details further to the right
            details_table = Table(details_data, colWidths=[350, 150])  # Left column wider, right column narrower

            # Apply the table styles
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Align content to the top
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Align the left column content to the left
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),  # Align the right column content to the right
            ]))
            # Add the table to the elements list
            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Define a ParagraphStyle for wrapping text
            wrap_style = ParagraphStyle(
                name='WrapStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
            )

            # Define a ParagraphStyle for right-aligned numeric columns
            right_align_style = ParagraphStyle(
                name='RightAlignStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
                alignment=TA_RIGHT  # Right-align the text
            )

            # sales_order Items Table
            items_data = [["Item", "Description", "Quantity", "Unit Price", "Discount", "Tax(%)", "Total"]]
            for item in items:
                discount_text = f"{item.discount_amount:,.2f}" if item.discount_amount > 0 else "-"
                tax_text = f"({item.tax_rate}%)" if item.tax_amount > 0 else "-"

                item_name, description = get_item_details(item)

                items_data.append([
                    Paragraph(item_name, wrap_style),  # Wrap item name
                    Paragraph(description, wrap_style),  # Wrap description
                    Paragraph(f"{item.quantity} {item.unit_of_measurement.abbreviation}", right_align_style),
                    # Right-align quantity
                    Paragraph(f"{item.unit_price:,.2f}", right_align_style),  # Right-align unit price
                    Paragraph(discount_text, right_align_style),  # Right-align discount
                    Paragraph(tax_text, right_align_style),  # Right-align tax
                    Paragraph(f"{item.total_price:,.2f}", right_align_style)  # Right-align total
                ])

            items_table = Table(items_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),  # Light gray header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),  # Right-align numeric columns
                # If your totals column is the last one (index 6), align it to the far right as well
                ('ALIGN', (6, 0), (6, -1), 'RIGHT'),  # Align totals column (index 6) to the right

                # Adding wrap style to enable text wrapping (if you are using this style)
                ('WRAP', (0, 0), (-1, -1), True),  # Enable text wrapping for all columns

                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),  # Regular font for rows
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),  # Header underline
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),  # Set white background for rows
                # Adding borders for each column
                # Add outer border only (do not overlap with inner grid lines)
                ('BOX', (0, 0), (-1, -1), 0.2, colors.black),  # Outer border

                # Apply inner grid lines for internal borders (excluding outer borders)
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),  # Internal grid lines
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Totals Section
            totals_data = [
                [" ", "", "", "", "Subtotal", "", f"{sales_order.total_line_subtotal:,.2f}"],
                ["", "", "", "", "Discount", "",
                 f"{sales_order.calculated_discount_amount:,.2f}" if sales_order.calculated_discount_amount > 0 else "-"],
                ["", "", "", "", "Tax(%)", "",
                 f"{sales_order.sales_order_tax_rate:,.2f}" if sales_order.sales_order_tax_rate > 0 else "-"],
                ["", "", "", "", "Shipping Cost", "",
                 f"{sales_order.shipping_cost:,.2f}" if sales_order.shipping_cost > 0 else "-"],
                ["", "", "", "", "Handling Cost", "",
                 f"{sales_order.handling_cost:,.2f}" if sales_order.handling_cost > 0 else "-"],
                ["", "", "", "", "Grand Total", "", f"{sales_order.total_amount:,.2f}"]
            ]

            totals_table = Table(totals_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),  # Right-align numeric columns
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),  # Bold for grand total
                ('LINEABOVE', (0, -1), (-1, -1), 0.2, colors.black),  # Line above grand total
            ]))
            elements.append(totals_table)
            elements.append(Spacer(1, 12))

            preformatted_style = ParagraphStyle(
                "Preformatted", parent=normal_style2, spaceAfter=6, leading=14
            )

            # Adjusting for Terms and Conditions
            terms_conditions = [
                Paragraph("Terms and Conditions:", bold_style2),
                Paragraph(sales_order.terms_and_conditions.replace("\n",
                                                                   "<br/>") if sales_order.terms_and_conditions else "N/A",
                          preformatted_style)
            ]
            elements.extend(terms_conditions)

            elements.append(Spacer(1, 12))

            # Generate and save PDF
            doc.build(
                elements,
                onFirstPage=partial(add_footer, company=company),
                onLaterPages=partial(add_footer, company=company)
            )
            buffer.seek(0)
            print("Sales Order tion PDF generated successfully")  # Debugging

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Sales Order_{sales_order.sales_order_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating sales order PDF: {e}")  # Debugging
        return f"An error occurred: {e}", 500


@download_route.route('/download_sales_transaction/<int:transaction_id>', methods=['GET'])
@login_required
def download_sales_transaction(transaction_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the sales transaction and related data
            transaction = db_session.query(SalesTransaction).filter_by(id=transaction_id).first()
            if not transaction:
                return "Sales Transaction not found", 404

            company = db_session.query(Company).filter_by(id=transaction.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=transaction.customer_id).first()
            invoice = db_session.query(SalesInvoice).filter_by(id=transaction.invoice_id).first()
            payment_allocations = db_session.query(PaymentAllocation).filter_by(payment_id=transaction_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)

            # Define custom styles
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'

            bold_style = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leading=14
            )

            normal_style = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leading=12
            )

            right_align_style = ParagraphStyle(
                name='RightAlign',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_RIGHT,
                leading=12
            )

            # Define styles with reduced left margin
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=20  # Line height matches font size
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            # Header Section with Logo and Transaction Details
            elements = []

            # Company Logo and Title
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        "", bold_style),
                    Paragraph("Sales Transaction", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=24,
                        leftIndent=-10,  # Right-align the title
                        leading=50  # Match the height of the logo (50 points)
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Transaction Details
            details_data = [
                [
                    # Left Column: Company Details
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    # Right Column: Transaction Details
                    [

                        Paragraph(f"Transaction Date: {transaction.payment_date.strftime('%Y-%m-%d')}", normal_style),
                        Paragraph(f"Reference Number: {transaction.reference_number or '-'}", normal_style),
                        Paragraph(f"Currency: {transaction.currency.user_currency}", normal_style),
                        Paragraph(f"Status: {transaction.payment_status.value.capitalize()}", normal_style)
                    ]
                ]
            ]

            details_table = Table(details_data, colWidths=[350, 150])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Add Heading Before Table
            invoice_heading = [
                Paragraph(f"Payment for Invoice: #{invoice.invoice_number}", bold_style2),
                Spacer(1, 12)
            ]
            elements.extend(invoice_heading)

            # Payment Allocations Table
            allocations_data = [["Payment Mode", "Amount Paid", "Allocated Base Amount", "Allocated Tax Amount"]]
            for allocation in payment_allocations:
                allocations_data.append([
                    Paragraph(
                        f"{transaction.payment_allocations[0].payment_modes.payment_mode if transaction.payment_allocations[0].payment_modes else '-'}",
                        normal_style),
                    Paragraph(f"{transaction.amount_paid:,.2f}", right_align_style),
                    Paragraph(f"{allocation.allocated_base_amount:,.2f}", right_align_style),
                    Paragraph(f"{allocation.allocated_tax_amount:,.2f}", right_align_style)
                ])

            allocations_table = Table(allocations_data, colWidths=[80, 150, 140, 120])
            allocations_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('ALIGN', (0, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                # Add borders to the entire table
                ('BOX', (0, 0), (-1, -1), 0.1, colors.black),  # Outer border for the entire table
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
            ]))

            elements.append(allocations_table)
            elements.append(Spacer(1, 12))

            # Generate and save PDF with footer
            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Sales_Transaction_{transaction.id}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating PDF: {e}")
        return f"An error occurred while generating the PDF: {e}", 500


@download_route.route('/download_direct_sale/<int:direct_sale_id>', methods=['GET'])
@login_required
def download_direct_sale(direct_sale_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the direct sale and related data
            direct_sale = db_session.query(DirectSalesTransaction).filter_by(id=direct_sale_id).first()
            if not direct_sale:
                return "Direct Sale not found", 404

            company = db_session.query(Company).filter_by(id=direct_sale.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=direct_sale.customer_id).first()
            items = db_session.query(DirectSaleItem).filter_by(transaction_id=direct_sale_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define styles with reduced left margin
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=20  # Line height matches font size
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=-17,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,  # LEFT alignment
                leftIndent=-6,  # Reduce left margin (adjust as needed)
                spaceBefore=0,  # No extra space before the paragraph
                spaceAfter=0,  # No extra space after the paragraph
                leading=15  # Line height matches font size
            )

            # Header Section with Logo and Direct Sale Details
            elements = []

            # Define a ParagraphStyle for the company name
            company_name_style = ParagraphStyle(
                name='CompanyNameStyle',
                fontName='Roboto-Bold',
                fontSize=32,
                leading=50  # Match the height of the logo (50 points)
            )

            # Create a table with two columns: Logo on the left, Title on the right
            header_data = [
                [
                    # Left Column: Logo (if present)
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"", company_name_style),

                    # Right Column: Title
                    Paragraph("Direct Sale", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=30,
                        alignment=TA_RIGHT,  # Right-align the title
                        leading=50  # Match the height of the logo (50 points)
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])  # Adjust column widths as needed
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),  # Vertically center "Direct Sale" text in its cell
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),  # Right-align the title
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Direct Sale Details
            details_data = [
                [
                    # Left Column: Company Details
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    # Right Column: Direct Sale Details
                    [
                        Paragraph(f"Direct Sale #: {direct_sale.direct_sale_number}", normal_style3),
                        Paragraph(f"Date: {direct_sale.payment_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(f"Currency: {direct_sale.currency.user_currency}", normal_style3),
                        Paragraph(f"Status: {direct_sale.status.value.capitalize()}", normal_style3)

                    ]
                ]
            ]

            # Create the table with company and direct sale details
            details_table = Table(details_data, colWidths=[350, 150])  # Left column wider, right column narrower
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Align content to the top
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Align the left column content to the left
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),  # Align the right column content to the right
            ]))
            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Define a ParagraphStyle for wrapping text
            wrap_style = ParagraphStyle(
                name='WrapStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
            )

            # Define a ParagraphStyle for right-aligned numeric columns
            right_align_style = ParagraphStyle(
                name='RightAlignStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
                alignment=TA_RIGHT  # Right-align the text
            )

            # Direct Sale Items Table
            items_data = [["Item", "Description", "Quantity", "Unit Price", "Discount", "Tax(%)", "Total"]]
            for item in items:
                discount_text = f"{item.discount_amount:,.2f}" if item.discount_amount > 0 else "-"
                tax_text = f"{item.tax_rate}%" if item.tax_amount > 0 else "-"

                item_name, description = get_item_details(item)

                items_data.append([
                    Paragraph(item_name, wrap_style),  # Wrap item name
                    Paragraph(description, wrap_style),  # Wrap description
                    Paragraph(f"{item.quantity} {item.unit_of_measurement.abbreviation}", right_align_style),
                    # Right-align quantity
                    Paragraph(f"{item.unit_price:,.2f}", right_align_style),  # Right-align unit price
                    Paragraph(discount_text, right_align_style),  # Right-align discount
                    Paragraph(tax_text, right_align_style),  # Right-align tax
                    Paragraph(f"{item.total_price:,.2f}", right_align_style)  # Right-align total
                ])

            items_table = Table(items_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),  # Light gray header background
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),  # Right-align numeric columns
                ('ALIGN', (6, 0), (6, -1), 'RIGHT'),  # Align totals column (index 6) to the right
                ('WRAP', (0, 0), (-1, -1), True),  # Enable text wrapping for all columns
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),  # Regular font for rows
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),  # Header underline
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),  # Set white background for rows
                ('BOX', (0, 0), (-1, -1), 0.2, colors.black),  # Outer border
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),  # Internal grid lines
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Totals Section
            totals_data = [
                [" ", "", "", "", "Subtotal", "", f"{direct_sale.total_line_subtotal:,.2f}"],
                ["", "", "", "", "Discount", "",
                 f"{direct_sale.calculated_discount_amount:,.2f}" if direct_sale.calculated_discount_amount > 0 else "-"],
                ["", "", "", "", "Tax(%)", "",
                 f"{direct_sale.sales_tax_rate:,.2f}" if direct_sale.sales_tax_rate > 0 else "-"],
                ["", "", "", "", "Shipping Cost", "",
                 f"{direct_sale.shipping_cost:,.2f}" if direct_sale.shipping_cost > 0 else "-"],
                ["", "", "", "", "Handling Cost", "",
                 f"{direct_sale.handling_cost:,.2f}" if direct_sale.handling_cost > 0 else "-"],
                ["", "", "", "", "Grand Total", "", f"{direct_sale.total_amount:,.2f}"]
            ]

            totals_table = Table(totals_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),  # Right-align numeric columns
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),  # Bold for grand total
                ('LINEABOVE', (0, -1), (-1, -1), 0.2, colors.black),  # Line above grand total
            ]))
            elements.append(totals_table)
            elements.append(Spacer(1, 12))

            preformatted_style = ParagraphStyle(
                "Preformatted", parent=normal_style2, spaceAfter=6, leading=14
            )

            # Adjusting for Terms and Conditions
            # Adjusting for Terms and Conditions
            terms_conditions = [
                Paragraph("Terms and Conditions:", bold_style2),
                Paragraph(
                    direct_sale.terms_and_conditions.replace("\n", "<br/>")
                    if direct_sale.terms_and_conditions else "N/A",
                    # Fixed: Check terms_and_conditions instead of sale_reference
                    preformatted_style
                )
            ]
            elements.extend(terms_conditions)

            elements.append(Spacer(1, 12))

            # Generate and save PDF
            doc.build(
                elements,
                onFirstPage=partial(add_footer, company=company),
                onLaterPages=partial(add_footer, company=company)
            )
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Direct Sale_{direct_sale.direct_sale_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f'An error occurred {e}\n{traceback.format_exc()}')

        return f"An error occurred: {e}", 500


@download_route.route('/download_payment_receipt/<int:receipt_id>', methods=['GET'])
@login_required
def download_payment_receipt(receipt_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the payment receipt and related data
            receipt = db_session.query(PaymentReceipt).filter_by(id=receipt_id).first()
            if not receipt:
                return "Payment Receipt not found", 404

            company = db_session.query(Company).filter_by(id=receipt.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=receipt.customer_id).first()
            currency = db_session.query(Currency).filter_by(id=receipt.currency_id).first()
            payment_mode = db_session.query(PaymentMode).filter_by(id=receipt.payment_mode).first()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define custom styles
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=-48,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            right_align_style = ParagraphStyle(
                name='RightAlignStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
                alignment=TA_RIGHT  # Right-align the text
            )

            # Header Section with Logo and Receipt Details
            elements = []

            # Company Name and Receipt Title
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"", ParagraphStyle(
                            name='CompanyNameStyle',
                            fontName='Roboto-Bold',
                            fontSize=32,
                            leading=50
                        )),
                    Paragraph("Payment Receipt", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=24,
                        leading=50,
                        leftIndent=0,
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Receipt Details
            details_data = [
                [
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    [
                        Paragraph(f"Receipt Number #: {receipt.payment_receipt_number}", normal_style3),
                        Paragraph(f"Date: {receipt.payment_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(f"Currency: {currency.user_currency}", normal_style3)
                    ]
                ]
            ]

            details_table = Table(details_data, colWidths=[350, 150])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Reason for Payment
            reason_for_payment = [
                Paragraph("Purpose of Payment:", bold_style2),
                Paragraph(receipt.description or "N/A", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(reason_for_payment)

            # Receipt Details Table
            receipt_data = [
                ["Payment Mode", "Amount Received", "Reference Number", "Status"],
                [
                    Paragraph(payment_mode.payment_mode if payment_mode else "-", normal_style),
                    Paragraph(f"{receipt.amount_received:,.2f}", right_align_style),
                    Paragraph(receipt.reference_number or "-", normal_style),
                    Paragraph(receipt.status.capitalize(), normal_style)
                ]
            ]

            receipt_table = Table(receipt_data, colWidths=[120, 120, 120, 120])
            receipt_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (0, 1), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('BOX', (0, 0), (-1, -1), 0.1, colors.black),
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
            ]))

            elements.append(receipt_table)
            elements.append(Spacer(1, 12))

            # Add Balance Due section after the receipt table
            if hasattr(receipt, 'balance_due') and receipt.balance_due is not None:
                balance_due_section = [
                    Spacer(1, 12),
                    Paragraph("Balance Due:", bold_style2),
                    Paragraph(f"{receipt.balance_due:,.2f}", normal_style2),
                    Spacer(1, 12)
                ]
                elements.extend(balance_due_section)

            # Add Notes section if notes exist
            if hasattr(receipt, 'notes') and receipt.notes:
                # Create a style that preserves line breaks
                notes_style = ParagraphStyle(
                    name='NotesStyle',
                    fontName='Roboto',
                    fontSize=10,
                    leading=12,
                    spaceBefore=6,
                    spaceAfter=6,
                    leftIndent=0
                )

                # Convert line breaks to <br/> tags for proper rendering
                notes_text = receipt.notes.replace('\n', '<br/>')

                notes_section = [
                    Paragraph("Notes:", bold_style2),
                    Paragraph(notes_text, notes_style),
                    Spacer(1, 12)
                ]
                elements.extend(notes_section)

            # Received by and Issued by Section
            received_by_issued_by_data = [
                [
                    Paragraph("Received by:", bold_style),
                    Paragraph("Issued by:", bold_style)
                ],
                [
                    Paragraph(receipt.received_by_name or "", normal_style),
                    Paragraph(receipt.issued_by_name or "N/A", normal_style)
                ]
            ]

            received_by_issued_by_table = Table(received_by_issued_by_data, colWidths=[250, 250])
            received_by_issued_by_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(received_by_issued_by_table)
            elements.append(Spacer(1, 12))

            # Generate and save PDF
            doc.build(
                elements,
                onFirstPage=partial(add_footer, company=company),
                onLaterPages=partial(add_footer, company=company)
            )
            buffer.seek(0)
            print("Payment Receipt PDF generated successfully")  # Debugging

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Payment_Receipt_{receipt.payment_receipt_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating payment receipt PDF: {e}")  # Debugging
        return f"An error occurred: {e}", 500



@download_route.route('/download_receipt/<int:receipt_id>', methods=['GET'])
@login_required
def download_receipt(receipt_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the bulk payment receipt and related data
            receipt = db_session.query(BulkPayment).filter_by(id=receipt_id).first()
            if not receipt:
                return "Receipt not found", 404

            company = db_session.query(Company).filter_by(id=receipt.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=receipt.customer_id).first()
            transactions = receipt.transactions  # SalesTransaction records linked to this receipt
            credits = receipt.customer_credits  # CustomerCredit records linked to this receipt

            # ===== FIX: Calculate totals in PAYMENT CURRENCY =====
            total_allocated = 0
            payment_rate = float(receipt.exchange_rate.rate) if receipt.exchange_rate else 1

            # Store converted amounts for each transaction to use in table
            transaction_amounts = []

            for tx in transactions:
                if tx.currency_id == receipt.currency_id:
                    # Same currency
                    amount_in_payment = float(tx.amount_paid)
                elif tx.invoice and tx.invoice.exchange_rate and receipt.exchange_rate:
                    # Convert via base currency
                    invoice_rate = float(tx.invoice.exchange_rate.rate)
                    amount_in_base = float(tx.amount_paid) * invoice_rate
                    amount_in_payment = amount_in_base / payment_rate
                else:
                    amount_in_payment = float(tx.amount_paid)

                total_allocated += amount_in_payment
                transaction_amounts.append({
                    'transaction': tx,
                    'amount_in_payment': amount_in_payment
                })

            total_amount = float(receipt.total_amount)
            unallocated = total_amount - total_allocated
            # ===== END FIX =====

            # Determine allocation status
            if total_allocated >= total_amount - 0.01:
                allocation_status = "Fully Allocated"
            elif total_allocated > 0:
                allocation_status = "Partially Allocated"
            else:
                allocation_status = "Unallocated"

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)

            # Define custom styles
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'

            bold_style = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leading=14
            )

            normal_style = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leading=12
            )

            right_align_style = ParagraphStyle(
                name='RightAlign',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_RIGHT,
                leading=12
            )

            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            # Header Section with Logo and Receipt Title
            elements = []

            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        "", bold_style),
                    Paragraph("Payment Receipt", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=24,
                        leftIndent=-10,
                        leading=50
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Receipt Details
            receipt_number = receipt.receipt_number if hasattr(receipt, 'receipt_number') else f"RCPT-{receipt.id:04d}"

            details_data = [
                [
                    # Left Column: Company Details
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    # Right Column: Receipt Details
                    [
                        Paragraph(f"Receipt #: {receipt_number}", normal_style),
                        Paragraph(f"Date: {receipt.payment_date.strftime('%Y-%m-%d')}", normal_style),
                        Paragraph(f"Reference: {receipt.reference or '-'}", normal_style),
                        Paragraph(f"Payment Method: {receipt.payment_mode.payment_mode if receipt.payment_mode else 'N/A'}", normal_style),
                        Paragraph(f"Status: {allocation_status}", normal_style)
                    ]
                ]
            ]

            details_table = Table(details_data, colWidths=[350, 150])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style2),
                Paragraph(f"{customer.vendor_name}", normal_style2),
                Paragraph(f"{customer.address or ''}", normal_style2),
                Paragraph(f"{customer.country or ''}", normal_style2),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(customer_details)

            # Exchange Rate Information (if applicable)
            if receipt.exchange_rate:
                exchange_rate_info = [
                    Paragraph("Exchange Rate Information:", bold_style2),
                    Paragraph(f"1 {receipt.currency.user_currency} = {receipt.exchange_rate.rate:.6f} {company.base_currency.user_currency}", normal_style2),
                    Paragraph(f"Date: {receipt.payment_date.strftime('%Y-%m-%d')}", normal_style2),
                    Spacer(1, 15)
                ]
                elements.extend(exchange_rate_info)

            # Payment Summary
            summary_data = [
                ["Description", "Amount", "Currency"],
                ["Total Amount Received", f"{total_amount:,.2f}", receipt.currency.user_currency],
                ["Total Allocated", f"{total_allocated:,.2f}", receipt.currency.user_currency],
                ["Unallocated Balance", f"{unallocated:,.2f}", receipt.currency.user_currency]
            ]

            summary_table = Table(summary_data, colWidths=[200, 150, 80])
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (1, 1), (1, -1), 'RIGHT'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('BOX', (0, 0), (-1, -1), 0.1, colors.black),
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
                ('BACKGROUND', (1, 1), (1, -1), colors.HexColor("#F5F5F5")),
            ]))

            elements.append(Paragraph("Payment Summary:", bold_style2))
            elements.append(Spacer(1, 8))
            elements.append(summary_table)
            elements.append(Spacer(1, 20))

            # Allocated Invoices Table (if any)
            if transactions:
                elements.append(Paragraph("Allocated Invoices:", bold_style2))
                elements.append(Spacer(1, 8))

                invoice_data = [["Invoice #", "Date", "Original Amount", "Curr", f"Amount in {receipt.currency.user_currency}", "Status"]]

                for i, tx in enumerate(transactions):
                    amount_in_payment = transaction_amounts[i]['amount_in_payment']

                    # Get status text
                    if hasattr(tx.payment_status, 'value'):
                        status_text = tx.payment_status.value.capitalize()
                    elif hasattr(tx.payment_status, 'name'):
                        status_text = tx.payment_status.name.capitalize()
                    else:
                        status_text = str(tx.payment_status).capitalize()

                    invoice_data.append([
                        Paragraph(tx.invoice.invoice_number, normal_style),
                        Paragraph(tx.payment_date.strftime('%Y-%m-%d'), normal_style),
                        Paragraph(f"{tx.amount_paid:,.2f}", right_align_style),
                        Paragraph(tx.currency.user_currency, normal_style),
                        Paragraph(f"{amount_in_payment:,.2f}", right_align_style),
                        Paragraph(status_text, normal_style)
                    ])

                invoice_table = Table(invoice_data, colWidths=[100, 70, 80, 50, 100, 80])
                invoice_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('ALIGN', (2, 1), (4, -1), 'RIGHT'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                    ('BOX', (0, 0), (-1, -1), 0.1, colors.black),
                    ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
                ]))

                elements.append(invoice_table)
                elements.append(Spacer(1, 20))

            # Customer Credits Table (if any)
            if credits:
                elements.append(Paragraph("Customer Credits Created:", bold_style2))
                elements.append(Spacer(1, 8))

                credit_data = [["Credit ID", "Date", "Original Amount", "Available Amount", "Currency", "Status"]]

                for credit in credits:
                    # Get status text
                    if hasattr(credit.status, 'value'):
                        status_text = credit.status.value.capitalize()
                    elif hasattr(credit.status, 'name'):
                        status_text = credit.status.name.capitalize()
                    else:
                        status_text = str(credit.status).capitalize() if credit.status else 'Active'

                    credit_data.append([
                        Paragraph(str(credit.id), normal_style),
                        Paragraph(credit.issued_date.strftime('%Y-%m-%d') if credit.issued_date else '-', normal_style),
                        Paragraph(f"{credit.original_amount:,.2f}", right_align_style),
                        Paragraph(f"{credit.available_amount:,.2f}", right_align_style),
                        Paragraph(credit.currency.user_currency, normal_style),
                        Paragraph(status_text, normal_style)
                    ])

                credit_table = Table(credit_data, colWidths=[70, 80, 100, 100, 70, 80])
                credit_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                    ('ALIGN', (2, 1), (3, -1), 'RIGHT'),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                    ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                    ('BOX', (0, 0), (-1, -1), 0.1, colors.black),
                    ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
                ]))

                elements.append(credit_table)

            # Generate and save PDF with footer
            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Payment_Receipt_{receipt_id}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating receipt PDF: {e}")
        return f"An error occurred while generating the PDF: {e}", 500


@download_route.route('/download_purchase_order/<int:purchase_order_id>', methods=['GET'])
@login_required
def download_purchase_order(purchase_order_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the purchase order and related data
            purchase_order = db_session.query(PurchaseOrder).filter_by(id=purchase_order_id).first()
            if not purchase_order:
                return "Purchase Order not found", 404

            company = db_session.query(Company).filter_by(id=purchase_order.app_id).first()
            vendor = db_session.query(Vendor).filter_by(id=purchase_order.vendor_id).first()
            items = db_session.query(PurchaseOrderItem).filter_by(purchase_order_id=purchase_order_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define custom styles
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=70,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )
            bold_style3 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leftIndent=70,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            right_align_style = ParagraphStyle(
                name='RightAlign',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_RIGHT,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            # Header Section with Logo and Purchase Order Details
            elements = []

            # Company Name and Logo
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"", bold_style),
                    Paragraph("Purchase Order", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=24,
                        alignment=TA_RIGHT,
                        leading=50
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Purchase Order Details
            details_data = [
                [
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    [
                        Paragraph(f"PO #: {purchase_order.purchase_order_number}", normal_style3),
                        Paragraph(f"Date: {purchase_order.purchase_order_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(
                            f"Delivery Date: {purchase_order.delivery_date.strftime('%Y-%m-%d') if purchase_order.delivery_date else 'N/A'}",
                            normal_style3),
                        Paragraph(f"Currency: {purchase_order.currencies.user_currency}", normal_style3)
                    ]
                ]
            ]

            details_table = Table(details_data, colWidths=[250, 250])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Vendor and Shipping Details (side by side)
            vendor_details = [
                Paragraph("Vendor Information:", bold_style),
                Paragraph(f"{vendor.vendor_name}", normal_style),
                Paragraph(f"{vendor.address or ''}", normal_style),
                Paragraph(f"{vendor.city or ''} {vendor.country or ''}".strip(), normal_style),
                Paragraph(f"Contact: {vendor.tel_contact or 'N/A'}", normal_style)
            ]

            shipping_details = [
                Paragraph("Shipping Information:", bold_style3),
                Paragraph(f"Method: {purchase_order.delivery_method or 'N/A'}", normal_style3),
                Paragraph(f"Address: {purchase_order.shipping_address or 'N/A'}", normal_style3)
            ]

            # Create a table for side-by-side layout
            side_by_side_data = [
                [vendor_details, shipping_details]
            ]

            side_by_side_table = Table(side_by_side_data, colWidths=[250, 250])
            side_by_side_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6)
            ]))

            elements.append(side_by_side_table)
            elements.append(Spacer(1, 12))

            # Purchase Order Items Table
            items_data = [["Item", "Description", "Qty", "Unit Price", "Discount", "Tax(%)", "Total"]]
            for item in items:
                discount_text = f"{item.discount_amount:,.2f}" if item.discount_amount > 0 else "-"
                tax_text = f"{item.tax_rate}%" if item.tax_rate > 0 else "-"

                item_name, description = get_item_details(item)

                items_data.append([
                    Paragraph(item_name, normal_style),
                    Paragraph(description, normal_style),
                    Paragraph(f"{item.quantity} {item.unit_of_measurement.abbreviation}", right_align_style),
                    Paragraph(f"{item.unit_price:,.2f}", right_align_style),
                    Paragraph(discount_text, right_align_style),
                    Paragraph(tax_text, right_align_style),
                    Paragraph(f"{item.total_price:,.2f}", right_align_style)
                ])

            items_table = Table(items_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('BOX', (0, 0), (-1, -1), 0.2, colors.black),
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Totals Section
            totals_data = [
                ["", "", "", "", "Subtotal:", "", f"{purchase_order.total_line_subtotal:,.2f}"],
                ["", "", "", "", "Discount:", "",
                 f"{purchase_order.calculated_discount_amount:,.2f}" if purchase_order.calculated_discount_amount > 0 else "-"],
                ["", "", "", "", "Tax:", "",
                 f"{purchase_order.purchase_order_tax_rate}%" if purchase_order.purchase_order_tax_rate > 0 else "-"],
                ["", "", "", "", "Shipping:", "",
                 f"{purchase_order.shipping_cost:,.2f}" if purchase_order.shipping_cost > 0 else "-"],
                ["", "", "", "", "Handling:", "",
                 f"{purchase_order.handling_cost:,.2f}" if purchase_order.handling_cost > 0 else "-"],
                ["", "", "", "", "Total:", "", f"{purchase_order.total_amount:,.2f}"]
            ]

            totals_table = Table(totals_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                ('LINEABOVE', (0, -1), (-1, -1), 0.2, colors.black),
            ]))

            elements.append(totals_table)
            elements.append(Spacer(1, 12))

            # Terms and Conditions
            if purchase_order.terms_and_conditions:
                terms_style = ParagraphStyle(
                    name='TermsStyle',
                    fontName='Roboto',
                    fontSize=10,
                    leading=12,
                    spaceBefore=6,
                    spaceAfter=6
                )

                terms = [
                    Paragraph("Terms & Conditions:", bold_style),
                    Paragraph(purchase_order.terms_and_conditions.replace("\n", "<br/>"), terms_style)
                ]
                elements.extend(terms)
                elements.append(Spacer(1, 12))

            # Generate and save PDF
            doc.build(elements, onFirstPage=add_pages, onLaterPages=add_pages)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Purchase_Order_{purchase_order.purchase_order_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating purchase order PDF: {e}")
        return f"An error occurred: {e}", 500


@download_route.route('/download_direct_purchase/<int:direct_purchase_id>', methods=['GET'])
@login_required
def download_direct_purchase(direct_purchase_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the direct purchase and related data
            direct_purchase = db_session.query(DirectPurchaseTransaction).filter_by(id=direct_purchase_id).first()
            if not direct_purchase:
                return "Direct Purchase not found", 404

            company = db_session.query(Company).filter_by(id=direct_purchase.app_id).first()
            vendor = db_session.query(Vendor).filter_by(id=direct_purchase.vendor_id).first()
            items = db_session.query(DirectPurchaseItem).filter_by(transaction_id=direct_purchase_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define styles with reduced left margin
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=0,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=0,
                leftIndent=-6,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            # Header Section with Logo and Direct Purchase Details
            elements = []

            # Company name style
            company_name_style = ParagraphStyle(
                name='CompanyNameStyle',
                fontName='Roboto-Bold',
                fontSize=32,
                leading=50
            )

            # Create header table with logo and title
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"", company_name_style),
                    Paragraph("Purchase", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=30,
                        alignment=TA_RIGHT,
                        rightIndent=18,
                        leading=50
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Direct Purchase Details
            details_data = [
                [
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    [
                        Paragraph(f"Purchase #: {direct_purchase.direct_purchase_number}", normal_style3),
                        Paragraph(f"Date: {direct_purchase.payment_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(f"Currency: {direct_purchase.currency.user_currency}", normal_style3),
                        Paragraph(f"Status: {direct_purchase.status.value.capitalize()}", normal_style3)
                    ]
                ]
            ]

            details_table = Table(details_data, colWidths=[350, 150])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))
            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Vendor Details
            vendor_details = [
                Paragraph("Vendor Information:", bold_style2),
                Paragraph(f"{vendor.vendor_name}", normal_style2),
                Paragraph(f"{vendor.address or ''}", normal_style2),
                Paragraph(f"{vendor.country or ''}", normal_style2),
                Paragraph(f"Contact: {vendor.tel_contact or 'N/A'}", normal_style2),
                Spacer(1, 15)
            ]
            elements.extend(vendor_details)

            # Define styles for table content
            wrap_style = ParagraphStyle(
                name='WrapStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
            )

            right_align_style = ParagraphStyle(
                name='RightAlignStyle',
                fontName='Roboto',
                fontSize=10,
                leading=10,
                spaceBefore=0,
                spaceAfter=0,
                wordWrap='CJK',
                alignment=TA_RIGHT
            )

            # Purchase Items Table
            items_data = [["Item", "Description", "Quantity", "Unit Price", "Discount", "Tax(%)", "Total"]]
            for item in items:
                discount_text = f"{item.discount_amount:,.2f}" if item.discount_amount > 0 else "-"
                tax_text = f"{item.tax_rate}%" if item.tax_amount > 0 else "-"

                item_name, description = get_item_details(item)
                items_data.append([
                    Paragraph(item_name, wrap_style),
                    Paragraph(description, wrap_style),
                    Paragraph(f"{item.quantity} {item.unit_of_measurement.abbreviation}", right_align_style),
                    Paragraph(f"{item.unit_price:,.2f}", right_align_style),
                    Paragraph(discount_text, right_align_style),
                    Paragraph(tax_text, right_align_style),
                    Paragraph(f"{item.total_price:,.2f}", right_align_style)
                ])

            items_table = Table(items_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('ALIGN', (6, 0), (6, -1), 'RIGHT'),
                ('WRAP', (0, 0), (-1, -1), True),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('BOX', (0, 0), (-1, -1), 0.2, colors.black),
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Totals Section
            totals_data = [
                [" ", "", "", "", "Subtotal", "", f"{direct_purchase.total_line_subtotal:,.2f}"],
                ["", "", "", "", "Discount", "",
                 f"{direct_purchase.calculated_discount_amount:,.2f}" if direct_purchase.calculated_discount_amount > 0 else "-"],
                ["", "", "", "", "Tax(%)", "",
                 f"{direct_purchase.purchase_tax_rate:,.2f}" if direct_purchase.purchase_tax_rate > 0 else "-"],
                ["", "", "", "", "Shipping Cost", "",
                 f"{direct_purchase.shipping_cost:,.2f}" if direct_purchase.shipping_cost > 0 else "-"],
                ["", "", "", "", "Handling Cost", "",
                 f"{direct_purchase.handling_cost:,.2f}" if direct_purchase.handling_cost > 0 else "-"],
                ["", "", "", "", "Grand Total", "", f"{direct_purchase.total_amount:,.2f}"]
            ]

            totals_table = Table(totals_data, colWidths=[80, 100, 50, 70, 70, 40, 80])
            totals_table.setStyle(TableStyle([
                ('ALIGN', (6, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, -1), (-1, -1), 'Roboto-Bold'),
                ('LINEABOVE', (0, -1), (-1, -1), 0.2, colors.black),
            ]))
            elements.append(totals_table)
            elements.append(Spacer(1, 12))

            # Terms and Conditions
            preformatted_style = ParagraphStyle(
                "Preformatted", parent=normal_style2, spaceAfter=6, leading=14
            )

            terms_conditions = [
                Paragraph("Terms and Conditions:", bold_style2),
                Paragraph(direct_purchase.terms_and_conditions.replace("\n",
                                                                       "<br/>") if direct_purchase.terms_and_conditions else "N/A",
                          preformatted_style)
            ]
            elements.extend(terms_conditions)

            elements.append(Spacer(1, 12))

            # Generate and save PDF
            doc.build(elements, onFirstPage=add_pages, onLaterPages=add_pages)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Purchase_Order_{direct_purchase.direct_purchase_number}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f'An error occurred {e}')
        return f"An error occurred: {e}", 500


@download_route.route('/download_delivery_note/<int:delivery_note_id>', methods=['GET'])
@login_required
def download_delivery_note(delivery_note_id):
    from app import UPLOAD_FOLDER_LOGOS
    try:
        with Session() as db_session:
            # Fetch the delivery note and related data
            delivery_note = db_session.query(DeliveryNote).filter_by(id=delivery_note_id).first()
            if not delivery_note:
                return "Delivery Note not found", 404

            company = db_session.query(Company).filter_by(id=delivery_note.app_id).first()
            customer = db_session.query(Vendor).filter_by(id=delivery_note.customer_id).first()
            items = db_session.query(DeliveryNoteItem).filter_by(delivery_note_id=delivery_note_id).all()

            # Prepare PDF layout
            buffer = BytesIO()
            doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=20,
                                    bottomMargin=72)
            styles = getSampleStyleSheet()
            styles['Normal'].fontName = 'Roboto'
            styles['Heading2'].fontName = 'Roboto-Bold'
            bold_style = styles['Heading2']
            normal_style = styles['Normal']

            # Define custom styles
            bold_style2 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            normal_style2 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=-17,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            normal_style3 = ParagraphStyle(
                name='Normal',
                fontName='Roboto',
                fontSize=10,
                alignment=TA_LEFT,
                leftIndent=70,
                spaceBefore=0,
                spaceAfter=0,
                leading=15
            )

            bold_style3 = ParagraphStyle(
                name='Bold',
                fontName='Roboto-Bold',
                fontSize=12,
                alignment=TA_LEFT,
                leftIndent=70,
                spaceBefore=0,
                spaceAfter=0,
                leading=20
            )

            # Header Section with Logo and Delivery Note Details
            elements = []

            # Company Name and Logo
            header_data = [
                [
                    Image(f"{UPLOAD_FOLDER_LOGOS}/{company.logo}", width=100, height=50) if company.logo else Paragraph(
                        f"{company.name}", bold_style),
                    Paragraph("Delivery Note", ParagraphStyle(
                        name='TitleStyle',
                        fontName='Roboto-Bold',
                        fontSize=28,
                        alignment=TA_RIGHT,
                        leading=50
                    ))
                ]
            ]

            header_table = Table(header_data, colWidths=[300, 200])
            header_table.setStyle(TableStyle([
                ('VALIGN', (1, 0), (1, 0), 'MIDDLE'),
                ('ALIGN', (1, 0), (1, 0), 'RIGHT'),
            ]))

            elements.append(header_table)
            elements.append(Spacer(1, 12))

            # Company and Delivery Note Details
            details_data = [
                [
                    [
                        Paragraph(f"{company.name}", bold_style),
                        Paragraph(f"{company.address or ''}", normal_style),
                        Paragraph(f"Phone: {company.phone or 'N/A'}", normal_style),
                        Paragraph(f"Email: {company.email or 'N/A'}", normal_style)
                    ],
                    [
                        Paragraph(f"Delivery Note #: {str(delivery_note.delivery_number)}", normal_style3),
                        Paragraph(f"Date: {delivery_note.delivery_date.strftime('%Y-%m-%d')}", normal_style3),
                        Paragraph(f"Delivery Reference #: {delivery_note.delivery_reference.delivery_reference_number}", normal_style3)
                        if delivery_note.delivery_reference else None,
                        Paragraph(f"Sales Order #: {delivery_note.delivery_reference.sales_orders.sales_order_number}",
                                  normal_style3)
                        if delivery_note.delivery_reference and delivery_note.delivery_reference.sales_orders else None,
                        Paragraph(f"Quotation #: {delivery_note.delivery_reference.quotations.quotation_number}",
                                  normal_style3)
                        if delivery_note.delivery_reference and delivery_note.delivery_reference.quotations else None,
                        Paragraph(f"Delivery Method: {delivery_note.delivery_method}", normal_style3),
                        Paragraph(f"Status: {delivery_note.status.value}", normal_style3),
                    ]
                ]
            ]

            # Remove None values from the list to avoid empty paragraphs
            details_data[0][1] = [p for p in details_data[0][1] if p is not None]

            details_table = Table(details_data, colWidths=[250, 250])
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
            ]))

            elements.append(details_table)
            elements.append(Spacer(1, 12))

            # Customer Details
            customer_details = [
                Paragraph("Customer Information:", bold_style),
                Paragraph(f"{customer.vendor_name}", normal_style),
                Paragraph(f"{customer.address or ''}", normal_style),
                Paragraph(f"{customer.country or ''}", normal_style),
                Paragraph(f"Contact: {customer.tel_contact or 'N/A'}", normal_style),
                Spacer(1, 15)  # This adds spacing below the customer details section
            ]

            # Shipping Details
            shipping_details = [
                Paragraph("Shipping To:", bold_style3),
                Paragraph(f"{str(delivery_note.shipping_address)}", normal_style3),  # Convert to string
                Spacer(1, 15)  # This adds spacing below the shipping details section
            ]

            # Place customer and shipping details side by side in a table
            details_data = [
                [
                    # Customer Details (1st Column)
                    customer_details,
                    # Shipping Details (2nd Column)
                    shipping_details
                ]
            ]

            # Define column widths for the side-by-side layout (adjust as needed)
            details_table = Table(details_data, colWidths=[250, 250])  # Adjust widths to your preference
            details_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Vertically align top
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),  # Text color black
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),  # Use Roboto font
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),  # Padding below
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Align text in the first column (Customer Details)
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),  # Align text in the second column (Shipping Details)
            ]))

            # Append the table to elements list
            elements.append(details_table)
            elements.append(Spacer(1, 12))  # Adds some space after the table for separation

            # Delivery Items Table
            items_data = [["Item Name", "Description", "Quantity", "Unit"]]
            for item in items:
                item_name, description = get_item_details(item)
                items_data.append([
                    Paragraph(item_name, normal_style),
                    Paragraph(description, normal_style),
                    Paragraph(f"{item.quantity_delivered}", normal_style),
                    Paragraph(item.unit_of_measurement.abbreviation, normal_style)
                ])

            items_table = Table(items_data, colWidths=[150, 200, 90, 50])
            items_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#E0E0E0")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTNAME', (1, 1), (-1, -1), 'Roboto'),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('LINEBELOW', (0, 0), (-1, 0), 1, colors.black),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('BOX', (0, 0), (-1, -1), 0.1, colors.black),
                ('INNERGRID', (0, 0), (-1, -1), 0.1, colors.black),
            ]))

            elements.append(items_table)
            elements.append(Spacer(1, 12))

            # Additional Notes
            additional_notes = [
                Paragraph("Additional Notes:", bold_style2),
                Paragraph(f"{str(delivery_note.additional_notes) or 'N/A'}", normal_style2),  # Convert to string
                Spacer(1, 25)
            ]
            elements.extend(additional_notes)

            # Delivered by and Received by
            delivered_received = [
                [
                    # Left column (Delivered by)
                    Paragraph("Delivered by:", bold_style),  # Label
                    Paragraph("Received by:", bold_style)  # Label
                ],
                [
                    # Left column (Delivered by - Name)
                    Paragraph(f"Name: {str(delivery_note.delivered_by_name) or ''}", normal_style),  # Name
                    # Right column (Received by - Name)
                    Paragraph(f"Name: {str(delivery_note.received_by_name) or ''}", normal_style)  # Name
                ],
                [
                    # Left column (Delivered by - Time)
                    Paragraph(
                        f"Time: {delivery_note.delivered_by_time.strftime('%H:%M') if delivery_note.delivered_by_time else ''}",
                        normal_style),  # Time
                    # Right column (Received by - Time)
                    Paragraph(
                        f"Time: {delivery_note.received_by_time.strftime('%H:%M') if delivery_note.received_by_time else ''}",
                        normal_style)  # Time
                ]
            ]

            # Create the table with two columns
            delivered_received_table = Table(delivered_received, colWidths=[250, 250])

            # Set the table style
            delivered_received_table.setStyle(TableStyle([
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),  # Vertically align top
                ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),  # Text color black
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto'),  # Use Roboto font
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),  # Padding below
                ('ALIGN', (0, 0), (0, -1), 'LEFT'),  # Align text in the first column (Delivered by)
                ('ALIGN', (1, 0), (1, -1), 'LEFT'),  # Align text in the second column (Received by)
                ('TOPPADDING', (0, 0), (-1, -1), 6),  # Padding above
            ]))

            # Add the table to your elements
            elements.append(delivered_received_table)
            elements.append(Spacer(1, 12))  # Adds some space after the table for separation

            # Generate and save PDF
            doc.build(elements)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Delivery_Note_{str(delivery_note.delivery_number)}_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                # Convert to string
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating delivery note PDF: {e}")
        return f"An error occurred: {e}", 500


@download_route.route('/download_payable_accounts_report', methods=['POST'])
@login_required
def download_payable_accounts_report():
    try:
        with Session() as db_session:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            if not company:
                return jsonify({"error": "Company not found"}), 404

            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_code = base_currency_info["base_currency"]

            filters = request.json or {}

            vendor_id = filters.get('vendor_id')
            account_id = filters.get('account_id')
            start_date = filters.get('start_date')
            end_date = filters.get('end_date')
            currency_id = filters.get('currency')

            # Convert string dates to date objects
            if start_date:
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

            # Determine report currency label
            if currency_id:
                currency_obj = db_session.query(Currency).filter_by(id=currency_id).first()
                report_currency_code = currency_obj.user_currency if currency_obj else base_currency_code
                currency_label = f"Currency: {report_currency_code}"
            else:
                report_currency_code = base_currency_code
                currency_label = f"Currency: Base Currency ({base_currency_code})"

            # Fetch payable accounts (filtered if applicable)
            payable_query = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_payable == True
            )
            if account_id:
                payable_query = payable_query.filter(ChartOfAccounts.id == account_id)
            payable_accounts = payable_query.all()

            # Prepare data for the report
            payable_report = []
            total_company_payable = Decimal('0')

            for account in payable_accounts:
                transaction_query = db_session.query(Transaction).filter(
                    Transaction.subcategory_id == account.id,
                    Transaction.app_id == app_id
                )

                if vendor_id:
                    transaction_query = transaction_query.filter(Transaction.vendor_id == vendor_id)
                if start_date:
                    transaction_query = transaction_query.filter(Transaction.date >= start_date)
                if end_date:
                    transaction_query = transaction_query.filter(Transaction.date <= end_date)
                if currency_id:
                    transaction_query = transaction_query.filter(Transaction.currency == currency_id)

                transactions = transaction_query.all()

                vendor_balances = {}

                for transaction in transactions:
                    # Calculate amounts in base currency for balance or filtered currency if specified
                    balance_amount = Decimal(str(transaction.amount))

                    if not currency_id:
                        # Convert to base currency for totals if no filter
                        if transaction.currency != base_currency_id:
                            balance_amount = get_converted_cost(
                                db_session,
                                balance_amount,
                                transaction.currency,
                                base_currency_id,
                                app_id,
                                end_date.strftime('%Y-%m-%d') if end_date else datetime.date.today().strftime(
                                    '%Y-%m-%d')
                            )
                    # else if currency filter is applied, show amounts in that currency directly

                    balance_amount = balance_amount if transaction.dr_cr == 'C' else -balance_amount

                    if transaction.vendor_id not in vendor_balances:
                        vendor_balances[transaction.vendor_id] = {
                            'vendor_name': transaction.vendor.vendor_name if transaction.vendor else 'Unknown',
                            'balance': Decimal('0')
                        }
                    vendor_balances[transaction.vendor_id]['balance'] += balance_amount

                account_total = sum(v['balance'] for v in vendor_balances.values()) if vendor_balances else Decimal('0')
                total_company_payable += account_total

                if transactions or not (vendor_id or account_id or start_date or end_date or currency_id):
                    payable_report.append({
                        'account_name': account.sub_category,
                        'account_code': account.sub_category_id,
                        'total_balance': account_total,
                        'vendors': sorted(vendor_balances.values(), key=lambda x: x['vendor_name'])
                    })

            # Sort accounts by total_balance descending
            payable_report.sort(key=lambda x: x['total_balance'], reverse=True)

            # Prepare PDF document
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=36,
                leftMargin=36,
                topMargin=36,
                bottomMargin=36
            )

            pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
            pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))

            styles = getSampleStyleSheet()

            styles.add(ParagraphStyle(
                name='Heading',
                fontName='Roboto-Bold',
                fontSize=12,
                spaceAfter=12
            ))
            styles.add(ParagraphStyle(
                name='NormalBold',
                fontName='Roboto-Bold',
                fontSize=10,
            ))

            styles.add(ParagraphStyle(
                name='Small',
                fontName='Roboto',
                fontSize=8,
                textColor=colors.grey,
            ))

            normal_style = styles['Normal']

            # Wrap style for table cells and headers
            wrap_style = ParagraphStyle(
                name='Wrap',
                fontName='Roboto',
                fontSize=10,
                leading=12,
                wordWrap='CJK'  # enables wrapping
            )

            elements = []

            # Header
            elements.append(Paragraph(company.name, styles['Title']))
            elements.append(Paragraph("Payable Accounts Report", styles['Heading']))

            # Reporting period display
            period_text = "All Time"
            if start_date and end_date:
                period_text = f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            elif start_date:
                period_text = f"From: {start_date.strftime('%Y-%m-%d')}"
            elif end_date:
                period_text = f"Up to: {end_date.strftime('%Y-%m-%d')}"
            elements.append(Paragraph(period_text, normal_style))

            # Currency info
            elements.append(Paragraph(currency_label, normal_style))
            elements.append(Spacer(1, 12))

            # Summary table header
            summary_data = [
                [
                    Paragraph('Account', wrap_style),
                    Paragraph('Code', wrap_style),
                    Paragraph(f'Total Balance ({report_currency_code})', wrap_style)
                ]
            ]

            # Add summary data rows
            for acc in payable_report:
                summary_data.append([
                    Paragraph(acc['account_name'], wrap_style),
                    Paragraph(str(acc['account_code']), wrap_style),
                    Paragraph(f"{acc['total_balance']:,.2f}", wrap_style)
                ])

            summary_table = Table(summary_data, colWidths=[300, 100, 100], hAlign='LEFT')
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('TOPPADDING', (0, 0), (-1, 0), 6),
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 24))

            # Vendor details for each account
            for acc in payable_report:
                account_header = f"{acc['account_name']} ({acc['account_code']})"
                elements.append(Paragraph(account_header, styles['Heading']))

                # Vendor balances table header
                vendor_data = [
                    [
                        Paragraph('Payee', wrap_style),
                        Paragraph(f'Balance ({report_currency_code})', wrap_style)
                    ]
                ]
                for v in acc['vendors']:
                    vendor_data.append([
                        Paragraph(v['vendor_name'], wrap_style),
                        Paragraph(f"{v['balance']:,.2f}", wrap_style)
                    ])

                vendor_table = Table(vendor_data, colWidths=[350, 150], hAlign='LEFT')
                vendor_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
                    ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                    ('TOPPADDING', (0, 0), (-1, 0), 6),
                ]))
                elements.append(vendor_table)
                elements.append(Spacer(1, 12))

            # Grand total section
            elements.append(Spacer(1, 24))
            elements.append(Paragraph("Company Total Outstanding Payables:", styles['NormalBold']))
            elements.append(Paragraph(f"{total_company_payable:,.2f} {report_currency_code}", styles['NormalBold']))

            # Build PDF and return
            doc.build(elements, onFirstPage=add_pages, onLaterPages=add_pages)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Payable_Accounts_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )
    except Exception as e:
        logger.error(f"Error generating payable accounts PDF report: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to generate report"}), 500


@download_route.route('/download_receivable_accounts_report', methods=['POST'])
@login_required
def download_receivable_accounts_report():
    try:
        with Session() as db_session:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            if not company:
                return jsonify({"error": "Company not found"}), 404

            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            base_currency_id = base_currency_info["base_currency_id"]
            base_currency_code = base_currency_info["base_currency"]

            filters = request.json or {}

            customer_id = filters.get('customer_id')
            account_id = filters.get('account_id')
            start_date = filters.get('start_date')
            end_date = filters.get('end_date')
            currency_id = filters.get('currency')

            # Convert string dates to date objects
            if start_date:
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

            # Determine report currency label
            if currency_id:
                currency_obj = db_session.query(Currency).filter_by(id=currency_id).first()
                report_currency_code = currency_obj.user_currency if currency_obj else base_currency_code
                currency_label = f"Currency: {report_currency_code}"
            else:
                report_currency_code = base_currency_code
                currency_label = f"Currency: Base Currency ({base_currency_code})"

            # Fetch receivable accounts (filtered if applicable)
            receivable_query = db_session.query(ChartOfAccounts).filter(
                ChartOfAccounts.app_id == app_id,
                ChartOfAccounts.is_receivable == True
            )
            if account_id:
                receivable_query = receivable_query.filter(ChartOfAccounts.id == account_id)
            receivable_accounts = receivable_query.all()

            # Prepare data for the report
            receivable_report = []
            total_company_receivable = Decimal('0')

            for account in receivable_accounts:
                transaction_query = db_session.query(Transaction).filter(
                    Transaction.subcategory_id == account.id,
                    Transaction.app_id == app_id
                )

                if customer_id:
                    transaction_query = transaction_query.filter(Transaction.vendor_id == customer_id)
                if start_date:
                    transaction_query = transaction_query.filter(Transaction.date >= start_date)
                if end_date:
                    transaction_query = transaction_query.filter(Transaction.date <= end_date)
                if currency_id:
                    transaction_query = transaction_query.filter(Transaction.currency == currency_id)

                transactions = transaction_query.all()

                customer_balances = {}

                for transaction in transactions:
                    # Calculate amounts in base currency for balance or filtered currency if specified
                    balance_amount = Decimal(str(transaction.amount))

                    if not currency_id:
                        # Convert to base currency for totals if no filter
                        if transaction.currency != base_currency_id:
                            balance_amount = get_converted_cost(
                                db_session,
                                balance_amount,
                                transaction.currency,
                                base_currency_id,
                                app_id,
                                end_date.strftime('%Y-%m-%d') if end_date else datetime.date.today().strftime(
                                    '%Y-%m-%d')
                            )
                    # else if currency filter is applied, show amounts in that currency directly

                    # Note: For receivables, debits increase the balance (opposite of payables)
                    balance_amount = balance_amount if transaction.dr_cr == 'D' else -balance_amount

                    if transaction.vendor_id not in customer_balances:
                        customer_balances[transaction.vendor_id] = {
                            'customer_name': transaction.vendor.vendor_name if transaction.vendor else 'Unknown',
                            'balance': Decimal('0')
                        }
                    customer_balances[transaction.vendor_id]['balance'] += balance_amount

                account_total = sum(v['balance'] for v in customer_balances.values()) if customer_balances else Decimal(
                    '0')
                total_company_receivable += account_total

                if transactions or not (customer_id or account_id or start_date or end_date or currency_id):
                    receivable_report.append({
                        'account_name': account.sub_category,
                        'account_code': account.sub_category_id,
                        'total_balance': account_total,
                        'customers': sorted(customer_balances.values(), key=lambda x: x['customer_name'])
                    })

            # Sort accounts by total_balance descending
            receivable_report.sort(key=lambda x: x['total_balance'], reverse=True)

            # Prepare PDF document
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=36,
                leftMargin=36,
                topMargin=36,
                bottomMargin=36
            )

            pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
            pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))

            styles = getSampleStyleSheet()

            styles.add(ParagraphStyle(
                name='Heading',
                fontName='Roboto-Bold',
                fontSize=12,
                spaceAfter=12
            ))
            styles.add(ParagraphStyle(
                name='NormalBold',
                fontName='Roboto-Bold',
                fontSize=10,
            ))

            styles.add(ParagraphStyle(
                name='Small',
                fontName='Roboto',
                fontSize=8,
                textColor=colors.grey,
            ))

            normal_style = styles['Normal']

            # Wrap style for table cells and headers
            wrap_style = ParagraphStyle(
                name='Wrap',
                fontName='Roboto',
                fontSize=10,
                leading=12,
                wordWrap='CJK'  # enables wrapping
            )

            elements = []

            # Header
            elements.append(Paragraph(company.name, styles['Title']))
            elements.append(Paragraph("Receivable Accounts Report", styles['Heading']))

            # Reporting period display
            period_text = "All Time"
            if start_date and end_date:
                period_text = f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            elif start_date:
                period_text = f"From: {start_date.strftime('%Y-%m-%d')}"
            elif end_date:
                period_text = f"Up to: {end_date.strftime('%Y-%m-%d')}"
            elements.append(Paragraph(period_text, normal_style))

            # Currency info
            elements.append(Paragraph(currency_label, normal_style))
            elements.append(Spacer(1, 12))

            # Summary table header
            summary_data = [
                [
                    Paragraph('Account', wrap_style),
                    Paragraph('Code', wrap_style),
                    Paragraph(f'Total Balance ({report_currency_code})', wrap_style)
                ]
            ]

            # Add summary data rows
            for acc in receivable_report:
                summary_data.append([
                    Paragraph(acc['account_name'], wrap_style),
                    Paragraph(str(acc['account_code']), wrap_style),
                    Paragraph(f"{acc['total_balance']:,.2f}", wrap_style)
                ])

            summary_table = Table(summary_data, colWidths=[300, 100, 100], hAlign='LEFT')
            summary_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('ALIGN', (2, 1), (-1, -1), 'RIGHT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                ('TOPPADDING', (0, 0), (-1, 0), 6),
            ]))
            elements.append(summary_table)
            elements.append(Spacer(1, 24))

            # Customer details for each account
            for acc in receivable_report:
                account_header = f"{acc['account_name']} ({acc['account_code']})"
                elements.append(Paragraph(account_header, styles['Heading']))

                # Customer balances table header
                customer_data = [
                    [
                        Paragraph('Customer', wrap_style),
                        Paragraph(f'Balance ({report_currency_code})', wrap_style)
                    ]
                ]
                for c in acc['customers']:
                    customer_data.append([
                        Paragraph(c['customer_name'], wrap_style),
                        Paragraph(f"{c['balance']:,.2f}", wrap_style)
                    ])

                customer_table = Table(customer_data, colWidths=[350, 150], hAlign='LEFT')
                customer_table.setStyle(TableStyle([
                    ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                    ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                    ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                    ('FONTSIZE', (0, 0), (-1, 0), 10),
                    ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
                    ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                    ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
                    ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
                    ('BOTTOMPADDING', (0, 0), (-1, 0), 6),
                    ('TOPPADDING', (0, 0), (-1, 0), 6),
                ]))
                elements.append(customer_table)
                elements.append(Spacer(1, 12))

            # Grand total section
            elements.append(Spacer(1, 24))
            elements.append(Paragraph("Company Total Outstanding Receivables:", styles['NormalBold']))
            elements.append(Paragraph(f"{total_company_receivable:,.2f} {report_currency_code}", styles['NormalBold']))

            # Build PDF and return
            doc.build(elements, onFirstPage=add_pages, onLaterPages=add_pages)
            buffer.seek(0)

            return send_file(
                buffer,
                as_attachment=True,
                download_name=f"Receivable_Accounts_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf",
                mimetype='application/pdf'
            )
    except Exception as e:
        logger.error(f"Error generating receivable accounts PDF report: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to generate report"}), 500


@download_route.route('/download_account_transactions', methods=['POST'])
@login_required
def download_account_transactions():
    try:
        with Session() as db_session:
            app_id = current_user.app_id
            company = db_session.query(Company).filter_by(id=app_id).first()
            if not company:
                return jsonify({"error": "Company not found"}), 404

            base_currency_info = get_base_currency(db_session, app_id)
            if not base_currency_info:
                return jsonify({"error": "Base currency not defined"}), 400

            filters = request.json or {}

            account_id = filters.get('account_id')
            start_date = filters.get('start_date')
            end_date = filters.get('end_date')
            currency_id = filters.get('currency_id')

            # Convert string dates to date objects
            if start_date:
                start_date = datetime.datetime.strptime(start_date, '%Y-%m-%d').date()
            if end_date:
                end_date = datetime.datetime.strptime(end_date, '%Y-%m-%d').date()

            # Get account details
            account = db_session.query(ChartOfAccounts).filter_by(id=account_id, app_id=app_id).first()
            if not account:
                return jsonify({"error": "Account not found"}), 404

            # Determine report currency
            if currency_id:
                currency_obj = db_session.query(Currency).filter_by(id=currency_id).first()
                report_currency_code = currency_obj.user_currency if currency_obj else base_currency_info[
                    "base_currency"]
            else:
                report_currency_code = base_currency_info["base_currency"]
                currency_id = base_currency_info["base_currency_id"]

            # Helper function to convert amounts to base currency
            def convert_to_base(journal, amount):
                if journal.currency_id != base_currency_info["base_currency_id"]:
                    exchange_rate = journal.exchange_rate.rate if journal.exchange_rate else None
                    if exchange_rate:
                        return float(Decimal(str(amount)) * Decimal(str(exchange_rate)))
                return float(amount)

            # Calculate starting balance using JournalEntry
            starting_balance_result = Decimal('0')
            if start_date:
                starting_balance_query = db_session.query(
                    func.sum(
                        case(
                            (and_(JournalEntry.subcategory_id == account_id, JournalEntry.dr_cr == 'D'),
                             JournalEntry.amount),
                            (and_(JournalEntry.subcategory_id == account_id, JournalEntry.dr_cr == 'C'),
                             -JournalEntry.amount),
                            else_=0
                        )
                    )
                ).join(Journal, JournalEntry.journal_id == Journal.id) \
                    .filter(
                    JournalEntry.subcategory_id == account_id,
                    JournalEntry.app_id == app_id,
                    Journal.status == 'Posted',  # Only posted journals
                    Journal.date < start_date
                )

                # Apply currency filter if specific currency is selected
                if currency_id and currency_id != base_currency_info["base_currency_id"]:
                    starting_balance_query = starting_balance_query.filter(Journal.currency_id == currency_id)

                starting_balance_result = starting_balance_query.scalar() or Decimal('0')

            # Adjust starting balance based on account normal balance
            if account.normal_balance == 'Credit':
                starting_balance = -Decimal(str(starting_balance_result))
            else:
                starting_balance = Decimal(str(starting_balance_result))

            # Get transactions using JournalEntry
            transaction_query = db_session.query(JournalEntry, Journal) \
                .join(Journal, JournalEntry.journal_id == Journal.id) \
                .filter(
                JournalEntry.subcategory_id == account_id,
                JournalEntry.app_id == app_id,
                Journal.status == 'Posted'  # Only posted journals
            )

            # Apply currency filter if specific currency is selected
            if currency_id and currency_id != base_currency_info["base_currency_id"]:
                transaction_query = transaction_query.filter(Journal.currency_id == currency_id)

            if start_date:
                transaction_query = transaction_query.filter(Journal.date >= start_date)
            if end_date:
                transaction_query = transaction_query.filter(Journal.date <= end_date)

            journal_entries = transaction_query.order_by(Journal.date.asc(), JournalEntry.line_number.asc()).all()

            # Prepare transaction data with running balance
            running_balance = starting_balance
            transaction_data = []

            for entry, journal in journal_entries:
                # Convert amount if using base currency but transaction is in different currency
                if not currency_id or currency_id == base_currency_info["base_currency_id"]:
                    amount = convert_to_base(journal, entry.amount)
                else:
                    amount = float(entry.amount)

                amount_decimal = Decimal(str(amount))

                # Calculate amount effect based on account normal balance
                if account.normal_balance == 'Debit':
                    # Debit increases, Credit decreases
                    amount_effect = amount_decimal if entry.dr_cr == 'D' else -amount_decimal
                else:
                    # Credit increases, Debit decreases
                    amount_effect = -amount_decimal if entry.dr_cr == 'D' else amount_decimal

                running_balance += amount_effect

                # Get project name if available
                project_name = journal.project.name if journal.project else '-'

                transaction_data.append({
                    'date': journal.date.strftime('%Y-%m-%d'),
                    'journal_number': journal.journal_number,
                    'description': entry.description or journal.narration or '-',
                    'project': project_name,
                    'type': entry.dr_cr,
                    'amount': float(amount),
                    'balance': float(running_balance),
                    'reconciled': entry.reconciled
                })

            # Prepare PDF document
            buffer = BytesIO()
            doc = SimpleDocTemplate(
                buffer,
                pagesize=letter,
                rightMargin=36,
                leftMargin=36,
                topMargin=36,
                bottomMargin=36
            )

            pdfmetrics.registerFont(TTFont('Roboto', 'Roboto-Regular.ttf'))
            pdfmetrics.registerFont(TTFont('Roboto-Bold', 'Roboto-Bold.ttf'))

            styles = getSampleStyleSheet()
            styles.add(ParagraphStyle(
                name='Heading',
                fontName='Roboto-Bold',
                fontSize=12,
                spaceAfter=12
            ))
            styles.add(ParagraphStyle(
                name='NormalBold',
                fontName='Roboto-Bold',
                fontSize=10,
            ))
            styles.add(ParagraphStyle(
                name='Small',
                fontName='Roboto',
                fontSize=8,
                textColor=colors.grey,
            ))

            wrap_style = ParagraphStyle(
                name='Wrap',
                fontName='Roboto',
                fontSize=9,
                leading=12,
                wordWrap='CJK'
            )

            styles.add(ParagraphStyle(name='AmountPositive', fontName='Roboto', fontSize=9, textColor=colors.green,
                                      alignment=TA_RIGHT))
            styles.add(ParagraphStyle(name='AmountNegative', fontName='Roboto', fontSize=9, textColor=colors.red,
                                      alignment=TA_RIGHT))
            styles.add(ParagraphStyle(name='BadgeDebit', fontName='Roboto-Bold', fontSize=9, textColor=colors.white,
                                      backColor=colors.green, alignment=TA_CENTER))
            styles.add(ParagraphStyle(name='BadgeCredit', fontName='Roboto-Bold', fontSize=9, textColor=colors.white,
                                      backColor=colors.red, alignment=TA_CENTER))

            elements = []

            # Header
            elements.append(Paragraph(company.name, styles['Title']))
            elements.append(Paragraph(f"Account Transactions - {account.sub_category}", styles['Heading']))

            # Account info
            elements.append(Paragraph(f"Account Code: {account.sub_category_id}", styles['Normal']))
            elements.append(Paragraph(f"Normal Balance: {account.normal_balance}", styles['Normal']))
            elements.append(Paragraph(f"Currency: {report_currency_code}", styles['Normal']))

            # Reporting period
            period_text = "All Transactions"
            if start_date and end_date:
                period_text = f"Period: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}"
            elif start_date:
                period_text = f"From: {start_date.strftime('%Y-%m-%d')}"
            elif end_date:
                period_text = f"Up to: {end_date.strftime('%Y-%m-%d')}"
            elements.append(Paragraph(period_text, styles['Normal']))
            elements.append(Spacer(1, 12))

            # Balances summary
            balance_data = [
                [
                    Paragraph('Starting Balance', wrap_style),
                    Paragraph(f"{float(starting_balance):,.2f} {report_currency_code}", wrap_style)
                ],
                [
                    Paragraph('Ending Balance', wrap_style),
                    Paragraph(f"{float(running_balance):,.2f} {report_currency_code}", wrap_style)
                ]
            ]
            balance_table = Table(balance_data, colWidths=[200, 100])
            balance_table.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, -1), 'Roboto-Bold'),
                ('ALIGN', (1, 0), (1, -1), 'RIGHT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 6),
            ]))
            elements.append(balance_table)
            elements.append(Spacer(1, 24))

            # Transactions table
            table_data = [
                [
                    Paragraph('Date', wrap_style),
                    Paragraph('Journal #', wrap_style),
                    Paragraph('Description', wrap_style),
                    Paragraph('Project', wrap_style),
                    Paragraph('Type', wrap_style),
                    Paragraph('Amount', wrap_style),
                    Paragraph('Balance', wrap_style)
                ]
            ]

            # Add starting balance row
            table_data.append([
                '',
                '',
                Paragraph('Starting Balance', wrap_style),
                '',
                '',
                '',
                Paragraph(f"{float(starting_balance):,.2f}",
                          styles['AmountNegative' if starting_balance < 0 else 'AmountPositive'])
            ])

            # Add transactions
            for trans in transaction_data:
                # Determine amount style
                if (trans['type'] == 'D' and account.normal_balance == 'Debit') or \
                        (trans['type'] == 'C' and account.normal_balance == 'Credit'):
                    amount_style = 'AmountPositive'
                else:
                    amount_style = 'AmountNegative'

                # Determine balance style
                balance_style = 'AmountNegative' if trans['balance'] < 0 else 'AmountPositive'

                table_data.append([
                    Paragraph(trans['date'], wrap_style),
                    Paragraph(trans['journal_number'], wrap_style),
                    Paragraph(trans['description'], wrap_style),
                    Paragraph(trans['project'], wrap_style),
                    Paragraph(trans['type'], wrap_style),
                    Paragraph(f"{trans['amount']:,.2f}", styles[amount_style]),
                    Paragraph(f"{trans['balance']:,.2f}", styles[balance_style])
                ])

            transactions_table = Table(table_data, colWidths=[60, 60, 150, 80, 40, 60, 60], repeatRows=1)
            transactions_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#d3d3d3")),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
                ('FONTNAME', (0, 0), (-1, 0), 'Roboto-Bold'),
                ('ALIGN', (5, 1), (6, -1), 'RIGHT'),
                ('VALIGN', (0, 0), (-1, -1), 'TOP'),
                ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
                ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
                ('FONTSIZE', (0, 0), (-1, -1), 8),
            ]))
            elements.append(transactions_table)

            # Footer note if company has multiple currencies
            if company.has_multiple_currencies:
                elements.append(Spacer(1, 12))
                elements.append(
                    Paragraph("Note: Running balances and amounts are shown in the original transaction currency only.",
                              styles['Small']))

            # Build PDF and return
            doc.build(elements)
            buffer.seek(0)

            # Generate filename
            filename = f"Account_Transactions_{account.sub_category_id}"
            if start_date:
                filename += f"_from_{start_date.strftime('%Y%m%d')}"
            if end_date:
                filename += f"_to_{end_date.strftime('%Y%m%d')}"
            filename += ".pdf"

            return send_file(
                buffer,
                as_attachment=True,
                download_name=filename,
                mimetype='application/pdf'
            )

    except Exception as e:
        logger.error(f"Error generating account transactions PDF report: {str(e)}", exc_info=True)
        return jsonify({"error": "Failed to generate report"}), 500

@download_route.route('/download_stock_movement_history', methods=['POST'])
@login_required
def download_stock_movement_history():
    """
    Download stock movement history as PDF with filtering
    """
    db_session = Session()
    app_id = current_user.app_id
    user_id = current_user.id

    try:
        filters = request.get_json() or {}

        # Extract filters
        item_id = filters.get('item_id')
        reference = filters.get('reference', '')
        location_id = filters.get('location_id')
        project_id = filters.get('project_id')
        movement_type = filters.get('movement_type', '')
        source_type = filters.get('source_type', '')
        start_date = filters.get('start_date', '')
        end_date = filters.get('end_date', '')

        # Get user's accessible locations
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        if not user_location_ids:
            return generate_empty_pdf_response(start_date, end_date, message="You don't have access to any locations.")

        # Build query
        query = db_session.query(InventoryTransactionDetail).join(
            InventoryEntryLineItem,
            InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
        ).join(
            InventoryEntry,
            InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink,
            InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
        ).filter(
            InventoryTransactionDetail.app_id == app_id,
            InventoryTransactionDetail.location_id.in_(user_location_ids)
        )

        # Apply filters
        if item_id:
            query = query.filter(InventoryTransactionDetail.item_id == item_id)
        if location_id and location_id in user_location_ids:
            query = query.filter(InventoryTransactionDetail.location_id == location_id)
        if movement_type:
            query = query.filter(InventoryTransactionDetail.movement_type == movement_type)
        if reference:
            query = query.filter(InventoryEntry.reference == reference)
        if source_type:
            query = query.filter(InventoryEntry.source_type == source_type)
        if project_id:
            query = query.filter(InventoryEntryLineItem.project_id == project_id)
        if start_date:
            try:
                start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
                query = query.filter(func.date(InventoryTransactionDetail.transaction_date) >= start_date_obj)
            except ValueError:
                pass
        if end_date:
            try:
                end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
                query = query.filter(func.date(InventoryTransactionDetail.transaction_date) <= end_date_obj)
            except ValueError:
                pass

        # Get all transactions
        transaction_details = query.order_by(
            InventoryTransactionDetail.transaction_date.desc(),
            InventoryTransactionDetail.id.desc()
        ).all()

        if not transaction_details:
            return generate_empty_pdf_response(start_date, end_date)

        # Calculate running totals (chronological order first)
        chronological = sorted(transaction_details, key=lambda x: (x.transaction_date, x.id))
        running_totals = {}
        transaction_running_totals = {}

        for trans in chronological:
            key = trans.item_id
            running_totals[key] = running_totals.get(key, 0) + trans.quantity
            transaction_running_totals[trans.id] = running_totals[key]

        # Prepare PDF data
        pdf_data = []
        source_map = {
            'purchase': 'Receipt', 'sale': 'Dispatch', 'transfer': 'Transfer',
            'adjustment': 'Adjustment', 'opening_balance': 'Opening Stock',
            'write_off': 'Write Off', 'missing': 'Missing', 'damaged': 'Damaged',
            'expired': 'Expired', 'manual': 'Manual'
        }

        for trans in transaction_details:
            line_item = trans.inventory_entry_line_item
            entry = line_item.inventory_entry if line_item else None
            variation_link = trans.inventory_item_variation_link
            item = variation_link.inventory_item if variation_link else None

            # Location display
            if trans.movement_type == 'transfer' and entry:
                from_loc = entry.from_inventory_location.location if entry.from_inventory_location else '-'
                to_loc = entry.to_inventory_location.location if entry.to_inventory_location else '-'
                location_display = f"{from_loc} → {to_loc}"
            else:
                location_display = trans.location.location if trans.location else '-'

            # Department
            department = '-'
            if line_item and line_item.project_id:
                project = db_session.query(Project).filter_by(id=line_item.project_id).first()
                department = project.name if project else '-'

            # Party name
            party = '-'
            if entry and entry.source_type in ['purchase', 'sale'] and entry.source_id:
                vendor = db_session.query(Vendor).filter_by(id=entry.source_id).first()
                party = vendor.vendor_name if vendor else '-'

            # Handled by
            handled_by = '-'
            if entry and entry.handled_by_employee:
                handled_by = f"{entry.handled_by_employee.first_name} {entry.handled_by_employee.last_name}".strip()

            pdf_data.append({
                'date': trans.transaction_date.strftime('%Y-%m-%d'),
                'source': source_map.get(entry.source_type if entry else 'manual', 'Manual'),
                'movement_type': trans.movement_type.replace('_', ' ').title() if trans.movement_type else '-',
                'reference': entry.reference if entry else '-',
                'item_name': item.item_name if item else '-',
                'attribute': variation_link.inventory_item_attributes.attribute_name if variation_link and variation_link.inventory_item_attributes else '-',
                'variation': variation_link.inventory_item_variation.variation_name if variation_link and variation_link.inventory_item_variation else '-',
                'uom': item.unit_of_measurement.abbreviation if item and item.unit_of_measurement else '-',
                'quantity': trans.quantity,
                'running_total': transaction_running_totals.get(trans.id, 0),
                'location': location_display,
                'department': department,
                'party': party,
                'handled_by': handled_by
            })

        # Generate PDF
        buffer = generate_movement_history_pdf(pdf_data, filters)
        filename = f"stock_movement_history_{start_date}_to_{end_date}.pdf" if start_date or end_date else "stock_movement_history.pdf"

        return send_file(buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')

    except Exception as e:
        logger.error(f"Error generating stock movement history PDF: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'An error occurred while generating the PDF report'}), 500
    finally:
        db_session.close()


def generate_movement_history_pdf(pdf_data, filters):
    """Generate PDF buffer for movement history"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=landscape(letter), rightMargin=20, leftMargin=20, topMargin=30, bottomMargin=30)
    styles = getSampleStyleSheet()

    # Custom styles
    styles.add(ParagraphStyle(name='Heading', fontName='Helvetica-Bold', fontSize=16, spaceAfter=12, alignment=TA_CENTER))
    styles.add(ParagraphStyle(name='Wrap', fontName='Helvetica', fontSize=8, leading=10, wordWrap='CJK'))
    styles.add(ParagraphStyle(name='AmountPositive', fontName='Helvetica', fontSize=8, textColor=colors.green, alignment=TA_RIGHT))
    styles.add(ParagraphStyle(name='AmountNegative', fontName='Helvetica', fontSize=8, textColor=colors.red, alignment=TA_RIGHT))

    elements = []

    # Header
    elements.append(Paragraph("Stock Movement History Report", styles['Heading']))
    elements.append(Spacer(1, 10))

    # Filter info
    filter_parts = []
    if filters.get('start_date'):
        filter_parts.append(f"From: {filters['start_date']}")
    if filters.get('end_date'):
        filter_parts.append(f"To: {filters['end_date']}")
    if filters.get('project_id'):
        filter_parts.append(f"Department Filter Applied")
    if filter_parts:
        elements.append(Paragraph(" | ".join(filter_parts), styles['Normal']))
        elements.append(Spacer(1, 12))

    # Table
    headers = ['Date', 'Source', 'Movement', 'Reference', 'Item', 'Variation', 'UOM', 'Qty', 'Running', 'Location', 'Dept']
    table_data = [[Paragraph(h, styles['Wrap']) for h in headers]]

    for entry in pdf_data:
        qty_style = 'AmountPositive' if entry['quantity'] >= 0 else 'AmountNegative'
        running_style = 'AmountPositive' if entry['running_total'] >= 0 else 'AmountNegative'

        variation = entry['variation'] if entry['variation'] else entry['attribute'] if entry['attribute'] else '-'

        table_data.append([
            Paragraph(entry['date'], styles['Wrap']),
            Paragraph(entry['source'], styles['Wrap']),
            Paragraph(entry['movement_type'], styles['Wrap']),
            Paragraph(entry['reference'], styles['Wrap']),
            Paragraph(entry['item_name'], styles['Wrap']),
            Paragraph(variation, styles['Wrap']),
            Paragraph(entry['uom'], styles['Wrap']),
            Paragraph(str(entry['quantity']), styles[qty_style]),
            Paragraph(str(entry['running_total']), styles[running_style]),
            Paragraph(entry['location'], styles['Wrap']),
            Paragraph(entry['department'], styles['Wrap']),
        ])

    col_widths = [60, 70, 60, 80, 120, 90, 35, 45, 55, 100, 40]
    table = Table(table_data, colWidths=col_widths, repeatRows=1)
    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#f8f9fa")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('ALIGN', (7, 0), (8, -1), 'RIGHT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTSIZE', (0, 1), (-1, -1), 7),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafafa")]),
        ('PADDING', (0, 0), (-1, -1), 3),
    ]))

    elements.append(table)
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"Total Entries: {len(pdf_data)}", styles['Normal']))
    elements.append(Paragraph(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))

    doc.build(elements)
    buffer.seek(0)
    return buffer


def generate_empty_pdf_response(start_date, end_date, message=None):
    """Generate empty PDF response"""
    buffer = BytesIO()
    doc = SimpleDocTemplate(buffer, pagesize=letter)
    styles = getSampleStyleSheet()
    elements = [
        Paragraph("Stock Movement History Report", styles['Heading1']),
        Spacer(1, 20),
        Paragraph(message or "No stock movement records found for the selected filters", styles['Normal']),
    ]
    if start_date:
        elements.append(Paragraph(f"From: {start_date}", styles['Normal']))
    if end_date:
        elements.append(Paragraph(f"To: {end_date}", styles['Normal']))
    doc.build(elements)
    buffer.seek(0)
    return send_file(buffer, as_attachment=True, download_name="stock_movement_history_empty.pdf", mimetype='application/pdf')

def prepare_pdf_data(db_session, transaction_details, running_totals):
    """Prepare data for PDF generation using InventoryTransactionDetail"""
    pdf_data = []

    for transaction in transaction_details:
        line_item = transaction.inventory_entry_line_item
        entry = line_item.inventory_entry if line_item else None

        # Get location name
        location_name = transaction.location.location if transaction.location else None

        # Get item details
        variation_link = transaction.inventory_item_variation_link
        item = variation_link.inventory_item if variation_link else None
        attribute = variation_link.inventory_item_attributes if variation_link else None
        variation = variation_link.inventory_item_variation if variation_link else None

        # 🆕 NEW: Get UOM information
        uom = None
        if item and item.unit_of_measurement:
            uom = item.unit_of_measurement.abbreviation or item.unit_of_measurement.full_name

        # Get inventory source directly from the entry
        inventory_source = entry.inventory_source if entry else 'Unknown'

        # Get the pre-calculated running totals for this transaction
        running_total = running_totals.get(transaction.id, 0)

        pdf_data.append({
            'transaction_date': transaction.transaction_date.strftime('%Y-%m-%d'),
            'item_name': item.item_name if item else 'Unknown Item',
            'item_code': item.item_code if item else '',

            'attribute': attribute.attribute_name if attribute else '',
            'variation': variation.variation_name if variation else '',
            'inventory_source': inventory_source,
            'source_type': entry.source_type if entry else 'Manual',
            'reference_number': entry.reference if entry else '',
            'uom': uom or '-',  # 🆕 NEW: Add UOM data
            'quantity': transaction.quantity,
            'running_total': running_total,
            'location': location_name,
            'from_location': entry.from_inventory_location.location if entry and entry.from_inventory_location else '',
            'to_location': entry.to_inventory_location.location if entry and entry.to_inventory_location else '',
            'created_at': transaction.created_at.strftime('%Y-%m-%d %H:%M:%S'),
            'created_by': entry.created_user.name if entry and entry.created_user else 'Unknown'
        })

    return pdf_data


def generate_pdf_buffer(pdf_data, filters):
    """Generate PDF buffer with the data"""
    buffer = BytesIO()

    # Document setup - use full landscape width
    doc = SimpleDocTemplate(
        buffer,
        pagesize=landscape(letter),
        rightMargin=20,  # Reduced margins for more width
        leftMargin=20,
        topMargin=30,
        bottomMargin=30
    )

    # Styles setup
    styles = setup_pdf_styles()

    # Build elements
    elements = build_pdf_elements(pdf_data, filters, styles)

    # Build PDF
    doc.build(elements)
    buffer.seek(0)

    return buffer


def setup_pdf_styles():
    """Setup PDF styles with slightly larger fonts for better readability"""
    styles = getSampleStyleSheet()

    # Custom styles
    custom_styles = {
        'Heading': ParagraphStyle(
            name='Heading',
            fontName='Helvetica-Bold',
            fontSize=16,  # Slightly larger
            spaceAfter=12,
            alignment=TA_CENTER
        ),
        'Subheading': ParagraphStyle(
            name='Subheading',
            fontName='Helvetica-Bold',
            fontSize=11,  # Slightly larger
            spaceAfter=6
        ),
        'NormalBold': ParagraphStyle(
            name='NormalBold',
            fontName='Helvetica-Bold',
            fontSize=10,  # Slightly larger
        ),
        'Wrap': ParagraphStyle(
            name='Wrap',
            fontName='Helvetica',
            fontSize=8,  # Slightly larger
            leading=10,
            wordWrap='CJK'
        ),
        'AmountPositive': ParagraphStyle(
            name='AmountPositive',
            fontName='Helvetica',
            fontSize=8,  # Slightly larger
            textColor=colors.green,
            alignment=TA_RIGHT
        ),
        'AmountNegative': ParagraphStyle(
            name='AmountNegative',
            fontName='Helvetica',
            fontSize=8,  # Slightly larger
            textColor=colors.red,
            alignment=TA_RIGHT
        )
    }

    for name, style in custom_styles.items():
        styles.add(style)

    return styles


def build_pdf_elements(pdf_data, filters, styles):
    """Build PDF elements"""
    elements = []

    # Header
    elements.append(Paragraph("Stock Movement History Report", styles['Heading']))
    elements.append(Spacer(1, 10))

    # Filter information
    filter_info = build_filter_info(filters, pdf_data)
    if filter_info:
        elements.append(Paragraph(filter_info, styles['Subheading']))

    elements.append(Spacer(1, 12))

    # Table data
    table_data = build_table_data(pdf_data, styles)

    # Create and style table
    table = create_styled_table(table_data)
    elements.append(table)

    # Summary
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"Total Entries: {len(pdf_data)}", styles['NormalBold']))

    # Timestamp
    elements.append(Paragraph(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                              styles['Normal']))

    return elements


def build_filter_info(filters, pdf_data):
    """Build filter information string"""
    filter_info = []

    if filters.get('start_date'):
        filter_info.append(f"From: {filters['start_date']}")
    if filters.get('end_date'):
        filter_info.append(f"To: {filters['end_date']}")
    if filters.get('item_id') and pdf_data:
        filter_info.append(f"Item: {pdf_data[0]['item_name']}")
    if filters.get('movement_type'):
        filter_info.append(f"Movement: {filters['movement_type'].capitalize()}")
    if filters.get('source_type'):
        filter_info.append(f"Source: {filters['source_type'].capitalize()}")

    return " | ".join(filter_info) if filter_info else "All Records"


def build_table_data(pdf_data, styles):
    """Build table data for PDF with better formatting"""
    # 🆕 UPDATED: Added UOM column
    headers = [
        'Date', 'Item', 'Variation', 'Inventory Source', 'Location', 'Reference', 'UOM',
        'Quantity', 'Running Total'
    ]

    table_data = [
        [Paragraph(header, styles['Wrap']) for header in headers]
    ]

    # Table rows - already in newest-first order from the query
    for entry in pdf_data:
        quantity_style = 'AmountPositive' if entry['quantity'] >= 0 else 'AmountNegative'
        running_total_style = 'AmountPositive' if entry['running_total'] >= 0 else 'AmountNegative'

        # Format item name with code if available
        item_display = entry['item_name']
        if entry['item_code']:
            item_display += f" ({entry['item_code']})"

        # Format variation (use attribute if variation is empty)
        variation_display = entry['variation'] or entry['attribute'] or '-'

        table_data.append([
            Paragraph(entry['transaction_date'], styles['Wrap']),
            Paragraph(item_display, styles['Wrap']),

            Paragraph(variation_display, styles['Wrap']),
            Paragraph(entry['inventory_source'].capitalize(), styles['Wrap']),
            Paragraph(entry['location'] or '-', styles['Wrap']),
            Paragraph(entry['reference_number'] or '-', styles['Wrap']),
            Paragraph(entry['uom'], styles['Wrap']),  # 🆕 NEW: Add UOM data
            Paragraph(str(entry['quantity']), styles[quantity_style]),
            Paragraph(str(entry['running_total']), styles[running_total_style])
        ])

    return table_data


def create_styled_table(table_data):
    """Create and style the table with increased width"""
    # 🆕 UPDATED: Added UOM column width (40px for abbreviation)
    col_widths = [55, 130, 100, 80, 90, 80, 40, 60, 80]  # Added UOM column (40px)

    table = Table(table_data, colWidths=col_widths, repeatRows=1)

    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#f8f9fa")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('ALIGN', (7, 1), (8, -1), 'RIGHT'),  # 🆕 UPDATED: Align quantity (col 7) and running total (col 8) to right
        ('ALIGN', (0, 0), (0, -1), 'CENTER'),  # Center align date column
        ('ALIGN', (2, 0), (2, -1), 'CENTER'),  # 🆕 NEW: Center align UOM column
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTSIZE', (0, 1), (-1, -1), 8),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafafa")]),
        ('PADDING', (0, 0), (-1, -1), 3),
    ]))

    return table


def generate_filename(start_date, end_date):
    """Generate filename for the PDF"""
    filename_parts = ["stock_movement_history"]
    if start_date:
        filename_parts.append(f"from_{start_date}")
    if end_date:
        filename_parts.append(f"to_{end_date}")
    return "_".join(filename_parts) + ".pdf"



@download_route.route('/download_stock_movement_history_excel', methods=['POST'])
@login_required
def download_stock_movement_history_excel():
    """
    Download stock movement history as Excel with filtering
    """
    import pandas as pd
    from io import BytesIO

    db_session = Session()
    app_id = current_user.app_id
    user_id = current_user.id

    try:
        filters = request.get_json() or {}

        # Extract filters
        item_id = filters.get('item_id')
        reference = filters.get('reference', '')
        location_id = filters.get('location_id')
        project_id = filters.get('project_id')
        movement_type = filters.get('movement_type', '')
        source_type = filters.get('source_type', '')
        start_date = filters.get('start_date', '')
        end_date = filters.get('end_date', '')

        # Get user's accessible locations
        user_locations = get_user_accessible_locations(user_id, app_id)
        user_location_ids = [loc.id for loc in user_locations] if user_locations else []

        if not user_location_ids:
            return generate_empty_excel_response(start_date, end_date, message="You don't have access to any locations.")

        # Build query
        query = db_session.query(InventoryTransactionDetail).join(
            InventoryEntryLineItem,
            InventoryTransactionDetail.inventory_entry_line_item_id == InventoryEntryLineItem.id
        ).join(
            InventoryEntry,
            InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink,
            InventoryTransactionDetail.item_id == InventoryItemVariationLink.id
        ).filter(
            InventoryTransactionDetail.app_id == app_id,
            InventoryTransactionDetail.location_id.in_(user_location_ids)
        )

        # Apply filters
        if item_id:
            query = query.filter(InventoryTransactionDetail.item_id == item_id)
        if location_id and location_id in user_location_ids:
            query = query.filter(InventoryTransactionDetail.location_id == location_id)
        if movement_type:
            query = query.filter(InventoryTransactionDetail.movement_type == movement_type)
        if reference:
            query = query.filter(InventoryEntry.reference == reference)
        if source_type:
            query = query.filter(InventoryEntry.source_type == source_type)
        if project_id:
            query = query.filter(InventoryEntryLineItem.project_id == project_id)
        if start_date:
            try:
                start_date_obj = datetime.datetime.strptime(start_date, "%Y-%m-%d").date()
                query = query.filter(func.date(InventoryTransactionDetail.transaction_date) >= start_date_obj)
            except ValueError:
                pass
        if end_date:
            try:
                end_date_obj = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
                query = query.filter(func.date(InventoryTransactionDetail.transaction_date) <= end_date_obj)
            except ValueError:
                pass

        # Get all transactions
        transaction_details = query.order_by(
            InventoryTransactionDetail.transaction_date.desc(),
            InventoryTransactionDetail.id.desc()
        ).all()

        if not transaction_details:
            return generate_empty_excel_response(start_date, end_date)

        # Calculate running totals (chronological order first)
        chronological = sorted(transaction_details, key=lambda x: (x.transaction_date, x.id))
        running_totals = {}
        transaction_running_totals = {}

        for trans in chronological:
            key = trans.item_id
            running_totals[key] = running_totals.get(key, 0) + trans.quantity
            transaction_running_totals[trans.id] = running_totals[key]

        # Prepare Excel data
        source_map = {
            'purchase': 'Receipt', 'sale': 'Dispatch', 'transfer': 'Transfer',
            'adjustment': 'Adjustment', 'opening_balance': 'Opening Stock',
            'write_off': 'Write Off', 'missing': 'Missing', 'damaged': 'Damaged',
            'expired': 'Expired', 'manual': 'Manual'
        }

        excel_data = []

        for trans in transaction_details:
            line_item = trans.inventory_entry_line_item
            entry = line_item.inventory_entry if line_item else None
            variation_link = trans.inventory_item_variation_link
            item = variation_link.inventory_item if variation_link else None

            # Location display
            if trans.movement_type == 'transfer' and entry:
                from_loc = entry.from_inventory_location.location if entry.from_inventory_location else '-'
                to_loc = entry.to_inventory_location.location if entry.to_inventory_location else '-'
                location_display = f"{from_loc} → {to_loc}"
            else:
                location_display = trans.location.location if trans.location else '-'

            # Department
            department = '-'
            if line_item and line_item.project_id:
                project = db_session.query(Project).filter_by(id=line_item.project_id).first()
                department = project.name if project else '-'

            # Party name
            party = '-'
            if entry and entry.source_type in ['purchase', 'sale'] and entry.source_id:
                vendor = db_session.query(Vendor).filter_by(id=entry.source_id).first()
                party = vendor.vendor_name if vendor else '-'

            # Handled by
            handled_by = '-'
            if entry and entry.handled_by_employee:
                handled_by = f"{entry.handled_by_employee.first_name} {entry.handled_by_employee.last_name}".strip()

            excel_data.append({
                'Date': trans.transaction_date.strftime('%Y-%m-%d'),
                'Source': source_map.get(entry.source_type if entry else 'manual', 'Manual'),
                'Movement Type': trans.movement_type.replace('_', ' ').title() if trans.movement_type else '-',
                'Reference': entry.reference if entry else '-',
                'Item Name': item.item_name if item else '-',
                'Attribute': variation_link.inventory_item_attributes.attribute_name if variation_link and variation_link.inventory_item_attributes else '-',
                'Variation': variation_link.inventory_item_variation.variation_name if variation_link and variation_link.inventory_item_variation else '-',
                'UOM': item.unit_of_measurement.abbreviation if item and item.unit_of_measurement else '-',
                'Quantity': trans.quantity,
                'Running Total': transaction_running_totals.get(trans.id, 0),
                'Location': location_display,
                'Department': department,
                'Party': party,
                'Handled By': handled_by
            })

        # Create DataFrame
        df = pd.DataFrame(excel_data)

        # Create Excel file
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            df.to_excel(writer, sheet_name='Stock Movement History', index=False)

            # Add summary sheet
            summary_data = {
                'Metric': ['Total Transactions', 'Generated Date', 'Start Date', 'End Date'],
                'Value': [
                    len(excel_data),
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    start_date or 'N/A',
                    end_date or 'N/A'
                ]
            }
            if project_id:
                summary_data['Metric'].append('Department Filter')
                summary_data['Value'].append('Applied')
            if location_id:
                summary_data['Metric'].append('Location Filter')
                summary_data['Value'].append('Applied')

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

        output.seek(0)

        # Generate filename
        filename = f"stock_movement_history_{start_date}_to_{end_date}.xlsx" if start_date or end_date else "stock_movement_history.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        logger.error(f"Error generating stock movement history Excel: {str(e)}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': 'An error occurred while generating the Excel report'}), 500
    finally:
        db_session.close()


def generate_empty_excel_response(start_date, end_date, message=None):
    """Generate empty Excel response"""
    import pandas as pd
    from io import BytesIO

    output = BytesIO()
    df = pd.DataFrame({'Message': [message or 'No stock movement records found for the selected filters']})

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Stock Movement History', index=False)

        summary_data = {
            'Metric': ['Start Date', 'End Date', 'Generated Date'],
            'Value': [start_date or 'N/A', end_date or 'N/A', datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')]
        }
        summary_df = pd.DataFrame(summary_data)
        summary_df.to_excel(writer, sheet_name='Summary', index=False)

    output.seek(0)
    return send_file(output, as_attachment=True, download_name="stock_movement_history_empty.xlsx", mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')


@download_route.route('/download_stock_list', methods=['POST'])
@login_required
def download_stock_list():
    """
    Download stock list as PDF with filtering
    Uses the same logic as the frontend - switches between summary and transaction-based
    """
    from routes.inventory.inventory_reports import _get_stock_list_data, _get_stock_list_data_with_dates
    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get filter parameters from request
        filters = request.get_json() or {}

        # Extract filters (including status and item)
        category_id = filters.get('category')
        subcategory_id = filters.get('subcategory')
        brand_id = filters.get('brand')
        attribute_id = filters.get('attribute')
        variation_id = filters.get('variation')
        location_id = filters.get('location')
        project_id = filters.get('project_id')  # ===== ADD PROJECT/DEPARTMENT FILTER =====
        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        hide_zero_items = filters.get('hide_zero_items', False)
        status_filter = filters.get('status')
        item_id = filters.get('item')
        sort_by = filters.get('sort_by', 'item_name')
        sort_order = filters.get('sort_order', 'asc')

        # IMPORTANT: Use the same logic as the frontend with status and item filters
        if start_date or end_date or project_id:  # ===== ADD project_id condition =====
            # Use transaction-based calculation for historical data or department filter
            stock_data, _ = _get_stock_list_data_with_dates(
                db_session,
                page=1,
                per_page=10000,  # Get all records
                hide_zero_items=hide_zero_items,
                start_date=start_date,
                end_date=end_date,
                category_id=category_id,
                subcategory_id=subcategory_id,
                brand_id=brand_id,
                attribute_id=attribute_id,
                variation_id=variation_id,
                location_id=location_id,
                project_id=project_id,  # ===== PASS PROJECT FILTER =====
                status_filter=status_filter,
                item_id=item_id,
                sort_by=sort_by,
                sort_order=sort_order
            )
        else:
            # Use InventorySummary for current stock
            stock_data, _ = _get_stock_list_data(
                db_session,
                page=1,
                per_page=10000,  # Get all records
                hide_zero_items=hide_zero_items,
                start_date=start_date,
                end_date=end_date,
                category_id=category_id,
                subcategory_id=subcategory_id,
                brand_id=brand_id,
                attribute_id=attribute_id,
                variation_id=variation_id,
                location_id=location_id,
                project_id=project_id,  # ===== PASS PROJECT FILTER =====
                status_filter=status_filter,
                item_id=item_id,
                sort_by=sort_by,
                sort_order=sort_order
            )

        # Check if stock_data is empty
        if not stock_data:
            return generate_empty_stock_list_pdf_response(filters)

        # Generate PDF using your existing styling functions
        buffer = generate_stock_list_pdf_buffer(stock_data, filters)

        # Generate filename (including status and item)
        filename = generate_stock_list_filename(filters)

        return send_file(
            buffer,
            as_attachment=True,
            download_name=filename,
            mimetype='application/pdf'
        )

    except Exception as e:
        logger.error(f"Error generating stock list PDF: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while generating the PDF report'
        }), 500

    finally:
        db_session.close()

def generate_stock_list_filename(filters):
    """Generate filename for the stock list PDF"""
    filename_parts = ["stock_list"]

    # Add status if applied
    if filters.get('status'):
        status_value = filters['status'].lower().replace(' ', '_')
        filename_parts.append(status_value)

    # Add department if applied
    if filters.get('project_id'):
        filename_parts.append("department_filtered")

    # Add item indicator if specific item selected
    if filters.get('item'):
        filename_parts.append("specific_item")

    # Add sort info to filename
    sort_by = filters.get('sort_by', 'item_name')
    sort_order = filters.get('sort_order', 'asc')
    filename_parts.append(f"sorted_by_{sort_by}_{sort_order}")

    # Add date range if applied
    if filters.get('start_date'):
        filename_parts.append(f"from_{filters['start_date']}")
    if filters.get('end_date'):
        filename_parts.append(f"to_{filters['end_date']}")

    # Add indicator if it's current stock (no dates)
    if not filters.get('start_date') and not filters.get('end_date'):
        filename_parts.append("current")

    # Add other indicators
    if filters.get('hide_zero_items'):
        filename_parts.append("no_zero_items")

    if filters.get('category'):
        filename_parts.append("filtered")

    return "_".join(filename_parts) + ".pdf"

def generate_stock_list_pdf_buffer(stock_data, filters):
    """Generate PDF buffer for stock list with hierarchical structure"""
    buffer = BytesIO()

    # Document setup - REDUCED margins for wider table
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=18,  # Reduced from 36
        leftMargin=18,  # Reduced from 36
        topMargin=36,
        bottomMargin=36
    )

    # Styles setup
    styles = setup_stock_list_pdf_styles()

    # Build elements
    elements = build_stock_list_pdf_elements(stock_data, filters, styles)

    # Build PDF
    doc.build(elements)
    buffer.seek(0)

    return buffer


def setup_stock_list_pdf_styles():
    """Setup PDF styles for stock list"""
    styles = getSampleStyleSheet()

    # Custom styles - FIX: Ensure 'Heading' style is properly defined
    custom_styles = {
        'Heading': ParagraphStyle(
            name='Heading',
            parent=styles['Heading1'],  # Use existing style as parent
            fontName='Helvetica-Bold',
            fontSize=16,
            spaceAfter=12,
            alignment=TA_CENTER
        ),
        'CategoryHeader': ParagraphStyle(
            name='CategoryHeader',
            parent=styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=12,
            textColor=colors.darkblue,
            spaceAfter=6,
            spaceBefore=12,
            leftIndent=5  # Reduced indentation
        ),
        'SubcategoryHeader': ParagraphStyle(
            name='SubcategoryHeader',
            parent=styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=10,
            textColor=colors.darkgreen,
            spaceAfter=4,
            spaceBefore=8,
            leftIndent=10  # Reduced indentation
        ),
        'Subheading': ParagraphStyle(
            name='Subheading',
            parent=styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=11,
            spaceAfter=6
        ),
        'NormalBold': ParagraphStyle(
            name='NormalBold',
            parent=styles['Normal'],
            fontName='Helvetica-Bold',
            fontSize=9,
        ),
        'Wrap': ParagraphStyle(
            name='Wrap',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            leading=10,
            wordWrap='CJK'
        ),
        'WrapSmall': ParagraphStyle(  # NEW: Smaller wrap style for better fit
            name='WrapSmall',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=7,
            leading=9,
            wordWrap='CJK'
        ),
        'AmountPositive': ParagraphStyle(
            name='AmountPositive',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            textColor=colors.green,
            alignment=TA_RIGHT
        ),
        'AmountNegative': ParagraphStyle(
            name='AmountNegative',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            textColor=colors.red,
            alignment=TA_RIGHT
        ),
        'StatusInStock': ParagraphStyle(
            name='StatusInStock',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            textColor=colors.green,
            alignment=TA_CENTER
        ),
        'StatusLowStock': ParagraphStyle(
            name='StatusLowStock',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            textColor=colors.orange,
            alignment=TA_CENTER
        ),
        'StatusOutOfStock': ParagraphStyle(
            name='StatusOutOfStock',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            textColor=colors.red,
            alignment=TA_CENTER
        ),
        'StatusNegative': ParagraphStyle(
            name='StatusNegative',
            parent=styles['Normal'],
            fontName='Helvetica',
            fontSize=8,
            textColor=colors.darkred,
            alignment=TA_CENTER
        )
    }

    for name, style in custom_styles.items():
        styles.add(style)

    return styles


def build_stock_list_pdf_elements(stock_data, filters, styles):
    """Build PDF elements for stock list with hierarchical structure"""
    elements = []

    # Header
    elements.append(Paragraph("Stock List Report", styles['Heading']))
    elements.append(Spacer(1, 10))

    # Add "As of Date" prominently
    as_of_date = filters.get('end_date') or datetime.datetime.now().strftime('%Y-%m-%d')
    elements.append(Paragraph(f"As of: {as_of_date}", styles['Subheading']))

    elements.append(Spacer(1, 5))

    # Filter information
    filter_info = build_stock_list_filter_info(filters)
    if filter_info:
        elements.append(Paragraph(filter_info, styles['Subheading']))

    elements.append(Spacer(1, 12))

    # Process data hierarchically
    total_items = 0

    # Group items by category and subcategory
    organized_data = {}
    for category, items in stock_data.items():
        organized_data[category] = {}
        for item in items:
            subcategory = item['subcategory_name'] or 'Uncategorized'
            if subcategory not in organized_data[category]:
                organized_data[category][subcategory] = []
            organized_data[category][subcategory].append(item)
            total_items += 1

    # Build hierarchical content
    for category, subcategories in organized_data.items():
        # Category header
        elements.append(Paragraph(category, styles['CategoryHeader']))

        for subcategory, items in subcategories.items():
            # Subcategory header (only if there are multiple subcategories or it's not "Uncategorized")
            if len(subcategories) > 1 or subcategory != 'Uncategorized':
                elements.append(Paragraph(subcategory, styles['SubcategoryHeader']))

            # Create table for items in this subcategory
            if items:
                table_data = build_subcategory_table_data(items, styles)
                table = create_stock_list_styled_table(table_data)
                elements.append(table)
                elements.append(Spacer(1, 8))

    # Summary
    elements.append(Spacer(1, 12))
    elements.append(Paragraph(f"Total Items: {total_items}", styles['NormalBold']))

    # Status summary
    status_counts = calculate_status_counts(stock_data)
    status_summary = " | ".join([f"{status}: {count}" for status, count in status_counts.items()])
    elements.append(Paragraph(f"Status Summary: {status_summary}", styles['Normal']))

    # Timestamp
    elements.append(
        Paragraph(f"Generated on: {datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", styles['Normal']))

    return elements


def build_subcategory_table_data(items, styles):
    """Build table data for items in a subcategory"""
    # UPDATED: Added Location to headers
    headers = [
        'Item Name', 'Variation', 'Location', 'UOM', 'Quantity', 'Status'
    ]

    table_data = [
        [Paragraph(header, styles['NormalBold']) for header in headers]
    ]

    # Table rows
    for item in items:
        # Determine quantity style
        quantity_style = 'AmountPositive' if item['quantity'] >= 0 else 'AmountNegative'

        # Determine status style
        status_style_map = {
            'In Stock': 'StatusInStock',
            'Low Stock': 'StatusLowStock',
            'Out of Stock': 'StatusOutOfStock',
            'Negative Stock': 'StatusNegative'
        }
        status_style = status_style_map.get(item['status'], 'Wrap')

        # Format variation
        variation = f"{item['attribute'] or ''} {item['variation'] or ''}".strip() or '-'

        # Get location - use the location from the item data
        location = item.get('location', '-')

        # Use smaller font for better fit with wider table
        table_data.append([
            Paragraph(item['item_name'], styles['WrapSmall']),  # Smaller font
            Paragraph(variation, styles['WrapSmall']),  # Smaller font
            Paragraph(location, styles['WrapSmall']),  # Smaller font
            Paragraph(item['uom'], styles['WrapSmall']),  # Smaller font
            Paragraph(str(item['quantity']), styles[quantity_style]),
            Paragraph(item['status'], styles[status_style])
        ])

    return table_data


def create_stock_list_styled_table(table_data):
    """Create and style the stock list table"""
    # UPDATED: Wider column widths to utilize reduced margins
    # Total available width: 612 - 18 - 18 = 576 points
    col_widths = [160, 120, 100, 35, 50, 60]  # Total: 525 points (leaves some padding)

    table = Table(table_data, colWidths=col_widths, repeatRows=1)

    table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#f8f9fa")),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.black),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 8),
        ('ALIGN', (4, 0), (4, -1), 'RIGHT'),  # Align quantity to right
        ('ALIGN', (3, 0), (3, -1), 'CENTER'),  # Center align UOM
        ('ALIGN', (5, 0), (5, -1), 'CENTER'),  # Center align status
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('INNERGRID', (0, 0), (-1, -1), 0.25, colors.grey),
        ('BOX', (0, 0), (-1, -1), 0.5, colors.grey),
        ('FONTSIZE', (0, 1), (-1, -1), 7),  # Smaller font for data rows
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.white, colors.HexColor("#fafafa")]),
        ('PADDING', (0, 0), (-1, -1), 3),
    ]))

    return table


def calculate_status_counts(stock_data):
    """Calculate status counts from stock data"""
    status_counts = {}
    for category, items in stock_data.items():
        for item in items:
            status = item['status']
            status_counts[status] = status_counts.get(status, 0) + 1
    return status_counts


def build_stock_list_filter_info(filters):
    """Build filter information string for stock list"""
    filter_info = []

    if filters.get('category'):
        filter_info.append(f"Category: {filters['category']}")
    if filters.get('subcategory'):
        filter_info.append(f"Subcategory: {filters['subcategory']}")
    if filters.get('start_date'):
        filter_info.append(f"From: {filters['start_date']}")
    if filters.get('end_date'):
        filter_info.append(f"To: {filters['end_date']}")
    if filters.get('hide_zero_items'):
        filter_info.append("Hide Zero Items: Yes")

    return " | ".join(filter_info) if filter_info else "All Items"


def generate_empty_stock_list_pdf_response(filters):
    """Generate response for empty stock list results"""
    buffer = BytesIO()

    # Use same reduced margins for empty response
    doc = SimpleDocTemplate(
        buffer,
        pagesize=letter,
        rightMargin=18,  # Reduced from 36
        leftMargin=18,  # Reduced from 36
        topMargin=36,
        bottomMargin=36
    )

    styles = getSampleStyleSheet()

    # FIX: Use styles that exist in the default stylesheet
    elements = [
        Paragraph("Stock List Report", styles['Heading1']),  # Use Heading1 instead of custom Heading
        Spacer(1, 20),
        Paragraph("No stock items found for the selected filters", styles['Normal']),
        Spacer(1, 10),
    ]

    filter_info = build_stock_list_filter_info(filters)
    if filter_info:
        elements.append(Paragraph(f"Filters: {filter_info}", styles['Normal']))

    doc.build(elements)
    buffer.seek(0)

    filename = generate_stock_list_filename(filters)

    return send_file(
        buffer,
        as_attachment=True,
        download_name=filename,
        mimetype='application/pdf'
    )



@download_route.route('/download_stock_list_excel', methods=['POST'])
@login_required
def download_stock_list_excel():
    """
    Download stock list as Excel with filtering
    Uses the same logic as the frontend - switches between summary and transaction-based
    """
    from routes.inventory.inventory_reports import _get_stock_list_data, _get_stock_list_data_with_dates
    import pandas as pd
    from io import BytesIO

    db_session = Session()
    app_id = current_user.app_id

    try:
        # Get filter parameters from request
        filters = request.get_json() or {}

        # Extract filters
        category_id = filters.get('category')
        subcategory_id = filters.get('subcategory')
        brand_id = filters.get('brand')
        attribute_id = filters.get('attribute')
        variation_id = filters.get('variation')
        location_id = filters.get('location')
        project_id = filters.get('project_id')  # ===== ADD PROJECT/DEPARTMENT FILTER =====
        start_date = filters.get('start_date')
        end_date = filters.get('end_date')
        hide_zero_items = filters.get('hide_zero_items', False)
        status_filter = filters.get('status')
        item_id = filters.get('item')
        sort_by = filters.get('sort_by', 'item_name')
        sort_order = filters.get('sort_order', 'asc')

        # IMPORTANT: Use the same logic as the frontend with status and item filters
        if start_date or end_date or project_id:  # ===== ADD project_id condition =====
            # Use transaction-based calculation for historical data or department filter
            stock_data, _ = _get_stock_list_data_with_dates(
                db_session,
                page=1,
                per_page=100000,  # Get all records for Excel
                hide_zero_items=hide_zero_items,
                start_date=start_date,
                end_date=end_date,
                category_id=category_id,
                subcategory_id=subcategory_id,
                brand_id=brand_id,
                attribute_id=attribute_id,
                variation_id=variation_id,
                location_id=location_id,
                project_id=project_id,  # ===== PASS PROJECT FILTER =====
                status_filter=status_filter,
                item_id=item_id,
                sort_by=sort_by,
                sort_order=sort_order
            )
        else:
            # Use InventorySummary for current stock
            stock_data, _ = _get_stock_list_data(
                db_session,
                page=1,
                per_page=100000,
                hide_zero_items=hide_zero_items,
                start_date=start_date,
                end_date=end_date,
                category_id=category_id,
                subcategory_id=subcategory_id,
                brand_id=brand_id,
                attribute_id=attribute_id,
                variation_id=variation_id,
                location_id=location_id,
                project_id=project_id,  # ===== PASS PROJECT FILTER =====
                status_filter=status_filter,
                item_id=item_id,
                sort_by=sort_by,
                sort_order=sort_order
            )

        # Check if stock_data is empty
        if not stock_data:
            return generate_empty_stock_list_excel_response(filters)

        # Prepare data for Excel
        excel_data = []
        for category, items in stock_data.items():
            for item in items:
                # Format variation
                variation = f"{item.get('attribute', '')} {item.get('variation', '')}".strip() or '-'

                excel_data.append({
                    'Category': category,
                    'Subcategory': item.get('subcategory_name', '-'),
                    'Item Name': item.get('item_name', '-'),
                    'Variation': variation,
                    'UOM': item.get('uom', '-'),
                    'Location': item.get('location', '-'),
                    'Quantity': item.get('quantity', 0),
                    'Status': item.get('status', '-')
                })

        # Create DataFrame
        df = pd.DataFrame(excel_data)

        # Apply sorting if needed
        sort_column_map = {
            'item_name': 'Item Name',
            'quantity': 'Quantity',
            'category': 'Category',
            'status': 'Status',
            'location': 'Location'
        }

        if sort_by in sort_column_map:
            df = df.sort_values(
                by=sort_column_map[sort_by],
                ascending=(sort_order == 'asc')
            )

        # Create Excel file
        output = BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            # Write main data sheet
            df.to_excel(writer, sheet_name='Stock List', index=False)

            # Add summary sheet
            summary_data = {
                'Metric': [
                    'Total Items',
                    'Categories',
                    'Generated Date',
                    'As Of Date',
                    'Hide Zero Items'
                ],
                'Value': [
                    len(df),
                    len(df['Category'].unique()) if not df.empty else 0,
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    filters.get('end_date') or datetime.datetime.now().strftime('%Y-%m-%d'),
                    'Yes' if hide_zero_items else 'No'
                ]
            }

            # Add filter info to summary
            if category_id:
                summary_data['Metric'].append('Category Filter')
                summary_data['Value'].append(category_id)
            if subcategory_id:
                summary_data['Metric'].append('Subcategory Filter')
                summary_data['Value'].append(subcategory_id)
            if location_id:
                summary_data['Metric'].append('Location Filter')
                summary_data['Value'].append(location_id)
            if project_id:  # ===== ADD DEPARTMENT TO SUMMARY =====
                summary_data['Metric'].append('Department Filter')
                summary_data['Value'].append(project_id)
            if start_date:
                summary_data['Metric'].append('Start Date')
                summary_data['Value'].append(start_date)
            if end_date:
                summary_data['Metric'].append('End Date')
                summary_data['Value'].append(end_date)
            if status_filter:
                summary_data['Metric'].append('Status Filter')
                summary_data['Value'].append(status_filter)
            if item_id:
                summary_data['Metric'].append('Item Filter')
                summary_data['Value'].append('Specific Item')

            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, sheet_name='Summary', index=False)

            # Add status breakdown sheet
            if not df.empty:
                status_counts = df['Status'].value_counts().reset_index()
                status_counts.columns = ['Status', 'Count']
                status_counts.to_excel(writer, sheet_name='Status Breakdown', index=False)

                # Add category breakdown sheet
                category_counts = df.groupby(['Category', 'Subcategory']).size().reset_index(name='Item Count')
                category_counts.to_excel(writer, sheet_name='Category Breakdown', index=False)

        output.seek(0)

        # Generate filename
        filename = generate_stock_list_excel_filename(filters)

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        logger.error(f"Error generating stock list Excel: {str(e)}\n{traceback.format_exc()}")
        return jsonify({
            'success': False,
            'message': 'An error occurred while generating the Excel report'
        }), 500

    finally:
        db_session.close()


def generate_stock_list_excel_filename(filters):
    """Generate filename for the stock list Excel"""
    filename_parts = ["stock_list"]

    # Add status if applied
    if filters.get('status'):
        status_value = filters['status'].lower().replace(' ', '_')
        filename_parts.append(status_value)

    # Add department if applied
    if filters.get('project_id'):
        filename_parts.append("by_department")

    # Add item indicator if specific item selected
    if filters.get('item'):
        filename_parts.append("specific_item")

    # Add sort info to filename
    sort_by = filters.get('sort_by', 'item_name')
    sort_order = filters.get('sort_order', 'asc')
    filename_parts.append(f"sorted_by_{sort_by}_{sort_order}")

    # Add date range if applied
    if filters.get('start_date'):
        filename_parts.append(f"from_{filters['start_date']}")
    if filters.get('end_date'):
        filename_parts.append(f"to_{filters['end_date']}")

    # Add indicator if it's current stock (no dates)
    if not filters.get('start_date') and not filters.get('end_date'):
        filename_parts.append("current")

    # Add other indicators
    if filters.get('hide_zero_items'):
        filename_parts.append("no_zero_items")

    if filters.get('category'):
        filename_parts.append("filtered")

    return "_".join(filename_parts) + ".xlsx"


def generate_empty_stock_list_excel_response(filters):
    """Generate response for empty stock list results (Excel format)"""
    output = BytesIO()

    # Create minimal DataFrame with "No data" message
    df = pd.DataFrame({'Message': ['No stock items found for the selected filters']})

    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='Stock List', index=False)

        # Add filter info
        filter_data = {
            'Filter': [],
            'Value': []
        }

        if filters.get('category'):
            filter_data['Filter'].append('Category')
            filter_data['Value'].append(filters['category'])
        if filters.get('subcategory'):
            filter_data['Filter'].append('Subcategory')
            filter_data['Value'].append(filters['subcategory'])
        if filters.get('location'):
            filter_data['Filter'].append('Location')
            filter_data['Value'].append(filters['location'])
        if filters.get('project_id'):  # ===== ADD DEPARTMENT TO EMPTY RESPONSE =====
            filter_data['Filter'].append('Department')
            filter_data['Value'].append(filters['project_id'])
        if filters.get('start_date'):
            filter_data['Filter'].append('Start Date')
            filter_data['Value'].append(filters['start_date'])
        if filters.get('end_date'):
            filter_data['Filter'].append('End Date')
            filter_data['Value'].append(filters['end_date'])

        if filter_data['Filter']:
            filter_df = pd.DataFrame(filter_data)
            filter_df.to_excel(writer, sheet_name='Applied Filters', index=False)

    output.seek(0)

    filename = generate_stock_list_excel_filename(filters)

    return send_file(
        output,
        as_attachment=True,
        download_name=filename,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )
