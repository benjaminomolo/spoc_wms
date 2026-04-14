import traceback
from collections import defaultdict
from datetime import datetime, date
from decimal import Decimal, InvalidOperation
from io import BytesIO
import io

from reportlab.lib import colors
from reportlab.lib.enums import TA_RIGHT, TA_LEFT, TA_CENTER
from reportlab.lib.pagesizes import letter, landscape
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, Image
from sqlalchemy import func, or_, case, and_, literal, desc, asc
from sqlalchemy.orm import joinedload

from io import BytesIO

import pandas as pd
from flask import request, jsonify, render_template, flash, redirect, url_for, abort, send_file
from flask_login import login_required, current_user
from sqlalchemy import or_, func, literal, desc
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import joinedload

from ai import get_base_currency
from db import Session
from models import Company, Module, InventoryLocation, ChartOfAccounts, Vendor, DirectSalesTransaction, \
    InventoryItemVariationLink, DirectSaleItem, Project, PaymentMode, UnitOfMeasurement, Currency, PaymentAllocation, \
    SalesTransaction, SalesPaymentStatus, SalesInvoice, InvoiceStatus, OrderStatus, CustomerCredit, \
    DirectPurchaseTransaction, InventoryItem, DirectPurchaseItem, PurchasePaymentAllocation, GoodsReceipt, \
    GoodsReceiptItem, PurchaseReturn, PurchaseOrder, InventoryEntry, PurchaseOrderItem, InventoryCategory, \
    InventoryItemVariation, ExchangeRate, InventoryEntryLineItem
from services.inventory_helpers import safe_clear_stock_history_cache
from services.post_to_ledger import post_direct_sale_cogs_to_ledger, post_sales_transaction_to_ledger, \
    bulk_post_sales_transactions, post_customer_credit_to_ledger, \
    post_purchase_transaction_to_ledger, post_goods_receipt_to_ledger, bulk_post_goods_receipts
from services.post_to_ledger_reversal import reverse_direct_sales_posting, reverse_sales_invoice_posting, \
    reverse_sales_transaction_posting, delete_journal_entries_by_source
from services.purchases_helpers import generate_direct_purchase_number, allocate_direct_purchase_payment, \
    get_inventory_entries_for_direct_purchase, reverse_purchase_inventory_entries, reverse_direct_purchase_posting, \
    calculate_direct_purchase_landed_costs, calculate_goods_receipt_landed_costs
from services.sales_helpers import generate_direct_sale_number, allocate_direct_sale_payment, \
    get_inventory_entries_for_direct_sale, reverse_sales_inventory_entries, allocate_payment
from services.vendors_and_customers import get_or_create_customer_credit_account
from utils import empty_to_none, normalize_form_value, generate_unique_journal_number, create_transaction, \
    generate_next_goods_receipt_number, get_total_received_for_purchase, get_item_details
from utils_and_helpers.cache_utils import clear_stock_history_cache
from utils_and_helpers.exchange_rates import get_exchange_rate_and_obj
from utils_and_helpers.lists import check_list_not_empty
from . import purchases_bp

import logging

logger = logging.getLogger(__name__)


@purchases_bp.route('/reports/purchases_by_item', methods=['GET'])
@login_required
def purchases_by_item_page():
    """Main page for Purchases by Item report"""
    db_session = Session()
    try:
        app_id = current_user.app_id

        # Get company and base currency
        company = db_session.query(Company).filter_by(id=app_id).first()

        # Get filter dropdown data
        vendor_types = ['vendor', 'vendors', 'supplier', 'suppliers', 'seller', 'sellers', 'partner', 'other']

        # Normalize to lowercase for consistency
        vendor_types = [v.lower() for v in vendor_types]

        # Query vendors whose vendor_type matches any in the list
        vendors = (
            db_session.query(Vendor)
            .filter(
                Vendor.app_id == app_id,
                func.lower(Vendor.vendor_type).in_(vendor_types)
            )
            .all()
        )

        # Get all inventory items with variations
        items = db_session.query(InventoryItemVariationLink).filter_by(app_id=app_id, status="active").all()

        # Get categories
        categories = db_session.query(InventoryCategory).filter_by(app_id=app_id).all()

        # Get currencies
        currencies = db_session.query(Currency).filter_by(app_id=app_id).all()
        base_currency = next((c for c in currencies if c.currency_index == 1), None)
        base_currency_code = base_currency.user_currency if base_currency else ''
        base_currency_id = base_currency.id

        # Get locations and projects
        locations = db_session.query(InventoryLocation).filter_by(app_id=app_id).all()
        projects = db_session.query(Project).filter_by(app_id=app_id, is_active=True).all()

        role = current_user.role
        modules_data = [mod.module_name for mod in
                        db_session.query(Module).filter_by(app_id=app_id, included='yes').all()]

        return render_template(
            '/purchases/purchases_by_item.html',
            vendors=vendors,
            items=items,
            categories=categories,
            currencies=currencies,
            base_currency=base_currency,
            base_currency_code=base_currency_code,
            base_currency_id=base_currency_id,
            company=company,
            locations=locations,
            projects=projects,
            role=role,
            module_name="Purchases",
            modules=modules_data
        )

    except Exception as e:
        logger.error(f"Error loading purchases by item page: {e}\n{traceback.format_exc()}")
        flash('Error loading report page', 'error')
        return render_template('error.html', message=str(e)), 500
    finally:
        db_session.close()


@purchases_bp.route('/reports/purchases_by_item/summary', methods=['GET'])
@login_required
def purchases_by_item_summary():
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        vendor_id = request.args.get('vendor_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'total_cost')
        sort_order = request.args.get('sort_order', 'desc')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)

        app_id = current_user.app_id

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        if not base_currency:
            return jsonify({'success': False, 'message': 'Base currency not configured'}), 400

        base_currency_id = base_currency.id
        base_currency_code = base_currency.user_currency

        # USE HELPER FUNCTION
        all_items = _get_purchases_by_item_summary_data(
            db_session, app_id, base_currency_id, base_currency_code,
            start_date, end_date, vendor_id, item_id, category_id,
            transaction_type, currency_id, project_id, location_id,
            sort_field, sort_order
        )

        # Apply pagination
        total_items = len(all_items)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_items = all_items[start_idx:end_idx]

        # Calculate totals
        total_qty = sum(item['total_quantity'] for item in all_items)
        total_cost = sum(item['total_cost'] for item in all_items)

        pagination_data = {
            'page': page,
            'per_page': per_page,
            'total_pages': (total_items + per_page - 1) // per_page if total_items > 0 else 1,
            'total_items': total_items,
            'has_next': page < ((total_items + per_page - 1) // per_page) if total_items > 0 else False,
            'has_prev': page > 1
        }

        return jsonify({
            'success': True,
            'data': paginated_items,
            'pagination': pagination_data,
            'totals': {
                'total_quantity': round(total_qty, 2),
                'total_cost': round(total_cost, 2),
                'currency': base_currency_code
            }
        }), 200

    except Exception as e:
        logger.error(f"Error in purchases_by_item_summary: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@purchases_bp.route('/reports/purchases_by_item/detail', methods=['GET'])
@login_required
def purchases_by_item_detail():
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        vendor_id = request.args.get('vendor_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'transaction_date')
        sort_order = request.args.get('sort_order', 'desc')
        page = request.args.get('page', 1, type=int)
        per_page = request.args.get('per_page', 50, type=int)

        app_id = current_user.app_id

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        if not base_currency:
            return jsonify({'success': False, 'message': 'Base currency not configured'}), 400

        base_currency_id = base_currency.id
        base_currency_code = base_currency.user_currency

        # USE HELPER FUNCTION
        all_transactions = _get_purchases_by_item_detail_data(
            db_session, app_id, base_currency_id, base_currency_code,
            start_date, end_date, vendor_id, item_id, category_id,
            transaction_type, currency_id, project_id, location_id,
            sort_field, sort_order
        )

        # Apply pagination
        total_items = len(all_transactions)
        start_idx = (page - 1) * per_page
        end_idx = start_idx + per_page
        paginated_transactions = all_transactions[start_idx:end_idx]

        pagination_data = {
            'page': page,
            'per_page': per_page,
            'total_pages': (total_items + per_page - 1) // per_page if total_items > 0 else 1,
            'total_items': total_items,
            'has_next': page < ((total_items + per_page - 1) // per_page) if total_items > 0 else False,
            'has_prev': page > 1
        }

        return jsonify({
            'success': True,
            'data': paginated_transactions,
            'pagination': pagination_data
        }), 200

    except Exception as e:
        logger.error(f"Error in purchases_by_item_detail: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@purchases_bp.route('/reports/purchases_by_item/export/pdf', methods=['GET'])
@login_required
def export_purchases_by_item_pdf():
    """Export Purchases by Item report to PDF"""
    db_session = Session()
    try:
        # Get filter parameters - MATCH THE SUMMARY/DETAIL FUNCTIONS
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        vendor_id = request.args.get('vendor_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'total_cost')
        sort_order = request.args.get('sort_order', 'desc')
        view_type = request.args.get('view', 'summary')

        app_id = current_user.app_id

        # Get base currency for conversion
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        if not base_currency:
            return jsonify({'success': False, 'message': 'Base currency not configured'}), 400

        base_currency_id = base_currency.id
        base_currency_code = base_currency.user_currency

        # Get company name
        company = db_session.query(Company).filter_by(id=app_id).first()
        company_name = company.name if company else "Company"

        # Get data based on view type
        if view_type == 'summary':
            data = _get_purchases_by_item_summary_data(
                db_session, app_id, base_currency_id, base_currency_code,
                start_date, end_date, vendor_id, item_id, category_id,
                transaction_type, currency_id, project_id, location_id,
                sort_field, sort_order
            )
        else:
            data = _get_purchases_by_item_detail_data(
                db_session, app_id, base_currency_id, base_currency_code,
                start_date, end_date, vendor_id, item_id, category_id,
                transaction_type, currency_id, project_id, location_id,
                sort_field, sort_order
            )

        # If no data, show empty report
        if not data:
            data = []

        # Prepare PDF with LANDSCAPE orientation
        from reportlab.lib.pagesizes import letter, landscape
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=landscape(letter),
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

        # Define styles
        styles = getSampleStyleSheet()
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

        # Report Header
        elements.append(Paragraph(company_name, styles['ReportTitle']))
        elements.append(Spacer(1, 4))
        elements.append(Paragraph("Purchases by Item Report", styles['SubHeader']))

        # Date range
        report_period = ""
        if start_date and end_date:
            report_period = f"{start_date} to {end_date}"
        elif start_date:
            report_period = f"From {start_date}"
        elif end_date:
            report_period = f"Until {end_date}"
        else:
            report_period = "All Time"

        elements.append(Paragraph(f"Period: {report_period}", styles['Normal']))

        # Filters applied
        filters_text = []
        if vendor_id:
            vendor = db_session.query(Vendor).filter_by(id=vendor_id).first()
            if vendor:
                filters_text.append(f"Vendor: {vendor.vendor_name}")
        if category_id:
            category = db_session.query(InventoryCategory).filter_by(id=category_id).first()
            if category:
                filters_text.append(f"Category: {category.category_name}")
        if transaction_type:
            type_display = {
                'purchase_order': 'Purchase Order',
                'direct_purchase': 'Direct Purchase',
                'inventory_entry': 'Inventory Entry'
            }.get(transaction_type, transaction_type.replace('_', ' ').title())
            filters_text.append(f"Type: {type_display}")
        if location_id:
            location = db_session.query(InventoryLocation).filter_by(id=location_id).first()
            if location:
                filters_text.append(f"Location: {location.location_name}")
        if project_id:
            project = db_session.query(Project).filter_by(id=project_id).first()
            if project:
                filters_text.append(f"Project: {project.name}")

        if filters_text:
            elements.append(Paragraph(f"Filters: {', '.join(filters_text)}", styles['SmallText']))

        elements.append(Paragraph(f"Currency: {base_currency_code}", styles['SmallText']))
        elements.append(
            Paragraph(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M')}", styles['SmallText']))
        elements.append(Spacer(1, 12))

        # Data Table
        if view_type == 'summary':
            # Summary table headers
            table_data = [["Item Name", "Item Code", "Category", "UOM", "Quantity", "Total Cost", "Avg Cost",
                           "Last Purchase Date"]]

            for item in data:
                table_data.append([
                    Paragraph(str(item.get('item_name', '-')), styles['TableCell']),
                    Paragraph(str(item.get('item_code', '-')), styles['TableCell']),
                    Paragraph(str(item.get('category_name', '-')), styles['TableCell']),
                    Paragraph(str(item.get('uom', 'pcs')), styles['TableCell']),
                    Paragraph(f"{item.get('total_quantity', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{item.get('total_cost', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{item.get('average_cost', 0):,.2f}", styles['AmountCell']),
                    Paragraph(str(item.get('last_purchase_date', '-')), styles['TableCell'])
                ])

            # Add totals row
            total_qty = sum(item.get('total_quantity', 0) for item in data)
            total_cost = sum(item.get('total_cost', 0) for item in data)

            table_data.append([
                "", "", "", "TOTALS:",
                Paragraph(f"{total_qty:,.2f}", styles['AmountCell']),
                Paragraph(f"{total_cost:,.2f}", styles['AmountCell']),
                "", ""
            ])

            col_widths = [150, 70, 80, 50, 70, 100, 90, 90]

        else:  # detail view
            # Detail table headers
            table_data = [
                ["Date", "Type", "Transaction #", "Vendor", "Item Name", "Item Code", "Category", "UOM", "Quantity",
                 "Unit Price", "Total Cost"]]

            for tx in data:
                table_data.append([
                    Paragraph(str(tx.get('transaction_date', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('transaction_type', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('transaction_number', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('vendor_name', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('item_name', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('item_code', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('category_name', '-')), styles['TableCell']),
                    Paragraph(str(tx.get('uom', 'pcs')), styles['TableCell']),
                    Paragraph(f"{tx.get('quantity', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{tx.get('unit_price', 0):,.2f}", styles['AmountCell']),
                    Paragraph(f"{tx.get('total_cost', 0):,.2f}", styles['AmountCell'])
                ])

            # Add totals row
            total_qty = sum(tx.get('quantity', 0) for tx in data)
            total_cost = sum(tx.get('total_cost', 0) for tx in data)

            table_data.append([
                "", "", "", "", "", "", "TOTALS:", "",
                Paragraph(f"{total_qty:,.2f}", styles['AmountCell']),
                "",
                Paragraph(f"{total_cost:,.2f}", styles['AmountCell'])
            ])

            col_widths = [50, 60, 70, 80, 80, 50, 60, 40, 50, 80, 100]

        # Create table with REPEAT HEADER ON EVERY PAGE
        table = Table(table_data, colWidths=col_widths, repeatRows=1)
        table.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), font_name_bold),
            ('FONTSIZE', (0, 0), (-1, 0), 8),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#343a40')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),

            ('FONTNAME', (0, 1), (-1, -2), font_name_regular),
            ('FONTSIZE', (0, 1), (-1, -2), 7),
            ('GRID', (0, 0), (-1, -2), 0.5, colors.HexColor('#dee2e6')),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
            ('LEFTPADDING', (0, 0), (-1, -1), 4),
            ('RIGHTPADDING', (0, 0), (-1, -1), 4),

            ('ALIGN', (4, 1), (5, -2), 'RIGHT') if view_type == 'summary' else ('ALIGN', (8, 1), (10, -2), 'RIGHT'),

            ('FONTNAME', (0, -1), (-1, -1), font_name_bold),
            ('BACKGROUND', (0, -1), (-1, -1), colors.HexColor('#e9ecef')),
            ('LINEABOVE', (0, -1), (-1, -1), 1, colors.black),

            ('ROWBACKGROUNDS', (0, 1), (-1, -2), [colors.white, colors.HexColor('#f8f9fa')]),
        ]))

        elements.append(table)

        # Build PDF
        doc.build(elements)
        buffer.seek(0)

        filename = f"Purchases_by_Item_{view_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf"
        return send_file(buffer, as_attachment=True, download_name=filename, mimetype='application/pdf')

    except Exception as e:
        logger.error(f"Error exporting purchases by item PDF: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


@purchases_bp.route('/reports/purchases_by_item/export/excel', methods=['GET'])
@login_required
def export_purchases_by_item_excel():
    """Export Purchases by Item report to Excel"""
    db_session = Session()
    try:
        # Get filter parameters
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        vendor_id = request.args.get('vendor_id')
        item_id = request.args.get('item_id')
        category_id = request.args.get('category_id')
        transaction_type = request.args.get('transaction_type')
        currency_id = request.args.get('currency_id')
        location_id = request.args.get('location_id')
        project_id = request.args.get('project_id')
        sort_field = request.args.get('sort_field', 'total_cost')
        sort_order = request.args.get('sort_order', 'desc')
        view_type = request.args.get('view', 'summary')

        app_id = current_user.app_id

        # Get base currency
        base_currency = db_session.query(Currency).filter_by(app_id=app_id, currency_index=1).first()
        base_currency_code = base_currency.user_currency if base_currency else "USD"

        # Get data based on view type
        if view_type == 'summary':
            data = _get_purchases_by_item_summary_data(db_session, app_id, base_currency.id, base_currency_code,
                                                       start_date, end_date, vendor_id, item_id, category_id,
                                                       transaction_type, currency_id)

            # Create DataFrame for summary
            df = pd.DataFrame(data)
            if not df.empty:
                df = df.rename(columns={
                    'item_name': 'Item Name',
                    'item_code': 'Item Code',
                    'category_name': 'Category',
                    'uom': 'UOM',
                    'total_quantity': 'Quantity Purchased',
                    'total_cost': 'Total Cost',
                    'average_cost': 'Average Cost',
                    'last_purchase_date': 'Last Purchase Date'
                })

                # Add totals row
                totals = {
                    'Item Name': 'TOTALS',
                    'Quantity Purchased': df['Quantity Purchased'].sum(),
                    'Total Cost': df['Total Cost'].sum()
                }
                df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

        else:  # detail view
            data = _get_purchases_by_item_detail_data(db_session, app_id, base_currency.id, base_currency_code,
                                                      start_date, end_date, vendor_id, item_id, category_id,
                                                      transaction_type, currency_id)

            # Create DataFrame for detail
            df = pd.DataFrame(data)
            if not df.empty:
                df = df.rename(columns={
                    'transaction_date': 'Transaction Date',
                    'transaction_type': 'Transaction Type',
                    'transaction_number': 'Transaction #',
                    'vendor_name': 'Vendor',
                    'item_name': 'Item Name',
                    'item_code': 'Item Code',
                    'category_name': 'Category',
                    'uom': 'UOM',
                    'quantity': 'Quantity',
                    'unit_price': 'Unit Price',
                    'total_cost': 'Total Cost'
                })

                # Add totals row
                totals = {
                    'Transaction #': 'TOTALS',
                    'Quantity': df['Quantity'].sum(),
                    'Total Cost': df['Total Cost'].sum()
                }
                df = pd.concat([df, pd.DataFrame([totals])], ignore_index=True)

        # Create Excel file
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            sheet_name = 'Purchases by Item - Summary' if view_type == 'summary' else 'Purchases by Item - Detail'
            df.to_excel(writer, sheet_name=sheet_name, index=False)

            # Auto-adjust column widths
            worksheet = writer.sheets[sheet_name]
            for column in worksheet.columns:
                max_length = 0
                column_letter = column[0].column_letter
                for cell in column:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = min(max_length + 2, 50)
                worksheet.column_dimensions[column_letter].width = adjusted_width

        output.seek(0)

        filename = f"Purchases_by_Item_{view_type}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(output, download_name=filename, as_attachment=True,
                         mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')

    except Exception as e:
        logger.error(f"Error exporting purchases by item Excel: {e}\n{traceback.format_exc()}")
        return jsonify({'success': False, 'message': str(e)}), 500
    finally:
        db_session.close()


def _get_purchases_by_item_summary_data(db_session, app_id, base_currency_id, base_currency_code,
                                        start_date, end_date, vendor_id, item_id, category_id,
                                        transaction_type, currency_id, project_id, location_id,
                                        sort_field='total_cost', sort_order='desc'):
    """Helper to get summary data for export (no pagination)"""
    try:

        # Query for Purchase Order items that have been received via Goods Receipt
        po_receipt_query = (db_session.query(
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.id.label('item_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            GoodsReceiptItem.quantity_received.label('quantity'),
            (GoodsReceiptItem.quantity_received * PurchaseOrderItem.unit_price).label('total_price_original'),
            PurchaseOrderItem.unit_price.label('unit_price'),
            GoodsReceipt.receipt_date.label('purchase_date'),
            PurchaseOrder.currency.label('currency_id'),
            PurchaseOrder.vendor_id.label('vendor_id'),
            Vendor.vendor_name.label('vendor_name'),
            ExchangeRate.rate.label('exchange_rate'),
            literal('purchase_order').label('source_type')
        ).join(
            GoodsReceiptItem, GoodsReceiptItem.purchase_order_item_id == PurchaseOrderItem.id
        ).join(
            GoodsReceipt, GoodsReceipt.id == GoodsReceiptItem.goods_receipt_id
        ).join(
            PurchaseOrder, PurchaseOrder.id == PurchaseOrderItem.purchase_order_id
        ).join(
            Vendor, Vendor.id == PurchaseOrder.vendor_id
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == PurchaseOrderItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).join(
            InventoryLocation,
            InventoryLocation.id == PurchaseOrderItem.location_id
        ).join(
            Project,
            Project.id == PurchaseOrder.project_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == PurchaseOrder.exchange_rate_id
        ).filter(
            PurchaseOrder.app_id == app_id,
            GoodsReceipt.is_posted_to_ledger == True,
            GoodsReceiptItem.is_posted_to_ledger == True
        ))

        # Query for Direct Purchase items
        direct_items_query = db_session.query(
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.id.label('item_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            DirectPurchaseItem.quantity.label('quantity'),
            DirectPurchaseItem.total_price.label('total_price_original'),
            DirectPurchaseItem.unit_price.label('unit_price'),
            DirectPurchaseTransaction.payment_date.label('purchase_date'),
            DirectPurchaseTransaction.currency_id.label('currency_id'),
            DirectPurchaseTransaction.vendor_id.label('vendor_id'),
            Vendor.vendor_name.label('vendor_name'),
            ExchangeRate.rate.label('exchange_rate'),
            literal('direct_purchase').label('source_type')
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == DirectPurchaseItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).join(
            InventoryLocation,
            InventoryLocation.id == DirectPurchaseItem.location_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).join(
            DirectPurchaseTransaction, DirectPurchaseTransaction.id == DirectPurchaseItem.transaction_id
        ).join(
            Vendor, Vendor.id == DirectPurchaseTransaction.vendor_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            PurchasePaymentAllocation, PurchasePaymentAllocation.direct_purchase_id == DirectPurchaseTransaction.id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == PurchasePaymentAllocation.exchange_rate_id
        ).filter(
            DirectPurchaseTransaction.app_id == app_id,
            DirectPurchaseTransaction.status.in_(['paid', 'draft'])
        )

        # ========== ADD INVENTORY ENTRY QUERY (PURCHASES) ==========
        # ========== ADD INVENTORY ENTRY QUERY (PURCHASES) ==========
        # Query for manual Inventory Entries that are purchases (source_id is NULL)
        # For purchases (stock in), use to_location (where stock is being received)
        inventory_purchase_query = db_session.query(
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.id.label('item_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            InventoryEntryLineItem.quantity.label('quantity'),
            (InventoryEntryLineItem.quantity * InventoryEntryLineItem.unit_price).label('total_price_original'),
            InventoryEntryLineItem.unit_price.label('unit_price'),
            InventoryEntry.transaction_date.label('purchase_date'),
            InventoryEntry.currency_id.label('currency_id'),
            InventoryEntry.supplier_id.label('vendor_id'),
            Vendor.vendor_name.label('vendor_name'),
            ExchangeRate.rate.label('exchange_rate'),
            literal('inventory_entry').label('source_type')
        ).join(
            InventoryEntryLineItem, InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == InventoryEntryLineItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).outerjoin(
            Vendor, Vendor.id == InventoryEntry.supplier_id
        ).outerjoin(
            InventoryLocation,
            InventoryLocation.id == InventoryEntry.to_location  # FIXED: Use to_location for purchases
        ).outerjoin(
            Project,
            Project.id == InventoryEntry.project_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == InventoryEntry.exchange_rate_id
        ).filter(
            InventoryEntry.app_id == app_id,
            InventoryEntry.stock_movement == 'in',
            InventoryEntry.source_id.is_(None),
            InventoryEntry.inventory_source.in_(['purchase', 'adjustment_in'])
        )

        # Apply date filters
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
            po_receipt_query = po_receipt_query.filter(GoodsReceipt.receipt_date >= start_date_obj)
            direct_items_query = direct_items_query.filter(DirectPurchaseTransaction.payment_date >= start_date_obj)
            inventory_purchase_query = inventory_purchase_query.filter(
                InventoryEntry.transaction_date >= start_date_obj)

        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()
            po_receipt_query = po_receipt_query.filter(GoodsReceipt.receipt_date <= end_date_obj)
            direct_items_query = direct_items_query.filter(DirectPurchaseTransaction.payment_date <= end_date_obj)
            inventory_purchase_query = inventory_purchase_query.filter(InventoryEntry.transaction_date <= end_date_obj)

        # Apply vendor filter
        if vendor_id and vendor_id != 'None' and vendor_id != '':
            po_receipt_query = po_receipt_query.filter(PurchaseOrder.vendor_id == vendor_id)
            direct_items_query = direct_items_query.filter(DirectPurchaseTransaction.vendor_id == vendor_id)
            inventory_purchase_query = inventory_purchase_query.filter(InventoryEntry.supplier_id == vendor_id)

        # Apply item filter
        if item_id and item_id != 'None' and item_id != '':
            po_receipt_query = po_receipt_query.filter(InventoryItemVariationLink.id == item_id)
            direct_items_query = direct_items_query.filter(InventoryItemVariationLink.id == item_id)
            inventory_purchase_query = inventory_purchase_query.filter(InventoryItemVariationLink.id == item_id)

        # Apply category filter
        if category_id and category_id != 'None' and category_id != '':
            po_receipt_query = po_receipt_query.filter(InventoryItem.item_category_id == category_id)
            direct_items_query = direct_items_query.filter(InventoryItem.item_category_id == category_id)
            inventory_purchase_query = inventory_purchase_query.filter(InventoryItem.item_category_id == category_id)

        # Apply currency filter
        if currency_id and currency_id != 'None' and currency_id != '':
            po_receipt_query = po_receipt_query.filter(PurchaseOrder.currency == currency_id)
            direct_items_query = direct_items_query.filter(DirectPurchaseTransaction.currency_id == currency_id)
            inventory_purchase_query = inventory_purchase_query.filter(InventoryEntry.currency_id == currency_id)

        # Apply location filter
        # Apply location filter
        if location_id and location_id != 'None' and location_id != '':
            po_receipt_query = po_receipt_query.filter(InventoryLocation.id == location_id)
            direct_items_query = direct_items_query.filter(InventoryLocation.id == location_id)
            inventory_purchase_query = inventory_purchase_query.filter(
                InventoryEntry.to_location == location_id)  # FIXED
        # Apply project filter
        if project_id and project_id != 'None' and project_id != '':
            po_receipt_query = po_receipt_query.filter(PurchaseOrder.project_id == project_id)
            direct_items_query = direct_items_query.filter(DirectPurchaseTransaction.project_id == project_id)
            inventory_purchase_query = inventory_purchase_query.filter(InventoryEntry.project_id == project_id)

        # Apply transaction type filter
        if transaction_type == 'purchase_order':
            direct_items_query = direct_items_query.filter(False)
            inventory_purchase_query = inventory_purchase_query.filter(False)
        elif transaction_type == 'direct_purchase':
            po_receipt_query = po_receipt_query.filter(False)
            inventory_purchase_query = inventory_purchase_query.filter(False)
        elif transaction_type == 'inventory_entry':
            po_receipt_query = po_receipt_query.filter(False)
            direct_items_query = direct_items_query.filter(False)

        # Get all results
        po_results = po_receipt_query.all()
        direct_results = direct_items_query.all()
        inventory_results = inventory_purchase_query.all()

        logger.info(
            f"PO results: {len(po_results)}, Direct results: {len(direct_results)}, Inventory results: {len(inventory_results)}")

        # Combine results
        all_purchases = []

        def convert_to_base(amount, currency_id_val, exchange_rate):
            if currency_id_val == base_currency_id:
                return float(amount)
            if exchange_rate:
                return float(amount) * float(exchange_rate)
            return float(amount)

        # Process PO results
        for item in po_results:
            converted_cost = convert_to_base(item.total_price_original, item.currency_id, item.exchange_rate)
            all_purchases.append({
                'variation_link_id': item.variation_link_id,
                'item_id': item.item_id,
                'item_name': item.base_item_name,
                'variation_name': item.variation_name or '',
                'item_code': item.item_code,
                'category_name': item.category_name,
                'uom': item.uom or 'pcs',
                'quantity': float(item.quantity),
                'total_cost_base': converted_cost,
                'purchase_date': item.purchase_date
            })

        # Process Direct results
        for item in direct_results:
            converted_cost = convert_to_base(item.total_price_original, item.currency_id, item.exchange_rate)
            all_purchases.append({
                'variation_link_id': item.variation_link_id,
                'item_id': item.item_id,
                'item_name': item.base_item_name,
                'variation_name': item.variation_name or '',
                'item_code': item.item_code,
                'category_name': item.category_name,
                'uom': item.uom or 'pcs',
                'quantity': float(item.quantity),
                'total_cost_base': converted_cost,
                'purchase_date': item.purchase_date
            })

        # Process Inventory Entry results
        for item in inventory_results:
            converted_cost = convert_to_base(item.total_price_original, item.currency_id, item.exchange_rate)
            all_purchases.append({
                'variation_link_id': item.variation_link_id,
                'item_id': item.item_id,
                'item_name': item.base_item_name,
                'variation_name': item.variation_name or '',
                'item_code': item.item_code,
                'category_name': item.category_name,
                'uom': item.uom or 'pcs',
                'quantity': float(item.quantity),
                'total_cost_base': converted_cost,
                'purchase_date': item.purchase_date
            })

        # Rest of the function remains the same (grouping, sorting, etc.)
        # Group by variation_link_id
        grouped_data = {}
        for purchase in all_purchases:
            key = purchase['variation_link_id']
            if key not in grouped_data:
                grouped_data[key] = {
                    'variation_link_id': key,
                    'item_id': purchase['item_id'],
                    'item_name': purchase['item_name'],
                    'variation_name': purchase['variation_name'],
                    'item_code': purchase['item_code'],
                    'category_name': purchase['category_name'],
                    'uom': purchase['uom'],
                    'total_quantity': 0,
                    'total_cost': 0,
                    'last_purchase_date': None
                }

            grouped_data[key]['total_quantity'] += purchase['quantity']
            grouped_data[key]['total_cost'] += purchase['total_cost_base']

            if purchase['purchase_date']:
                if not grouped_data[key]['last_purchase_date'] or purchase['purchase_date'] > grouped_data[key][
                    'last_purchase_date']:
                    grouped_data[key]['last_purchase_date'] = purchase['purchase_date']

        # Convert to list
        items_list = []
        for key, data in grouped_data.items():
            avg_cost = data['total_cost'] / data['total_quantity'] if data['total_quantity'] > 0 else 0

            if data['variation_name']:
                item_name = f"{data['item_name']} ({data['variation_name']})"
            else:
                item_name = data['item_name']

            items_list.append({
                'item_name': item_name,
                'item_code': data['item_code'] or '-',
                'category_name': data['category_name'] or '-',
                'uom': data['uom'] or 'pcs',
                'total_quantity': round(data['total_quantity'], 2),
                'total_cost': round(data['total_cost'], 2),
                'average_cost': round(avg_cost, 2),
                'last_purchase_date': data['last_purchase_date'].isoformat() if data['last_purchase_date'] else None
            })

        # Sort
        sort_field_mapping = {
            'total_cost': 'total_cost',
            'total_quantity': 'total_quantity',
            'item_name': 'item_name',
            'last_purchase_date': 'last_purchase_date'
        }
        sort_by = sort_field_mapping.get(sort_field, 'total_cost')
        reverse_order = sort_order == 'desc'
        items_list.sort(key=lambda x: x[sort_by], reverse=reverse_order)

        return items_list

    except Exception as e:
        logger.error(f"Error in _get_purchases_by_item_summary_data: {e}\n{traceback.format_exc()}")
        return []


def _get_purchases_by_item_detail_data(db_session, app_id, base_currency_id, base_currency_code,
                                       start_date, end_date, vendor_id, item_id, category_id,
                                       transaction_type, currency_id, project_id, location_id,
                                       sort_field='transaction_date', sort_order='desc'):
    """Helper to get detail data for export (no pagination)"""
    try:

        all_transactions = []

        def convert_to_base(amount, currency_id_val, exchange_rate):
            if not amount:
                return 0.0
            if currency_id_val == base_currency_id:
                return float(amount)
            if exchange_rate:
                return float(amount) * float(exchange_rate)
            return float(amount)

        # Parse date objects
        start_date_obj = None
        end_date_obj = None
        if start_date:
            start_date_obj = datetime.strptime(start_date, '%Y-%m-%d').date()
        if end_date:
            end_date_obj = datetime.strptime(end_date, '%Y-%m-%d').date()

        # ========== 1. GET PURCHASE ORDER ITEMS ==========
        po_detail_query = db_session.query(
            GoodsReceipt.receipt_date.label('transaction_date'),
            literal('Purchase Order').label('transaction_type'),
            PurchaseOrder.purchase_order_number.label('transaction_number'),
            Vendor.vendor_name.label('vendor_name'),
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            GoodsReceiptItem.quantity_received.label('quantity'),
            PurchaseOrderItem.unit_price.label('unit_price'),
            (GoodsReceiptItem.quantity_received * PurchaseOrderItem.unit_price).label('total_cost_original'),
            PurchaseOrder.currency.label('currency_id'),
            PurchaseOrder.id.label('transaction_id'),
            ExchangeRate.rate.label('exchange_rate')
        ).join(
            GoodsReceiptItem, GoodsReceiptItem.goods_receipt_id == GoodsReceipt.id
        ).join(
            PurchaseOrderItem, PurchaseOrderItem.id == GoodsReceiptItem.purchase_order_item_id
        ).join(
            PurchaseOrder, PurchaseOrder.id == PurchaseOrderItem.purchase_order_id
        ).join(
            Vendor, Vendor.id == PurchaseOrder.vendor_id
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == PurchaseOrderItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == PurchaseOrder.exchange_rate_id
        ).filter(
            PurchaseOrder.app_id == app_id,
            GoodsReceipt.is_posted_to_ledger == True,
            GoodsReceiptItem.is_posted_to_ledger == True
        )

        # Apply filters for PO
        if start_date_obj:
            po_detail_query = po_detail_query.filter(GoodsReceipt.receipt_date >= start_date_obj)
        if end_date_obj:
            po_detail_query = po_detail_query.filter(GoodsReceipt.receipt_date <= end_date_obj)
        if vendor_id and vendor_id != 'None' and vendor_id != '':
            po_detail_query = po_detail_query.filter(PurchaseOrder.vendor_id == vendor_id)
        if item_id and item_id != 'None' and item_id != '':
            po_detail_query = po_detail_query.filter(InventoryItemVariationLink.id == item_id)
        if category_id and category_id != 'None' and category_id != '':
            po_detail_query = po_detail_query.filter(InventoryItem.item_category_id == category_id)
        if currency_id and currency_id != 'None' and currency_id != '':
            po_detail_query = po_detail_query.filter(PurchaseOrder.currency == currency_id)
        if location_id and location_id != 'None' and location_id != '':
            po_detail_query = po_detail_query.filter(PurchaseOrderItem.location_id == location_id)
        if project_id and project_id != 'None' and project_id != '':
            po_detail_query = po_detail_query.filter(PurchaseOrder.project_id == project_id)

        if transaction_type not in ['direct_purchase', 'inventory_entry']:
            po_results = po_detail_query.all()
            logger.info(f"PO results: {len(po_results)}")
            for item in po_results:
                # Convert both unit price and total cost
                converted_unit_price = convert_to_base(item.unit_price, item.currency_id, item.exchange_rate)
                converted_cost = convert_to_base(item.total_cost_original, item.currency_id, item.exchange_rate)

                all_transactions.append({
                    'transaction_date': item.transaction_date,
                    'transaction_type': item.transaction_type,
                    'transaction_number': item.transaction_number,
                    'transaction_id': item.transaction_id,
                    'vendor_name': item.vendor_name,
                    'base_item_name': item.base_item_name,
                    'variation_name': item.variation_name or '',
                    'item_code': item.item_code,
                    'category_name': item.category_name,
                    'uom': item.uom or 'pcs',
                    'quantity': float(item.quantity),
                    'unit_price': converted_unit_price,
                    'total_cost_original': converted_cost,
                    'currency_id': item.currency_id,
                    'exchange_rate': item.exchange_rate
                })

        # ========== 2. GET DIRECT PURCHASE ITEMS ==========
        direct_detail_query = db_session.query(
            DirectPurchaseTransaction.payment_date.label('transaction_date'),
            literal('Direct Purchase').label('transaction_type'),
            DirectPurchaseTransaction.direct_purchase_number.label('transaction_number'),
            Vendor.vendor_name.label('vendor_name'),
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            DirectPurchaseItem.quantity.label('quantity'),
            DirectPurchaseItem.unit_price.label('unit_price'),
            DirectPurchaseItem.total_price.label('total_cost_original'),
            DirectPurchaseTransaction.currency_id.label('currency_id'),
            DirectPurchaseTransaction.id.label('transaction_id'),
            ExchangeRate.rate.label('exchange_rate')
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == DirectPurchaseItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).join(
            DirectPurchaseTransaction, DirectPurchaseTransaction.id == DirectPurchaseItem.transaction_id
        ).join(
            Vendor, Vendor.id == DirectPurchaseTransaction.vendor_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            PurchasePaymentAllocation, PurchasePaymentAllocation.direct_purchase_id == DirectPurchaseTransaction.id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == PurchasePaymentAllocation.exchange_rate_id
        ).filter(
            DirectPurchaseTransaction.app_id == app_id,
            DirectPurchaseTransaction.status.in_(['paid', 'draft'])
        )

        # Apply filters for Direct
        if start_date_obj:
            direct_detail_query = direct_detail_query.filter(DirectPurchaseTransaction.payment_date >= start_date_obj)
        if end_date_obj:
            direct_detail_query = direct_detail_query.filter(DirectPurchaseTransaction.payment_date <= end_date_obj)
        if vendor_id and vendor_id != 'None' and vendor_id != '':
            direct_detail_query = direct_detail_query.filter(DirectPurchaseTransaction.vendor_id == vendor_id)
        if item_id and item_id != 'None' and item_id != '':
            direct_detail_query = direct_detail_query.filter(InventoryItemVariationLink.id == item_id)
        if category_id and category_id != 'None' and category_id != '':
            direct_detail_query = direct_detail_query.filter(InventoryItem.item_category_id == category_id)
        if currency_id and currency_id != 'None' and currency_id != '':
            direct_detail_query = direct_detail_query.filter(DirectPurchaseTransaction.currency_id == currency_id)
        if location_id and location_id != 'None' and location_id != '':
            direct_detail_query = direct_detail_query.filter(DirectPurchaseItem.location_id == location_id)
        if project_id and project_id != 'None' and project_id != '':
            direct_detail_query = direct_detail_query.filter(DirectPurchaseTransaction.project_id == project_id)

        if transaction_type not in ['purchase_order', 'inventory_entry']:
            direct_results = direct_detail_query.all()
            logger.info(f"Direct results: {len(direct_results)}")
            for item in direct_results:
                # Convert both unit price and total cost
                converted_unit_price = convert_to_base(item.unit_price, item.currency_id, item.exchange_rate)
                converted_cost = convert_to_base(item.total_cost_original, item.currency_id, item.exchange_rate)

                all_transactions.append({
                    'transaction_date': item.transaction_date,
                    'transaction_type': item.transaction_type,
                    'transaction_number': item.transaction_number,
                    'transaction_id': item.transaction_id,
                    'vendor_name': item.vendor_name,
                    'base_item_name': item.base_item_name,
                    'variation_name': item.variation_name or '',
                    'item_code': item.item_code,
                    'category_name': item.category_name,
                    'uom': item.uom or 'pcs',
                    'quantity': float(item.quantity),
                    'unit_price': converted_unit_price,
                    'total_cost_original': converted_cost,
                    'currency_id': item.currency_id,
                    'exchange_rate': item.exchange_rate
                })

        # ========== 3. GET INVENTORY ENTRY ITEMS (PURCHASES) ==========
        inventory_detail_query = db_session.query(
            InventoryEntry.transaction_date.label('transaction_date'),
            literal('Inventory Entry').label('transaction_type'),
            InventoryEntry.reference.label('transaction_number'),
            Vendor.vendor_name.label('vendor_name'),
            InventoryItemVariationLink.id.label('variation_link_id'),
            InventoryItem.item_name.label('base_item_name'),
            InventoryItemVariation.variation_name.label('variation_name'),
            InventoryItem.item_code.label('item_code'),
            InventoryCategory.category_name.label('category_name'),
            UnitOfMeasurement.abbreviation.label('uom'),
            InventoryEntryLineItem.quantity.label('quantity'),
            InventoryEntryLineItem.unit_price.label('unit_price'),
            (InventoryEntryLineItem.quantity * InventoryEntryLineItem.unit_price).label('total_cost_original'),
            InventoryEntry.currency_id.label('currency_id'),
            InventoryEntry.id.label('transaction_id'),
            ExchangeRate.rate.label('exchange_rate')
        ).join(
            InventoryEntryLineItem, InventoryEntryLineItem.inventory_entry_id == InventoryEntry.id
        ).join(
            InventoryItemVariationLink, InventoryItemVariationLink.id == InventoryEntryLineItem.item_id
        ).join(
            InventoryItem, InventoryItem.id == InventoryItemVariationLink.inventory_item_id
        ).join(
            UnitOfMeasurement, UnitOfMeasurement.id == InventoryItem.uom_id
        ).outerjoin(
            InventoryItemVariation, InventoryItemVariation.id == InventoryItemVariationLink.inventory_item_variation_id
        ).outerjoin(
            InventoryCategory, InventoryCategory.id == InventoryItem.item_category_id
        ).outerjoin(
            Vendor, Vendor.id == InventoryEntry.supplier_id
        ).outerjoin(
            ExchangeRate, ExchangeRate.id == InventoryEntry.exchange_rate_id
        ).filter(
            InventoryEntry.app_id == app_id,
            InventoryEntry.stock_movement == 'in',
            InventoryEntry.source_id.is_(None),
            InventoryEntry.inventory_source.in_(['purchase', 'adjustment_in'])
        )

        # Apply filters for Inventory
        if start_date_obj:
            inventory_detail_query = inventory_detail_query.filter(InventoryEntry.transaction_date >= start_date_obj)
        if end_date_obj:
            inventory_detail_query = inventory_detail_query.filter(InventoryEntry.transaction_date <= end_date_obj)
        if vendor_id and vendor_id != 'None' and vendor_id != '':
            inventory_detail_query = inventory_detail_query.filter(InventoryEntry.supplier_id == vendor_id)
        if item_id and item_id != 'None' and item_id != '':
            inventory_detail_query = inventory_detail_query.filter(InventoryItemVariationLink.id == item_id)
        if category_id and category_id != 'None' and category_id != '':
            inventory_detail_query = inventory_detail_query.filter(InventoryItem.item_category_id == category_id)
        if currency_id and currency_id != 'None' and currency_id != '':
            inventory_detail_query = inventory_detail_query.filter(InventoryEntry.currency_id == currency_id)
        if location_id and location_id != 'None' and location_id != '':
            inventory_detail_query = inventory_detail_query.filter(InventoryEntry.to_location == location_id)
        if project_id and project_id != 'None' and project_id != '':
            inventory_detail_query = inventory_detail_query.filter(InventoryEntry.project_id == project_id)

        if transaction_type not in ['purchase_order', 'direct_purchase']:
            inventory_results = inventory_detail_query.all()
            logger.info(f"Inventory results: {len(inventory_results)}")
            for item in inventory_results:
                # Convert both unit price and total cost for inventory entries
                converted_unit_price = convert_to_base(item.unit_price, item.currency_id, item.exchange_rate)
                converted_cost = convert_to_base(item.total_cost_original, item.currency_id, item.exchange_rate)

                all_transactions.append({
                    'transaction_date': item.transaction_date,
                    'transaction_type': item.transaction_type,
                    'transaction_number': item.transaction_number,
                    'transaction_id': item.transaction_id,
                    'vendor_name': item.vendor_name or '-',
                    'base_item_name': item.base_item_name,
                    'variation_name': item.variation_name or '',
                    'item_code': item.item_code,
                    'category_name': item.category_name,
                    'uom': item.uom or 'pcs',
                    'quantity': float(item.quantity),
                    'unit_price': converted_unit_price,
                    'total_cost_original': converted_cost,
                    'currency_id': item.currency_id,
                    'exchange_rate': item.exchange_rate
                })

        # If no transactions, return empty list
        if not all_transactions:
            logger.info("No transactions found")
            return []

        # ========== SORT ==========
        sort_reverse = sort_order == 'desc'

        if sort_field == 'transaction_date':
            all_transactions.sort(key=lambda x: (x['transaction_date'] or date.min, x['transaction_id']),
                                  reverse=sort_reverse)
        elif sort_field == 'transaction_type':
            all_transactions.sort(key=lambda x: (x['transaction_type'], x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'transaction_number':
            all_transactions.sort(key=lambda x: (x['transaction_number'] or '', x['transaction_id']),
                                  reverse=sort_reverse)
        elif sort_field == 'vendor_name':
            all_transactions.sort(key=lambda x: (x['vendor_name'] or '', x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'item_name':
            all_transactions.sort(key=lambda x: (x['base_item_name'] or '', x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'quantity':
            all_transactions.sort(key=lambda x: (x['quantity'], x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'unit_price':
            all_transactions.sort(key=lambda x: (x['unit_price'], x['transaction_id']), reverse=sort_reverse)
        elif sort_field == 'total_cost':
            all_transactions.sort(key=lambda x: (x['total_cost_original'], x['transaction_id']), reverse=sort_reverse)
        else:
            all_transactions.sort(key=lambda x: (x['transaction_date'] or date.min, x['transaction_id']),
                                  reverse=sort_reverse)

        # ========== FORMAT RESULTS FOR FRONTEND ==========
        transactions = []
        for tx in all_transactions:
            if tx.get('variation_name'):
                item_name = f"{tx['base_item_name']} ({tx['variation_name']})"
            else:
                item_name = tx['base_item_name']

            transactions.append({
                'transaction_date': tx['transaction_date'].isoformat() if tx['transaction_date'] else None,
                'transaction_type': tx['transaction_type'],
                'transaction_number': tx['transaction_number'],
                'transaction_id': tx['transaction_id'],
                'vendor_name': tx['vendor_name'],
                'item_name': item_name,
                'item_code': tx.get('item_code', '-'),
                'category_name': tx.get('category_name', '-'),
                'uom': tx.get('uom', 'pcs'),
                'quantity': round(tx['quantity'], 2),
                'unit_price': round(tx['unit_price'], 2),
                'total_cost': round(tx['total_cost_original'], 2)
            })

        logger.info(f"Total formatted transactions: {len(transactions)}")
        return transactions

    except Exception as e:
        logger.error(f"Error in _get_purchases_by_item_detail_data: {e}\n{traceback.format_exc()}")
        return []
