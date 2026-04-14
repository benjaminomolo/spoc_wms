def calculate_fifo_cogs(db_session, item_id, quantity_sold, app_id):
    """
    Calculate COGS using FIFO for a given item and quantity.
    Returns the total COGS amount.
    """
    total_cogs = 0.0
    remaining_quantity = quantity_sold

    # Fetch the oldest inventory entries for the item
    entries = db_session.query(InventoryEntry).filter(
        InventoryEntry.app_id == app_id,
        InventoryEntry.inventory_item_id == item_id,
        InventoryEntry.quantity > 0  # Only consider entries with available stock
    ).order_by(InventoryEntry.transaction_date.asc()).all()

    for entry in entries:
        if remaining_quantity <= 0:
            break

        # Determine the quantity to deduct from this entry
        deduct_quantity = min(remaining_quantity, entry.quantity)

        # Calculate the COGS for this entry
        total_cogs += deduct_quantity * entry.unit_price

        # Update the remaining quantity
        remaining_quantity -= deduct_quantity

    if remaining_quantity > 0:
        raise Exception(f"Insufficient stock for item {item_id}. Could only deduct {quantity_sold - remaining_quantity} out of {quantity_sold}.")

    return total_cogs


def update_inventory_quantity(db_session, item_id, quantity_sold, sales_invoice_id, created_by, app_id):
    """
    Update inventory quantity and create an InventoryEntry record for the sale.
    """
    # Fetch the inventory item
    inventory_item = db_session.query(InventoryItem).get(item_id)
    if not inventory_item:
        raise Exception(f"Inventory item {item_id} not found")

    # Check if there is sufficient stock
    if inventory_item.quantity < quantity_sold:
        raise Exception(f"Insufficient stock for item {inventory_item.name}")

    # Reduce the inventory quantity
    inventory_item.quantity -= quantity_sold

    # Create an InventoryEntry record for the outgoing stock
    inventory_entry = InventoryEntry(
        app_id=app_id,
        inventory_item_id=item_id,
        inventory_category_id=inventory_item.category_id,
        inventory_subcategory_id=inventory_item.subcategory_id,
        project_id=None,  # Optional, if linked to a project
        transaction_date=datetime.datetime.now().date(),
        supplier_id=None,  # No supplier for sales
        date_added=datetime.datetime.now().date(),
        created_by=created_by,
        updated_by=None,  # No update for now
        from_location=inventory_item.location_id,  # Current location of the item
        to_location=None,  # No destination for outgoing stock
        lot=None,  # Optional, if linked to a batch
        stock_movement="out",  # Outgoing stock for sales
        variation_id=inventory_item.variation_id,  # Optional, if linked to a variation
        attribute_id=inventory_item.attribute_id,  # Optional, if linked to an attribute
        currency_id=inventory_item.currency_id,  # Currency of the item
        uom=inventory_item.uom,  # Unit of measurement
        unit_price=inventory_item.unit_price,  # Cost price of the item
        expiration_date=None,  # Optional, if the item has an expiration date
        selling_price=inventory_item.selling_price,  # Selling price of the item
        quantity=quantity_sold,  # Quantity sold
        is_posted_to_ledger=False,  # Not posted to ledger yet
        source_type="sales_invoice",  # Link to sales invoice
        source_id=sales_invoice_id  # Link to the specific sales invoice
    )

    # Add the inventory entry to the session
    db_session.add(inventory_entry)
    db_session.commit()


@app.route('/post_sales_invoice_to_ledger', methods=['POST'])
@login_required
def post_sales_invoice_to_ledger():
    db_session = Session()
    try:
        # Get form data from the modal
        invoice_id = request.form.get('ledgerInvoiceId')
        debit_account_id = request.form.get('debitAccount')
        debit_account_category_id = request.form.get('debitAccountCategory')
        credit_account_id = request.form.get('creditAccount')
        credit_account_category_id = request.form.get('creditAccountCategory')
        invoice_amount = float(request.form.get('ledgerInvoiceAmount'))

        # Optional fields for Inventory and COGS
        inventory_account_id = request.form.get('inventoryAccount')
        inventory_account_category = request.form.get('inventoryAccountCategory')
        cogs_account_id = request.form.get('cogsAccountCategory')
        cogs_account_category = request.form.get('cogsAccount')

        print(f'Form data is {request.form}')

        # Fetch the sales invoice
        sales_invoice = db_session.query(SalesInvoice).get(invoice_id)
        if not sales_invoice:
            return jsonify(success=False, message="Invoice not found"), 404

        if sales_invoice.is_posted_to_ledger:
            return jsonify(success=False, message="Invoice already posted to ledger"), 400

        # Step 1: Record the sale (revenue)
        # Debit Accounts Receivable
        create_transaction(
            db_session=db_session,
            transaction_type="Asset",  # Accounts Receivable is an Asset
            date=sales_invoice.invoice_date,
            category_id=debit_account_category_id,  # Optional, if needed
            subcategory_id=debit_account_id,  # Accounts Receivable account
            currency=sales_invoice.currency,
            amount=invoice_amount,
            dr_cr="D",  # Debit
            description=f"Invoice {sales_invoice.invoice_number}",
            payment_mode_id=None,  # Optional, if needed
            project_id=None,  # Optional, if needed
            vendor_id=sales_invoice.customer_id,
            created_by=sales_invoice.created_by,
            source_type="sales_invoice",
            source_id=sales_invoice.id,
            app_id=sales_invoice.app_id
        )

        # Credit Sales Revenue
        create_transaction(
            db_session=db_session,
            transaction_type="Income",  # Sales Revenue is an Income
            date=sales_invoice.invoice_date,
            category_id=credit_account_category_id,  # Optional, if needed
            subcategory_id=credit_account_id,  # Sales Revenue account
            currency=sales_invoice.currency,
            amount=invoice_amount,
            dr_cr="C",  # Credit
            description=f"Invoice {sales_invoice.invoice_number}",
            payment_mode_id=None,  # Optional, if needed
            project_id=None,  # Optional, if needed
            vendor_id=sales_invoice.customer_id,
            created_by=sales_invoice.created_by,
            source_type="sales_invoice",
            source_id=sales_invoice.id,
            app_id=sales_invoice.app_id
        )

        # Step 2: Record the cost of goods sold (COGS) for inventory items (if applicable)
        if inventory_account_id and cogs_account_id:
            for item in sales_invoice.invoice_items:
                if item.item_type == "inventory":
                    # Calculate COGS using FIFO
                    try:
                        cogs_amount = calculate_fifo_cogs(
                            db_session=db_session,
                            item_id=item.inventory_item_variation_link.inventory_item.id,
                            quantity_sold=item.quantity,
                            app_id=sales_invoice.app_id
                        )
                    except Exception as e:
                        db_session.rollback()
                        return jsonify(success=False, message=str(e)), 400

                    # Debit Cost of Goods Sold (COGS)
                    create_transaction(
                        db_session=db_session,
                        transaction_type="Expense",  # COGS is an Expense
                        date=sales_invoice.invoice_date,
                        category_id=cogs_account_category,  # Optional, if needed
                        subcategory_id=cogs_account_id,  # COGS account
                        currency=sales_invoice.currency,
                        amount=cogs_amount,
                        dr_cr="D",  # Debit
                        description=f"COGS for Invoice {sales_invoice.invoice_number}, Item {item.inventory_item_variation_link.inventory_item.item_name}",
                        payment_mode_id=None,  # Optional, if needed
                        project_id=None,  # Optional, if needed
                        vendor_id=sales_invoice.customer_id,
                        created_by=sales_invoice.created_by,
                        source_type="sales_invoice",
                        source_id=sales_invoice.id,
                        app_id=sales_invoice.app_id
                    )

                    # Credit Inventory
                    create_transaction(
                        db_session=db_session,
                        transaction_type="Asset",  # Inventory is an Asset
                        date=sales_invoice.invoice_date,
                        category_id=inventory_account_category,  # Optional, if needed
                        subcategory_id=inventory_account_id,  # Inventory account
                        currency=sales_invoice.currency,
                        amount=cogs_amount,
                        dr_cr="C",  # Credit
                        description=f"COGS for Invoice {sales_invoice.invoice_number}, Item {item.inventory_item_variation_link.inventory_item.item_name}",
                        payment_mode_id=None,  # Optional, if needed
                        project_id=None,  # Optional, if needed
                        vendor_id=sales_invoice.customer_id,
                        created_by=sales_invoice.created_by,
                        source_type="sales_invoice",
                        source_id=sales_invoice.id,
                        app_id=sales_invoice.app_id
                    )

                    # Update inventory quantity and create an InventoryEntry record
                    update_inventory_quantity(
                        db_session=db_session,
                        item_id=item.inventory_item_variation_link.inventory_item.id,
                        quantity_sold=item.quantity,
                        sales_invoice_id=sales_invoice.id,
                        created_by=sales_invoice.created_by,
                        app_id=sales_invoice.app_id
                    )

        # Mark the invoice as posted to the ledger
        sales_invoice.is_posted_to_ledger = True
        db_session.commit()

        flash("Invoice posted to ledger successfully", "success")
        return redirect(url_for("invoice_management"))

    except Exception as e:
        db_session.rollback()
        return jsonify(success=False, message=str(e)), 500
    finally:
        db_session.close()
