@app.route('/download_income_expense_report')
@login_required
def download_income_expense_report():
    with Session() as db_session:
        app_id = current_user.app_id

        # Fetch and aggregate expense data
        expenses = db_session.query(
            Category.category.label("category"),
            ChartOfAccounts.sub_category.label("subcategory"),
            func.sum(Transaction.amount).label("amount")
        ).join(Category, Transaction.category_id == Category.id)\
         .join(ChartOfAccounts, Transaction.subcategory_id == ChartOfAccounts.id)\
         .filter(
            Transaction.app_id == app_id,
            Transaction.transaction_type == 'Expense',
            Transaction.dr_cr == "D"  # Only select debit entries for expenses
         ).group_by(Category.id, ChartOfAccounts.id).all()

        # Fetch and aggregate income data
        income = db_session.query(
            Category.category.label("category"),
            ChartOfAccounts.sub_category.label("subcategory"),
            func.sum(Transaction.amount).label("amount")
        ).join(Category, Transaction.category_id == Category.id)\
         .join(ChartOfAccounts, Transaction.subcategory_id == ChartOfAccounts.id)\
         .filter(
            Transaction.app_id == app_id,
            Transaction.transaction_type == 'Income',
            Transaction.dr_cr == "C"  # Only select credit entries for income
         ).group_by(Category.id, ChartOfAccounts.id).all()

        # Calculate totals
        total_income = sum(item.amount for item in income)
        total_expenses = sum(item.amount for item in expenses)
        net_income = total_income - total_expenses

        # Prepare PDF layout
        buffer = BytesIO()
        doc = SimpleDocTemplate(buffer, pagesize=letter, rightMargin=72, leftMargin=72, topMargin=72, bottomMargin=72)
        styles = getSampleStyleSheet()
        bold_style = styles['Heading2']

        elements = []

        # Income Section
        elements.append(Paragraph("Income", bold_style))
        income_data = [["Category", "Subcategory", "Amount"]]
        current_category = None
        category_subtotal = 0

        # Sort the income by category
        income.sort(key=lambda x: x.category)

        for item in income:
            # Check if the category has changed
            if item.category != current_category:
                if current_category is not None:
                    # Add subtotal for the previous category
                    income_data.append(["", "Subtotal", f"{category_subtotal:,.2f}"])

                # Update current category and reset subtotal
                current_category = item.category
                category_subtotal = 0

                # Append the new category only once
                income_data.append([item.category, "", ""])  # Add category row with empty subcategory and amount
                print(f'Item category is {current_category}')

            # Accumulate the amount for the current category
            category_subtotal += item.amount
            income_data.append(["", item.subcategory, f"{item.amount:,.2f}"])  # Append subcategory and amount

        # Add final subtotal for the last category
        if current_category is not None:
            income_data.append(["", "Subtotal", f"{category_subtotal:,.2f}"])

        # Add total income
        income_data.append(["Total Income", "", f"{total_income:,.2f}"])

        # Add Income Table with word wrapping
        income_table = Table(income_data, colWidths=[150, 150, 100])
        income_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#34495E")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('WORDWRAP', (0, 0), (-1, -1), True)  # Ensures wrapping
        ]))
        elements.append(income_table)
        elements.append(Spacer(1, 12))

        # Expense Section
        elements.append(Paragraph("Expense", bold_style))
        expense_data = [["Category", "Subcategory", "Amount"]]
        current_category = None
        category_subtotal = 0

        # Sort the expenses by category
        expenses.sort(key=lambda x: x.category)

        for item in expenses:
            # Check if the category has changed
            if item.category != current_category:
                if current_category is not None:
                    # Add subtotal for the previous category
                    expense_data.append(["", "Subtotal", f"{category_subtotal:,.2f}"])

                # Update current category and reset subtotal
                current_category = item.category
                category_subtotal = 0

                # Append the new category only once
                expense_data.append([item.category, "", ""])  # Add category row with empty subcategory and amount
                print(f'Item Expense category is {current_category}')

            # Accumulate the amount for the current category
            category_subtotal += item.amount
            expense_data.append(["", item.subcategory, f"{item.amount:,.2f}"])  # Append subcategory and amount

        # Add final subtotal for the last category
        if current_category is not None:
            expense_data.append(["", "Subtotal", f"{category_subtotal:,.2f}"])

        # Add total expenses
        expense_data.append(["Total Expenses", "", f"{total_expenses:,.2f}"])

        # Add Expense Table with word wrapping
        expense_table = Table(expense_data, colWidths=[150, 150, 100])
        expense_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#34495E")),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('WORDWRAP', (0, 0), (-1, -1), True)  # Ensures wrapping
        ]))
        elements.append(expense_table)
        elements.append(Spacer(1, 12))

        # Net Income Section
        net_income_data = [["Net Income", "", f"{net_income:,.2f}"]]
        net_income_table = Table(net_income_data, colWidths=[150, 150, 100])
        net_income_table.setStyle(TableStyle([
            ('TEXTCOLOR', (0, 0), (-1, -1), colors.black),
            ('ALIGN', (2, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, -1), 'Helvetica-Bold'),
        ]))
        elements.append(net_income_table)

        # Generate and save PDF
        doc.build(elements)
        buffer.seek(0)
        return send_file(buffer, as_attachment=True, download_name=f"Income_Expense_Report_{datetime.datetime.now().strftime('%Y%m%d')}.pdf", mimetype='application/pdf')
