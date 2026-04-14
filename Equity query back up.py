equity_query = db_session.query(
            func.concat(
                case(
                    (func.sum(case((Transaction.dr_cr == "D", Transaction.amount), else_=0)) > 0, "Less: "),  # Check if there is a debit balance
                    else_=""
                ),
                Category.category
            ).label("category"),  # Prefix category conditionally
            func.concat(
                case(
                    (func.sum(case((Transaction.dr_cr == "D", Transaction.amount), else_=0)) > 0, "Less: "),  # Check if there is a debit balance
                    else_=""
                ),
                ChartOfAccounts.sub_category
            ).label("subcategory"),  # Prefix subcategory conditionally
            func.sum(
                case(
                    (Transaction.dr_cr.in_(["D", "C"]), Transaction.amount),  # Include both debit and credit entries
                    else_=0  # Default value if the condition is not met
                )
            ).label("amount")
        ).join(Category, Transaction.category_id == Category.id)\
         .join(ChartOfAccounts, Transaction.subcategory_id == ChartOfAccounts.id)\
         .filter(
            Transaction.app_id == app_id,
            Transaction.transaction_type == 'Equity',
            Transaction.dr_cr.in_(["D", "C"])  # Include both debit and credit entries for equity
        )
