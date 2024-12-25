# Expense Tracker Application

class User :
    def __init__(self, username, password):
        self.username = username
        self.password = password
        self.expenses = []

    def add_expense(self, expense):
        self.expenses.append(expense)

    def remove_expense(self, expense_id):
        self.expenses = [ exp for exp in self.expenses if exp.expense_id != expense_id]

    def view_expenses(self):
        for expense in self.expenses:
            print(expense)


