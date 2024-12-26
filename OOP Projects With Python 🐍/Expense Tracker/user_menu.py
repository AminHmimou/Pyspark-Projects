from Expense import Expense
from ExpenseTracker import ExpenseTracker
from User import User



def user_menu(user):
    while True:
        print("\n1. Add Expense")
        print("\n2. Remove Expense")
        print("\n3. View Expenses")
        print("\n4. Logout")
        choice = input('Enter your choice : ')

        if choice == "1":
            amount = float(input("Enter amount : "))
            category = input("Enter category : ")
            date = input("Enter date (YYYY-MM-DD) : ")
            description = input("Enter description : ")
            expense = Expense(amount, category, date, description)
            User.add_expense(expense)
            print("Expense added successfully")

        elif choice == "2":
            expense_id =input("Enter expense id : ")
            User.remove_expense(expense_id)
            print("Expense removed successfully")

        elif choice == "3":
            print("\nYour Expense List")
            User.view_expenses()

        elif choice == "4":
            print("Logging out...")
            break

        else :
            print("Invalid choice. Try again")


if __name__ == "__user_menu__":
    user_menu(User)

