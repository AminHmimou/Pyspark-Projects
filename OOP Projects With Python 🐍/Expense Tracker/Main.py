from Expense import Expense
from ExpenseTracker import ExpenseTracker
from User import User


def main():
    tracker = ExpenseTracker()
    while True :
        print("\n1. Register")
        print("\n2. Login")
        print("\n3. Exit")

        choice = input('Enter your choice : ')

        if choice =='1':
            username = input("Enter your username : ")
            password = input("Enter password : ")
            tracker.register_user(username, password)

        elif choice =='2':
            username = input("Enter your username : ")
            password = input("Enter password : ")
            user= tracker.authenticate_user(username, password)
            if user :
                user_menu(user)

        elif choice =='3':
            print("Exiting...Goodbye")
            break
        else :
            print("Invalid choice. Try again")

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
            user.add_expense(expense)
            print("Expense added successfully")

        elif choice == "2":
            expense_id =input("Enter expense id : ")
            user.remove_expense(expense_id)
            print("Expense removed successfully")

        elif choice == "3":
            print("\nYour Expense List")
            user.view_expenses()

        elif choice == "4":
            print("Logging out...")
            break

        else :
            print("Invalid choice. Try again")




if __name__ =='__main__':
    main()















