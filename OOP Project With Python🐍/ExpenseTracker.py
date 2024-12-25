

from User import User

class ExpenseTracker :
    def __init__(self):
        self.users = []

    def register_user(self, username, password):
        user = User(username, password)
        self.users.append(user)
        print(f" User {username} registered successfully")

    def authenticate_user(self, username, password):
        for user in self.users :
            if user.username ==username and user.password == password:
                return user
        print(f" Invalid username  or password")
        return None




















