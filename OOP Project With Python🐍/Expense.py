

class Expense:

    _id_counter = 1
    def __init__(self,amount , category, date, description=""):
        self.expense_id = Expense._id_counter
        self.amount = amount
        self.category = category
        self.date = date
        self.description = description


    def _str_(self):
        return (f" ID: {self.expense_id} , Amount:  {self.amount} , Category : {self.category},"
                f"Date :  {self.date} , Description : {self.description}")






