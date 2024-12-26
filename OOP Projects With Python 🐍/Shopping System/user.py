#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 11:20:45 2024

@author: aminehmimou
"""

class User :
    def __init__(self, username, password):
        self.username = username
        self.password = password
        
    def __str__(self):
        return f"User : {self.username}"

class Customer(User) :
    def __init__(self, username, password, email)     :
        super().__init__(username, password)
        self.email = email
        
    def __str__(self):
        return f"Customer : {self.username}, Email : {self.email}"
    
class Admin(User):
    def __init__(self, username, password):
        super().__init__(username, password)
        
        
    def manage_product(self, product, quantity):
        """admin can directly update product stock"""
        
        product.update_stock(quantity)
        
        
if __name__ == "__main__" : 

    customer1  = Customer("John_doe", "securepassword", "john@example.com")
    print(customer1)
    admin1= Admin("admin_user", "adminpassword")
    print(admin1)
    
    