#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Nov  4 11:47:34 2024

@author: aminehmimou
"""


class Product : 
    def __init__(self, name, price, category, stock) :
        self.name = name
        self.price = price
        self.category = category
        self.stock = stock
        
    def __str__(self):
        return f"{self.name} - {self.category} : ${self.price} (Stock : {self.stock})"
    
    def update_stock (self, quantity):
        """ Updates the stock by adding the specified quantity (can be negtive for reduction) """
        if self.stock + quantity >=0:
           self.stock += quantity
           print(f"Stock updated. New stock for {self.name} = {self.stock}")
        else :
            print("Error : Nt enough stock to reduce by that amount.")
            
            
            
            
if __name__ == "__main__":
    product1 = Product("laptop", 1200.0, "electronics", 50)
    print(product1)
    product1.update_stock(-5)
    print(product1)
    