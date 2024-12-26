#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 11:57:29 2024

@author: aminehmimou
"""
from product import Product


class ShoppingCart : 
    def __init__(self):
        self.items = {}
        
    def add_item(self, product, quantity) : 
        """ add a product to the cart with the specified quantity"""
        if product.stock >= quantity : 
            self.items[product] = self.items.get(product, 0) + quantity
            product.update_stock(-quantity)
            print(f"added {quantity} of {product.name} to cart.")
            
        else : 
            print("Not enough stock available")
            
            
    def remove_item (self, product, quantity):
        """remove a product from the cart """
        if product in self.items : 
            if self.items[product] >= quantity :
                self.items[product] -= quantity 
                product.update_stock(quantity)
                print (f"Removed {quantity} of {product.name} from cart.")
                if self.items[product] == 0 : 
                    del self.items[product]
            else : 
                print("You don't have that many in the cart.")
                
        else :
            print(f"{product.name} not in the cart.")
           
            
    def calculate_total (self): 
        """calculate the total price of all items in the cart ."""
        total = sum(product.price * quantity for product , quantity in self.items.items())
        return total 
    
    def display_cart(self):
        """ display all items in the cart with their quantities"""
        if not self.items : 
            print("Your cart is empty")
        else : 
            print("Your shopping cart : ")
            for product, quantity in self.items.items():
                print(f"{product.name} (x{quantity}) - ${product.price * quantity}")
            print(f"Total : ${self.calculate_total()}")    
            
            
            
if __name__ == "__main__" :
    product1 = Product("Laptop", 1200.0, "Electronics", 50)
    product2 = Product("SmartPhone", 800.0, "Electronics", 30)
    cart = ShoppingCart()
    cart.add_item(product1,2)
    cart.add_item(product2, 1)
    cart.display_cart()
    cart.remove_item(product1, 1)
    cart.display_cart()
            
       
        