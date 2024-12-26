#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 14:54:10 2024

@author: aminehmimou
"""

from product import Product
from user import Customer
from  shopping_cart import ShoppingCart
from order import Order
from data_handler import DataHandler


def main():
    #Load data 
    products = [
        Product("Laptop", 1200.0, "Electronics", 50),
        Product("SmartPhone", 800.0, "Electronics", 30)
    ]
    
    customer = Customer("Amine", "securepassword", "AMine@email.com")
    cart = ShoppingCart()
    orders = []
    
    
    while True : 
        print("\n=== E-commerce System ===")
        print("1. View Products")
        print("2. Add Products to cart")
        print("3. View cart")
        print("4 Checkout")
        print("5. View Order History")
        print("6. Exit")
        choice = input("Enter your choice : ")
        
        if choice =='1' : 
            print("\nAvailable Products: ")
            for idx, product in enumerate(products, start=1):
                print(f"{idx}. {product}")
                
                
        elif choice == '2':
            print("\n Select a product to add to your cart: ")
            for idx, product in enumerate(products, start=1):
                print(f"{idx}. {product}")
            prod_choice = int(input("Enter product nulber: ")) -1
            quantity = int(input("Enter quantity: "))
            if 0 <= prod_choice < len(products): 
                cart.add_item(products[prod_choice], quantity)
            else : 
                print("Invalid choice.")
               
        elif choice == '3' :
            cart.display_cart()
            
            
        elif choice == '4' :
            if cart.items : 
                order = Order(cart, customer)
                order.confirm_order()
                orders.append(order)
                print("\n Order placed successfully!")
                cart = ShoppingCart()
            else :
                print("Your cart is empty")
                
                
        elif choice == '5'     :
            print("\n Order History: ")
            for order in orders : 
                order.display_order()
                
                
        elif choice == '6':
            print("Exiting the system")
            break 
        
        
        else :
            print("Invalid choice. Please select again.")
            
            
            
#run the main program

if __name__ == "__main__":
    main()            
    
    
           
                
                