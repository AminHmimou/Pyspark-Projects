#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 13:20:43 2024

@author: aminehmimou
"""

class Order :
    order_id_counter = 1 # Class varibale to track order IDs
    
    def __init__(self, cart , customer) :
        self.order_id = Order.order_id_counter
        Order.order_id_counter +=1
        self.customer = customer
        self.items = cart.items.copy()
        self.total = cart.calculate_total()
        self.status = "Pending"# Order status can be "Pending", "Confirmed", etc.
        
    def confirm_order(self):
        """Set the order status to Confirmed"""
        self.status = "Confirmed"
        print(f"Order {self.order_id} confirmed for {self.customer.username}" ) 
        
        
    def display_order (self):
        """Display the order details."""
        print(f"Order ID : {self.order_id}")
        print(f"Customer : {self.customer.username}")
        for product, quantity in self.items.items():
            print(f" {product.name} (x{quantity}) - ${product.price * quantity}")
        print(f"Total : ${self.total}")    
        print(f"Status : {self.status}")
        
        
        
if __name__ == "__main__" : 
    from user import Customer
    from shopping_cart import ShoppingCart
    from product import Product

    
    customer1 = Customer("john_doe", "securepassword", "john@email).com")      
    product1 = Product("Laptop", 1200.0, "Electronics", 50)
    product2 = Product("SmartPhone", 800.0, "Electronics", 30)
    cart = ShoppingCart()
    cart.add_item( product1, 1)
    cart.add_item(product2, 1)
    
    order = Order(cart, customer1)
    order.display_order()
    order.confirm_order()
    
    