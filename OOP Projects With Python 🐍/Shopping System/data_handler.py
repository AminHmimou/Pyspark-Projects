#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Nov  6 14:30:48 2024

@author: aminehmimou
"""

import json
from product import Product
from user import Customer, Admin


class DataHandler :
    
    @staticmethod
    def save_data(filename, data) : 
        """Save data (list of dictionaries ) to a JSON file."""
        with open (filename, 'w') as file : 
            json.dump(data, file, indent=4)
        print(f"Data saved to {filename}")
        
    @staticmethod
    def load_data(filename):
        """Load data from a JSON file."""
        try : 
            with open (filename, 'r') as file :
                data = json.load(file)
            print(f"Data loaded from {filename}")
            return data
        except FileNotFoundError : 
            print(f"{filename} not found. Returning empty data.")
            return []
        
        
if __name__ == "__main__" :
    products = [
        Product("Laptop", 1200.0, "Electronics", 50).__dict__,
        Product("SmartPhone", 800.0, "Electronics", 30).__dict__
    ]   
    DataHandler.save_data("products.json", products)
    loaded_products = DataHandler().load_data("products.json")    
    print(loaded_products)
    
    
    
    