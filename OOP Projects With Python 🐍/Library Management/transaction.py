#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 15:15:59 2024

@author: aminehmimou
"""

from book import Book
from Member import Member
from datetime import datetime

class Transaction : 
    
    def __init__(self, book, member,transaction_type):
        self.book = book
        self.member = member
        self.transaction_type = transaction_type # borrow or return
        self.date = datetime.now()
        
        
    def __str__(self):
        
        action ="Borrowed" if self.transaction_type == "Borrow" else "Returned"
        return f"{self.member.name} {action} the '{self.book.title}' book on {self.date.strftime('%Y-%m-%d %H:%M:%S')}"
    
    
    
    
if __name__ == "__main__":
    member = Member("Amine", "Amine182m")
    book = Book("The Power of Positive Thinking", " Norman Vincent Peale", "123456")
    transaction = Transaction(book, member, "Borrow") 
    print(transaction)
    
    
        
        
    