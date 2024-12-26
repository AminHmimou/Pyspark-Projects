#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 13:52:19 2024

@author: aminehmimou
"""

class Book :
    
    def __init__(self,title, author, isbn, copies=1):
        self.title = title 
        self.author = author
        self.isbn = isbn
        self.copies = copies # number of copies available
        self.borrowed_count = 0 # how many times the book has been borrowed
        
        
    def borrow (self):
        
        if self.copies >0 :
            self.copies -= 1
            self.borrowed_count +=1
            print(f"the book {self.title} has been borrowed")
            return True
            
        else : 
            print(f"the book {self.title} is not available for the moment")
            return False
            
            
    def return_book(self) :
        self.copies += 1
        print(f" A copie of the book {self.title} has been returned")
        
    
    def __str__(self) :
        return f"{self.title} by {self.author} (ISBN : {self.isbn} - Copies available : {self.copies} ) "
    
    
    
    
if __name__ == "__main__" :
 
    book = Book("The Power of Positive Thinking", " Norman Vincent Peale", "123456")
    book.borrow()
    book.return_book()
    book.return_book()
    print(book.__str__())
    
    
    
    
    
    
    
    
        
            