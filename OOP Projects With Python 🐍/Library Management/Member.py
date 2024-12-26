#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 14:45:01 2024

@author: aminehmimou
"""
from book import Book



class Member :
    def __init__(self, name, membership_id):
        self.name = name
        self.membership_id = membership_id
        self.borrowed_books = []
        
        
    def borrow_book(self, book):
        if book.borrow() :
            self.borrowed_books.append(book.isbn)
            print(f"the book {book.title} has been added to borrowed books")
            return True
        else: 
            
            print("the book {book.title} was not borrowed")
            return False
        
        
        
    def return_book(self, book):
        if book.isbn in self.borrowed_books : 
            book.return_book()
            self.borrowed_books.remove(book.isbn)
            return True
        return False
    
    
    def __str__(self):
        return f"Member: {self.name}, ID: {self.membership_id}, Borrowed Books {len(self.borrowed_books)}"
    
    
    

    
if __name__ == "__main__":
    book1 = Book("The Power of Positive Thinking", " Norman Vincent Peale", "123456")
    book2 = Book("Rich Dad and Poor dad ", " Robert T.Kiyosaki", "111111")
    
    member = Member("Amine", "Amine182m")
    
    member.borrow_book(book1)
    member.borrow_book(book2)
    member.return_book(book1)
    print(member.__str__())
                    
    
    
            
         
    