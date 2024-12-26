#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 15:33:58 2024

@author: aminehmimou
"""

from transaction import Transaction
from book import Book
from Member import Member

class Library :
    def __init__(self):
        self.catalog = {}
        self.members = {}
        self.transactions = []
        
        
    def add_book(self, book):
        if book.isbn in self.catalog: 
            self.catalog[book.isbn].copies +=1
            #print(f"Copies of '{book.title}' increased to {self.catalog[book.isbn].copies}.")

        else : 
            self.catalog[book.isbn] = book
            #print(f"Book '{book.title}' added to catalog with {book.copies} copies.")

        
        
    def register_member(self, member):
        self.members[member.membership_id] = member
        #print(f"Member '{member.name}' has been registered")
        
        
    def borrow_book(self, member_id, isbn):
        if member_id in self.members and isbn in self.catalog :
            member = self.members[member_id]
            book = self.catalog[isbn]
            if book.copies >0 :
                if member.borrow_book(book):
                    transaction = Transaction(book, member, "Borrow")
                    self.transactions.append(transaction)
                    print(f"'{book.title}' Borrowed by {member.name}. Copies left: {book.copies}.")
                    
                else:
                    print(f"{member.name} already borrowed '{book.title}'.")    
                
            else : 
                print(f"Sorry , '{book.title} is currently unavailable")
        else:
            print("Invalid member ID or ISBN")
            
            
                
    def return_book(self, member_id, isbn):
        if member_id in self.members and isbn in self.catalog : 
            member = self.members[member_id]
            book = self.catalog[isbn]
            if member.return_book(book):
                transaction = Transaction(book, member, "return")
                self.transactions.append(transaction)
                print(transaction)
            else : 
                print(f" the book : '{book.title}' was not borrowed by '{member.name}'")
                
        else : 
            print("Invalid member ID or ISBN")
            
            
    def list_books(self):
      for book in self.catalog.values():
          print(book)
          
          
    def list_members(self):
        for member in self.members.values():
            print(member)
            
            
            
            
if __name__ == "__main__":

    book1 = Book("The Power of Positive Thinking", " Norman Vincent Peale", "123456")
    book2 = Book("Rich Dad and Poor dad ", " Robert T.Kiyosaki", "111111")
    book3 = Book("To Kill a Mockingbird", "Harper Lee", "222222")
    book4 = Book("The Great Gatsby", "F. Scott Fitzgerald", "333333")
    book5 = Book("1984", "George Orwell", "444444")
    book6 = Book("Pride and Prejudice", "Jane Austen", "555555")

    
    member1 = Member("Amine", "Amine182") 
    member2 = Member("Aness", "Aness182") 
    member3 = Member("Ayman", "Ayman182") 
    member4 = Member("Saad", "Saad182") 
    member5 = Member("hazim", "Hazim182") 
    
    transaction = Transaction(book1, member1, "Borrow")           
    library = Library()
    
    library.add_book(book1)
    library.add_book(book1)
    library.add_book(book1)
    library.add_book(book2)
    library.add_book(book3)
    library.add_book(book4)
    library.add_book(book5)
    library.add_book(book6)
    library.add_book(book6)
    library.register_member(member1)
    library.register_member(member2)
    library.register_member(member3)
    library.register_member(member4)
    library.register_member(member5)
    library.borrow_book("Amine182", "123456")
    library.borrow_book("Aness182", "123456")
    library.borrow_book("Hazim182", "555555")
    library.return_book("Aness182", "123456")
    library.return_book("Amine182", "123456")
    library.list_books()
    library.list_members()
    
    
    
    
    
    
    
            
            