#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Nov  7 19:49:40 2024

@author: aminehmimou
"""

from book import Book
from Member import Member
from Library import Library


def main():
    library = Library()
    
    while True : 
        print("\n====Library Management System====\n")
        print("1. Add Book")
        print("2. Register Member")
        print("3. Borrow Book")
        print("4. Return Book")
        print("5. List All Books")
        print("6. List All Members")
        print("7. Exit")
        
        choice = input("Enter Your Choice : ")
        print("\n=================================\n")
        
        if choice == '1':
            title = input("Book Title : ")
            author = input("Author Name : ")
            isbn = input("ISBN : ")
            copies = int(input("Number of Copies : "))
            book = Book(title, author, isbn,copies)
            
            library.add_book(book)
            
        elif choice == '2' : 
            name = input("Member Name : ")
            membership_id = input("Membership ID : ")
            member  = Member(name, membership_id)
            library.register_member(member)
            
        elif choice == '3':
            member_id = input("Enter Your member ID : ")
            isbn = input("Enter ISBN of the book you wanna borrow : ")
            library.borrow_book(member_id, isbn)
            
        elif choice == '4':
            member_id = input("Enter Your member ID : ")
            isbn = input("Enter ISBN of the book you wanna return : ")
            library.return_book(member_id, isbn)
            
        elif choice == '5':
            print("\nAvailable Books : ")
            library.list_books()
            
        elif choice == '6':
            print("\nLibrary Members : ")
            library.list_members()
            
        elif choice == '7':
            print("\n========See You Next Time========")
           
            break
        
        else: 
            print("Invalid Choice. Please Choose a number from 1 to 7 ")
            
            
            
if __name__ == "__main__" : 
    main()
            
            
            
            
            
    