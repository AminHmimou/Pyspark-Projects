class Customer:
    def __init__(self, name, contact, email):
        self.name = name
        self.contact = contact
        self.email = email
        self.booked_rooms =[]

    def add_booking(self, room):
        if room is self.booked_rooms:
            raise Exception(f"you already booked this room number {room.room_number}")
        self.booked_rooms.append(room)

    def cancel_booking(self, room):
        if room not in self.booked_rooms:
            raise Exception (f" you didn't book this room number {room.room_number}")
        self.booked_rooms.remove(room)

    def __repr__(self):
        return f"the customer M/Mr {self.name} has booked {len(self.booked_rooms)} rooms, to contact her/him use this : {self.contact} or {self.email}"



class PremiumCustomer(Customer):
    def __init__(self, name, contact, email):
        super().__init__(name,contact, email)

    def calculate_discount(self, total):
        return total * 0.9


if __name__ == "__main__":
    from Room import Room
    room1= Room(1, "Single", 50)
    customer1 = Customer("John", "0786456758", "email@gmail.com")
    customer1.add_booking(room1)

    print(customer1)


