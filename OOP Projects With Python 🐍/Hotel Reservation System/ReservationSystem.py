import json
from Room import Room
from Customer import Customer,PremiumCustomer



class ReservationSystem:
    def __init__(self):
        self.rooms = []
        self.customers = []

    def add_room(self, room):
        if room in self.rooms:
            raise Exception( f" room already exists")
        self.rooms.append(room)


    def add_customer(self, customer):
        if customer in self.customers:
            raise Exception( f"customer already exists")
        self.customers.append(customer)

    def find_available_rooms(self, room_type):
        return [room for room in self.rooms if room.room_type == room_type and room.check_available()]

    def make_reservation(self, room, customer):
        if room.check_available():
            room.book_room()
            customer.add_room(room)
        else :
            raise Exception (f"Room number {room.room_number} is not available")

    def cancel_reservation(self, room, customer):
        customer.cancel_reservation(room)
        room.release_room()


    def generate_invoice(self, customer):
        total = sum(room.price for room in customer.booked_rooms)
        if isinstance (customer, PremiumCustomer):
            total = customer.calculate_discount(total)
        return {
            "Customer": customer.name,
            "Contact": customer.contact_name,
            "Email": customer.email,
            "Total Amount": total,
            "Booked Rooms": [room.room_number for room in customer.booked_rooms]
        }


    def save_to_file(self, filename):
        data = {
            "Rooms": [
                {
                    "room_number" : room.room_number,
                    "room_type": room.room_type,
                    "room_price": room.price,
                    "availability": room.availability,
                }
                for room in self.rooms
            ],
            "Customers": [
                {
                    "Customer" : customer.name,
                    "contact": customer.contact,
                    "email": customer.email,
                    "booked_rooms": [room.room_number for room in customer.booked_rooms],
                }
                for customer in self.customers
            ]
        }
        with open(filename, "w") as file:
            json.dump(data, file)


    def load_from_file(self, filename):
        with open(filename, "r") as file:
            data = json.load(file)
            self.rooms = [
                Room(r["room_number"], r["room_type"], r["price"]) for r in data["rooms"]
            ]
            for room, room_data in zip(self.rooms, data["rooms"]):
                room.availability = room_data["availability"]
            self.customers = [
                Customer(c["name"], c["contact"], c["email"]) for c in data["customers"]
            ]


