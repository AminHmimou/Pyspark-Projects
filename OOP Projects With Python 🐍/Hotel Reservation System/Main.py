from ReservationSystem import ReservationSystem
from Room import Room
from RoomType import SingleRoom, DoubleRoom, SuiteRoom
from Customer import Customer, PremiumCustomer


def main():
    system = ReservationSystem

    room=SingleRoom(11)
    system.add_room(room)
    system.add_room(DoubleRoom(201))
    system.add_room(SuiteRoom(301))

    customer1 = Customer("Alice", "123456789", "alice@example.com")
    premium_customer = PremiumCustomer("Bob", "987654321", "bob@example.com")
    system.add_customer(customer1)
    system.add_customer(premium_customer)

    room1 = system.find_available_rooms('Single')[0]
    system.make_reservation(customer1, room1)


    room2 =system.find_available_rooms('suite')[0]
    system.make_reservation(premium_customer, room2)


    print(system.generate_invoice(customer1))
    print(system.generate_invoice(premium_customer))

    # Save to and load from file
    system.save_to_file("hotel_data.json")
    system.load_from_file("hotel_data.json")
    print("Data loaded successfully!")

if __name__ == "__main__":
    main()




