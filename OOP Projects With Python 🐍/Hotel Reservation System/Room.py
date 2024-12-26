
class Room:
    def __init__(self, room_number, room_type, price) :
        self.room_number = room_number
        self.room_type = room_type
        self.price = price
        self.availability = True

    def check_available(self) :
        return self.availability

    def book_room(self) :
        if not self.availability :
            raise Exception(f"Room {self.room_number} not available")
        self.availability = False

    def release_room(self):
        self.availability = True


    def __repr__(self):
        return f"Room number {self.room_number} with {self.room_type} bed costs {self.price} "



if __name__ =="__main__" :
    room1 = Room(1, "Single", 100)
    room2= Room(2, "Single", 200)

    print(room1.check_available())
    print(room1)
    room1.book_room()
    print(room1.check_available())
    print(room1)
    room1.release_room()
    print(room1.check_available())






