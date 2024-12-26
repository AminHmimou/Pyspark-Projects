from Room import Room


class SingleRoom(Room):
    def __init__(self, room_number):
        super().__init__(room_number , "Single", 50)

class DoubleRoom(Room):
    def __init__(self, room_number):
        super().__init__(room_number, "Double", 100)

class SuiteRoom(Room):
    def __init__(self, room_number):
        super().__init__(room_number, "Suite", 200)



if __name__ == "__main__":
    room1 = SingleRoom(1)
    room2 = DoubleRoom(2)
    room3 = SuiteRoom(3)

    room1.book_room()
    print(room1)
    room2.book_room()
    room3.book_room()
    print(room2)
    print(room3)

