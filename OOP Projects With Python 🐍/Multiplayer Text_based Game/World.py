import random

class World :
    def __init__(self, size):
        self.size = size
        self.grid = [["Empty for _ in range(size)] for _ in range(size)]"]]

    def place_monsters(self, num_monsters):
        for _ in range(num_monsters):
            x,y = random.randint(0,self.size-1), random.randint(0,self.size-1)
            self.grid[x][y]= "Monster"

    def place_treasures(self, num_treasures):
        for _ in range(num_treasures):
            x,y = random.randint(0,self.size-1), random.randint(0,self.size-1)
            self.grid[x][y]= "Treasure"