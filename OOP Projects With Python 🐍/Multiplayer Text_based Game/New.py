from Monster import Monster
from Monster import Monster
import random

class Game :
    def __init__(self):
        self.players= None
        self.world
        self.positions = {player.name : [0,0] for player in players



    def take_turn (self, player):
        print(f"\n{player.name}'s turn. Current position : {self.positions[player.name]}")
        action = input("make an action (move, inventory, quit").lower()

        if action =="move":
            direction = input("choose direction (up, down , left , right").lower()
            result = self.move(player, direction)
            print(result)

        elif action == "inventory":
            print(f"{player.name}'s inventory: {player.invetory}")

        elif action == "quit":
            print(f"{player.name} quits the game")
            return "quit"

        else:
            print("Invalid action")


    def move(self, player, direction):
        pos = self.positions[player.name]

        if direction == "up" and pos[0] >0:
            pos[0]-=1
        elif direction == "down" and pos[0] <self.world.size-1:
            pos[0]+=1
        elif direction =="left" and pos[1]>0 :
            pos[1]-= 1
        elif direction =="right" and pos[1]<self.world.size-1:
            pos[1]+=1
        else :
            "You can't move in that direction"

        cell = self.world.grid[pos[0]][pos[1]]
        if cell == "Monster":
            return self.battle(player)
        elif cell == "Treasure":
            player.invetory["gold"]+= 10
            return "You found and earned 10 gold"
        elif cell == "Puzzle":
            return self.solve_puzzle(player)
        return "You moved to an empty cell"

    def battle(self, player):

        monster = Monster("Goblin", 50, 10)
        while player.is_alive and monster.is_alive():
            print(player.attack(monster))
            if monster.is_alive():
                print(monster.attack(player))
        if player.is_alive():
            player.experience +=50
            return f" You defeated the monster {monster.name}!"
        return f"{player.name} was defeated by the monster {monster.name}"

    def solve_puzzle(self, player):
        puzzle = random.choice(["what is DAG", " what is 9*6", " what's the capital of Morocco"])
        answer = input(f"Solve this puzzle to proceed : {puzzle}").strip().lower()

        if (puzzle =="what is DAG" and answer ==" direct acyclic graph") or (puzzle =="what is 9*6" and answer =="54") or (puzzle == "what's the capital of Morocco" and answer =="rabat") :
            player.inventory["gold"] +=5
            return "Correct! You earned 5 gold"
        return "wrong answer. Try again next turn"
    






