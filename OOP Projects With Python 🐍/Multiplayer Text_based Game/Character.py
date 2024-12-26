

import random

class Character:
    def __init__(self,name, health, attack_power):
        self.name = name
        self.health = health
        self.attack_power = attack_power


    def attack(self, target):
        damage = random.randint(self.attack_power -2, self.attack_power+2)
        target.health -= damage
        return f" {self.name} attacks {target.name} for {damage} damage"


    def is_alive(self):
        return self.health > 0




