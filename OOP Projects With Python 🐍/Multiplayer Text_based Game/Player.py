from Character import Character


class Player(Character):
    def __init__(self, name, player_class):
        super().__init__(name, health=100, attack_power =10)
        self.player_class = player_class
        self.inventory = {"gold": 0 , "potions": 0}
        self.experience = 0
        self.level = 1

    def special_ability(self):
        if self.player_class == "Warrior":
            return f" {self.name} produce a shield block, reducing damage by 50%"

        elif self.player_class == "Mage":
            self.health +=10
            return f"{self.name} casts a healing spell, restoring 10 health"

        elif self.player_class == "Archer":
            return f"{self.name} fires a critical arrow, dealing double damage"


    def level_up(self):
        self.level+=1
        self.health +=20
        self.attack_power +=5
        return f"{self.name} leveled up to level {self.level}!"



