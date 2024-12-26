from Character import Character


class Monster(Character):
    def __init__(self, name, health, attack_power):
        super().__init__(name, health, attack_power)