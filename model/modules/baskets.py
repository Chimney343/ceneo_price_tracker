from model.modules.parts import Part
from pprint import pprint

class Basket:
    def __init__(self, name):
        self.name = name
        self.parts = []

    def add_part(self, part: Part):
        self.parts.append(part)

    def show(self):
        print(f"{self.name} basket:")
        for part in self.parts:
            print('\t', part)





