from model.modules.parts import Part
import pandas as pd

class Basket:
    def __init__(self, name):
        self.name = name
        self.parts = []

    def add_part(self, part: Part):
        self.parts.append(part)

    def make_df(self):
        if self.parts == []:
            self.df = pd.DataFrame(pd.Series(None, name=self.name))
        d = {part.name: part.price for part in self.parts}
        self.df = pd.DataFrame(pd.Series(d, name=self.name))

    def show(self):
        print(f"{self.name} basket:")
        for part in self.parts:
            print('\t', part)








