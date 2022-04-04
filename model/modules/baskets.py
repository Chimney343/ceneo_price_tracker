import pandas as pd

from model.modules.parts import Part


class Basket:
    def __init__(self, name):
        self.name = name
        self.parts = []

    def add_part(self, part: Part):
        self.parts.append(part)

    def make_df(self):
        if self.parts == []:
            self.df = pd.DataFrame(pd.Series(None, name=self.name))
        else:
            d = {part.name: part.__dict__ for part in self.parts}
            df = pd.DataFrame(data=d).T.drop(labels=['name'], axis=1)
            df['basket_name'] = self.name
            self.df = df

    def show(self):
        print(f"{self.name} basket:")
        for part in self.parts:
            print("\t", part)
