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
            d = {
                part.name: [part.brand, part.category, part.price]
                for part in self.parts
            }
            df = pd.DataFrame(d).T.rename(
                columns={0: "brand", 1: "category", 2: self.name}
            )
            df = df.reset_index()
            df = df.set_index(["index", "brand", "category"])
            self.df = df

    def show(self):
        print(f"{self.name} basket:")
        for part in self.parts:
            print("\t", part)
