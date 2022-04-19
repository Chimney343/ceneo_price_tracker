import pandas as pd

from model.modules.parts import Product


class Basket:
    def __init__(self, name):
        self.name = name
        self.products = []

    def add_part(self, part: Product):
        self.products.append(part)

    def make_df(self):
        if self.products == []:
            self.df = pd.DataFrame(pd.Series(None, name=self.name))
        else:
            d = {part.name: part.__dict__ for part in self.products}
            df = pd.DataFrame(data=d).T.drop(labels=['name'], axis=1)
            df['basket_name'] = self.name
            self.df = df

    def show(self):
        print(f"{self.name} basket:")
        for part in self.products:
            print("\t", part)
