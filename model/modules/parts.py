from typing import Union


class Part:
    def __init__(
        self,
        name: str,
        price: Union[str, int, float],
        price_format: str = None,
        value: str = None,
        penny: str = None,
        part_id: Union[str, int] = None,
        category: str = None,
        brand: str = None,
        shop: str = None,
        n_opinions: int = None,

    ):
        self.name = name
        self.price = self.price_string_to_float(price)
        self.price_format = price_format
        self.value = value
        self.penny = penny
        self.part_id = part_id
        self.category = category
        self.brand = brand
        self.shop = shop
        self.n_opinions = n_opinions


    def price_string_to_float(self, price):
        return float(price.replace(",", ".").replace(" ", ""))

    def __repr__(self):
        return repr(f"{self.name} / @ {self.shop} / @ {self.price}")
