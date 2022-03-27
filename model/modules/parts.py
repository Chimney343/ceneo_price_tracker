from typing import Union


class Part:
    def __init__(
        self,
        name: str,
        price: Union[str, int, float],
        category: str,
        brand: str = None,
        part_id: Union[str, int] = None,
        shop: str = None,
        price_format: str = None,
        value: str = None,
        penny: str = None,
    ):
        self.name = name
        self.price = self.price_string_to_float(price)
        self.category = category
        self.brand = brand
        self.part_id = part_id
        self.shop = shop
        self.price_format = price_format
        self.value = value
        self.penny = penny

    def price_string_to_float(self, price):
        return float(price.replace(",", ".").replace(" ", ""))

    def __repr__(self):
        return repr(f"{self.name} / @ {self.shop} / @ {self.price}")
