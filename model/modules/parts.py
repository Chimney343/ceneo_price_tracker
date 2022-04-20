from typing import Union, List, Tuple


class Product:
    def __init__(
        self,
        name: str,
        price: Union[str, int, float],
        price_format: str = None,
        value: str = None,
        penny: str = None,
        product_id: Union[str, int] = None,
        category: str = None,
        brand: str = None,
        shop_name: str = None,
        shop_id: Union[str, int] = None,
        n_opinions: int = None,
        offers: List[Tuple] = [],
        status: str = 'ok',
    ):
        self.name = name
        self.price = self.price_string_to_float(price)
        self.price_format = price_format
        self.value = value
        self.penny = penny
        self.part_id = product_id
        self.category = category
        self.brand = brand
        self.shop_name = shop_name
        self.shop_id = shop_id
        self.n_opinions = n_opinions
        self.offers = offers
        self.status = status

    def price_string_to_float(self, price):
        return float(price.replace(",", ".").replace(" ", ""))

    def __repr__(self):
        return repr(f"{self.name} / @ {self.shop_name} / @ {self.price}")
