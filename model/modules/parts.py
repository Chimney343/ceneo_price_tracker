class Part:
    def __init__(self, name: str, shop: str, price_format: str, price: str, value: str, penny: str):
        self.name = name
        self.shop = shop
        self.price_format = price_format
        self.price = self.price_string_to_float(price)
        self.value = value
        self.penny = penny

    def price_string_to_float(self, price):
        return float(price.replace(',', '.').replace(' ', ''))

    def __repr__(self):
        return repr(f"{self.name} / @ {self.shop} / @ {self.price}")

