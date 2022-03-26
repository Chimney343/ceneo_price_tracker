class Part:
    def __init__(self, name: str, shop: str, price_format: str, price: str, value: str, penny: str):
        self.name = name
        self.shop = shop
        self.price_format = price_format
        self.price = price
        self.value = value
        self.penny = penny

    def __repr__(self):
        return repr(f"{self.name} / @ {self.shop} / @ {self.price}")

