from functools import reduce

import pandas as pd
import requests
import validators
from bs4 import BeautifulSoup

from model.modules.baskets import Basket
from model.modules.parts import Part


class CeneoSummaryPage:
    def __init__(self, url):
        assert validators.url(url), "Invalid url."
        self.url = url
        self.baskets = {}
        self.products = set()

    def _get_response(self, url):
        self.response = requests.get(self.url)

    def _parse_page(self, url):
        self._get_response(url)
        if self.response.status_code == 200:
            self.page = BeautifulSoup(self.response.text, "html.parser")

    def _parse_products(self):
        for tag in self.page.find_all('td'):
            # Product name is hidden under 'input' and further under 'img' tag.
            if tag.find('input'):
                img = tag.find('img', alt=True)
                self.products.add(img['alt'])

    def _slice_product_basket_tags(self):
        tags = self.page.find_all("td")
        # Make a lookup with products as keys and a list of their basket tags as values.
        product_basket_tags_lookup = dict.fromkeys(self.products)
        n_products = len(self.products)

        # Slice the tag list into n_products chunks.
        for i in range(0, len(tags), n_products):
            product_tags = tags[i : i + n_products]
            # First tag always contains product names.
            product_name = product_tags[0].find("img", alt=True)["alt"]
            # Remaining tags always contain product basket offers.
            product_basket_tags_lookup[product_name] = product_tags[1:]

        self.product_basket_tags_lookup = product_basket_tags_lookup

    def _make_baskets(self):
        basket_names = set()
        for tag in self.page.find_all('td'):
            # Basket names are hidden under every tag that is different to 'input'.
            if tag.find('input'):
                continue
            basket_names.add(tag['class'][0])

        self.baskets = {basket_name: Basket(name=basket_name) for basket_name in basket_names}

    def _read_basket_tag(self, basket_tag):
        basket_name = basket_tag["class"][0]
        basket_offer_keys = [span["class"][0] for span in basket_tag.find_all("span")]
        basket_offer_values = [span.text for span in basket_tag.find_all("span")]
        return basket_name, basket_offer_keys, basket_offer_values

    def _fill_baskets(self):
        for product, basket_tag_list in self.product_basket_tags_lookup.items():
            for basket_tag in basket_tag_list:
                basket_name, basket_offer_keys, basket_offer_values = self._read_basket_tag(basket_tag)
                if basket_offer_keys:
                    part_data = dict(zip(basket_offer_keys, basket_offer_values))
                    part = Part(
                        name=product,
                        shop=part_data["offer-shop-domain"],
                        price_format=part_data["price-format"],
                        price=part_data["price"],
                        value=part_data["value"],
                        penny=part_data["penny"],
                    )

                    # Add part to current basket.
                    self.baskets[basket_name].add_part(part)

    def _make_df(self):
        dfs = []
        for basket in self.baskets.values():
            basket.make_df()
            dfs.append(basket.df)

        df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how='outer'), dfs)
        self.df = df

    def make(self):
        self._parse_page(url=self.url)
        self._parse_products()
        self._slice_product_basket_tags()
        self._make_baskets()
        self._fill_baskets()
        self._make_df()

    def display_baskets(self):
        for basket in self.baskets.values():
            basket.show()
