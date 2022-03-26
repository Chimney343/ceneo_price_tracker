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
        """
        Gets HTTP response from url.
        :param url:
        """
        self.response = requests.get(self.url)

    def _parse_page(self, url):
        """
        Parses a webpage with BeatifoulSoup.
        :param url:
        """
        self._get_response(url)
        if self.response.status_code == 200:
            self.page = BeautifulSoup(self.response.text, "html.parser")

    def _parse_products(self):
        """
        Parses unique products displayed in a Ceneo summary webpage.
        """
        for tag in self.page.find_all('td'):
            # Product name is hidden under 'input' and further under 'img' tag.
            if tag.find('input'):
                img = tag.find('img', alt=True)
                self.products.add(img['alt'])

    def _slice_product_basket_tags(self):
        """
        Slices summary tags to a lookup where each unique product is a key and values are HTML `basket` tags relevant to
        that product.
        """
        tags = self.page.find_all("td")
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
        """
        Makes baskets.Basket objects relevant to current Ceneo summary.
        """
        basket_names = set()
        for tag in self.page.find_all('td'):
            # Basket names are hidden under every tag that is different to 'input'.
            if tag.find('input'):
                continue
            basket_names.add(tag['class'][0])

        self.baskets = {basket_name: Basket(name=basket_name) for basket_name in basket_names}

    def _read_basket_tag(self, basket_tag):
        """
        Read Ceneo `basket` HTML tag.
        :param basket_tag:
        :return:
        """
        basket_name = basket_tag["class"][0]
        basket_offer_keys = [span["class"][0] for span in basket_tag.find_all("span")]
        basket_offer_values = [span.text for span in basket_tag.find_all("span")]
        return basket_name, basket_offer_keys, basket_offer_values

    def _fill_baskets(self):
        """
        Adds products to baskets through analyzing basket tags.
        """
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

    def find_most_expensive_offer(self, part: str, return_type: Union[str, float]):
        """
        Find the most expensive offer and either return it's shop or it's value.
        :param part:
        :param return_type:
        :return:
        """
        price_to_shop_lookup = {}
        for basket in self.baskets.values():
            for basket_part in basket.parts:
                if basket_part.name == part:
                    price_to_shop_lookup[basket_part.price] = basket_part.shop

        if return_type == 'shop':
            return price_to_shop_lookup[max(price_to_shop_lookup.keys())]
        elif return_type == 'price':
            return max(price_to_shop_lookup.keys())

    def find_cheapest_offer(self, part: str, return_type: Union[str, float]):
        """
        Find the cheapest offer and either return it's shop or it's value.
        :param part:
        :param return_type:
        :return:
        """
        price_to_shop_lookup = {}
        for basket in self.baskets.values():
            for basket_part in basket.parts:
                if basket_part.name == part:
                    price_to_shop_lookup[basket_part.price] = basket_part.shop

        if return_type == 'shop':
            return price_to_shop_lookup[min(price_to_shop_lookup.keys())]
        elif return_type == 'price':
            return min(price_to_shop_lookup.keys())

    def _enhance_df(self, df):
        df['cheapest-shop'] = df.index.to_series().apply(self.find_cheapest_offer, return_type='shop')
        df['most-expensive-shop'] = df.index.to_series().apply(self.find_most_expensive_offer, return_type='shop')
        df['cheapest-offer'] = df.index.to_series().apply(self.find_cheapest_offer, return_type='price')
        df['most-expensive-offer'] = df.index.to_series().apply(self.find_most_expensive_offer, return_type='price')
        df['timestamp'] = self.timestamp
        return df

    def _make_df(self):
        """
        Make a dataframe from existing baskets.
        """
        dfs = []
        for basket in self.baskets.values():
            basket.make_df()
            dfs.append(basket.df)

        df = reduce(lambda left, right: pd.merge(left, right, left_index=True, right_index=True, how='outer'), dfs)
        df = self._enhance_df(df)
        self.df = df

    def make(self):
        """
        Main flow; parses the page and products, initializes and fills the baskets, makes the result dataframe.
        """
        self._parse_page(url=self.url)
        # Timestamp is set immediately following the URL parsing.
        self.timestamp = datetime.now()
        self._parse_products()
        self._slice_product_basket_tags()
        self._make_baskets()
        self._fill_baskets()
        self._make_df()

    def display_baskets(self):
        for basket in self.baskets.values():
            basket.show()
