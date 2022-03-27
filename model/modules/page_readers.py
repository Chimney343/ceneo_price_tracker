from functools import reduce

import pandas as pd
import requests
import validators
from bs4 import BeautifulSoup
from datetime import datetime
from typing import Union

from model.modules.baskets import Basket
from model.modules.parts import Part


class CeneoSummaryPageReader:
    def __init__(self, url: str):
        assert validators.url(url), "Invalid url."
        self.timestamp = None
        self.url = url
        self.baskets = {}
        self.part_name_to_id = {}
        self.part_id_to_name = {}

    def _get_response(self, url: str):
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

    def parse_products(self):
        """
        Parses unique products displayed in a Ceneo summary webpage and stores them as lookups.
        """
        part_name_to_id = {}
        for tag in self.page.find_all("td"):
            # Product name is hidden under 'input' and further under 'img' tag.
            if tag.find("input"):
                part_name = tag.find("img", alt=True)["alt"]
                part_id = int(tag.find("input")["value"].replace('"', ""))
                part_name_to_id[part_name] = part_id
        part_id_to_name = {
            part_id: part_name for part_name, part_id in part_name_to_id.items()
        }
        return part_name_to_id, part_id_to_name

    def make_baskets(self):
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
        try:
            basket_part_id = int(
                basket_tag.find("a")["data-productid"].replace('"', "")
            )
        except (TypeError, KeyError):
            basket_part_id = None
        try:
            basket_part_brand = basket_tag.find("a")["data-brand"].replace('"', "")
        except (TypeError, KeyError):
            basket_part_brand = None
        try:
            basket_part_category = basket_tag.find("a")["data-gacategoryname"].replace(
                '"', ""
            )
        except (TypeError, KeyError):
            basket_part_category = None
        basket_span_tag_keys = [
            span["class"][0] for span in basket_tag.find_all("span")
        ]
        basket_span_tag_values = [span.text for span in basket_tag.find_all("span")]
        return (
            basket_name,
            basket_part_id,
            basket_part_brand,
            basket_part_category,
            basket_span_tag_keys,
            basket_span_tag_values,
        )

    def fill_baskets(self):
        """
        Adds products to baskets through analyzing basket tags.
        """
        tags = self.page.find_all("td")
        # Filter basket tags from all tags.
        basket_tags = [tag for tag in tags if not tag.find("input")]
        for tag in basket_tags:
            (
                basket_name,
                basket_part_id,
                basket_part_brand,
                basket_part_category,
                basket_span_tag_keys,
                basket_span_tag_values,
            ) = self.read_basket_tag(tag)
            if basket_span_tag_keys:
                # Collate information of part included in the basket tag.
                part_name = self.part_id_to_name.get(basket_part_id)
                part_data = dict(zip(basket_span_tag_keys, basket_span_tag_values))
                part = Part(
                    name=part_name,
                    price=part_data["price"],
                    brand=basket_part_brand,
                    category=basket_part_category,
                    part_id=basket_part_id,
                    shop=part_data["offer-shop-domain"],
                    price_format=part_data["price-format"],
                    value=part_data["value"],
                    penny=part_data["penny"],
                )
                # Add part to its relevant basket.
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

    def make_df(self, baskets=None) -> pd.DataFrame:
        """
        Make a dataframe from existing baskets.
        """
        if not baskets:
            baskets = self.baskets
        dfs = []
        for basket in baskets.values():
            basket.make_df()
            if not basket.df.empty:
                dfs.append(basket.df)

        df = (
            reduce(
                lambda left, right: pd.merge(
                    left, right, left_index=True, right_index=True, how="outer"
                ),
                dfs,
            )
            .reset_index()
            .dropna(subset="index")
            .set_index("index")
        )
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
