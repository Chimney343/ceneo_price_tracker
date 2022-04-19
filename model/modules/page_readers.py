import concurrent.futures
import logging
import traceback
from datetime import datetime
from functools import reduce
from typing import Dict

import bs4
import pandas as pd
import requests
import validators
from bs4 import BeautifulSoup
from tqdm.autonotebook import tqdm

from model.modules.baskets import Basket
from model.modules.parts import Product

logger = logging.getLogger(__name__)

from typing import List


class BaseReader:
    def __init__(self, url):
        assert validators.url(url), "Invalid url."
        self.url = url
        self.status = 'unscraped'

    def _get_response(self, url: str):
        """
        Gets HTTP response from url.
        :param url:
        """
        self.response = requests.get(url)

    def parse_page(self, url=None):
        """
        Parses a webpage with BeatifoulSoup.
        :param url:
        """
        self._get_response(url)
        if self.response.status_code == 200:
            return BeautifulSoup(self.response.text, "html.parser")

    def get_title(self, page=None):
        if page == None:
            page = self.page
        return page.title


class ProductPageReader(BaseReader):
    def __init__(self, url):
        super().__init__(url)
        self.url = f"{self.url};0280-0.htm"
        self.product = None

    def find_offer_tags(self, page):
        return page.find_all(
            'div', class_="product-offer__container clickable-offer js_offer-container-click js_product-offer"
        )

    def get_product_name_from_tag(self, tag):
        name = tag['data-gaproductname']
        if '/' in name:
            return name.split('/')[1]
        return name

    def get_brand_from_tag(self, tag):
        return tag['data-brand']

    def get_price_from_tag(self, tag):
        return tag['data-price']

    def get_shop_name_from_tag(self, tag):
        return tag['data-shopurl']

    def get_shop_id_from_tag(self, tag):
        return tag['data-shopurl']

    def get_shop_id_from_tag(self, tag):
        return tag['data-shop']

    def get_category_from_tag(self, tag):
        return tag['data-gacategoryname']

    def get_product_id_from_tag(self, tag):
        return tag['data-productid']

    def read(self):
        page = self.parse_page(self.url)
        tags = self.find_offer_tags(page)
        first_tag = tags[0]

        name = self.get_product_name_from_tag(first_tag)
        cheapest_price = self.get_price_from_tag(first_tag)
        product_id = self.get_product_id_from_tag(first_tag)
        brand = self.get_brand_from_tag(first_tag)
        category = self.get_category_from_tag(first_tag)
        cheapest_shop_name = self.get_shop_name_from_tag(first_tag)
        cheapest_shop_id = self.get_shop_id_from_tag(first_tag)
        offers = [
            (i + 1, self.get_shop_name_from_tag(tag), self.get_price_from_tag(tag)) for i, tag in enumerate(tags)
        ]

        product = Product(
            name=name,
            price=cheapest_price,
            brand=brand,
            shop_name=cheapest_shop_name,
            shop_id=cheapest_shop_id,
            category=category,
            product_id=product_id,
            offers=offers,
        )
        self.product = product


class CategoryReader(BaseReader):
    def __init__(self, url):
        super().__init__(url)
        self.timestamp = None
        self.basket = {}
        self.n_category_pages = None
        self.category_urls = None

    def get_title(self, page):
        return page.title.text.split('-')[0].strip()

    def find_n_total_pages(self, page):
        n_pages = int(page.find('input', class_="js_pagination-top-input")['data-pagecount'])
        return n_pages

    def generate_category_urls(self, base_url, n_category_pages):
        urls = [f"https://www.ceneo.pl/Karty_graficzne;0020-30-0-0-{i}.htm" for i in range(0, n_category_pages)]
        urls[0] = base_url
        return urls

    def find_starting_tag(self, main_content_span_tags):
        for i, tag in enumerate(main_content_span_tags):
            if tag.find(text="Więcej produktów"):
                return (i + 1, main_content_span_tags[i + 1])

    def find_product_tags(self, page):
        main_content_span_tags = page.find_all('div')[0].find_all('div', class_='main-content')[0].find_all('span')
        starting_tag = self.find_starting_tag(main_content_span_tags)[0]
        return main_content_span_tags[starting_tag:]

    def read_products(self, url):
        parts = []
        page = self.parse_page(url)
        tags = self.find_product_tags(page)
        for i, tag in enumerate(tags):
            product_review_tag = tag.find('span', class_='prod-review__qo')
            if product_review_tag:
                part_name = product_review_tag.find('a')['title'].split(' o ')[-1]
                part_id = tags[i + 7]['data-pid']
                part_price = tags[i + 11].text
                part = Product(name=part_name, price=part_price, product_id=part_id)
                parts.append(part)
        return parts

    def make_df(self, basket=None) -> pd.DataFrame:
        """
        Make a dataframe from existing basket.
        """
        basket.make_df()
        df = basket.df.copy()
        return df

    def _enhance_df(self, df):
        """
        Result dataframe feature engineering.
        :param df:
        :return:
        """
        df["timestamp"] = self.timestamp
        return df

    def read(self):
        # Staging.
        self.main_page = self.parse_page(url=self.url)
        # Timestamp is set immediately after parsing main category url.
        self.timestamp = datetime.now()
        self.title = self.get_title(page=self.main_page)
        self.n_category_pages = self.find_n_total_pages(page=self.main_page)
        self.category_urls = self.generate_category_urls(base_url=self.url, n_category_pages=self.n_category_pages)
        self.basket = Basket(name=self.title)

        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_to_url = {executor.submit(self.read_products, url): url for url in self.category_urls}
            for future in tqdm(concurrent.futures.as_completed(future_to_url), total=self.n_category_pages):
                url = future_to_url[future]
                logger.debug((f"{url} scraped."))
                try:
                    parts = future.result()
                    for part in parts:
                        self.basket.add_part(part)
                except Exception as e:
                    logger.critical(
                        f"Page at {url} returned an unhandled exception during scraping attempt. \n---TRACEBACK---\n"
                    )
                    traceback.print_exc()

        df = self.make_df(basket=self.basket)
        df = self._enhance_df(df=df)
        self.df = df


class ProductSetReader(BaseReader):
    def __init__(self, url: str):
        super().__init__(url)
        self.timestamp = None
        self.baskets = {}
        self.part_name_to_id = {}
        self.part_id_to_name = {}

    def get_title(self, page: bs4.BeautifulSoup = None) -> str:
        """
        Returns product set name from the page title.
        :param page:
        :return:
        """
        if page == None:
            page = self.page
        return page.title.text.split("-")[0].strip()

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
        part_id_to_name = {part_id: part_name for part_name, part_id in part_name_to_id.items()}
        return part_name_to_id, part_id_to_name

    def make_baskets(self):
        """
        Makes baskets.Basket objects relevant to current Ceneo summary.
        """
        basket_names = set()
        for tag in self.page.find_all("td"):
            # Basket names are hidden under every tag that is different to 'input'.
            if tag.find("input"):
                continue
            basket_names.add(tag["class"][0])

        return {basket_name: Basket(name=basket_name) for basket_name in basket_names}

    def read_basket_tag(self, basket_tag: bs4.element.Tag) -> tuple:
        """
        Read Ceneo `basket` HTML tag
        :rtype: Tuple with basket name, id of part in basket, brand of part in basket, category of part in basket and a collection of keys/values from `span` tag.

        """
        basket_name = basket_tag["class"][0]
        try:
            basket_part_id = int(basket_tag.find("a")["data-productid"].replace('"', ""))
        except (TypeError, KeyError):
            basket_part_id = None
        try:
            basket_part_brand = basket_tag.find("a")["data-brand"].replace('"', "")
        except (TypeError, KeyError):
            basket_part_brand = None
        try:
            basket_part_category = basket_tag.find("a")["data-gacategoryname"].replace('"', "")
        except (TypeError, KeyError):
            basket_part_category = None
        basket_span_tag_keys = [span["class"][0] for span in basket_tag.find_all("span")]
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
                part = Product(
                    name=part_name,
                    price=part_data["price"],
                    brand=basket_part_brand,
                    category=basket_part_category,
                    product_id=basket_part_id,
                    shop_name=part_data["offer-shop-domain"],
                    price_format=part_data["price-format"],
                    value=part_data["value"],
                    penny=part_data["penny"],
                )
                # Add part to its relevant basket.
                self.baskets[basket_name].add_part(part)

    def find_most_expensive_offer(self, part: str, return_type: str):
        """
        Find the most expensive offer and either return it's shop or it's value.
        :param part:
        :param return_type:
        :return:
        """
        price_to_shop_lookup = {}
        for basket in self.baskets.values():
            for basket_part in basket.products:
                if basket_part.name == part:
                    price_to_shop_lookup[basket_part.price] = basket_part.shop_name

        if return_type == "shop":
            return price_to_shop_lookup[max(price_to_shop_lookup.keys())]
        elif return_type == "price":
            return max(price_to_shop_lookup.keys())

    def find_cheapest_offer(self, part: str, return_type: str):
        """
        Find the cheapest offer and either return it's shop or it's value.
        :param part:
        :param return_type:
        :return:
        """
        price_to_shop_lookup = {}
        for basket in self.baskets.values():
            for basket_part in basket.products:
                if basket_part.name == part:
                    price_to_shop_lookup[basket_part.price] = basket_part.shop_name

        if return_type == "shop":
            return price_to_shop_lookup[min(price_to_shop_lookup.keys())]
        elif return_type == "price":
            return min(price_to_shop_lookup.keys())

    def make_df(self, baskets=List[Basket]) -> pd.DataFrame:
        """
        Make a dataframe from existing baskets.
        """
        if not baskets:
            baskets = self.baskets
        self.dfs = []
        for basket in baskets.values():
            basket.make_df()
            if not basket.df.empty:
                self.dfs.append(
                    basket.df.rename(columns={'price': basket.name})
                    .drop(
                        labels=['basket_name', 'price_format', 'value', 'penny', 'shop_name', 'shop_id', 'offers'],
                        axis=1,
                    )
                    .reset_index()
                    .set_index(
                        ['index', 'brand', 'category', 'part_id', 'n_opinions'],
                    )
                )

        self.dfs.sort(key=len, reverse=True)
        df = (
            reduce(
                lambda left, right: pd.merge(
                    left,
                    right,
                    left_index=True,
                    right_index=True,
                    how="outer",
                ),
                self.dfs,
            )
            .reset_index()
            .dropna(subset="index")
            .set_index("index")
        )
        df = self._enhance_df(df)
        return df

    def status_check(self, df: pd.DataFrame, part_name_to_id: Dict) -> str:
        """
        Checks if all products included in the product set have been properly scraped. XXX: Usually if a product wasn't
        scraped, it's because Ceneo does not provide it's price at the moment.
        :param df: input dataframe
        :param part_name_to_id: lookup between part names and their id's.
        :return: string describing scraping status
        """
        result = all(elem in list(df.index) for elem in list(part_name_to_id))
        if result:
            return 'ok'
        else:
            return 'missing_products'

    def _enhance_df(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enhancing the final dataframe with some features and metadata.
        :param df:
        :return:
        """
        df["cheapest-shop"] = df.index.to_series().apply(self.find_cheapest_offer, return_type="shop")
        df["most-expensive-shop"] = df.index.to_series().apply(self.find_most_expensive_offer, return_type="shop")
        df["cheapest-price"] = df.index.to_series().apply(self.find_cheapest_offer, return_type="price")
        df["most-expensive-price"] = df.index.to_series().apply(self.find_most_expensive_offer, return_type="price")
        df["timestamp"] = self.timestamp
        df["title"] = self.title
        df['status'] = self.status
        return df

    def read(self):
        """
        Main flow; parses the page and products, initializes and fills the baskets, makes the result dataframe.
        """
        # Staging.
        self.page = self.parse_page(url=self.url)
        self.title = self.get_title(page=self.page)
        self.timestamp = datetime.now()
        # Collate data on products in summary.
        self.part_name_to_id, self.part_id_to_name = self.parse_products()
        # Initialize product baskets.
        self.baskets = self.make_baskets()
        self.fill_baskets()
        # Make summary dataframe and enhance the final dataframe.
        df = self.make_df(self.baskets)
        self.status = self.status_check(df=df, part_name_to_id=self.part_name_to_id)
        self.df = self._enhance_df(df=df)

    def display_baskets(self):
        """
        Prints on-screen the contents of all baskets.
        """
        for basket in self.baskets.values():
            basket.show()
