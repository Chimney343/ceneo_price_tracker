import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Union, Dict
import concurrent.futures

import pandas as pd
from tqdm.autonotebook import tqdm
from model.modules.page_readers import CategoryReader, ProductSetReader, ProductPageReader
from model.modules.baskets import Basket

logger = logging.getLogger(__name__)


class BaseScraper:
    def __init__(self, output_name: str = None, output_folder: Path = Path.cwd()):
        self.output_name = output_name
        self.output_folder = Path(output_folder)
        self.df = None

    def save_result_df(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        if self.output_name:
            filename = f"{self.output_name}_{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.pkl"
        else:
            filename = f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.pkl"
        self.df.to_pickle(path / filename)


class MultipleBasketsScraper(BaseScraper):
    def __init__(self, baskets_lookup: Dict, output_folder=Path.cwd()):
        super().__init__(output_name='product_set', output_folder=output_folder)
        self.baskets_lookup = baskets_lookup
        self.dfs = []

    def scrape_basket(self, basket_name, product_urls):
        basket_scraper = BasketScraper(basket_name=basket_name, product_urls=product_urls)
        basket_scraper.run()
        return basket_scraper.df

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_to_url = {executor.submit(self.scrape_basket, basket_name, product_urls): (basket_name, product_urls) for (basket_name, product_urls) in self.baskets_lookup.items()}
            for future in tqdm(concurrent.futures.as_completed(future_to_url), total=len(self.baskets_lookup)):
                try:
                    df = future.result()
                    self.dfs.append(df)
                except Exception as e:
                    traceback.print_exc(e)
        self.df = pd.concat(self.dfs)
        self.save_result_df()

class BasketScraper(BaseScraper):
    def __init__(self, basket_name: str, product_urls: List):
        super().__init__()
        self.product_urls = product_urls
        self.basket = Basket(name=basket_name)
        self.timestamp = None
        self.status = 'not scraped'

    def _status_check(self):
        if len(self.basket.df) == len(self.product_urls):
            return 'ok'
        return 'missing_products'

    def _enhance_basket_df(self, basket_df):
        basket_df['status'] = self.status
        basket_df['timestamp'] = self.timestamp
        basket_df.dropna(how='all', axis=1, inplace=True)
        return basket_df

    def scrape_product(self, url):
        reader = ProductPageReader(url=url)
        reader.read()
        return reader.product

    def run(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            future_to_url = {executor.submit(self.scrape_product, url): url for url in self.product_urls}
            for future in concurrent.futures.as_completed(future_to_url):
                url = future_to_url[future]
                try:
                    product = future.result()
                    self.basket.add_product(product)
                except Exception as e:
                    logger.critical(
                        f"Page at {url} returned an unhandled exception during scraping attempt. \n---TRACEBACK---\n"
                    )
                    traceback.print_exc(e)

        # Timestamp is set immediately after parsing main category url.
        self.timestamp = datetime.now()

        # Make the basket dataframe.
        self.basket.make_df()
        basket_df = self.basket.df.copy()

        # Enhance the basket dataframe.
        self.status = self._status_check()
        self.df = self._enhance_basket_df(basket_df=basket_df)

class CategoryScraper(BaseScraper):
    def __init__(self, url, category_name: str, output_folder: Path = Path.cwd()):
        super().__init__(output_name=category_name, output_folder=output_folder)
        self.url = url

    def read_category(self, url):
        reader = CategoryReader(url=url)
        try:
            reader.read()
            return reader.df
        except Exception as e:
            traceback.print_exc(e)
            return pd.DataFrame(None)

    def _make_result_df(self, df):
        df = df.rename(columns={df.columns[0]: 'price'}).sort_values(by='price', ascending=False)
        return df

    def run(self):
        df = self.read_category(url=self.url)
        self.df = self._make_result_df(df=df)
        self.df = df
        self.save_result_df()


class ProductSetScraper(BaseScraper):
    def __init__(self, ceneo_summaries: Union[str, List], output_folder: Path = Path.cwd()):
        super().__init__(output_name='product_set', output_folder=output_folder)
        self.ceneo_summaries = ceneo_summaries
        self.dfs = []

    def read_summary(self, url):
        reader = ProductSetReader(url=url)
        try:
            reader.read()
            return reader.df
        except Exception as e:
            logger.critical(f"{url} - scraping error; see traceback beloew.")
            traceback.print_exc(e)
            return pd.DataFrame(None)

    def _make_result_df(self):
        df = pd.concat(self.dfs).sort_values(by="timestamp")
        return df

    def run(self):
        if isinstance(self.ceneo_summaries, List):
            for url in tqdm(
                self.ceneo_summaries,
                desc="Scraping progress...",
                total=len(self.ceneo_summaries),
            ):
                result = self.read_summary(url)
                self.dfs.append(result)
        elif isinstance(self.ceneo_summaries, str):
            result = self.read_summary(self.ceneo_summaries)
            self.dfs.append(result)

        self.df = self._make_result_df()
        self.save_result_df()
