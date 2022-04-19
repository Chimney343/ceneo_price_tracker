import logging
import traceback
from datetime import datetime
from pathlib import Path
from typing import List, Union

import pandas as pd
from tqdm.autonotebook import tqdm

import SETTINGS
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


class BasketScraper(BaseScraper):
    def __init__(self, basket_name: str, product_lookup: List):
        super().__init__()
        self.product_lookup = product_lookup
        self.basket = Basket(name=basket_name)

    def run(self):
        for url in self.product_lookup:
            reader = ProductPageReader(url=url)
            reader.read()
            self.basket.add_part(part=reader.product)
            self.basket.make_df()


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
