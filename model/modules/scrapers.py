from pathlib import Path
from typing import List, Union
from datetime import datetime

import pandas as pd
from tqdm.autonotebook import tqdm

from model.modules.page_readers import CeneoSummaryPageReader


class CeneoScraper:
    def __init__(
        self, ceneo_summaries: Union[str, List], output_folder: Path = Path.cwd()
    ):
        self.ceneo_summaries = ceneo_summaries
        self.dfs = []
        self.output_folder = Path(output_folder)
        self.df = None

    def read_summary(self, url):
        summary = CeneoSummaryPageReader(url=url)
        try:
            summary.read_summary()
            return summary.df
        except:
            return pd.DataFrame(None)

    def _make_result_df(self):
        result = pd.concat(self.dfs).sort_values(by="timestamp")
        return result

    def save_result_df(self):
        path = Path(self.output_folder)
        path.mkdir(parents=True, exist_ok=True)
        filename = f"{datetime.now().strftime('%Y_%m_%d_%H_%M_%S')}.pkl"
        self.df.to_pickle(path / filename)

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

