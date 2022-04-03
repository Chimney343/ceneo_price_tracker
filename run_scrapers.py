import warnings

import SETTINGS
from model.modules.scrapers import CategoryScraper, ProductSetScraper

warnings.filterwarnings("ignore")
import logging.config

# Logger settings.
logging.config.dictConfig(SETTINGS.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def run():
    logger.info(f"Commencing scraping of {len(SETTINGS.SUMMARY_URLS)} Ceneo product sets.")
    product_set_scraper = ProductSetScraper(
        ceneo_summaries=SETTINGS.SUMMARY_URLS, output_folder=SETTINGS.PRODUCT_SET_OUTPUT_FOLDER
    )
    product_set_scraper.run()

    logger.info(f"Commencing scraping of {len(SETTINGS.CATEGORIES)} Ceneo categories.")
    category_scraper = CategoryScraper(
        url=SETTINGS.CATEGORIES['graphic_cards'],
        category_name="graphic_cards",
        output_folder=SETTINGS.CATEGORIES_OUTPUT_FOLDER,
    )
    category_scraper.run()


if __name__ == "__main__":
    run()
