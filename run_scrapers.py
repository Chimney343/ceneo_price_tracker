from model.modules.scrapers import CeneoScraper
import SETTINGS
import warnings
warnings.filterwarnings("ignore")
import logging.config

# Logger settings.
logging.config.dictConfig(SETTINGS.LOGGING_CONFIG)
logger = logging.getLogger(__name__)


def run():
    logger.info(f"Commencing scraping of {len(SETTINGS.SUMMARY_URLS)} Ceneo summaries.")
    scraper = CeneoScraper(ceneo_summaries=SETTINGS.SUMMARY_URLS, output_folder=SETTINGS.OUTPUT_FOLDER)
    scraper.run()

if __name__ == '__main__':
    run()