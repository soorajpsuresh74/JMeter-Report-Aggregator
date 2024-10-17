import logging
import os

from dotenv import load_dotenv


class Secrets:
    load_dotenv()

    ENABLE_LOGGING = os.getenv('ENABLE_LOGGING', 'False').lower() in ('true', '1', 't')
    FILE_PATH = os.getenv('FILE_PATH')
    SAVE_REPORT_LOCALLY = os.getenv('SAVE_REPORT_LOCALLY', 'False').lower() in ('true', '1', 't')
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_REGION = os.getenv('AWS_REGION')
    S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')
    OUTPUT_FILE_NAME = os.getenv('OUTPUT_FILE_NAME', 'Aggregated.csv')
    SAVE_TO_S3 = os.getenv('SAVE_TO_S3', 'False').lower() in ('true', '1', 't')
    LOG_FILE_PATH = os.getenv('LOG_FILE_PATH', 'application.log')


class MyLogger:
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        log_level = logging.DEBUG if Secrets.ENABLE_LOGGING else logging.INFO

        try:
            logging.basicConfig(
                level=log_level,
                format='%(asctime)s - %(levelname)s - %(message)s',
                handlers=[
                    logging.FileHandler(Secrets.LOG_FILE_PATH),
                    logging.StreamHandler()
                ]
            )
            self.logger.info("Logging Initialized")
        except Exception as e:
            print(f"Error initializing logging: {e}")

    def log_info(self, message):
        self.logger.info(message)

    def log_error(self, message):
        self.logger.error(message)
