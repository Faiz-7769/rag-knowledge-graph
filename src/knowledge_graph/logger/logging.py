import logging
import os
from datetime import datetime


LOG_FILE = f"{datetime.now().strftime('%m_%d_%Y_%H_%M_%S')}.log"
logging_str = "[%(asctime)s: %(levelname)s: %(module)s: %(message)s]"


log_path = os.path.join(os.getcwd(), "logs")
os.makedirs(log_path, exist_ok=True)
LOG_FILE_PATH = os.path.join(log_path, LOG_FILE)


logging.basicConfig(
    level=logging.INFO,
    format=logging_str,
    handlers=[
        logging.FileHandler(LOG_FILE_PATH), # Saves to file
        logging.StreamHandler()             # Prints to console
    ]
)

logger = logging.getLogger("my_app")

