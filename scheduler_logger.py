import logging
import os
from time import gmtime, strftime

current_time = strftime("%Y_%m_%dT%H_%M_%S", gmtime())
os.makedirs("./logging/", exist_ok=True)

log_file = f"./logging/{current_time}.log"
logging.basicConfig(
    filename=log_file,
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)
