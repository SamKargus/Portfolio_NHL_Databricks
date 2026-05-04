import requests
import logging
import os
from pathlib import Path
from datetime import datetime

_cwd = Path(__file__).resolve().parent
LOG_DIR = next((p / "logs" for p in [_cwd, *_cwd.parents] if (p / "logs").is_dir()), _cwd / "logs")
LOG_DIR.mkdir(exist_ok=True)

log_file = LOG_DIR / f"batch_bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("batch_bronze")
logger.info(f"batch_bronze started — log: {log_file}")

BASE_URL = "https://api-web.nhle.com/v1"
game_id = "2024030416"

url = f"{BASE_URL}/gamecenter/{game_id}/play-by-play"
logger.info(f"Fetching play-by-play for game_id={game_id} — {url}")

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    logger.info(f"Request succeeded — {len(response.content)} bytes received")
    print(data)
else:
    logger.error(f"Request failed with status {response.status_code}")