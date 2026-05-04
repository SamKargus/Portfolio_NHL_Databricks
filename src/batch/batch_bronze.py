import requests
import logging
import os
from pathlib import Path
from datetime import datetime

BASE_URL = "https://api-web.nhle.com/v1"
game_id = "2024030416"

url = f"{BASE_URL}/gamecenter/{game_id}/play-by-play"
logger.info(f"Fetching play-by-play for game_id={game_id} — {url}")

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    logger.info(f"Request succeeded — {len(response.content)} bytes received")
    display(data)
else:
    logger.error(f"Request failed with status {response.status_code}")

url = f"{BASE_URL}/gamecenter/{game_id}/play-by-play"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()   # if response is JSON
    display(data)
    
else:
    print(f"Request failed with status {response.status_code}")