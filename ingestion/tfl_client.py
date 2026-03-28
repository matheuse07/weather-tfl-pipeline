import requests
import logging
import json
import os
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO)

class TFLClient:
    BASE_URL = "https://api.tfl.gov.uk"
