import os
from flask import Flask, jsonify, request
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from chromedriver_py import binary_path
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from bs4 import BeautifulSoup
from yahoo.yahoo import Yahoo
from job_controller.job_controller import JobController
from predict.predict import Predict
import logging
from logging import Formatter, FileHandler
from datetime import datetime, timezone
import time
import threading
import traceback
import random
import requests
from requests.exceptions import HTTPError
from tenacity import retry, stop_after_attempt, wait_random
from dotenv import load_dotenv
import google.cloud.logging as gcp_logging
from google.cloud import storage
import sys
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from google.oauth2 import service_account
from google.cloud import storage
import json


path = "/app/svc_acc_key.json"
if os.environ.get("APP_ENV", None) != "PRODUCTION":
    path = "svc_acc_key.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path

logging_client = gcp_logging.Client(project="awesome-pilot-437816-c2")
logging_handler = logging_client.setup_logging()

stream_handler = logging.StreamHandler()
logging.getLogger().addHandler(stream_handler)

logger = logging.getLogger(__name__)

app = Flask(__name__)
app.debug = True

# build logger
logger = logging.getLogger(__name__)

# build storage client
storage_client = storage.Client()

uri = os.environ["MONGO_URI"]
db_client = MongoClient(uri, server_api=ServerApi('1'))
db = db_client.get_database()

yahoo_scraper = Yahoo(logger, storage_client, db)
pred = Predict(logger, storage_client, db)

@app.route("/start-jobs")
def start_jobs():
    try:
        start_time = time.time()

        jc = JobController(logger, storage_client, db)
        run_id = jc.start()

        total_elapsed_time = int(time.time() - start_time)  # Convert to integer seconds

        return {"success": True, "run_id": run_id, "total_elapsed_time": total_elapsed_time}
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/predict", methods=["POST"])
def predict():
    try: 
        data = request.get_json()
        lookback = data.get('lookback')
        run_id = data.get('run_id')

        if not lookback:
            return jsonify({"success": False, "error": "lookback required"}), 401
        if not run_id:
            return jsonify({"success": False, "error": "run_id required"}), 401

        lookback = int(lookback)
        ts = pred.start(lookback, run_id)
        return {"success": True, "run_id": run_id}
    except Exception as e:
        app.logger.error(f"[scraper: error is {e}]")
        app.logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/scrape-list", methods=["POST"])
def scrape_list():       
    try:  
        start_time = time.time()

        data = request.get_json()
        stock_list = data.get('stock_list')
        run_id = data.get('run_id')

        if not stock_list:
            return jsonify({"success": False, "error": "stock_list required"}), 401

        if not run_id:
            return jsonify({"success": False, "error": "run_id required"}), 401


        run_id = yahoo_scraper.start(stock_list, run_id) 
        total_elapsed_time = int(time.time() - start_time)  # Convert to integer seconds

        return jsonify({"success": True, "elapsed_time": f"{total_elapsed_time}s", "run_id": run_id})
    except Exception as e:
        app.logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/")
def hello_world():
    logger = logging.getLogger(__name__)
    logger.info('Creating a logging message')

    return f"Hello world!"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 5001)))