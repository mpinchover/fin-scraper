import os
# https://cloud-run-helloworld-1002527450505.us-central1.run.app
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
from predict.predict import Predict
import logging
from datetime import datetime, timezone
import time
import threading
import traceback
import random
import requests
from requests.exceptions import HTTPError
from tenacity import retry, stop_after_attempt, wait_random
from dotenv import load_dotenv
import google.cloud.logging


app = Flask(__name__)
app.debug = True

client = google.cloud.logging.Client()
client.setup_logging()

load_dotenv()

yahoo_scraper = Yahoo()
pred = Predict()

@app.route("/predict", methods=["POST"])
def predict():
    try: 
        data = request.get_json()
        lookback = data.get('lookback')
        if not lookback:
            return jsonify({"success": False, "error": "lookback required"}), 401

        lookback = int(lookback)
        ts = pred.start(lookback)
        return {"success": True, "timestamp": 123}
    except Exception as e:
        print(f"[scraper: error is {e}]")
        print(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/execute-scrape-jobs", methods=["POST"])
def execute_scrape_jobs():       
    try:  
        start_time = time.time()

        # get the stocks list, lookback
        data = request.get_json()
        stocks = data.get('stocks')
        lookback = data.get('lookback')

        if not stocks:
            return jsonify({"success": False, "error": "stocks list required"}), 401
        if not lookback:
            return jsonify({"success": False, "error": "lookback required"}), 401

        lookback = int(lookback)

        run_id = yahoo_scraper.start(stocks) 
        pred.start(run_id, lookback)

        total_elapsed_time = int(time.time() - start_time)  # Convert to integer seconds

        return jsonify({"success": True, "elapsed_time": f"{total_elapsed_time}s", "run_id": run_id})
    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/")
def hello_world():
    logging.info("You have created a log")
    logging.error("You have created an error log")
    return f"Hello world!"


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))