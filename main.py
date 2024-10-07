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
from google.cloud import logging as gcp_logging
import sys

def setup_logging(app):
    print("Setting up logs...")

    path = "/app/svc_acc_key.json"
    if os.environ.get("APP_ENV", None) != "PRODUCTION":
        path = "svc_acc_key.json"
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
    
    
    # Create a separate logger for your application
    app_logger = logging.getLogger("my_app_logger")
    app_logger.setLevel(logging.DEBUG)
    
    # Remove any existing handlers for this logger
    for handler in app_logger.handlers[:]:
        app_logger.removeHandler(handler)

    client = gcp_logging.Client(project="awesome-pilot-437816-c2")
    # Configure logging to GCP
    client.setup_logging(log_level=logging.DEBUG)

    # Configure logging to stdout
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setLevel(logging.DEBUG)
    app_logger.addHandler(stream_handler)
   
    # Use the app logger to log messages
    app_logger.info('first test message...')
    
    # Replace the default Flask logger with the custom logger
    app.logger = app_logger

app = Flask(__name__)
app.debug = True
setup_logging(app)
load_dotenv()

yahoo_scraper = Yahoo(app.logger)
pred = Predict(app.logger)



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
        app.logger.error(f"[scraper: error is {e}]")
        app.logger.error(traceback.format_exc())
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
        app.logger.error(traceback.format_exc())
        return jsonify({"success": False, "error": str(e)}), 500


@app.route("/")
def hello_world():
    # os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "svc_acc_key.json"
    # print("GOOGLE CREDS ARE ")
    app.logger.info('Creating a logging message')

    # logging_client = google.cloud.logging.Client()
    # logging_client.setup_logging()
    # logging_handler.info("NEW MESSAGE")
    # logger.info("Example message")

    
    # # logger = logging.getLogger(__name__)
    # # logger.info('my log entry')
    # # logging_client = google.cloud.logging.Client()
    # # logging_client = google.cloud.logging.Client()
    # # logging_handler = logging_client.setup_logging()
    # # app.logger.info("Example message")

   

    # # print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

    # logger = logging_client.logger("NAME")

    # logger.log_text(
    #     f"THIS IS A TEST LOG", 
    #     severity="INFO"
    # )

    # logging.info("You have created a log")
    # logging.error("You have created an error log")
    return f"Hello world!"


if __name__ == "__main__":
    

    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))