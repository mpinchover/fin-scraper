# bloomberg scraper
import requests
from bs4 import BeautifulSoup
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from chromedriver_py import binary_path
from selenium.webdriver.support import expected_conditions as EC
import time
import os
from datetime import datetime, timezone
import json
import threading
from urllib.parse import urlparse
import re
from google.cloud import storage
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from database.db import DB
import string
import logging
from tenacity import retry, stop_after_attempt, wait_random
import uuid
import traceback
import platform

class Yahoo:
    def __init__(self, logger, storage, db):

        self.db = db
        self.logger = logger
        self.storage = storage

        STORAGE_BUCKET=os.environ["STORAGE_BUCKET"]
        self.bucket = self.storage.get_bucket(STORAGE_BUCKET)
    
    def get_blob_key(self, article, directory):
            # sanitized_title = re.sub(r'[\/:*?"<>|]', '', article['title']).lower().translate(str.maketrans('', '', string.punctuation)).replace(" ", "_")
            key = f"{directory}/{article["title"]}.txt"
            new_blob = self.bucket.blob(key)
            return new_blob, key

    def create_scrape_record(self, article, directory, stock_sym, run_id):
        new_blob, key = self.get_blob_key(article, directory)

        try: 
            new_blob.upload_from_string(json.dumps(article))
        except Exception as e:
            self.logger.info("[scraper]: failed to save article to cloud bucket")
            return

        app_env = os.environ.get('APP_ENV', 'LOCAL')
        timestamp = datetime.now(timezone.utc)
        scrape = {
            "stock": stock_sym,
            "scraped_at": timestamp,
            "bucket_key": key,
            "app_env": app_env,
            "source": "yahoo",
            "url": article["link"],
            "run_id": run_id,
        }

        if article['published_at']:
            parsed_time = datetime.strptime(article['published_at'], "%Y-%m-%dT%H:%M:%S.%fZ")
            scrape['published_at'] = parsed_time
        return scrape


    def save_articles_to_storage(self, articles, stock_sym, run_id):
        if not articles:
            return 

        directory = f"scrapes/{run_id}/{stock_sym.lower()}/yahoo"
        
        scrapes_collection = self.db['scrapes']
        scrapes = []

        for article in articles:

            scrape = self.create_scrape_record(article, directory, stock_sym, run_id)
            if not scrape:
                continue
            
            scrapes.append(scrape)
        scrapes_collection.insert_many(scrapes)
        

    def get_published_at(self, soup):
        datetime_value = None
        # Find the <time> tag with the specific class
        time_tag = soup.find('time', class_='byline-attr-meta-time')
        if time_tag:
            datetime_value = time_tag['datetime']
        else:
            time_wrapper = soup.find('div', class_='caas-attr-time-style') 
            if time_wrapper:
                time_tag = time_wrapper.find('time')
                if time_tag:
                    datetime_value = time_tag['datetime']

        return datetime_value

    def get_article_content(self, soup, link):
        article_text = []
        article_content = soup.find('div', class_=("caas-body", "body yf-5ef8bf"))
        if not article_content:
            self.logger.info(f"skipped link: {link}")
            return
            
        p_tags = article_content.find_all('p')
        if not p_tags:
            return
            
        for p_tag in p_tags:
            ptext = p_tag.get_text().strip()
            article_text.append(ptext)
            
        article_text_str = '\n'.join(article_text)
        return article_text_str
 

    def scrape_recent_news_for_sym(self, link):
        # print("Link is ", link)
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
            }
            response = requests.get(link, headers=headers)
            if response.status_code != 200:
                raise Exception("Failed to get 200 response from Yahoo link: ", link)

            main_page_source = response.text
            soup = BeautifulSoup(main_page_source, 'html.parser')

            title = str(uuid.uuid4())
            published_at = self.get_published_at(soup)
            
            if not published_at:
                self.logger.info(f"[scraper] No published at found for {title}")

            article_text_str = self.get_article_content(soup, link)
            if not article_text_str:
                return
            
            res = {
                "content": article_text_str, 
                "title": title, 
                "link": link,
            }
            if published_at:
                res["published_at"] = published_at

            return res
        except Exception as e:
            print("CAUGHT FAILED SCRAPE")

    # @retry(stop=stop_after_attempt(3), wait=wait_random(min=1, max=5))
    def get_articles_for_stock(self, url):
        try:
            articles_for_stock = set()
            response = requests.get(url)
            if response.status_code != 200:
                raise Exception("Failed to get 200 response from Yahoo")
        
            # self.wait_for_stock_article_links(driver)
            
            main_page_source = response.text
            soup = BeautifulSoup(main_page_source, 'html.parser')

            filtered_stories = soup.find('div', class_=lambda x: x and 'filtered-stories' in x)
            if not filtered_stories:
                # self.logger.info(f"[scraper]: No filtered stories found for url {url}")
                raise Exception("no stories found")
                
            atags = filtered_stories.find_all("a", class_=lambda x: x and 'subtle-link' in x)
            if not atags:
                self.logger.info(f"scraper] No atags found for url {url}")
                raise Exception("no tags found")
                
            for atag in atags:
                link = atag.get('href')
                if not link:
                    continue
                articles_for_stock.add(link)

            res = {
                "articles_for_stock": list(articles_for_stock),
            }
            return res

        except Exception as e:
            self.logger.info(e)
            raise Exception(e)
  
    def get_stories_for_stock(self, articles_for_stock, stock):
        if not articles_for_stock:
            return

        stories_for_stock = []

        # these are all the stories for the stock
        for link in list(articles_for_stock):
            time.sleep(10)
            self.logger.info(f"[scraper] scraping {link}, stock {stock}")
            try: 
                story = self.scrape_recent_news_for_sym(link)
                if not story:
                    continue

                stories_for_stock.append(story)
            except Exception as e:
                self.logger.info("Failed to get story for ", link)
        
        return stories_for_stock
    
    def save_scraped_stock_data(self, stock, run_id, success=True):
        sps_collection = self.db["stock_prices"]

        query = {
            "stock": stock,
            "run_id": run_id
        }

        cur_time = datetime.now(timezone.utc)
        update = {
            "$set": {
                "success": success,
                "updated_at": cur_time,
            },
            "$setOnInsert": {
                "created_at": cur_time,
            }
        }

        sps_collection.update_one(query, update, upsert=True)

    def get_failed_stocks(self, run_id):
        sps_collection = self.db["stock_prices"]
        failed_stocks = sps_collection.distinct("stock", {"run_id": run_id, "success": False})
        return failed_stocks
        
        
    def run_scraper(self, stock, run_id, worker_idx):
        url = f"https://finance.yahoo.com/quote/{stock}"
        self.logger.info(f"[scraper] getting articles for url {url}, worker_idx {worker_idx}")

        try:
            scraped_stock_res = self.get_articles_for_stock(url)

            articles_for_stock = scraped_stock_res["articles_for_stock"]
            if not articles_for_stock:
                self.logger.info(f"[scraper] no articles found for stock {stock}")
                return
                
            stories_for_stock = self.get_stories_for_stock(articles_for_stock, stock)
            print("GOT BACK STORIES FOR STOCK ", len(stories_for_stock))
            if not stories_for_stock:
                self.logger.info(f"No stories found for stock {stock}")
                return

            # self.logger.info(f"[scraper] found {len(stories_for_stock)} for stock {stock}")
            self.logger.info(f"[scraper] Saving articles to storage for stock {stock}")
            self.save_articles_to_storage(stories_for_stock, stock, run_id)
            return scraped_stock_res
            # self.logger.info(f"[scraper] completed for stock {stock}, ts: {timestamp}")
        except Exception as e:
            self.logger.info(e)
            raise Exception(e)
     
    def run_job(self, stock, timestamp, sema, run_id, worker_idx):
        sema.acquire()   
        self.logger.info(f"Starting scraper for worker {worker_idx}, stock {stock} at time {datetime.now(timezone.utc)}")
        
        # opts = webdriver.ChromeOptions()
        app_env = os.environ.get('APP_ENV', 'LOCAL')
        # if app_env != "LOCAL":
        #     opts.add_argument("--headless")
        #     opts.add_argument("--disable-gpu")
        #     opts.add_argument("window-size=1920,1080")
        #     opts.add_argument("--no-sandbox")
        #     opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
        
        # local dev
        # path = "/Users/mattpinchover/.wdm/drivers/chromedriver/mac64/129.0.6668.89/chromedriver-mac-arm64/chromedriver"
        # if platform.system() == "Linux":
        #     path = "/webdrivers/chromedriver"

        # if not os.path.exists(path):
        #     self.logger.info(f"Invalid file path, downloading chromedriver")
        #     path = ChromeDriverManager().install()
        
        # svc = ChromeService(path)
        # path = "/Users/mattpinchover/.wdm/drivers/chromedriver/mac64/129.0.6668.89/chromedriver-mac-arm64/chromedriver"
        # svc = ChromeService(path)
        

        try: 
            scraped_stock_res = self.run_scraper(stock, run_id, worker_idx)
            self.save_scraped_stock_data(stock, run_id, scraped_stock_res is not None)
            self.logger.info(f"[scraper] SUCCESS on stock {stock}")
        except Exception as e:
            self.logger.info(e)
            self.logger.info(traceback.format_exc())
            self.save_scraped_stock_data(stock, run_id, False)
            self.logger.info(f"[scraper] FAILED on stock {stock}")
        finally:
            sema.release()

    def get_stocks_list(self, stock_list):
        key = f"stocks_list/{stock_list}"
        stock_list_as_blob = self.bucket.blob(key)
        stock_list = stock_list_as_blob.download_as_string().decode("utf-8-sig")
        stocks = stock_list.splitlines()
        stocks = [stock.strip() for stock in stocks if stock.strip()]
        
        return stocks

    def start(self, stock_list):
        run_id = str(uuid.uuid4())
        stocks = self.get_stocks_list(stock_list)
        self.logger.info(f"Starting scrapes for run id: {run_id}, num stocks: {len(stocks)}")

        max_threads = 5
        sema = threading.Semaphore(value=max_threads)
        threads = list()
        
        utc_now = datetime.now(timezone.utc)
        # self.save_run(run_id, utc_now)

        for idx, stock in enumerate(stocks):
            args = (stock, utc_now, sema, run_id, idx)
            thread = threading.Thread(target=self.run_job, args=args)
            threads.append(thread)
            
        for idx, thread in enumerate(threads):
            time.sleep(5)
            thread.start()

            # if idx >= 1 and idx % 10 == 0:
            #     self.logger.info("Sleeping for 5 minutes")
            #     # time.sleep(300) # 5 min
            #     time.sleep(900)
        
        for thread in threads:
            thread.join()
        
        self.logger.info(f"[scraper] Yahoo scraper completed with run_id {run_id}, time {datetime.now(timezone.utc)}")

