# bloomberg scraper
import requests
from bs4 import BeautifulSoup
import random
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from chromedriver_py import binary_path
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
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

class Yahoo:
    def __init__(self):
        self.set_envs()
        self.db = None

    def get_db(self):
        if self.db is None: 
            uri = os.environ["MONGO_URI"]
            client = MongoClient(uri, server_api=ServerApi('1'))
            self.db = client.get_database()
        return self.db
       
    def set_envs(self):
        path = "/app/svc_acc_key.json"

        if os.environ.get("APP_ENV", None) != "PRODUCTION":
            path = "../svc_acc_key.json"
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path
        self.storage_client = storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
        self.bucket = self.storage_client.get_bucket(os.environ["STORAGE_BUCKET"])
        

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
        try:
            article_text = []
            article_content = soup.find('div', class_=("caas-body", "body yf-5ef8bf"))
            if not article_content:
                print(f"skipped link: {link}")
                return
                
            p_tags = article_content.find_all('p')
            if not p_tags:
                return
                
            for p_tag in p_tags:
                ptext = p_tag.get_text().strip()
                article_text.append(ptext)
                
            article_text_str = '\n'.join(article_text)
            return article_text_str
        except Exception as e:
            print(e)
            print("[scraper]: unable to scrape article content")
            return None

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=25, max=35))
    def scrape_recent_news_for_sym(self, link, driver):
        driver.get(link)
        time.sleep(3)

        soup = BeautifulSoup(driver.page_source, 'html.parser')
        title = driver.title
        published_at = self.get_published_at(soup)
        if not published_at:
            print("[scraper] No published at found for ", title)

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

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=25, max=35))
    def get_articles_for_stock(self, driver, url):
        try:
            articles_for_stock = set()
            driver.get(url)
            time.sleep(5)
            main_page_source = driver.page_source
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            filtered_stories = soup.find('div', class_=lambda x: x and 'filtered-stories' in x)
            if not filtered_stories:
                print(f"[scraper]: ]No filtered stories found for url {url}")
                return 
                
            atags = filtered_stories.find_all("a", class_=lambda x: x and 'subtle-link' in x)
            if not atags:
                print(f"scraper] No atags found for url {url}")
                return 
                
            for atag in atags:
                link = atag.get('href')
                if not link:
                    continue
                    
                articles_for_stock.add(link)
            return list(articles_for_stock)
        except Exception as e:
            print(e)
            raise ValueError("[scraper]: unable to scrape articles for stock")

    def get_stories_for_stock(self, driver, articles_for_stock, stock):
        if not articles_for_stock:
            return

        stories_for_stock = []

        # these are all the stories for the stock
        for link in list(articles_for_stock):
            print(f"[scraper] scraping {link}, stock {stock}")
            story = self.scrape_recent_news_for_sym(link, driver)
            if not story:
                continue
            stories_for_stock.append(story)
        
        return stories_for_stock

    def save_articles_to_storage(self, articles, stock_sym, timestamp, run_id):
        if not articles:
            return 

        time_as_str_formatted = timestamp.strftime('%Y-%m-%d-%H-%M-%S').replace('-', '_')
        directory = f"scrapes/{run_id}/{stock_sym.lower()}/yahoo"
        
        db = self.get_db()
        scrapes_collection = db['scrapes']
        scrapes = []

        for article in articles:
            sanitized_title = re.sub(r'[\/:*?"<>|]', '', article['title']).lower().translate(str.maketrans('', '', string.punctuation)).replace(" ", "_")
            key = f"{directory}/{sanitized_title}.txt"
            new_blob = self.bucket.blob(key)

            try: 
                new_blob.upload_from_string(json.dumps(article))
            except Exception as e:
                print("[scraper]: failed to save article to cloud bucket")
                continue

            app_env = os.environ.get('APP_ENV', 'LOCAL')
            scrape = {
                "stock": stock_sym.lower(),
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

            scrapes.append(scrape)
        scrapes_collection.insert_many(scrapes)

    def run_scraper(self, stock, timestamp, driver, run_id, url):
        print(f"[scraper] getting articles for url {url}")
        articles_for_stock = self.get_articles_for_stock(driver, url)
        if not articles_for_stock:
            print(f"[scraper] no articles found for stock {stock}")
            return
            
        stories_for_stock = self.get_stories_for_stock(driver, articles_for_stock, stock)
        if not stories_for_stock:
            print(f"No stories found for stock {stock}")
            return

        print(f"[scraper] found {len(stories_for_stock)} for stock {stock}")
        print(f"[scraper] Saving articles to storage for stock {stock}")
        self.save_articles_to_storage(stories_for_stock, stock, timestamp, run_id)
        print(f"[scraper] completed for stock {stock}, ts: {timestamp}")

    def run_job(self, stock, timestamp, sema, run_id):
        sema.acquire()

        url = f"https://finance.yahoo.com/quote/{stock}"
        opts = webdriver.ChromeOptions()

        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
        opts.add_argument("window-size=1920,1080")
        opts.add_argument("--no-sandbox")
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
        svc = ChromeService(ChromeDriverManager().install())

        driver = None
        try: 
            driver = webdriver.Chrome(service=svc, options=opts)
            self.run_scraper(stock, timestamp, driver, run_id, url)
        except Exception as e:
            print(e)
            print(traceback.format_exc())
            print(f"[scraper] failed on stock {stock}")
        finally:
            if driver:
                driver.quit()
            sema.release()


    # @retry(stop=stop_after_attempt(10), wait=wait_random(min=25, max=35))
            
    def start(self, stocks):
        run_id = str(uuid.uuid4())
        print(f"[scraper] Starting Yahoo scraper on {len(stocks)} stocks, run id {run_id}")
    
        utc_now = datetime.now(timezone.utc)
        maxthreads = 5
        sema = threading.Semaphore(value=maxthreads)
        threads = []

        for stock in stocks:
            args = (stock, utc_now, sema, run_id)
            thread = threading.Thread(target=self.run_job,args=args)
            threads.append(thread)

        # run http requests in threads
        for thread in threads:
            thread.start()
        
        for thread in threads:
            thread.join()

        print(f"[scraper] Yahoo scraper completed with run_id {run_id}")
        return run_id

        