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

class Yahoo:
    def __init__(self, logger):
        self.set_envs()
        self.db = None
        self.logger = logger

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
 

    def wait_for_article_body(self, driver):
        try:
            # Wait up to 20 seconds for the element with both classes to appear
            WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.body.yf-5ef8bf"))
            )
            return True

        except Exception:
            # If the timeout occurs, just return False and move forward without raising or logging an error
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=25, max=35))
    def scrape_recent_news_for_sym(self, link, driver):
        driver.get(link)
        self.wait_for_article_body(driver)
    
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        title = driver.title
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

    def wait_for_stock_article_links(self, driver):
        try:
            # Wait up to 20 seconds for the element with both classes to appear
            wait = WebDriverWait(driver, 20)  # You can adjust the timeout
            wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "div[class*='filtered-stories']")))
            return True

        except Exception:
            # If the timeout occurs, just return False and move forward without raising or logging an error
            return False

    @retry(stop=stop_after_attempt(3), wait=wait_random(min=25, max=35))
    def get_articles_for_stock(self, url, opts, svc):
        try:
            with webdriver.Chrome(service=svc, options=opts) as driver:
                articles_for_stock = set()
                driver.get(url)
                self.wait_for_stock_article_links(driver)
               
                main_page_source = driver.page_source
                soup = BeautifulSoup(driver.page_source, 'html.parser')

                filtered_stories = soup.find('div', class_=lambda x: x and 'filtered-stories' in x)
                if not filtered_stories:
                    self.logger.info(f"[scraper]: No filtered stories found for url {url}")
                    return 
                    
                atags = filtered_stories.find_all("a", class_=lambda x: x and 'subtle-link' in x)
                if not atags:
                    self.logger.info(f"scraper] No atags found for url {url}")
                    return 
                    
                for atag in atags:
                    link = atag.get('href')
                    if not link:
                        continue
                        
                    articles_for_stock.add(link)
                return list(articles_for_stock)
        except Exception as e:
            print(e)
            raise ValueError(e)
  

    
    def get_stories_for_stock(self, articles_for_stock, stock, opts, svc):
        if not articles_for_stock:
            return

        with webdriver.Chrome(service=svc, options=opts) as driver:
            stories_for_stock = []

            # these are all the stories for the stock
            for link in list(articles_for_stock):
                self.logger.info(f"[scraper] scraping {link}, stock {stock}")
                try: 
                    story = self.scrape_recent_news_for_sym(link, driver)
                    if not story:
                        continue
                    stories_for_stock.append(story)
                except Exception as e:
                    print("Failed to get story for ", link)
                    continue
            
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
                self.logger.info("[scraper]: failed to save article to cloud bucket")
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

    def run_scraper(self, stock, timestamp, run_id, opts, svc, worker_idx):
        url = f"https://finance.yahoo.com/quote/{stock}"
        self.logger.info(f"[scraper] getting articles for url {url}, worker {worker_idx}")

        try:
            articles_for_stock = self.get_articles_for_stock(url, opts, svc)
            if not articles_for_stock:
                self.logger.info(f"[scraper] no articles found for stock {stock}")
                return
                
            stories_for_stock = self.get_stories_for_stock(articles_for_stock, stock, opts, svc)
            if not stories_for_stock:
                self.logger.info(f"No stories found for stock {stock}")
                return

            self.logger.info(f"[scraper] found {len(stories_for_stock)} for stock {stock}")
            self.logger.info(f"[scraper] Saving articles to storage for stock {stock}")
            self.save_articles_to_storage(stories_for_stock, stock, timestamp, run_id)
            self.logger.info(f"[scraper] completed for stock {stock}, ts: {timestamp}")
        except Exception as e:
            print(e)
            raise ValueError(e)
        

    def run_job(self, stock, timestamp, sema, run_id, worker_idx):
        sema.acquire()       
        opts = webdriver.ChromeOptions()

        opts.add_argument("--headless")
        opts.add_argument("--disable-gpu")
        opts.add_argument("window-size=1920,1080")
        opts.add_argument("--no-sandbox")
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
        svc = ChromeService(ChromeDriverManager().install())

        try: 
            self.run_scraper(stock, timestamp, run_id, opts, svc, worker_idx)
            self.logger.info(f"[scraper] SUCCESS on stock {stock}")
        except Exception as e:
            self.logger.error(e)
            self.logger.error(traceback.format_exc())
            self.logger.error(f"[scraper] FAILED on stock {stock}")
        finally:
            sema.release()


    # @retry(stop=stop_after_attempt(10), wait=wait_random(min=25, max=35))
            
    def start(self, stocks):
        run_id = str(uuid.uuid4())
        self.logger.info(f"[scraper] Starting Yahoo scraper on {len(stocks)} stocks, run id {run_id}")

       
    
        utc_now = datetime.now(timezone.utc)
        maxthreads = 3
        sema = threading.Semaphore(value=maxthreads)
        threads = []

        for idx, stock in enumerate(stocks):
            args = (stock, utc_now, sema, run_id, idx)
            thread = threading.Thread(target=self.run_job,args=args)
            threads.append(thread)

        # run http requests in threads
        for thread in threads:
            time.sleep(10)
            thread.start()
        
        for thread in threads:
            thread.join()

        self.logger.info(f"[scraper] Yahoo scraper completed with run_id {run_id}")
        return run_id

        