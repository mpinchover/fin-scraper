# run predictions on individual stocks
from openai import OpenAI
import os
import json
from datetime import datetime, timezone, timedelta, time as dt_time
import pandas as pd
from google.cloud import storage
import sys
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from tenacity import retry, stop_after_attempt, wait_random

model = "gpt-4o-mini"
MAX_LEN_WORDS_PER_REQ = 60000

class Predict:
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

    def get_latest_articles_scrape(self):
        scraped_dates = []
        dated_scrapes = self.bucket.list_blobs(prefix='scrapes/', delimiter='/')
        
        for page in dated_scrapes.pages:
            for prefix in page.prefixes:
                ts = prefix.split('scrapes/')[1].strip('/')
                scraped_dates.append(ts)

        if not scraped_dates:
            return 
            
        latest_directory = sorted(scraped_dates, reverse=True)[0]
        return latest_directory

    def collect_saved_articles_from_storage(self, scrapes):
        articles = []
        for scrape in scrapes:
            if 'bucket_key' not in scrape:
                # print("Bucket key not found")
                continue

            key = scrape['bucket_key']
            blob = self.bucket.blob(key)
            content = blob.download_as_string()
            if not content:
                self.logger.info("Couldn't get article content from bucket")
                continue

            if len(content) < 100:
                self.logger.info("article is too short, skipping")
                continue 
            
            try:
            # Try converting the content to a JSON object
                json_content = json.loads(content)
                articles.append(json_content)
            except Exception as e:
                # If conversion fails, just skip and continue
                continue
        return articles
        

    # modify this to save it in storage
    def save_openai_resp_as_csv(self, df, df_filtered, run_id, lookback):   
        cur_time = datetime.now(timezone.utc)     
        key = f"predictions/{run_id}/{lookback}_predictions.csv"
        csv_data = df.to_csv(index=False)

        # Create a blob (the object in GCS)
        blob = self.bucket.blob(key)

        # Upload the CSV string as a file to the bucket
        blob.upload_from_string(csv_data, content_type='text/csv')
        db = self.get_db()
        db["predictions"].insert_one({
            "bucket_key": key,
            "lookback": lookback,
            "predicted_at": cur_time,
        })

        key_filtered = f"predictions/{run_id}/{lookback}_predictions_filtered.csv"
        blob_filtered = self.bucket.blob(key_filtered)
        csv_data_filtered = df_filtered.to_csv(index=False)
        blob_filtered.upload_from_string(csv_data_filtered, content_type='text/csv')
        self.logger.info(f"saved {lookback}_predictions.csv")

    def generate_analysis_for_article(self, stock_sym, article):
        client = OpenAI()
        system_content = f"""
        For the given article, predict if {stock_sym} will rise in the next trading window.
        If the article does not mention anything about {stock_sym}, return an answer of NA.
        Otherwise, return a YES or NO. The only acceptable responses are YES, NO or NA.
        """
        
        completion = client.chat.completions.create(
        model=model,
        messages=[
                {"role": "system", "content": system_content},
                {
                    "role": "user",
                    "content": article
                }
            ]
        )

        resp = completion.choices[0].message.content
        return resp

    def normalize_datetime_cnbc(date_str):
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
        dt_utc = dt.astimezone(timezone.utc)
        
        return dt_utc

    def get_published_at(self, file_content_formatted, source):
        published_at = None
            
        if "published_at" in file_content_formatted:
            time_as_str = file_content_formatted["published_at"]
            if not time_as_str:
                self.logger.info(f"time not found for {file_content_formatted["title"]}")
                return 
            
            if source == 'cnbc':
                return self.normalize_datetime_cnbc(time_as_str)

            try:
                published_at = datetime.strptime(time_as_str, '%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
            except ValueError:
                # If that fails, try without milliseconds
                published_at = datetime.strptime(time_as_str, '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)
        return published_at

    def generate_analysis_for_stock(self, stock_sym, scrapes_for_stock):
        self.logger.info(f"Generate analysis for stock {stock_sym}")
        # you need to get all the articles into an array based on the scrapes

        saved_articles = self.collect_saved_articles_from_storage(scrapes_for_stock)
        self.logger.info(f"Got saved articles of length {len(saved_articles)} for stock {stock_sym}")

        all_responses = []
        for article in saved_articles:
            if "content" not in article:
                self.logger.info("skipping openai, 'content' not found in article")
                continue 
            article_content = article['content']

            try:
                resp = self.generate_analysis_for_article(stock_sym, article_content)
                formatted_resp = resp.lower()
                if "yes" in formatted_resp:
                    all_responses.append("YES")
                elif "no" in formatted_resp:
                    all_responses.append("NO")
                else:
                    all_responses.append("NA")
            except Exception as e:
                self.logger.error(f"failure calling openai: {e}")
                
        return all_responses

    def convert_rows_to_csv(self, rows):
        # Initialize a list to hold the rows for the DataFrame
        data = []
        
        # Process each key-value pair in the dictionary
        for symbol, values in rows.items():
            # Count occurrences of "YES", "NO", and "NA"
            yes_count = values.count('YES')
            no_count = values.count('NO')
            na_count = values.count('NA')
            
            # Append the data as a new row
            data.append([symbol, yes_count, no_count, na_count])
        
        # Create a DataFrame from the data
        df = pd.DataFrame(data, columns=['Symbol', 'YES', 'NO', 'NA'])
        # Sort the DataFrame by the "YES" column in descending order
        df.sort_values(by='YES', ascending=False, inplace=True)
        return df
    
    def filter_and_order_df(self, df):
        # Filter rows where "YES" count is greater than 1 and greater than "NO" count
        filtered_df = df[(df['YES'] > 1) & (df['YES'] > df['NO'])].copy()
        
        # Calculate the difference and add it as a new column using .loc
        filtered_df.loc[:, 'YES_NO_DIFF'] = filtered_df['YES'] - filtered_df['NO']
        
        # Sort the DataFrame by the "YES_NO_DIFF" in descending order
        filtered_df.sort_values(by='YES_NO_DIFF', ascending=False, inplace=True)
        
        # Drop the "YES_NO_DIFF" column as it's not needed in the final output
        filtered_df.drop(columns=['YES_NO_DIFF'], inplace=True)
        
        return filtered_df

    # store this in the bucket
    def get_stocks_list(self, run_id, lookback): 
        cur_time = datetime.now(timezone.utc)

        self.logger.info(f"[predict] Get stocks list with lookback {lookback}, from time {cur_time}")
    
        lookback_from = cur_time - timedelta(hours=lookback) # change this to 6 hours lookback
        dup_articles_set = set()

        db = self.get_db()
        collection = db['scrapes']

        # get everything from the run id that has been published in the last {lookback} hours
        recent_scrapes = collection.find({"run_id": run_id, "published_at": {"$gte": lookback_from}})

        recent_scrapes_dict = {}
        for scrape in recent_scrapes:
            if 'url' not in scrape:
                print("url is not in scrape")
                continue

            url_key = scrape['url']
            if url_key in dup_articles_set:
                continue 

            dup_articles_set.add(url_key)
      
            stock = scrape['stock']
            if stock not in recent_scrapes_dict:
                recent_scrapes_dict[stock] = []
                del scrape["stock"]
                
            recent_scrapes_dict[stock].append(scrape)
        # should convert this to a map and list stock -> dup
        self.logger.info(f"[predict] found {len(dup_articles_set)} duplicate articles")
        return recent_scrapes_dict
    
    
    # start point
    # @retry(stop=stop_after_attempt(3), wait=wait_random(min=25, max=35))
    def run_analysis(self, run_id, lookback):
        stocks = self.get_stocks_list(run_id, lookback)
        if not stocks:
            self.logger.info(f"[predict] not stocks found for prediction")
            return 

        rows = {}
        current_time = datetime.now().astimezone(timezone.utc)

        for stock, scrapes_for_stock in stocks.items():
            stock_sym = stock.lower()
            responses = self.generate_analysis_for_stock(stock_sym, scrapes_for_stock)
            if not responses:
                continue
                
            rows[stock_sym] = responses
        df = self.convert_rows_to_csv(rows)
        df_filtered = self.filter_and_order_df(df)
        self.save_openai_resp_as_csv(df, df_filtered, run_id, lookback)
    
    def start(self, run_id, lookback):
        self.run_analysis(run_id, lookback)