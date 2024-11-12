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
    def __init__(self, logger, storage, db, email_controller, trading_controller):
        self.db = db
        self.logger = logger
        self.storage = storage
        self.email = email_controller
        self.trading_controller = trading_controller

        STORAGE_BUCKET=os.environ["STORAGE_BUCKET"]
        self.bucket = self.storage.get_bucket(STORAGE_BUCKET)
        
    def get_db(self):
        if self.db is None: 
            uri = os.environ["MONGO_URI"]
            client = MongoClient(uri, server_api=ServerApi('1'))
            self.db = client.get_database()
        return self.db

    def collect_saved_articles_from_storage(self, scrapes):
        articles = []
        for scrape in scrapes:
            if 'bucket_key' not in scrape:
                # print("Bucket key not found")
                continue

            key = scrape['bucket_key']
            try:
                blob = self.bucket.blob(key)
            except Exception as e:
                self.logger.info(f"failed to get blob article from storage: {key}")
                continue
            
            try: 
                content = blob.download_as_string()
                if not content:
                    self.logger.info("Couldn't get article content from bucket")
                    continue
            except Exception as e:
                self.logger.info(f"failed to download article from storage as string {key}")
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
    def save_openai_resp_as_csv(self, df, run_id, lookback):   
        cur_time = datetime.now(timezone.utc)    
        cur_time_str = cur_time.strftime("%Y%m%d_%H%M%S")  # Format as a string 
        key = f"predictions/{run_id}/{cur_time_str}_{lookback}h_predictions.csv"
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

        df_filtered = self.filter_and_order_df(df)
        key_filtered = f"predictions/{run_id}/{cur_time_str}_{lookback}_predictions_filtered.csv"
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
    def get_stocks_list(self, lookback, run_id): 
        cur_time = datetime.now(timezone.utc)

        self.logger.info(f"[predict] Get stocks list with lookback {lookback}, from time {cur_time}")
    
        lookback_from = cur_time - timedelta(hours=lookback) # change this to 6 hours lookback
        # dup_articles_set = set()

        db = self.get_db()
        collection = db['scrapes']

        # get everything from the run id that has been published in the last {lookback} hours
        recent_scrapes = collection.find({"run_id": run_id, "published_at": {"$gte": lookback_from}})

        recent_scrapes_dict = {}
        for scrape in recent_scrapes:
            if 'url' not in scrape:
                print("url is not in scrape")
                continue

            # url_key = scrape['url']
            # if url_key in dup_articles_set:
            #     continue 

            # dup_articles_set.add(url_key)
      
            stock = scrape['stock']
            if stock not in recent_scrapes_dict:
                recent_scrapes_dict[stock] = []
                del scrape["stock"]
                
            recent_scrapes_dict[stock].append(scrape)
        # should convert this to a map and list stock -> dup
        # self.logger.info(f"[predict] found {len(dup_articles_set)} duplicate articles")
        return recent_scrapes_dict
    
    def get_email_body(self, top_symbols_dict, run_id, lookback):
        
        email_body = "\n".join(f"{symbol}: {yes_count}" for symbol, yes_count in top_symbols_dict.items())
        email_body += f"\nrun_id: {run_id}, lookback: {lookback}"
    
        return email_body
    
    def get_stocks_by_yes_count(self, df, count):
        # Create a copy of the DataFrame to avoid modifying the original
        df_copy = df.copy()

        # Get unique YES counts
        unique_yes_counts = df_copy['YES'].unique()

        # Filter counts to include only those >= 4
        filtered_yes_counts = [yes for yes in unique_yes_counts if yes >= 4]

        # Sort the filtered counts in descending order
        sorted_yes_counts = sorted(filtered_yes_counts, reverse=True)

        # Get the top 'count' values if available
        top_yes_counts = sorted_yes_counts[:count] if len(sorted_yes_counts) >= count else sorted_yes_counts

        # Filter the DataFrame based on the filtered top YES counts
        top_symbols_df = df_copy[df_copy['YES'].isin(top_yes_counts)]

        # Convert the result to a dictionary with Symbol as key and YES count as value
        top_symbols_dict = top_symbols_df.set_index('Symbol')['YES'].to_dict()

        return top_symbols_dict
    
    def send_out_stock_info(self, top_symbols_dict, run_id, lookback):
        email_body = self.get_email_body(top_symbols_dict, run_id, lookback)
        recipient_email = os.environ["TO_EMAIL"]
        self.email.send_email(email_body, recipient_email)

    def execute_trade(self, top_symbols_dict):
        symbols = list(top_symbols_dict.keys())
        orders = self.trading_controller.build_orders(symbols)
        self.trading_controller.submit_orders(orders)

    # start point
    # @retry(stop=stop_after_attempt(3), wait=wait_random(min=25, max=35))
    def run_analysis(self, lookback, run_id):
        stocks = self.get_stocks_list(lookback, run_id)
        if not stocks:
            self.logger.info(f"[predict] no stocks found for prediction")
            return 

        rows = {}
        # current_time = datetime.now().astimezone(timezone.utc)

        for stock, scrapes_for_stock in stocks.items():
            stock_sym = stock.lower()
            responses = self.generate_analysis_for_stock(stock_sym, scrapes_for_stock)
            if not responses:
                continue
                
            rows[stock_sym] = responses
        df = self.convert_rows_to_csv(rows)
        
        self.save_openai_resp_as_csv(df, run_id, lookback)
        top_symbols_dict = self.get_stocks_by_yes_count(df, 4)

        self.send_out_stock_info(top_symbols_dict, run_id, lookback)
        # self.execute_trade(top_symbols_dict)
        # now execute the trade for symbol, yes_count in top_symbols_dict.items()
        # email out top stocks by YES value (just the top 3 )
    
    def start(self, run_id, lookback):
        self.run_analysis(run_id, lookback)