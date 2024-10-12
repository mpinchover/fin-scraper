import requests 
import uuid
import threading
from datetime import datetime, timezone
import time

stock_lists = ["test_list.txt", "test_list.txt", "test_list.txt", "test_list.txt"]
class JobController:
    

    def __init__(self, logger, storage, db):
        self.storage = storage
        self.logger = logger
        self.db = db
    
    def make_scrape_request(self, run_id, stock_list):
        url = 'http://localhost:8080/scrape-list'  # Replace with the actual URL
        headers = {
            'Content-Type': 'application/json'
        }

        data = {
            "run_id": run_id,
            "stock_list": stock_list,
        }

        response = requests.post(url, headers=headers, json=data)

    def save_run(self, run_id, cur_time):
        runs_collection = self.db["runs"]

        doc = {
            "run_id": run_id,
            "created_at": cur_time,
        }
        runs_collection.insert_one(doc)

    def start(self):
        run_id = str(uuid.uuid4())
        self.logger.info(f"[jobs_controller] Starting scrapes for run id: {run_id}")

        utc_now = datetime.now(timezone.utc)
        self.save_run(run_id, utc_now)

        max_threads = 5
        sema = threading.Semaphore(value=max_threads)
        threads = list()
        
        for stock_list in stock_lists:
            args = (run_id, stock_list)
            thread = threading.Thread(target=self.make_scrape_request, args=args)
            threads.append(thread)

        for idx, thread in enumerate(threads):
            self.logger.info(f"[jobs_controller] Starting jobs for worker {idx} run_id: {run_id}")
            time.sleep(5)
            thread.start() 

        self.logger.info(f"[jobs_controller] Waiting for jobs to complete for run_id: {run_id}")
        for thread in threads:
            thread.join()
        
        self.logger.info(f"[jobs_controller] Jobs completed for run_id: {run_id}")
        return run_id


    