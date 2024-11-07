#!/bin/bash

curl -X POST "localhost:5001/scrape-list" -H "Content-Type: application/json" -d '{"stock_list": "list_test_1", "run_id": "test_id"}'