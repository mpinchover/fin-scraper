#!/bin/bash

curl -X POST "localhost:5001/scrape-list" -H "Content-Type: application/json" -d '{"stock_list": "list_5",  "run_id": "adf1d1cb-51bd-4a9e-8136-bd81ae8eb4d3"}'