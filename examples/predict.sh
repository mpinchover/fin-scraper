#!/bin/bash

curl -X POST "localhost:5001/predict" -H "Content-Type: application/json" -d '{"stock_list": "list_5",  "run_id": "adf1d1cb-51bd-4a9e-8136-bd81ae8eb4d3", "lookback": 6}'