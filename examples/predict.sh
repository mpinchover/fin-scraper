#!/bin/bash

curl -X POST "localhost:5001/predict" -H "Content-Type: application/json" -d '{"run_id": "test_id", "lookback": 24}'