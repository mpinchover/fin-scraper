docker build -t markets-scraper .
docker run --rm -p 5000:5000 markets-scraper
gcloud run deploy
gcloud run deploy --memory 4Gi --timeout=10m

WORKING CONFIGS
gcloud run deploy --memory 16Gi --timeout=15m --cpu 4

DEPLOY
gcloud run deploy --source . --memory 16Gi --timeout=30m --cpu 8 --concurrency 3
gcloud run deploy --source . --memory 32Gi --timeout=30m --cpu 8 --concurrency 5 ATTEMPTING

curl -X POST "HOST" \
 -H "Content-Type: application/json" \
 -d '{"run_id": "999e7073-8118-4052-84e2-1220765a92d8", "stock": "amzn"}'

curl -X POST http://localhost:8080/scrape -H "Content-Type: application/json" -d '{"run_id": "954e7073-8118-4052-84e2-1220765a92d8", "stock": "wmt"}'

curl -X POST http://localhost:8080/predict -H "Content-Type: application/json" -d '{"run_id": "954e7073-8118-4052-84e2-1220765a92d9"}'

https://stackoverflow.com/questions/49045725/gsutil-gcloud-storage-file-listing-sorted-date-descending

Connect to mongo server
mongosh "mongodb+srv://root:root@cluster0.hgj8f.mongodb.net/markets?retryWrites=true"

curl -X POST http://localhost:8080/predict -H "Content-Type: application/json" -d '{"lookback": "24"}'

GET DOCS PUBLISHED AFTER SCRAPED TIME
db.scrapes.find({ scraped_at: { $gt: ISODate("2024-10-05T20:00:00Z") } })

GET NUM DOCS PUBLISHED AFTER SCRAPED TIME
db.scrapes.find({ scraped_at: { $gt: ISODate("2024-10-05T20:00:00Z") } }).count()

RUN ALL JOBS
curl -X POST "localhost:8080/execute-jobs" \
-H "Content-Type: application/json" \
-d '{"stocks": ["wmt", "hd", "amzn", "msft", "nvda", "jpm", "t", "vz", "gs", "ge", "XOM", "googl", "mck", "cvx", "abc", "cost", "f", "vlo", "psx", "ci", "JBL", "AAL", "MDLZ", "TIAA", "CI", "PUSH", "COP", "GIS"]}'

curl -X POST "localhost:8080/execute-jobs" \
-H "Content-Type: application/json" \
-d '{"stocks": ["TMO", "BMY", "GS", "EPD", "USAA", "PM", "DHR", "NWM", "RAD", "MMM", "SBUX", "QCOM", "NOC", "COF", "TRV", "ARW", "HON", "DG", "DOW", "WHR", "ARMK", "PFGC", "CHSCP", "PBF", "AEP", "NRG", "CBRE", "GPS", "BKR", "DLTR", "LUMN", "PAG", "MU"]}'

Next steps
Try to retry on the scraper with backoff

CHECK active project
gcloud config get-value project

SET PROJECT
gcloud config set project $MY_PROJECT_ID

INSTALL DOCKER
https://docs.sevenbridges.com/docs/install-docker-on-linux

COPY file
gcloud compute scp svc_acc_key.json markets-scraper:PATH_TO_DIR --zone ZONE
