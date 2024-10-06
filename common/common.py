def get_stocks_list():
    filename = "stocks_list.txt"
    with open(filename, 'r') as file:
        file_contents = file.read().split("\n")
        return file_contents

def save_articles_to_storage(articles, timestamp, stock_sym):
    client = storage.Client.from_service_account_json('svc_acc_key.json')
    bucket = client.get_bucket('news-article-scrapes')

    directory = f"scrapes/{timestamp}/yahoo/{stock_sym.lower()}/"
    
    for article in articles:
        sanitized_title = re.sub(r'[\/:*?"<>|]', '', article['title']).lower().replace("'", "").replace(" ", "_")
        key = f"{directory}{sanitized_title}.txt"
        new_blob = bucket.blob(key)
        new_blob.upload_from_string(json.dumps(article))
