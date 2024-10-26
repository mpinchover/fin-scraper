from dotenv import load_dotenv
import os
import sys 

load_dotenv("../.env")

ZONE = os.environ["ZONE"]
PROJECT = os.environ["PROJECT"]
instance = sys.argv[1]  

script = f'gcloud compute ssh --project={PROJECT} --zone={ZONE} --ssh-flag="-ServerAliveInterval=30" {instance}'
os.system(script)