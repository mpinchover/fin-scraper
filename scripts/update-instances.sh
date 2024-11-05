#!/bin/bash

gcloud compute ssh VM_INSTANCE_NAME --zone=ZONE --command="docker ps -a -q --filter='name=markets-scraper'"