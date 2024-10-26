#!/bin/bash

sudo apt update
sudo apt -y install git
curl -fsSL https://get.docker.com/ | sh
sudo usermod -aG docker USER