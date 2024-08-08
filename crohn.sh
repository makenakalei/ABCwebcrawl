#!/bin/bash

cd /Users/makenarobison/Desktop/DistributedSystems
source env/bin/activate
export ELASTIC_PASSWORD="ELASTIC"
python crawlBot.py > crawlBot.log
