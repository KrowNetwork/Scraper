#!/bin/bash
git pull

screen -dm -S scraper python scrape.py

