import mechanicalsoup as ms
from bs4 import BeautifulSoup
import redis 
import configparser
from elasticsearch import Elasticsearch, helpers
import sqlite3
import pandas as pd
import numpy as np
from neo4j import GraphDatabase

print("hello")
def init_db(db_name="ABCnews_politics.db"):
    connection = sqlite3.connect(db_name)
    cursor = connection.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS headlines (
            id INTEGER PRIMARY KEY,
            url TEXT,
            headline TEXT,
            date TEXT
        )
    ''')
    connection.commit()
    connection.close()

init_db()

class Neo4JConnector:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def print_greeting(self, message):
        with self.driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)
    
    def add_links(self, page, links):
        with self.driver.session() as session:
            session.execute_write(self._create_links, page, links)

    @staticmethod
    def _create_links(tx, page, links):
        page = page.decode('utf-8')
        tx.run("CREATE (:Page {url: $page})", page=page)
        for link in links:
            tx.run("MATCH (p:Page) WHERE p.url = $page "
                "CREATE (:Page {url: $link}) -[:LINKS_TO]-> (p)",
                link=link, page=page)
            # tx.run("CREATE (:Page {url: $link}) -[:LINKS_TO]-> (:Page {url: $page})",
            #     link=link, page=page.decode('utf-8'))

    def flush_db(self):
        print("clearing graph db")
        with self.driver.session() as session: 
            session.execute_write(self._flush_db)

    @staticmethod
    def _flush_db(tx):
        tx.run("MATCH (a) -[r]-> () DELETE a, r")
        tx.run("MATCH (a) DELETE a")

neo4j_connector = Neo4JConnector("bolt://localhost:7687", "neo4j", "webcrawl")
neo4j_connector.flush_db()
#connector.print_greeting("hello y'all")
#neo4j_connector.add_links(page, links)


config = configparser.ConfigParser()
config.read('example.ini')


es = Elasticsearch(
  "https://5da88b7e56c34616af8020a00b7210aa.us-central1.gcp.cloud.es.io:443",
  api_key="V3ZGWExwRUJ2Nm9nRHBfQmxLY1A6MXRsYjJlekxUcEN3WFNPZm56ZGN6QQ=="
)


print(es.info())

def write_to_elastic(es, url, html):
    url = url.decode('utf-8')
    es.index(index='webpages', document={'url': 'url','html': 'html'})



def crawl(browser, r, es, neo4j_connector, url, db_name="crawl_data.db"):
    print("downloading page")
    browser.open(url)

    write_to_elastic(es, url, str(browser.page))

    print("parsing for links")
    a_tags = browser.page.find_all("a")
    hrefs = [a.get("href") for a in a_tags]

    abc_domain = "https://abcnews.go.com"
    print('parsing webpage for links')
    links = []
    for href in hrefs:
        if href:
            if href.startswith("/Politics/"):
                full_url = abc_domain + href
            elif href.startswith("https://abcnews.go.com/Politics/"):
                full_url = href
            else:
                continue
            links.append(full_url)

    # Debugging output
    print(f"Links to push: {links}")

    if links:  # Ensure links is not empty
        r.lpush("links", *links)
    else:
        print("No valid links to push.")

    if neo4j_connector:
        neo4j_connector.add_links(url, links)

    # Extract headline and date
    soup = BeautifulSoup(browser.page.content, 'html.parser')
    headline = soup.find('h2').text if browser.page.find('h2') else 'No headline found'
    timestamp_elem = soup.find('h3', class_='video-info-module__text--subtitle__timestamp')
    date = timestamp_elem.text if timestamp_elem else 'No date found'
    

   
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    cursor.execute('''
        INSERT INTO headlines (url, headline, date) VALUES (?, ?, ?)
    ''', (url, headline, date))
    conn.commit()
    conn.close()

browser = ms.StatefulBrowser()

r = redis.Redis()
r.flushall()

start_url= "https://abcnews.go.com/Politics"
r.lpush("links", start_url)
print(r.keys("*"))
while link := r.rpop('links'): 
    print(str(link))
    crawl(browser, r, es, None, link)


neo4j_connector.close()