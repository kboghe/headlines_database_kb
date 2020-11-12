#import required packages#
from time import time,sleep
import sys
import os
import re
import json
import pandas as pd
from nordvpn_switcher import initialize_VPN,rotate_VPN
import snscrape.modules.twitter as sntwitter
import spacy
import preprocessor as p
import psycopg2
import psycopg2.extras as extras
pd.options.mode.chained_assignment = None
p.set_options(p.OPT.URL,p.OPT.MENTION,p.OPT.RESERVED,p.OPT.SMILEY)
nlp = spacy.load('nl_core_news_sm')

#################
#write functions#
#################
#0.general
def get_filenames(folders):
    for folder in folders:
        files = []
        for r, d, f in os.walk(folder):
            for file in f:
                if '.csv' in file:
                    files.append(os.path.join(r, file))
    return files
#1.connect to database
def connect_todb():
    print("\nConnecting to database...\n"
          "========================\n")
    con = psycopg2.connect(database="newsheadlines_db", user="news_db_manager", password="natesilver538", host="192.168.2.66", port="5432")
    print("Done!\n")
    return con
#1.1. create table
def create_table_headlines(con):
    cur = con.cursor()
    cur.execute('''CREATE TABLE headlines
          (id   BIGINT  PRIMARY KEY,
          account VARCHAR NOT NULL,
          url   TEXT  NOT NULL,
          date  TIMESTAMPTZ NOT NULL,
          headline  TEXT    NOT NULL,
          links TEXT,
          media TEXT,
          replies INT,
          retweets INT,
          likes INT,
          quotes INT,
          locations TEXT,
          persons   TEXT,
          organisations TEXT,
          events    TEXT,
          groups TEXT,
          places TEXT,
          objects TEXT);''')
    con.commit()
    print("\nTable created successfully")
#2.retrieve latest headlines from database
def db_latestnews(connection):
    print("\nRetrieving most recent headlines in database...\n"
          "===============================================\n")
    cur = connection.cursor()
    cur.execute('''SELECT DISTINCT ON (account)
       date,account
       FROM   headlines
       ORDER  BY account, "date" DESC;;''')
    latest_headlines = cur.fetchall()
    con.commit()
    print("Done!\n")
    return latest_headlines
#3.scrape latest headlines from twitter
def get_newsupdate(account,latestpost):
    print("\nScraping latest headlines of " + account + "\n"
          "----------------------------------")
    keys = ['url','date','renderedContent','id','tcooutlinks','conversationId',
            'media','replyCount','retweetCount','likeCount','quoteCount']
    for attempt in range(3):
        try:
            twitterposts = {key: [] for key in keys}
            for i, post in enumerate(sntwitter.TwitterUserScraper(account).get_items()):
                post = json.loads(post.json())
                #check whether current post is already in datase or not (if so, break the operation)
                current_latestdate = pd.to_datetime(post['date']).isoformat() < latestpost.isoformat()
                if current_latestdate is True:
                    break
                for key in keys:
                    if key in keys:
                        twitterposts[key].append(post[key])
                    else:
                        pass
        except AttributeError:
            print("Attribute error received. Retrying to fetch tweets after short pauze...\n")
            sleep(10)
            continue
        else:
            twitterposts['account'] = [account] * len(twitterposts['id'])
            twitterposts = pd.DataFrame(twitterposts)
            break
    else:
        raise Exception("Sorry, something went wrong while fetching tweets...")
    print("Scraped " + str(len(twitterposts)) + " new headlines.\n")
    return twitterposts
#4.clean posts for headlines table
def clean_newstweets(df):
    news = df
    news = news[['id','account','url','date','renderedContent','tcooutlinks','media','replyCount','retweetCount','likeCount','quoteCount']]
    news['date'] = [x.isoformat() for x in pd.to_datetime(news.date.copy())]
    ner_categories = ["GPE", "PERSON", "ORG", "EVENT","NORP","LOC","FAC"]  # only allow these NER categories to be detected
    news['renderedContent'] = [p.clean(x) for x in news['renderedContent'].copy()]
    news['renderedContent'] = [re.sub(r'\[.*?]|liveblog|live|\||breaking|breaking news|(premium)', '', x,flags=re.IGNORECASE) for x in news['renderedContent'].copy()]
    entities = [nlp(x) for x in news['renderedContent']]
    ner_cleaned = {key: [] for key in ner_categories}
    for doc in entities:
        for key in ner_categories:
            ner_cleaned[key].append([ent.lemma_ for ent in doc.ents if ent.label_ == key])
    for key in ner_categories:
        news[key] = ner_cleaned[key]
    news['media'] = list(map(lambda x: json.dumps(x), news['media'].copy()))
    news.columns = ['id','account','url','date','headline','links','media','replies','retweets','likes','quotes','locations','persons','organisations','events','groups','places','objects']
    return news
#5. upload new headlines to database
def upload_db_latestnews(con,df,table):
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query = "INSERT INTO %s(%s) VALUES %%s" % (table, cols)
    cursor = con.cursor()
    try:
        extras.execute_values(cursor, query, tuples)
        con.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        con.rollback()
        cursor.close()
        raise Exception("Error!")
    print("Uploaded to database.")
    cursor.close()

#other stuff#
class color:
   PURPLE = '\033[95m'
   CYAN = '\033[96m'
   DARKCYAN = '\033[36m'
   BLUE = '\033[94m'
   GREEN = '\033[92m'
   YELLOW = '\033[93m'
   RED = '\033[91m'
   BOLD = '\033[1m'
   UNDERLINE = '\033[4m'
   END = '\033[0m'
def welcome_message():
    print(color.DARKCYAN + color.BOLD + """
 _  _  ___    _    ___   _     ___  _  _  ___  ___        ___   ___  ___    _    ___  ___  ___ 
| || || __|  /_\  |   \ | |   |_ _|| \| || __|/ __|      / __| / __|| _ \  /_\  | _ \| __|| _ \ 
| __ || _|  / _ \ | |) || |__  | | | .` || _| \__ \      \__ \| (__ |   / / _ \ |  _/| _| |   /
|_||_||___|/_/ \_\|___/ |____||___||_|\_||___||___/      |___/ \___||_|_\/_/ \_\|_|  |___||_|_\ 
#################################################################################################\n\n""" + color.END)
def update_message(df):
    print(color.BOLD + color.GREEN + "Update completed!\n"
          "Added " + str(len(df)) + " headlines to the database.\n"+ color.END)
def countdown(t):
    print("\nUpdating headlines again in:\n"
          "################################")
    while t:
        mins, secs = divmod(t, 60)
        timer = '{:02d}:{:02d}'.format(mins, secs)
        sys.stdout.write("\r"+timer)
        sleep(1)
        t -= 1
        sys.stdout.flush()
    print('\nUpdate initiated!\n')
def request_newvpn():
    print("\nSwitching to a different VPN for scraping...\n"
          "==============================================\n")
    rotate_VPN()

#######
#SETUP#
#######
welcome_message()
#connect to server#
con = connect_todb()
#create tables
#1: headlines table
#create_table_headlines(con) #remove hashtag if you're setting up a new database and need to create a new table
#2: outlets table

#check vpn settings (remove 'stored settings' and set 'save = 1' for initializing new settings)
#initialize_VPN(save=1)

####################################
#launch periodic update of database#
####################################
while True:
    countdown(1800)
    request_newvpn()
    latest = pd.DataFrame(db_latestnews(con))
    latest.columns = ['date','account']
    updated_headlines_total = pd.DataFrame()
    for index,row in latest.iterrows():
        for attempt in range(3):
            try:
                headlines = get_newsupdate(account=row['account'],latestpost=row['date'])
                sleep(5)
                headlines = clean_newstweets(headlines)
            except:
                print("Something went wrong! Retrying after 5 minutes...")
                sleep(300)
                continue
            else:
                updated_headlines_total = updated_headlines_total.append(headlines)
                break
        else:
            raise Exception("\nSCRAPER IS BROKEN\n")

    upload_db_latestnews(con,updated_headlines_total,"headlines")
    update_message(updated_headlines_total)