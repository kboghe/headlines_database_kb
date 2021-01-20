#import required packages#
from time import time,sleep
from datetime import datetime
from tabulate import tabulate
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
def connect_todb(info):
    #this function expects a csv file with 5 columns (database,user,password,host,port)...
    #...with a single row (besides the header) containing the login info
    info = pd.read_csv(info, sep=",", encoding="utf-8")
    print("\nConnecting to database...\n"
          "========================\n")
    con = psycopg2.connect(database=info.database[0], user=info.user[0], password=info.password[0], host=info.host[0], port=info.port[0])
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
    print("\nHeadlines table created successfully")
def create_table_outlets(con):
    cur = con.cursor()
    cur.execute('''CREATE TABLE outlets
    (outlet VARCHAR NOT NULL,
    outlet_id INT,
    account VARCHAR PRIMARY KEY,
    category TEXT NOT NULL,
    importance VARCHAR NOT NULL,
    country VARCHAR NOT NULL);''')
    con.commit()
    print("\nOutlets table created successfully")
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
def update_message(df,connection):
    print(color.BOLD + color.GREEN + "Update completed!\n"
          "Added " + str(len(df)) + " headlines to the database.\n"+ color.END)
    cur = connection.cursor()
    print("\nNumber of articles scraped today by country:\n")
    cur.execute('''
    SELECT
    country, COUNT(headline)
    FROM(SELECT
    account, headline
    FROM
    headlines
    WHERE
    date(date) = '{0}') as headlines_today
    INNER
    JOIN
    outlets
    ON(headlines_today.account = outlets.account) GROUP
    BY
    outlets.country;;'''.format(str(datetime.today().strftime('%Y-%m-%d'))))
    table_current = tabulate(cur.fetchall(),tablefmt="grid")
    con.commit()
    print(table_current+"\n\n")
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

###############
#FIRST TIMER?##
###############
firsttimer = input('Is your database currently empty and do you want me to create all the necessary tables for you? (y/n)')

#######
#SETUP#
#######
welcome_message()
#connect to server#
con = connect_todb("connection_db.csv")

if 'y' in firsttimer:
    #create tables
    #1: headlines table
    create_table_headlines(con) #remove hashtag if you're setting up a new database and need to create a new table
    #2: outlets table
    create_table_outlets(con)
    outlets = pd.read_csv('news_channels.csv',sep=",",encoding="utf-8",quotechar="'")
    upload_db_latestnews(con,outlets,'outlets')
    #check vpn settings (remove 'stored settings' and set 'save = 1' for initializing new settings)
    initialize_VPN(save=1)


####################################
#launch periodic update of database#
####################################
while True:
    countdown(10)
    request_newvpn()
    latest = pd.DataFrame(db_latestnews(con))
    if len(latest) == 0:
        accounts = pd.read_csv('news_channels.csv',sep=',',quotechar='"',usecols=["account","importance"])
        accounts = accounts[accounts.importance != 'international']['account']
        dates = [pd.to_datetime('1900-01-01')] * len(accounts)
        latest = pd.DataFrame(list(accounts),list(dates)).reset_index()
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
        request_newvpn()

    upload_db_latestnews(con,updated_headlines_total,"headlines")
    update_message(updated_headlines_total,con)