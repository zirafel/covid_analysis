# Import the libraries
from tweepy import OAuthHandler
import numpy as np
from datetime import datetime
import tweepy
import pandas as pd

import glob

import os
import time
# import twitter keys and tokens
from config import *



## Automating Scraping
# Calls API every 15 minutes to prevent overcalling

# 1. define a for-loop
# 2. define search parameter
# 3. define date period
# 4. define maxId for pagination
# 5. define no. of tweets to pull

class TweetBatchCollector(object):
    """ Tweet batch collector """

    def __init__(self, consumer_key, consumer_secret, access_token, access_token_secret):
        """
        :param consumer_key: consumer key
        :param consumer_secret: consumer_secret
        :param access_token: access_token
        :param access_token_secret: access_token_secret
        """
        # Authenticating Twitter API
        # set twitter keys/tokens
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        self.api = tweepy.API(auth)

    def collect_tweets(self, search_words, date_since, maxId, numTweets, numRuns ):
        ## Arguments:
        # search_words -> define a string of keywords for this function to extract
        # date_since -> define a date from which to start extracting the tweets
        # numTweets -> number of tweets to extract per run
        # numRun -> number of runs to perform in this program - API calls are limited to once every 15 mins, so each run will be 15 mins apart.
        # sinceId -> starts from this id
        ##

        # Define a pandas dataframe to store the date:
        db_tweets = pd.DataFrame(columns=['id','username', 'acctdesc', 'userlocation', 'following',
                                          'followers', 'totaltweets', 'usercreatedts', 'tweetcreatedts',
                                          'retweetcount', 'text', 'hashtags','lat','lon','placeName']
                                 )

        max_id = maxId

        # Define a for-loop to generate tweets at regular intervals
        for i in range(0, numRuns):
            try:
                # We will time how long it takes to scrape tweets for each run:
                start_run = time.time()

                # Collect tweets using the Cursor object
                # .Cursor() returns an object that you can iterate or loop over to access the data collected.
                # Each item in the iterator has various attributes that you can access to get information about each tweet
                tweets = tweepy.Cursor(self.api.search, q=search_words, lang="it", since=date_since, max_id = max_id, tweet_mode='extended').items(
                    numTweets)

                # Store these tweets into a python list
                tweet_list = [tweet for tweet in tweets]

                # Obtain the following info (methods to call them out):
                # user.screen_name - twitter handle
                # user.description - description of account
                # user.location - where is he tweeting from
                # user.friends_count - no. of other users that user is following (following)
                # user.followers_count - no. of other users who are following this user (followers)
                # user.statuses_count - total tweets by user
                # user.created_at - when the user account was created
                # created_at - when the tweet was created
                # retweet_count - no. of retweets
                # retweeted_status.full_text - full text of the tweet
                # tweet.entities['hashtags'] - hashtags in the tweet
                # tweet.coordinates - coordinates of tweet
                # tweet.placeName - place name
                # Begin scraping the tweets individually:
                noTweets = 0
                for tweet in tweet_list:

                    # Pull the values
                    id=tweet.id
                    username = tweet.user.screen_name
                    acctdesc = tweet.user.description
                    userlocation = tweet.user.location
                    following = tweet.user.friends_count
                    followers = tweet.user.followers_count
                    totaltweets = tweet.user.statuses_count
                    usercreatedts = tweet.user.created_at
                    tweetcreatedts = tweet.created_at
                    retweetcount = tweet.retweet_count
                    hashtags = tweet.entities['hashtags']
                    lat = np.nan
                    lon = np.nan
                    placeName = ''
                    try:
                        if(tweet.coordinates is not None):
                            lat = tweet.coordinates['coordinates'][1]
                            lon = tweet.coordinates['coordinates'][0]
                    except Exception:
                        lat = np.nan
                        lon = np.nan
                    try:
                        if(tweet.place is not None):
                            placeName = tweet.place.full_name
                    except Exception:
                        placeName = ''

                    try:
                        text = tweet.retweeted_status.full_text
                    except AttributeError:  # Not a Retweet
                        text = tweet.full_text

                    # Add the 15 variables to the empty list - ith_tweet:
                    ith_tweet = [id,username, acctdesc, userlocation, following, followers, totaltweets,
                                 usercreatedts, tweetcreatedts, retweetcount, text, hashtags, lat, lon, placeName]

                    # Append to dataframe - db_tweets
                    db_tweets.loc[len(db_tweets)] = ith_tweet

                    # increase counter - noTweets
                    noTweets += 1

                # Run ended:
                end_run = time.time()
                duration_run = round(end_run - start_run, 2)
            except Exception as ex:
                print(ex)
                print('Exception during collecting tweets,trying to continue..')

            print('no. of tweets scraped for run {} is {}'.format(i, noTweets))
            print('time take for {} run to complete is {}'.format(i, duration_run))

            #get max id
            if(len(db_tweets))>0:
                max_id = db_tweets.id.min()-1
            time.sleep(900)  # 15 minute sleep time

        self.flush_tweets(db_tweets)
        print('Scraping has completed!')

    def flush_tweets(self, db_tweets):
        to_csv_timestamp = datetime.today().strftime('%Y%m%d_%H%M%S')

        # Define working path and filename
        path = os.getcwd()
        filename = path + '/data/' + to_csv_timestamp + '_covid_tweets_it.csv'

        # Store dataframe in csv with creation date timestamp
        db_tweets.to_csv(filename, index=False)

    def get_max_id(self):
        db_tweets = self.read_db()
        if (db_tweets is None):
            return 0;
        max_id = db_tweets.id.max()
        return max_id

    def get_min_id(self):
        db_tweets = self.read_db()
        if(db_tweets is None):
            return 0;
        min_id = db_tweets.id.min()
        return min_id

    def read_db(self):
        # Define working path and filename
        dirpath = os.getcwd() + '/data/'

        all_files = glob.glob(dirpath + "/*.csv")
        if (len(all_files) == 0):
            return None

        li = []

        for filename in all_files:
            df = pd.read_csv(filename, header=0)
            li.append(df)

        db_tweets = pd.concat(li, axis=0, ignore_index=True)
        return db_tweets

if __name__ == '__main__':
    # Authenticating Twitter API
    # set twitter keys/tokens
    tweetBatchCollector = TweetBatchCollector(consumer_key=consumer_key, consumer_secret=consumer_secret, access_token=access_token, access_token_secret=access_token_secret )
    #get min id from previous tweets
    minId = tweetBatchCollector.get_min_id()-1
    #start collect
    tweetBatchCollector.collect_tweets("covid", "2020-04-01", minId, 2500, 4)