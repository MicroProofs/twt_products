


import tweepy
import re
import time

import sys
import os
import inspect
currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)
from aws_rds.rds_connector import insertor
# or "ebay" in urlThing['expanded_url'] or "etsy" in urlThing['expanded_url']

# for i in api.search(q='(deals amazon) OR (deals etsy) OR (deals ebay) OR (halloween amazon) OR (halloween etsy) OR (halloween ebay)'):
#     print('\n')
#     for urlThing in i.entities['urls']:
#         if not "twitter" in urlThing['expanded_url']:
#             print(urlThing['expanded_url'])
#             print(i.text)
#             flag = True
#             for hashT in i.entities['hashtags']:
#                 print("#" + hashT['text'], end=' ')


auth = tweepy.OAuthHandler('',
                           '')
auth.set_access_token('',
                      '')

api = tweepy.API(auth)
linkholder = {}
hashtagholder = {}
keywords = ['sold out', 'blowout sale', 'flash sale',
            'amazon sale', 'ebay sale', 'holiday sale']


class MyStreamListener(tweepy.StreamListener):

    def on_status(self, status):
        non_url = re.compile(r'http[s]*://[^\s\t\n]+')
        non_hashtag = re.compile(r'#[^\s\t\n]+')
        flag = False
        for urlThing in status.entities['urls']:
            if(not "twitter" in urlThing['expanded_url'] and not "onlyfans.com" in urlThing['expanded_url']
               and not "manyvids.com" in urlThing['expanded_url'] and not "niteflirt.com" in urlThing['expanded_url'] and not "clips4sale.com" in urlThing['expanded_url']):
                print(urlThing['expanded_url'])
                print(status.text)
                if urlThing['expanded_url'] in linkholder:
                    linkholder[urlThing['expanded_url']
                               ] = linkholder[urlThing['expanded_url']]+1
                else:
                    linkholder[urlThing['expanded_url']] = 1
        for hashT in status.entities['hashtags']:
            print("#" + hashT['text'], end=' ')
            if ("#" + hashT['text']) in hashtagholder:
                hashtagholder[("#" + hashT['text'])
                              ] = hashtagholder["#" + hashT['text']]+1
            else:
                hashtagholder[("#" + hashT['text'])] = 1
        print("")
        # if (status.entities['urls'] and flag) or status.entities['hashtags']:
        #     print("\n")
        #     flag = False


myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth=api.auth, listener=myStreamListener)


myStream.filter(track=keywords, languages=['en'], is_async=True)


time.sleep(900)
myStream.disconnect()


print(linkholder)
print("\n\n")
print(hashtagholder)
insertor(linkholder, hashtagholder, keywords)
