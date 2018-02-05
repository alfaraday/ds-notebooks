import socket
import sys
import requests
import requests_oauthlib
import json

ACCESS_TOKEN = '8534722-Jo2pEsKJEp1YGNLhd5lEHcOn3tRmXxwqcQf0Gn1kYf'
ACCESS_SECRET = 'GRvwvqyPRuBSuSZUU3vhnGog3TpXyUUedk7gglY5j5S3O'
CONSUMER_KEY = 'dnlL1XnN0RXoHjfxNZEP2Ea5D'
CONSUMER_SECRET = 'JOa3zoK6m5vYZ9sb5q7BI2dE5F5BHdB7lsYrfDKzDdp6O2ARWI'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,
                                   ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'),
                  ('locations', '-130,-20,100,50'),
                  ('track', '#')]
    query_url = url + '?' + '&'.join(
        [str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        try:
            full_tweet = json.loads(line)
            tweet_text = full_tweet['text']
            print("Tweet Text: " + tweet_text)
            print ("------------------------------------------")
            tcp_connection.send(tweet_text + '\n')
        except Exception:
            e = sys.exc_info()[0]
            print(f"Error: {e}")
