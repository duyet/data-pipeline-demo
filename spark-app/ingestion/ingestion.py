import json
import os
import time

import tweepy
import yaml
from kafka import KafkaProducer
from kafka.client import KafkaClient

TIME_DELAY = 5


class TweeterStreamListener(tweepy.StreamListener):
    """ A class to read the twiiter stream and push it to Kafka"""

    def __init__(self, api, kafka_host='localhost:9092', stream_config={}):
        super(tweepy.StreamListener, self).__init__()

        self.api = api
        self.stream_config = stream_config

        print('bootstrap_servers:', kafka_host)
        self.producer = KafkaProducer(bootstrap_servers=kafka_host)

        # Add Kafka topics
        topic = self.stream_config.get('kafka_topic')
        if topic:
            client = KafkaClient(bootstrap_servers=kafka_host)
            client.add_topic(topic)

    def on_status(self, status):
        """ This method is called whenever new data arrives from live stream.
        We asynchronously push this data to kafka queue"""
        msg = status.text
        id_str = status.id_str
        created_at = status.created_at
        messages = '{}##{}'.format(id_str, msg).encode('utf-8')
        
        topic = self.stream_config.get('kafka_topic')
        self.producer.send(topic, messages)
        print("Send " + id_str)

        time.sleep(TIME_DELAY)
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True  # Don't kill the stream

    def on_timeout(self):
        return True  # Don't kill the stream

if __name__ == '__main__':
    dir_path = os.path.dirname(os.path.realpath(__file__))
    config_path = os.path.join(dir_path, './twitter_config.yml')
    config = yaml.safe_load(open(config_path, 'r'))
    consumer_key = config['consumerKey']
    consumer_secret = config['consumerSecret']
    access_key = config['accessToken']
    access_secret = config['accessTokenSecret']
    kafka_host = os.getenv('KAFKA_HOST', default='localhost:9092')
    stream_config = config['stream_config']

    # Create Auth object
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_key, access_secret)
    api = tweepy.API(auth)

    # Custom Filter rules pull all traffic for those filters in real time.
    #stream.filter(track = ['love', 'hate'], languages = ['en'])
    for config in stream_config:
        # Create stream and bind the listener to it
        print(f'Connect to {kafka_host}')
        stream = tweepy.Stream(auth, listener=TweeterStreamListener(
            api, kafka_host, stream_config=config))
        print(f'Track by keywords: {config.get("twitter_keywords")}')
        stream.filter(track=config.get('twitter_keywords'))
