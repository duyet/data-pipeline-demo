from __future__ import print_function

import argparse
import json
import os
import re

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils


def main():
    parser = argparse.ArgumentParser(
        description="Gets twitter data from Kafka and work with it.")
    parser.add_argument("broker", nargs=1, help="broker name")
    parser.add_argument("topics", nargs="+", help="topics list")
    parser.add_argument("batch", nargs=1, type=int,
                        help="Batch duration for StreamingContext")
    args = parser.parse_args()

    broker = args.broker[0]
    topics = args.topics
    batch_duration = args.batch[0]

    print(broker, topics, type(batch_duration))

    spark_master = os.getenv('SPARK_MASTER', default='local[2]')
    conf = SparkConf().setMaster(spark_master).setAppName("Streamer")
    sc = SparkContext(conf=conf)

    # sc = SparkContext(appName="PythonStreamingKafkaWordCount")
    sc.setLogLevel("WARN")
    ssc = StreamingContext(sc, batch_duration)

    # brokers, topics = 'localhost:9092', 'test2'
    kvs = KafkaUtils.createDirectStream(
        ssc, topics, {"metadata.broker.list": broker})

    text_pattern = r'[A-z]*'

    lines = kvs.map(lambda x: x[1])
    ssc.checkpoint("./checkpoint-tweet")

    lines.count().map(lambda x: 'Tweets in this batch: %s' % x).pprint()

    sentient_tweet = lines.map(lambda line: process_text(line))
    sentient_tweet.pprint()

    ssc.start()
    ssc.awaitTermination()


def process_text(text):
    from nltk.corpus import stopwords
    from nltk.tokenize import TweetTokenizer
    from textblob import TextBlob

    stop_words = stopwords.words('english')
    tknzr = TweetTokenizer()
    tokenized_words = tknzr.tokenize(text)
    processed_text = [
        word for word in tokenized_words if word not in stop_words]

    sentence = TextBlob(' '.join(processed_text))

    return (text, sentence.noun_phrases, sentence.sentiment.polarity, sentence.sentiment.subjectivity)


if __name__ == "__main__":
    main()
