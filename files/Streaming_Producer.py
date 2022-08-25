import json
import time
import pandas as pd
from confluent_kafka import Producer
import socket

class Produce():

        def __init__(self):
                self.conf = {'bootstrap.servers': "0.0.0.0:9092",
                        'client.id': socket.gethostname()}
                self.topic_1 = 'comments'
                self.topic_2 = 'posthistory'
                self.producer = Producer(self.conf)

        def produce_comments_data(self):
                '''read comments data, produce rows'''
                self.comments_data = pd.read_xml('data/Comments.xml')
                # self.posthistory_data = pd.read_xml('data/PostHistory.xml')
                for i,j in self.comments_data.iterrows():
                        print(i,str(j['Id'] )+ str(j['Text']))
                        self.producer.produce(self.topic_1, key=str(i), value=str(j['Id'] )+ str(j['Text']))
                        print('comment data produced')
                        time.sleep(0.5)
                self.producer.flush()

        def produce_posthistory_data(self):
                '''
                read posthistory data, produce rows
                :return:
                '''
                # self.comments_data = pd.read_xml('data/Comments.xml')
                self.posthistory_data = pd.read_xml('data/PostHistory.xml')
                for i,j in self.posthistory_data.iterrows():
                        print(i, str(j['Id']) + str(j['Text']))
                        self.producer.produce(self.topic_1, key=str(i), value=str(j['Id']) + str(j['Text']))
                        print('comment data produced')
                        time.sleep(0.5)
                self.producer.flush()

if __name__ == '__main__':
        produce = Produce()
        produce.produce_comments_data()
        produce.produce_posthistory_data()