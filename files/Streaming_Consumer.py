import sys
from confluent_kafka import KafkaError, KafkaException
from confluent_kafka import Consumer

class Consume():

    def __init__(self):

        self.conf = {'bootstrap.servers': "0.0.0.0:9092",
        'group.id': "foo1",
        'auto.offset.reset': 'latest'}

        self.consumer = Consumer(self.conf)

    def basic_consume_loop(self, topics):
        try:
            self.consumer.subscribe(topics)

            while True:
                self.msg = self.consumer.poll(timeout=1.0)
                if self.msg is None: continue

                if self.msg.error():
                    if self.msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition event
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (self.msg.topic(), self.msg.partition(), self.msg.offset()))
                    elif self.msg.error():
                        raise KafkaException(self.msg.error())
                else:
                    print(self.msg.value())
        finally:
            # Close down consumer to commit final offsets.
            self.consumer.close()

if __name__ == '__main__':
    consumer = Consume()
    consumer.basic_consume_loop(["comments"])