#!/usr/bin/env python3.8

from random import choice
from argparse import ArgumentParser, FileType
from configparser import ConfigParser
from confluent_kafka import Producer

if __name__ == '__main__':
    # Parse the command line.
    parser = ArgumentParser()
    parser.add_argument('config_file', type=FileType('r'))
    args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    config_parser = ConfigParser()
    config_parser.read_file(args.config_file)
    config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

    # Produce data by selecting random values from these lists.
    topic = "smart-raw"
    date = '2013-04-10'
    serial_numbers = ['MJ0351YNG9Z0XA','MJ0351YNG9WJSA','MJ0351YNG9Z7LA','MJ0351YNGAD37A','MJ0351YNGABYAA','MJ1311YNG7ESHA','S2F0BE6T','W1F0LRXG','6XW099YJ']
    capacity_bytes = [30005,300059,300059,30005,300059,300059,15003,30006,150016]
    failure = 0
    count = 0
    for _ in range(10):

        serial_number = choice(serial_numbers)
        capacity_byte = choice(capacity_bytes)
        # TODO: check the form of the data
        producer.produce(topic, capacity_byte, serial_number, callback=delivery_callback)
        count += 1

    # Block until the messages are sent.
    producer.poll(10000)
    producer.flush()
