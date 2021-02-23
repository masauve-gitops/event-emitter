import argparse
import json
import logging
import os
import random
import time
import uuid

from kafka import KafkaProducer

EVENT_TEMPLATES = [
    { "eventCategory": "STATION", "eventValue": "HIGH_VOLUME", "eventSource": "CAMERA"},
    { "eventCategory": "STATION", "eventValue": "HIGH_VOLUME", "eventSource": "CAMERA"},
    { "eventCategory": "TRAIN", "eventValue": "PARTS_WARNING", "eventSource": "SENSOR"},
    { "eventCategory": "STATION", "eventValue": "HIGH_VOLUME", "eventSource": "CAMERA"}
]


ATM_EVENT = [
    { "eventCategory": "ATM_WITHDRAWAL", "eventValue": "Geo-US", "eventSource": "ATM"}

]



CUSTOMER = [

    'Berri',
    'Rosemont'
]

def generate_event():
    ret = EVENT_TEMPLATES[random.randint(0, 3)]
    return ret




def main(args):
    logging.info('brokers={}'.format(args.brokers))
    logging.info('topic={}'.format(args.topic))
    logging.info('rate={}'.format(args.rate))

    logging.info('creating kafka producer')
    producer = KafkaProducer(bootstrap_servers=args.brokers)

    logging.info('begin sending events')
    while True:
        logging.info(json.dumps(generate_event()).encode())
        producer.send(args.topic, json.dumps(generate_event()).encode(), json.dumps(CUSTOMER[random.randint(0, 1)]).encode())
        time.sleep(10.0)
    logging.info('end sending events')




def get_arg(env, default):
    return os.getenv(env) if os.getenv(env, '') is not '' else default


def parse_args(parser):
    args = parser.parse_args()
    args.brokers = get_arg('KAFKA_BROKERS', args.brokers)
    args.topic = get_arg('KAFKA_TOPIC', args.topic)
    args.rate = get_arg('RATE', args.rate)
    return args


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('starting kafka-openshift-python emitter')
    parser = argparse.ArgumentParser(description='emit some stuff on kafka')
    parser.add_argument(
        '--brokers',
        help='The bootstrap servers, env variable KAFKA_BROKERS',
        default='localhost:9092')
    parser.add_argument(
        '--topic',
        help='Topic to publish to, env variable KAFKA_TOPIC',
        default='event-input-stream')
    parser.add_argument(
        '--rate',
        type=int,
        help='Lines per second, env variable RATE',
        default=1)
    args = parse_args(parser)
    main(args)
    logging.info('exiting')
