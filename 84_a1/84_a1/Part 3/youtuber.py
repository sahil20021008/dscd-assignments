#!/usr/bin/env python

import pika
import sys 
import json
import os


def publishVideo(youtuber, videoName):

    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host='35.225.124.206'))
    # channel = connection.channel()

    credentials = pika.PlainCredentials('dscd', 'dscd')
    parameters = pika.ConnectionParameters('35.225.124.206', credentials=credentials)
    
    # Establish connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare('youtube_server', exchange_type='direct')

    try:
        channel.basic_publish(exchange='youtube_server', routing_key='youtuber_to_server', body=json.dumps({'youtuber': youtuber, 'videoName': videoName}))
        print('SUCCESS')
    except pika.exceptions.UnroutableError:
        print('FAILED')

    connection.close()


if __name__ == '__main__':
    try:
        youtuber = sys.argv[1]
        videoName = ' '.join(sys.argv[2:])
        publishVideo(youtuber, videoName)
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
