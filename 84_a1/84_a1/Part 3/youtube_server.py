#!/usr/bin/env python

import pika
import sys 
import json
import os

users = {}


def sendNotification(youtuber, videoName):
    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host='35.225.124.206'))
    # channel = connection.channel()

    credentials = pika.PlainCredentials('dscd', 'dscd')
    parameters = pika.ConnectionParameters('35.225.124.206', credentials=credentials)
    
    # Establish connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare('youtube_server', exchange_type='direct')

    for user in users:
        if youtuber in users[user]:
            channel.basic_publish(exchange='youtube_server', routing_key='server_notify_'+user, body=json.dumps({'youtuber': youtuber, 'videoName': videoName}), properties=pika.BasicProperties(delivery_mode = 2))
    connection.close()


def callback(ch, method, properties, body):
    dict1 = json.loads(body)
    print(f" {dict1['youtuber']} uploaded {dict1['videoName']}")
    sendNotification(dict1['youtuber'],dict1['videoName'])

          
def callback2(ch, method, properties, body):
    dict1 = json.loads(body)
    print(f"{dict1['user']} logged in")
    if dict1['status'] == 's':
        if dict1['user'] in users.keys():
            if dict1['youtuber'] in users[dict1['user']]:
                print(f"{dict1['user']} is already subscribed to {dict1['youtuber']}")
            else:
                users[dict1['user']].append(dict1['youtuber'])
                print(f"{dict1['user']} subscribed to {dict1['youtuber']}")
        else:
            users[dict1['user']] = [dict1['youtuber']]
            print(f"{dict1['user']} subscribed to {dict1['youtuber']}")
    else:
        if dict1['user'] in users.keys():
            if dict1['youtuber'] in users[dict1['user']]:
                users[dict1['user']].remove(dict1['youtuber'])
                print(f"{dict1['user']} unsubscribed to {dict1['youtuber']}")
            else:
                print(f"{dict1['user']} is not subscribed to {dict1['youtuber']}")
        else:
            print(f"{dict1['user']} is not subscribed to {dict1['youtuber']}")
    ch.basic_ack(delivery_tag = method.delivery_tag)
    

def main():
    # connection = pika.BlockingConnection(
    # pika.ConnectionParameters(host='35.225.124.206'))
    # channel = connection.channel()

    credentials = pika.PlainCredentials('dscd', 'dscd')
    parameters = pika.ConnectionParameters('35.225.124.206', credentials=credentials)
    
    # Establish connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare(exchange='youtube_server', exchange_type='direct')

    result = channel.queue_declare(queue='', exclusive=True)
    queue_name = result.method.queue

    result2 = channel.queue_declare(queue='', exclusive=True)
    queue_name2 = result2.method.queue

    channel.queue_bind(exchange='youtube_server', queue=queue_name, routing_key='youtuber_to_server')
    channel.queue_bind(exchange='youtube_server', queue=queue_name2, routing_key='user_to_server')

    channel.basic_consume(
    queue=queue_name, on_message_callback=callback)

    channel.basic_consume(
    queue=queue_name2, on_message_callback=callback2)

    print('Youtube Server is running ')
    channel.start_consuming()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)