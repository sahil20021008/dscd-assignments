#!/usr/bin/env python

import pika
import sys 
import json
import os

def updateSubscription(user,status,youtuber):
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
        channel.basic_publish(exchange='youtube_server', routing_key='user_to_server', body=json.dumps({'user': user, 'status': status, 'youtuber': youtuber}))
        print('SUCCESS')
    except pika.exceptions.UnroutableError:
        print('FAILED')
        
    connection.close()

    receiveNotification(user)


def callback(ch, method, properties, body):
    dict1 = json.loads(body)
    print(f" New Notification: {dict1['youtuber']} uploaded {dict1['videoName']}")
    ch.basic_ack(delivery_tag = method.delivery_tag)
    
 
def receiveNotification(user):
    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host='35.225.124.206'))
    # channel = connection.channel()

    credentials = pika.PlainCredentials('dscd', 'dscd')
    parameters = pika.ConnectionParameters('35.225.124.206', credentials=credentials)
    
    # Establish connection
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.exchange_declare('youtube_server', exchange_type='direct')
    channel.queue_declare(queue='server_to_'+user, durable=True)

    channel.queue_bind(exchange='youtube_server', queue='server_to_'+user, routing_key='server_notify_'+user)

    channel.basic_consume(queue='server_to_'+user, on_message_callback=callback)

    print(' [*] Waiting for notifications to come. To exit press CTRL+C')

    channel.start_consuming()



if __name__ == '__main__':
    try:
        user = sys.argv[1]
        try:
            subscribeStatus = sys.argv[2]
        except IndexError:
            subscribeStatus = None
        if subscribeStatus == 's' or subscribeStatus == 'u':
            youtuber = sys.argv[3]
        
        if subscribeStatus == 's' or subscribeStatus == 'u':
            updateSubscription(user,subscribeStatus,youtuber)
        else:
            receiveNotification(user)
        
    except KeyboardInterrupt:
        print('Interrupted')
