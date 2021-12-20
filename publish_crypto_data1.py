#!/usr/bin/env python3

# environment variable setup for private key file
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="corded-fragment-335523-5b12d0a70a4c.json"

import json
from google.cloud import pubsub_v1
from concurrent.futures import TimeoutError
import time
import asyncio
import json
import websockets

# GCP topic, project & subscription ids
PUB_SUB_TOPIC = "trainingtopic"
PUB_SUB_PROJECT = "corded-fragment-335523"
PUB_SUB_SUBSCRIPTION = "trainingtopic-sub"

# Pub/Sub consumer timeout
timeout = 3.0

# callback function for processing consumed payloads
# prints recieved payload
def process_payload(message):
    print(f"Received {message.data}.")
    message.ack()

# producer function to push a message to a topic
def push_payload(payload, topic, project):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project, topic)
    #payload['Content-Type'] = 'application/json;charset=UTF-8'
    data = json.dumps(payload).encode("utf-8")
    #j_dump = "["+ json.dumps(payload) + "]"
    #data = j_dump.encode("utf-8")
    future = publisher.publish(topic_path, data=data)
    print("Pushed message to topic.")

# consumer function to consume messages from a topics for a given timeout period
def consume_payload(project, subscription, callback, period):
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project, subscription)
    print("Listening for messages on {subscription_path}..\n")
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result(timeout=period)
        except TimeoutError:
            streaming_pull_future.cancel()

# loop to test producer and consumer functions with a 3 second delay
#while(True):
#    print("===================================")
#    payload = {"data" : "Payload data", "timestamp": time.time()}
#    print(f"Sending payload: {payload}.")
#    push_payload(payload, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
#    consume_payload(PUB_SUB_PROJECT, PUB_SUB_SUBSCRIPTION, process_payload, timeout)
#    time.sleep(3)


async def cryptocompare():
    # this is where you paste your api key
    api_key = "fd58154803f33429be6b8518c542e2c83b46552cfe68ac7d1be52a9388d296e7"
    url = "wss://streamer.cryptocompare.com/v2?api_key=" + api_key
    async with websockets.connect(url) as websocket:
        await websocket.send(json.dumps({
        "action": "SubAdd",
        "subs": ["0~Coinbase~BTC~USD"],
        }))
        while True:
            try:
                data = await websocket.recv()
            except websockets.ConnectionClosed:
                break
            try:
                data = json.loads(data)
                print("===================================")
                #payload = {"data" : data, "timestamp": time.time()}
                payload = data
                print("Sending payload: {payload}.")
                push_payload(payload, PUB_SUB_TOPIC, PUB_SUB_PROJECT)
                consume_payload(PUB_SUB_PROJECT, PUB_SUB_SUBSCRIPTION, process_payload, timeout)
                #time.sleep(3)
                #print(json.dumps(data, indent=4))
            except ValueError:
                print(data)

asyncio.get_event_loop().run_until_complete(cryptocompare())