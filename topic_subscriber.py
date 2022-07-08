import os
from azure.servicebus import ServiceBusClient, ServiceBusMessage

CONNECTION_STR = os.environ['CONNECTION_STR']
TOPIC_NAME = os.environ['TOPIC_NAME']
SUBSCRIPTION_NAME = os.environ['SUBSCRIPTION_NAME']

servicebus_client = ServiceBusClient.from_connection_string(conn_str=CONNECTION_STR, logging_enable=True)

while True:
    with servicebus_client:
        receiver = servicebus_client.get_subscription_receiver(topic_name=TOPIC_NAME, subscription_name=SUBSCRIPTION_NAME, max_wait_time=10)
        print('receiver {}'.format(receiver))
        with receiver:
            print('messages in receiver'.format(receiver))
            for msg in receiver:
                print("Received: " + str(msg))
                receiver.complete_message(msg)