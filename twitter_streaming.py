import requests
import os
import json
import asyncio
from azure.eventhub import EventHubProducerClient, EventData, TransportType

bearer_token = os.environ['BEARER_TOKEN']
CONNECTION_STRING = os.environ['EVENT_HUB_CONN_STR']
EVENTHUB_NAME = os.environ['EVENT_HUB_NAME']

async def send_event_data_batch(json_array):
    producer = create_client()
    with producer:
        event_data_batch = producer.create_batch()
        for js in json_array:
            event_data_batch.add(EventData(js))
        producer.send_batch(event_data_batch)

def create_client():
    # Create producer client from connection string.
    producer_client = EventHubProducerClient.from_connection_string(
        conn_str=CONNECTION_STRING,
        eventhub_name=EVENTHUB_NAME,  # EventHub name should be specified if it doesn't show up in connection string.
        logging_enable=False,  # To enable network tracing log, set logging_enable to True.
        retry_total=3,  # Retry up to 3 times to re-do failed operations.
        transport_type=TransportType.Amqp  # Use Amqp as the underlying transport protocol.
    )
    print("Calling producer client get eventhub properties:", producer_client.get_eventhub_properties())
    return producer_client


async def send_to_event_hub(json_array):
    print('send_to_event_hub with {}'.format(json_array))
    await send_event_data_batch(json_array)


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = "Bearer {}".format(bearer_token)
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r


def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()


def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))


def set_rules(delete):
    sample_rules = [
        {"value": "dog has:images", "tag": "dog pictures"},
        {"value": "cat has:images -grumpy", "tag": "cat pictures"}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))


async def get_stream(set):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print('json_response {}'.format(json_response))
            id = json_response['data']['id']
            tag = json_response['matching_rules']
            tags = json_response['matching_rules']
            events = []
            for tag in tags:
                tag_value = tag['tag']
                print('id: {}'.format(id))
                print('tag = {}'.format(tag_value))
                event_content = {
                    "id": id,
                    "tag": tag_value
                }
                print('eventContent {}'.format(event_content))
                json_str = json.dumps(event_content, indent=4, sort_keys=True)
                print(json_str)
                events.append(json_str)
                await send_to_event_hub(events)


async def main():
    while True:
        rules = get_rules()
        delete = delete_all_rules(rules)
        set = set_rules(delete)
        await get_stream(set)


loop = asyncio.get_event_loop()
loop.run_until_complete(main())

