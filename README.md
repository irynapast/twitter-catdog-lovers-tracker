# twitter-catdog-lovers-tracker

### To start the project set your environment variables by running the following commands:

```
export BEARER_TOKEN='<twitter_bearer_token>'
export EVENT_HUB_CONN_STR='<event_hub_connection_string>'
export EVENT_HUB_NAME='<event_hub_name>'
export CONNECTION_STR='<service_bus_connection_string>'
export TOPIC_NAME='<topic_name>'
export SUBSCRIPTION_NAME='<topic_subscription_name>'
```
### Then run:
```
pip install -r requirements.txt
python3 twitter_streaming.py
python3 topic_subscriber.py
```
