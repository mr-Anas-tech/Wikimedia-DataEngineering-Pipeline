import os
import threading
import time
import json
import requests
from flask import Flask
from azure.eventhub import EventHubProducerClient, EventData

app = Flask(__name__)

EVENT_HUB_CONNECTION_STR = os.environ.get("EVENT_HUB_CONNECTION_STR")
EVENT_HUB_NAME = os.environ.get("EVENT_HUB_NAME")

def producer_worker():
    print("Wikipedia Producer worker started...", flush=True)

    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=EVENT_HUB_CONNECTION_STR,
            eventhub_name=EVENT_HUB_NAME
        )
    except Exception as e:
        print(f"Initialization Error: {e}", flush=True)
        return

    url = "https://stream.wikimedia.org/v2/stream/recentchange"
    
    # Wikimedia 403 avoid karne ke liye custom User-Agent Header zarori hai
    headers = {
        'User-Agent': 'MyAnalyticsPipeline/1.0 (contact@example.com)'
    }

    while True:
        try:
            with requests.get(url, headers=headers, stream=True, timeout=20) as response:
                if response.status_code == 200:
                    batch = producer.create_batch()
                    count = 0

                    for line in response.iter_lines():
                        if line:
                            decoded_line = line.decode('utf-8')

                            if decoded_line.startswith("data:"):
                                data_str = decoded_line[5:].strip()
                                try:
                                    event_json = json.loads(data_str)

                                    wiki_payload = {
                                        "id": event_json.get("id"),
                                        "title": event_json.get("title", ""),
                                        "user": event_json.get("user", ""),
                                        "bot": event_json.get("bot", False),
                                        "type": event_json.get("type", ""),
                                        "wiki": event_json.get("wiki", ""),
                                        "timestamp": event_json.get("timestamp")
                                    }

                                    batch.add(EventData(json.dumps(wiki_payload)))
                                    count += 1

                                    if count >= 20:
                                        producer.send_batch(batch)
                                        print("Sent 20 live Wikipedia events to Event Hubs.", flush=True)
                                        batch = producer.create_batch()
                                        count = 0

                                except Exception:
                                    continue
                else:
                    print(f"API Returned status code: {response.status_code}", flush=True)
                    time.sleep(10)

        except Exception as e:
            print(f"Error in stream loop: {e}", flush=True)
            time.sleep(5)

t = threading.Thread(target=producer_worker)
t.daemon = True
t.start()

@app.route('/')
def health_check():
    return "Wikipedia Event Producer is Running 24/7!"

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000)