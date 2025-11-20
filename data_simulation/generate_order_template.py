import json
import random
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

fake = Faker()

# Azure Event Hub credentials
# Azure Event Hub credentials
# BOOTSTRAP_SERVERS = 'data-streaming-sit.servicebus.windows.net:9093'
# EVENT_HUB_CONNECTION_STRING = 'Endpoint=sb://data-streaming-sit.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=/Cx8H9p9w5cLqNSbpLk2niAtKgHELet3s+AEhGhcDZI='
# EVENT_HUB_NAME = 'ecom_data'

producer = KafkaProducer(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=EVENT_HUB_CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

categories = ['Electronics', 'Books', 'Clothing', 'Home Decor', 'Toys']
locations = [
    {"city": "New Delhi", "state": "DL", "lat": 28.7041, "lon": 77.1025},
    {"city": "Mumbai Angeles", "state": "MU", "lat": 18.9582, "lon": 72.8321},
    {"city": "Pune", "state": "PN", "lat": 18.5246, "lon": 73.8786},
    {"city": "Bengaluru", "state": "BR", "lat": 12.9629, "lon": 77.5775},
    {"city": "Kolkata", "state": "KL", "lat": 22.5744, "lon": 88.3629}
]

def generate_order():
    location = random.choice(locations)
    category = random.choice(categories)
    price = round(random.uniform(10, 2000), 2)
    quantity = random.randint(1, 5)
    
    return {
        "order_id": fake.uuid4(),
        "timestamp": datetime.utcnow().isoformat(),
        "customer_id": fake.uuid4(),
        "product_id": fake.uuid4(),
        "category": category,
        "price": price,
        "quantity": quantity,
        "total_amount": round(price * quantity, 2),
        "city": location["city"],
        "state": location["state"],
        "country": "IND",
        "latitude": location["lat"],
        "longitude": location["lon"],
        "delivery_status": random.choice(["Processing", "Shipped", "Delivered", "Cancelled"])
    }

if __name__ == "__main__":
    print("Streaming fake U.S. e-commerce orders to Azure Event Hub...")
    while True:
        event = generate_order()
        producer.send(EVENT_HUB_NAME, value=event)
        print("Sent:", event)
        time.sleep(2)
