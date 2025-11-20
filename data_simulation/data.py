import json
import random
import time
from datetime import datetime
from faker import Faker

fake = Faker()

categories = ['Electronics', 'Books', 'Clothing', 'Home Decor', 'Toys']
locations = [
    {"city": "New Delhi", "state": "DL", "lat": 28.7041, "lon": 77.1025},
    {"city": "Mumbai", "state": "MU", "lat": 18.9582, "lon": 72.8321},
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
    while True:
        event = generate_order()
        print("Sent:", event)
        time.sleep(2)