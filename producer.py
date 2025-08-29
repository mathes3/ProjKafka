import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Fakerp
import uuid

class ECommerceProducer:
    def __init__(self, topic_name):
        self.topic = topic_name
        self.fake = Faker()
        
        # Initialize Kafka producer with basic configuration
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',
            retries=3,
            batch_size=16384,
            linger_ms=10,
            buffer_memory=33554432
        )
        
        # Sample product data
        self.products = [
            {"id": "PROD001", "name": "Laptop", "category": "Electronics", "price": 999.99},
            {"id": "PROD002", "name": "Smartphone", "category": "Electronics", "price": 699.99},
            {"id": "PROD003", "name": "Headphones", "category": "Electronics", "price": 199.99},
            {"id": "PROD004", "name": "T-Shirt", "category": "Clothing", "price": 29.99},
            {"id": "PROD005", "name": "Jeans", "category": "Clothing", "price": 79.99},
            {"id": "PROD006", "name": "Sneakers", "category": "Footwear", "price": 129.99},
            {"id": "PROD007", "name": "Watch", "category": "Accessories", "price": 249.99},
            {"id": "PROD008", "name": "Book", "category": "Books", "price": 19.99}
        ]
        
        self.event_types = ["page_view", "add_to_cart", "purchase", "remove_from_cart", "search"]
        
    def generate_user_event(self):
        """Generate a random e-commerce user event"""
        event_type = random.choice(self.event_types)
        user_id = f"USER_{random.randint(1001, 9999)}"
        session_id = str(uuid.uuid4())
        product = random.choice(self.products)
        
        base_event = {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "user_id": user_id,
            "session_id": session_id,
            "timestamp": datetime.now().isoformat(),
            "ip_address": self.fake.ipv4(),
            "user_agent": self.fake.user_agent()
        }
        
        # Add event-specific data
        if event_type == "page_view":
            base_event.update({
                "page": random.choice(["home", "category", "product", "cart", "checkout"]),
                "referrer": random.choice([None, "google.com", "facebook.com", "direct"])
            })
        elif event_type == "add_to_cart":
            base_event.update({
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": random.randint(1, 5)
            })
        elif event_type == "purchase":
            quantity = random.randint(1, 3)
            base_event.update({
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "quantity": quantity,
                "total_amount": product["price"] * quantity,
                "payment_method": random.choice(["credit_card", "debit_card", "paypal", "bank_transfer"])
            })
        elif event_type == "remove_from_cart":
            base_event.update({
                "product_id": product["id"],
                "product_name": product["name"],
                "quantity": random.randint(1, 2)
            })
        elif event_type == "search":
            base_event.update({
                "search_query": random.choice(["laptop", "phone", "shoes", "clothes", "books", "electronics"]),
                "results_count": random.randint(0, 50)
            })
            
        return base_event
    
    def send_event(self, event, key=None):
        """Send event to Kafka topic"""
        try:
            future = self.producer.send(self.topic, value=event, key=key)
            record_metadata = future.get(timeout=10)
            print(f"✓ Event sent to topic '{record_metadata.topic}' "
                  f"partition {record_metadata.partition} "
                  f"offset {record_metadata.offset}")
            return True
        except Exception as e:
            print(f"✗ Failed to send event: {str(e)}")
            return False
    
    def produce_events(self, num_events=10, delay=1):
        """Produce multiple events with delay"""
        print(f"\nProducing {num_events} events to topic '{self.topic}'...")
        print("-" * 60)
        
        successful_sends = 0
        for i in range(num_events):
            event = self.generate_user_event()
            key = event["user_id"]  # Use user_id as partition key
            
            print(f"\nEvent {i+1}/{num_events}:")
            print(f"Type: {event['event_type']}")
            print(f"User: {event['user_id']}")
            if 'product_name' in event:
                print(f"Product: {event['product_name']}")
            
            if self.send_event(event, key):
                successful_sends += 1
            
            if i < num_events - 1:  # Don't sleep after the last event
                time.sleep(delay)
        
        print(f"\n{'='*60}")
        print(f"Summary: {successful_sends}/{num_events} events sent successfully")
        return successful_sends
    
    def close(self):
        """Close the producer"""
        self.producer.flush()
        self.producer.close()
        print("Producer closed.")

def main():
    print("E-Commerce Event Producer (On-Premise Kafka)")
    print("=" * 50)
    
    # Get topic name
    topic = input("Enter topic name (default: user-events): ").strip()
    if not topic:
        topic = "user-events"
    
    try:
        # Create producer
        producer = ECommerceProducer(topic)
        
        while True:
            print(f"\nOptions:")
            print("1. Send single event")
            print("2. Send multiple events")
            print("3. Send continuous events (Ctrl+C to stop)")
            print("4. Exit")
            
            choice = input("\nEnter your choice (1-4): ").strip()
            
            if choice == "1":
                event = producer.generate_user_event()
                key = event["user_id"]
                print(f"\nGenerated Event:")
                print(json.dumps(event, indent=2))
                producer.send_event(event, key)
                
            elif choice == "2":
                try:
                    num_events = int(input("Number of events to send (default: 10): ") or "10")
                    delay = float(input("Delay between events in seconds (default: 1): ") or "1")
                    producer.produce_events(num_events, delay)
                except ValueError:
                    print("Invalid input. Please enter valid numbers.")
                    
            elif choice == "3":
                try:
                    delay = float(input("Delay between events in seconds (default: 2): ") or "2")
                    print(f"\nSending continuous events to '{topic}' (Press Ctrl+C to stop)...")
                    print("-" * 60)
                    
                    count = 0
                    while True:
                        count += 1
                        event = producer.generate_user_event()
                        key = event["user_id"]
                        
                        print(f"\nEvent {count}:")
                        print(f"Type: {event['event_type']}, User: {event['user_id']}")
                        if 'product_name' in event:
                            print(f"Product: {event['product_name']}")
                        
                        producer.send_event(event, key)
                        time.sleep(delay)
                        
                except KeyboardInterrupt:
                    print(f"\n\nStopped after sending {count} events.")
                    
            elif choice == "4":
                break
            else:
                print("Invalid choice. Please enter 1-4.")
                
    except KeyboardInterrupt:
        print("\nShutting down producer...")
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        try:
            producer.close()
        except:
            pass

if __name__ == "__main__":
    main()