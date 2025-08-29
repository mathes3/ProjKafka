import json
from kafka import KafkaConsumer
from datetime import datetime

def main():
    topic = input("Topic (default: user-events): ").strip() or "user-events"
    group = input("Group ID (default: simple-consumer): ").strip() or "simple-consumer"
    
    print(f"🚀 Consuming from '{topic}' with group '{group}'...")
    print("Press Ctrl+C to stop\n")
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['localhost:9092'],
        group_id=group,
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest'
    )
    
    count = 0
    try:
        for message in consumer:
            count += 1
            event = message.value
            timestamp = datetime.now().strftime("%H:%M:%S")
            
            print(f"[{timestamp}] #{count} - {event.get('event_type', '?')} from {event.get('user_id', '?')}")
            
            # Show product info if available
            if 'product_name' in event:
                print(f"  └─ Product: {event['product_name']} (${event.get('price', 0)})")
                
    except KeyboardInterrupt:
        print(f"\n✅ Consumed {count} messages. Goodbye!")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()