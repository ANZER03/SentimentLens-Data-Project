import threading
import os
from confluent_kafka import Producer

def delivery_report(err, msg):
    """Callback for delivery reports."""
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def stream_ratings(thread_id, lines):
    # Configure the producer
    producer_conf = {
        'bootstrap.servers': 'localhost:29092'
    }
    producer = Producer(producer_conf)

    for line in lines:
        producer.produce(
            topic='rating-topic',
            value=line.encode('utf-8'),
            callback=delivery_report
        )
        # Poll to trigger delivery callbacks
        producer.poll(0)

    # Flush outstanding messages
    producer.flush()
    print(f"Thread {thread_id} finished streaming.")

def main():
    file_path = os.path.join(os.path.dirname(__file__), '..', '..', 'Data', 'ml-1m', 'ratings.dat')
    with open(file_path, 'r') as f:
        lines = f.readlines()

    num_threads = 15
    chunk_size = len(lines) // num_threads
    threads = []

    for i in range(num_threads):
        start = i * chunk_size
        end = start + chunk_size
        if i == num_threads - 1:
            end = len(lines)
        
        thread = threading.Thread(target=stream_ratings, args=(i, lines[start:end]))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    print("All threads finished streaming.")

if __name__ == "__main__":
    main()
