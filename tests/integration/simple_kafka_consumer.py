from confluent_kafka import Consumer, KafkaException
import sys

def simple_consumer():
    consumer_conf = {
        'bootstrap.servers': 'localhost:29092',
        'group.id': 'simple-consumer-group-haha',
        'auto.offset.reset': 'earliest'
    }

    consumer = Consumer(consumer_conf)

    try:
        consumer.subscribe(['rating-topic'])

        while True:
            msg = consumer.poll(1.0)  # Poll for messages with a timeout of 1 second

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' % (
                        msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                # Proper message
                print(f"Received message: {msg.value().decode('utf-8')}")

    except KeyboardInterrupt:
        sys.stderr.write('%% Aborted by user\n')
    finally:
        # Close down consumer to commit final offsets. 
        consumer.close()

if __name__ == '__main__':
    simple_consumer()
