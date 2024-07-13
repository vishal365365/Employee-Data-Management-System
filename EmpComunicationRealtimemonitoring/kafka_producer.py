from kafka import KafkaProducer
import json
import time


# Define Kafka broker
bootstrap_servers = "ec2-34-237-234-93.compute-1.amazonaws.com:9092"

# Create KafkaProducer specifying the bootstrap servers and a value serializer function takes an input 'v', 
# serializes it into a JSON-formatted string using json.dumps(), and 
# then encodes it into UTF-8 format using .encode('utf-8')..
producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Define topic
topic = 'emp_com'


# Function to read a JSON file and send its content to Kafka
def send_file_to_kafka(file_path):
    try:
        with open(file_path, 'r') as file:
            # convert the file content to python object which is list 
            # dictionary as list items
            file_content = json.load(file)
            # iterating the list which and sending to producer
            for i in file_content:
                print(i)
                time.sleep(2)
            # Produce the file content as a message to the Kafka topic
                producer.send(topic, value=i)
            print(f"File '{file_path}' sent to Kafka topic '{topic}'")

    except Exception as e:
        print(f"Failed to send file '{file_path}' to Kafka: {e}")


    


# Path to JSON file
file_path = "/home/ubuntu/kafka_producer_data/messages.json"

# Send the file to Kafka
send_file_to_kafka(file_path)
# Close producer
producer.close()
