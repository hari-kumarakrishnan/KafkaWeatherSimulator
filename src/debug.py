from kafka import KafkaConsumer
import report_pb2  
import time

consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
consumer.subscribe(['temperatures'])

while True:
    batch = consumer.poll(1000)
    for topic_partition, messages in batch.items():
        for message in messages:
            try:
                report = report_pb2.Report()  
                report.ParseFromString(message.value) 
                message_dict = {
                    'partition': message.partition,
                    'key': message.key.decode(),
                    'date': report.date,
                    'degrees': report.degrees
                }
                print(message_dict)
            except Exception as e:
                pass
    time.sleep(1)
