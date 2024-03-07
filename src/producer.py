from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic
import time
import weather
import report_pb2 
import datetime

broker = 'localhost:9092'
admin = KafkaAdminClient(bootstrap_servers=[broker])
try:
    admin.delete_topics(["temperatures", "__consumer_offsets"])
    print("Deleted")
    time.sleep(3) 
except Exception as e:
    pass
try:
    admin.create_topics([NewTopic(name="temperatures", num_partitions=4, replication_factor=1)])
    print("Created")
except Exception as e:
    pass
producer = KafkaProducer(bootstrap_servers=[broker])
for date, degrees in weather.get_next_weather(delay_sec=0.1):
    report = report_pb2.Report(date=date, degrees=degrees).SerializeToString()
    month_key = datetime.datetime.strptime(date, "%Y-%m-%d").strftime("%B")
    producer.send("temperatures", key=bytes(month_key, 'utf-8'), value=report)
producer.close()
