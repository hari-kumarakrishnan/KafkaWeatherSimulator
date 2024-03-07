import sys
import json
import os
from kafka import KafkaConsumer, TopicPartition
from datetime import datetime
from threading import Thread
from report_pb2 import Report 

broker = "localhost:9092"
def write(data, partition):
    path = f"/files/partition-{partition}.json"
    path2 = f"{path}.tmp"
    with open(path2, "w") as file:
        json.dump(data, file, indent=4)
    os.rename(path2, path)
def stats(data, message):
    report = Report()
    report.ParseFromString(message.value)
    date_str = report.date
    temperature = report.degrees
    if temperature >= 100000:
        return
    date = datetime.strptime(date_str, '%Y-%m-%d')
    month, year = date.strftime('%B'), date.year
    if month not in data:
        data[month] = {}
    if year not in data[month]:
        data[month][year] = {"count": 0, "sum": 0, "avg": 0, "start": date_str, "end": date_str}
    stats = data[month][year]
    stats['count'] += 1
    stats['sum'] += float(temperature)
    stats['avg'] = stats['sum'] / stats['count']
    stats['end'] = date_str  
def consumer(partitions):
    consumer = KafkaConsumer(bootstrap_servers=broker)
    assigned_partitions = [TopicPartition("temperatures", p) for p in partitions]
    consumer.assign(assigned_partitions)
    partition_data = {}
    for p in partitions:
        path = f"/files/partition-{p}.json"
        if not os.path.exists(path):
            partition_data[p] = {"partition": p, "offset": 0}
        else:
            with open(path, 'r') as file:
                partition_data[p] = json.load(file)
        consumer.seek(TopicPartition("temperatures", p), partition_data[p]["offset"])
    while True:
        batch = consumer.poll(timeout_ms=1000)
        for tp, messages in batch.items():
            for message in messages:
                stats(partition_data[tp.partition], message)
                partition_data[tp.partition]["offset"] = message.offset + 1
            write(partition_data[tp.partition], tp.partition)
if __name__ == "__main__":
    assigned_partitions = [int(p) for p in sys.argv[1:]]
    Thread(target=consumer, args=(assigned_partitions,)).start()
