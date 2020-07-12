from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
producer.send('sample', b'testing')
producer.send('sample', key=b'message-two', value=b'this is first kafka python testing')

