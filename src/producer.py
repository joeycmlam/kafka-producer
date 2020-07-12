from kafka import KafkaProducer


if __name__ == '__main__':

    print ('start')
    producer = KafkaProducer(bootstrap_servers='localhost:9092')

    print('send message')

    #producer.send(topic='sample', value=b'testing-2')
    #producer.send('sample', b'send message')

    producer.send('sample', key=b'message-two', value=b'this is first kafka python testing')
    producer.flush()
    #myMessage = input('input message: ')
    #producer.send('sample', value=myMessage)
    print ('done')

